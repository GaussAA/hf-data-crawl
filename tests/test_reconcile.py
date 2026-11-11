"""S5 · 全量对账的行为测试。

最要紧的一条：没扫完就不许判下架。否则一次有界试跑会把整站判成下架。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import polars as pl
import pytest

from hf_data_crawl import raw, state
from hf_data_crawl.api import HfDatasetApi, HfResponse
from hf_data_crawl.derive import derive_day
from hf_data_crawl.reconcile import Reconciler
from hf_data_crawl.store import DatasetStore

DAY1 = date(2026, 11, 1)
DAY2 = date(2026, 11, 2)
DAY3 = date(2026, 11, 3)


class FakeCursorTransport:
    def __init__(
        self,
        pages: list[list[dict[str, Any]]],
        detail: dict[str, Any] | None = None,
    ) -> None:
        self.pages = pages
        self.calls: list[dict[str, Any]] = []
        #: 详情探测的预设回应：id → 状态码 或 {"id": 新id}。缺省一律 404（真删了）。
        self.detail = detail or {}
        #: 实际探过哪些 id，按调用顺序。
        self.probed: list[str] = []
        base = "https://huggingface.co/api/datasets"
        self._links = [f"{base}?cursor=c{index + 2}" for index in range(len(pages) - 1)] + [None]
        self._map = {
            parse_qs(urlparse(link).query)["cursor"][0]: index + 1
            for index, link in enumerate(self._links)
            if link is not None
        }

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        import json

        # 带 blobs 的是单库详情探测，与列表分页走不同分支。
        if "blobs" in params:
            dataset_id = url.split("/api/datasets/", 1)[-1]
            self.probed.append(dataset_id)
            value = self.detail.get(dataset_id, 404)
            if isinstance(value, int):
                return HfResponse(status=value, headers={}, body=b"{}")
            return HfResponse(
                status=200, headers={}, body=json.dumps(value, ensure_ascii=False).encode()
            )

        self.calls.append(dict(params))
        cursor = params.get("cursor")
        index = 0 if cursor is None else self._map[str(cursor)]
        link = self._links[index]
        response_headers = {"RateLimit": '"api";r=480;t=180'}
        if link is not None:
            response_headers["Link"] = f'<{link}>; rel="next"'

        return HfResponse(
            status=200,
            headers=response_headers,
            body=json.dumps(self.pages[index]).encode(),
        )


def record(name: str, sha: str, downloads: int = 100) -> dict[str, Any]:
    return {
        "id": f"acme/{name}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": sha,
        "downloads": downloads,
        "downloadsAllTime": downloads * 2,
        "likes": 1,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en"],
    }


def seed(store: DatasetStore, snapshot: date, records: list[dict[str, Any]]) -> None:
    raw.write(store, snapshot_date=snapshot, page_index=1, records=records, meta={})
    derive_day(store, snapshot)


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    seed(instance, DAY1, [record("a", "sha-a1"), record("b", "sha-b1")])
    return instance


def reconciler(transport: FakeCursorTransport, store: DatasetStore, **kwargs: Any) -> Reconciler:
    # `sleeper` 固定成 no-op：探测重试要验次数，不能让用例真的等退避。
    return Reconciler(
        api=HfDatasetApi(transport=transport, sleeper=lambda _: None), store=store, **kwargs
    )


def status_of(store: DatasetStore, dataset_id: str) -> str:
    frame = state.read_current(store).filter(pl.col("id") == dataset_id)
    return frame.row(0, named=True)["status"]


def test_下架集被标记但观测史保留(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a2"), record("c", "sha-c1")]])

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert summary.stopped_reason == "end"
    assert summary.disappeared == 1
    assert summary.new_ids == 1
    assert summary.sha_changed == 1
    assert status_of(store, "acme/b") == "disappeared"
    assert store.read_all("observations").filter(pl.col("id") == "acme/b").height == 1


def test_有界扫描不得判定下架(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a1")], [record("c", "sha-c1")]])

    summary = reconciler(transport, store).run(DAY2, limit=100, max_pages=1)

    assert summary.stopped_reason == "max_pages"
    assert summary.disappeared == 0
    assert status_of(store, "acme/b") == "active"


def test_重新出现恢复在架状态(store: DatasetStore) -> None:
    reconciler(FakeCursorTransport([[record("a", "sha-a2"), record("c", "sha-c1")]]), store).run(
        DAY2, limit=100
    )
    assert status_of(store, "acme/b") == "disappeared"

    summary = reconciler(
        FakeCursorTransport(
            [[record("a", "sha-a2"), record("b", "sha-b1"), record("c", "sha-c1")]]
        ),
        store,
    ).run(DAY3, limit=100)

    assert summary.reactivated == 1
    assert status_of(store, "acme/b") == "active"


def test_差异清单落盘且含三类(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a2"), record("c", "sha-c1")]])

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert summary.report_path is not None and summary.report_path.exists()
    kinds = pl.read_csv(summary.report_path)["kind"].to_list()
    assert "new" in kinds
    assert "disappeared" in kinds
    assert "sha_changed" in kinds
    assert "run" in kinds


def test_有界对账保留游标以便续跑(store: DatasetStore) -> None:
    first = FakeCursorTransport([[record("a", "sha-a1")], [record("c", "sha-c1")]])
    reconciler(first, store).run(DAY2, limit=100, max_pages=1)

    resumed = FakeCursorTransport([[record("a", "sha-a1")], [record("c", "sha-c1")]])
    reconciler(resumed, store).run(DAY2, limit=100)

    assert "cursor" in resumed.calls[0]


def test_续跑后不把前半段已扫过的判成下架(store: DatasetStore) -> None:
    """只存游标不存「见过谁」，续跑时前半段会被整片误判——实测误杀过 4.6 万条。"""
    first = FakeCursorTransport([[record("a", "sha-a1")], [record("c", "sha-c1")]])
    reconciler(first, store).run(DAY2, limit=100, max_pages=1)

    resumed = FakeCursorTransport([[record("a", "sha-a1")], [record("c", "sha-c1")]])
    summary = reconciler(resumed, store).run(DAY2, limit=100)

    assert summary.resumed is True
    assert summary.disappeared == 1
    assert status_of(store, "acme/a") == "active"
    assert status_of(store, "acme/c") == "active"
    assert status_of(store, "acme/b") == "disappeared"


def test_完成后清理扫描会话文件(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a1")]])

    reconciler(transport, store).run(DAY2, limit=100)

    assert not list(store.state_dir.glob("reconcile-*.csv"))
    assert not (store.state_dir / "reconcile-baseline.parquet").exists()


def test_全量扫描使用createdAt稳定序(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a1")]])

    reconciler(transport, store).run(DAY2, limit=100)

    assert transport.calls[0]["sort"] == "createdAt"
    assert transport.calls[0]["direction"] == "-1"


# ---------------------------------------------------------------- 下架原因复核


def test_转私有的不算删除(store: DatasetStore) -> None:
    """实测 463 条「下架」里约有两成是 401——库还在，只是不让看了。

    把它们记成 disappeared，会让下架数虚高，也让这类库在恢复公开时被当成「新增」。
    """
    transport = FakeCursorTransport([[record("a", "sha-a2")]], detail={"acme/b": 401})

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert summary.restricted == 1
    assert summary.disappeared == 0
    assert status_of(store, "acme/b") == "restricted"


def test_改名的标记为renamed(store: DatasetStore) -> None:
    transport = FakeCursorTransport(
        [[record("a", "sha-a2")]], detail={"acme/b": {"id": "acme/b-v2"}}
    )

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert summary.renamed == 1
    assert status_of(store, "acme/b") == "renamed"


def test_真删除才标disappeared(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a2")]], detail={"acme/b": 404})

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert summary.disappeared == 1
    assert status_of(store, "acme/b") == "disappeared"


def test_探测异常不标记留待下轮(store: DatasetStore) -> None:
    """网络抖动不等于数据集消失，把抖动写成下架是不可逆的脏数据。"""
    transport = FakeCursorTransport([[record("a", "sha-a2")]], detail={"acme/b": 0})

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert summary.disappeared == 0
    assert summary.restricted == 0
    assert status_of(store, "acme/b") == "active"


def test_差异清单区分三种消失方式(store: DatasetStore) -> None:
    seed(store, DAY1, [record("c", "sha-c1"), record("d", "sha-d1")])
    transport = FakeCursorTransport(
        [[record("a", "sha-a2")]],
        detail={"acme/b": 404, "acme/c": 401, "acme/d": {"id": "acme/d-v2"}},
    )

    summary = reconciler(transport, store).run(DAY2, limit=100)

    kinds = pl.read_csv(summary.report_path)["kind"].to_list()
    assert "disappeared" in kinds
    assert "restricted" in kinds
    assert "renamed" in kinds


def test_恢复判定覆盖私有与改名(store: DatasetStore) -> None:
    """上轮被标成 restricted / renamed 的库重新出现时，要能被识别为恢复。"""
    reconciler(FakeCursorTransport([[record("a", "sha-a1")]], detail={"acme/b": 401}), store).run(
        DAY2, limit=100
    )
    assert status_of(store, "acme/b") == "restricted"

    summary = reconciler(
        FakeCursorTransport([[record("a", "sha-a1"), record("b", "sha-b1")]]), store
    ).run(DAY3, limit=100)

    assert summary.reactivated == 1
    assert status_of(store, "acme/b") == "active"


# ---------------------------------------------------------------- 探测自愈


def test_探测证明还在的误标库恢复在架并计入reactivated(store: DatasetStore) -> None:
    """历史上被误标成 renamed 的存活库，下一次对账探测到 present 时自愈。"""
    state.mark_status(store, {"acme/b": "renamed"})
    transport = FakeCursorTransport([[record("a", "sha-a1")]], detail={"acme/b": {"id": "acme/b"}})

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert status_of(store, "acme/b") == "active"
    assert summary.reactivated == 1
    assert summary.renamed == 0
    report = pl.read_csv(summary.report_path)
    probed = report.filter((pl.col("kind") == "reactivated") & (pl.col("detail") == "probe"))
    assert probed["id"].to_list() == ["acme/b"]


def test_探测证明还在但本就在架的库不计入reactivated(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a1")]], detail={"acme/b": {"id": "acme/b"}})

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert status_of(store, "acme/b") == "active"
    assert summary.reactivated == 0


def test_差异清单的note列不再被schema丢弃(store: DatasetStore) -> None:
    """`note` 是给人看的中文说明，不能只留下机器编码 `detail`；旧列保持兼容。"""
    path = reconciler(FakeCursorTransport([[]]), store)._write_report(
        DAY2, "end", [], [], [], [], undetectable=["acme/x"]
    )

    report = pl.read_csv(path)

    assert report.columns == ["kind", "id", "detail", "note"]
    row = report.filter(pl.col("kind") == "undetectable_change").row(0, named=True)
    assert row["detail"] == "sha_null"
    assert row["note"] == "无法检测变更（sha为null）"


# ---------------------------------------------------------------- 续跑一致性


def _pages() -> list[list[dict[str, Any]]]:
    return [
        [record("a", "sha-a2"), record("c", "sha-c1")],
        [record("d", "sha-d1"), record("b", "sha-b1")],
    ]


def test_分段跑的差异与单次跑完全一致(store: DatasetStore, tmp_path: Path) -> None:
    """有界停止那一轮也会更新 current；若续跑再读 current 当基线，上半段的
    new / sha_changed 就丢了（实测：单次 new=2 sha_changed=1，分段 new=1 sha_changed=0）。"""
    other = DatasetStore(tmp_path / "other")
    seed(other, DAY1, [record("a", "sha-a1"), record("b", "sha-b1")])
    single = reconciler(FakeCursorTransport(_pages()), other).run(DAY2, limit=100)

    split_transport = FakeCursorTransport(_pages())
    first = reconciler(split_transport, store).run(DAY2, limit=100, max_pages=1)
    final = reconciler(split_transport, store).run(DAY2, limit=100)

    assert first.stopped_reason == "max_pages"
    assert final.resumed is True
    assert (final.new_ids, final.sha_changed, final.reactivated, final.disappeared) == (
        single.new_ids,
        single.sha_changed,
        single.reactivated,
        single.disappeared,
    )
    assert (single.new_ids, single.sha_changed) == (2, 1)
    single_rows = pl.read_csv(single.report_path).sort(["kind", "id"])
    split_rows = pl.read_csv(final.report_path).sort(["kind", "id"])
    assert split_rows.equals(single_rows)


def test_隔天续跑不把上半段送去探测(store: DatasetStore) -> None:
    """已见集合若按日期命名，隔天续跑时它是空的，上半段全被当成「没扫到」。"""
    pages = [[record("a", "sha-a1")], [record("c", "sha-c1")]]
    first = FakeCursorTransport(pages)
    reconciler(first, store).run(DAY2, limit=100, max_pages=1)

    second = FakeCursorTransport(pages)
    summary = reconciler(second, store).run(DAY3, limit=100)

    assert second.probed == ["acme/b"]
    assert summary.disappeared == 1
    assert status_of(store, "acme/a") == "active"


def test_完成后基线与已见集合一并清除(store: DatasetStore) -> None:
    reconciler(FakeCursorTransport([[record("a", "sha-a1")]]), store).run(DAY2, limit=100)

    assert not list(store.state_dir.glob("reconcile-baseline*"))
    assert not list(store.state_dir.glob("reconcile-seen*"))


def test_旧会话被丢弃并在摘要中标明(store: DatasetStore) -> None:
    store.state_dir.mkdir(parents=True, exist_ok=True)
    (store.state_dir / "reconcile-checkpoint.json").write_text('{"cursor": "c2"}', encoding="utf-8")

    summary = reconciler(FakeCursorTransport([[record("a", "sha-a1")]]), store).run(DAY2, limit=100)

    assert summary.discarded_stale_session is True
    assert summary.resumed is False


# ---------------------------------------------------------------- 探测限额与轮转


def seed_many(store: DatasetStore, count: int) -> list[str]:
    """在 current 里铺 count 个库，作为「本轮没扫到」的候选。"""
    names = [chr(ord("a") + index) for index in range(count)]
    seed(store, DAY1, [record(name, f"sha-{name}1") for name in names])
    return [f"acme/{name}" for name in names]


def test_探测按限额分批_其余顺延(store: DatasetStore) -> None:
    """全站扫描已花掉上千次请求，配额总共 1000/5 分钟；一次探完几千个必然大批限流。"""
    ids = seed_many(store, 5)
    transport = FakeCursorTransport([[]], detail={item: {"id": item} for item in ids})

    summary = reconciler(transport, store, probe_limit=2).run(DAY2, limit=100)

    assert summary.probe_probed == 2
    assert summary.probe_deferred == 3
    assert len(transport.probed) == 2


def test_探测轮转_下一轮换一批而不是重复前几个(store: DatasetStore) -> None:
    """按 id 排序取前 N 而不记游标，后 4248 个永远轮不到。"""
    ids = seed_many(store, 5)
    detail = {item: {"id": item} for item in ids}

    first = FakeCursorTransport([[]], detail=detail)
    reconciler(first, store, probe_limit=2).run(DAY2, limit=100)
    second = FakeCursorTransport([[]], detail=detail)
    reconciler(second, store, probe_limit=2).run(DAY2, limit=100)

    assert len(first.probed) == 2
    assert len(second.probed) == 2
    assert set(first.probed).isdisjoint(second.probed)


def test_探测失败要计数而不是被当成已判定(store: DatasetStore) -> None:
    """429 落成 unknown，状态按住不动——但这件事必须留下数字，否则永远没人知道。"""
    ids = seed_many(store, 3)
    transport = FakeCursorTransport([[]], detail={item: 429 for item in ids})

    summary = reconciler(transport, store, probe_limit=10).run(DAY2, limit=100)

    assert summary.probe_probed == 3
    assert summary.probe_failed == 3
    assert summary.disappeared == 0
    assert status_of(store, "acme/a") == "active"


def test_探测统计进差异清单(store: DatasetStore) -> None:
    ids = seed_many(store, 5)
    transport = FakeCursorTransport([[]], detail={item: {"id": item} for item in ids})

    summary = reconciler(transport, store, probe_limit=2).run(DAY2, limit=100)

    rows = pl.read_csv(summary.report_path)
    probe_rows = rows.filter(pl.col("kind") == "probe")
    assert probe_rows.height == 1
    assert "probed=2" in probe_rows["detail"][0]
    assert "deferred=3" in probe_rows["detail"][0]


def test_探测优先在架的_已判过的排后面(store: DatasetStore) -> None:
    """已判为非在架的库每轮都被重探，等于拿一半配额去确认旧结论。"""
    from hf_data_crawl.vocab import Status

    ids = seed_many(store, 5)
    state.mark_status(store, {"acme/a": Status.DISAPPEARED, "acme/b": Status.DISAPPEARED})
    transport = FakeCursorTransport([[]], detail={item: {"id": item} for item in ids})

    summary = reconciler(transport, store, probe_limit=2).run(DAY2, limit=100)

    assert summary.probe_probed == 2
    # 在架的是 c/d/e，先探它们；a/b 已判过，排到后面。
    assert set(transport.probed) == {"acme/c", "acme/d"}


def test_在架的不够限额时才复核已判过的(store: DatasetStore) -> None:
    """非在架的不能被饿死：命中概率低，但真被恢复时要能翻回来。"""
    from hf_data_crawl.vocab import Status

    ids = seed_many(store, 4)
    state.mark_status(store, {"acme/a": Status.DISAPPEARED, "acme/b": Status.DISAPPEARED})
    transport = FakeCursorTransport([[]], detail={item: {"id": item} for item in ids})

    reconciler(transport, store, probe_limit=4).run(DAY2, limit=100)

    assert set(transport.probed) == {"acme/a", "acme/b", "acme/c", "acme/d"}


def test_在架优先后排到前面_轮转仍不重复(store: DatasetStore) -> None:
    from hf_data_crawl.vocab import Status

    ids = seed_many(store, 6)
    state.mark_status(store, {"acme/a": Status.DISAPPEARED})
    detail = {item: {"id": item} for item in ids}

    first = FakeCursorTransport([[]], detail=detail)
    reconciler(first, store, probe_limit=3).run(DAY2, limit=100)
    second = FakeCursorTransport([[]], detail=detail)
    reconciler(second, store, probe_limit=3).run(DAY2, limit=100)

    assert set(first.probed).isdisjoint(second.probed)
    assert set(first.probed) | set(second.probed) == set(ids)


def test_排序键改版后旧游标作废(store: DatasetStore) -> None:
    """判据一改，旧游标指向的是另一个序列里的位置；续用只会从中间切开。"""
    from hf_data_crawl.reconcile import probe_cursor_path

    ids = seed_many(store, 6)
    path = probe_cursor_path(store)
    path.parent.mkdir(parents=True, exist_ok=True)
    # 不带 order 的就是改版前的档案；3001 % 6 = 1，若被沿用会从第二个开始。
    path.write_text('{"offset": 3001}', encoding="utf-8")
    transport = FakeCursorTransport([[]], detail={item: {"id": item} for item in ids})

    reconciler(transport, store, probe_limit=2).run(DAY2, limit=100)

    assert set(transport.probed) == {"acme/a", "acme/b"}
