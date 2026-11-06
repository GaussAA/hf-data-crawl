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

from hf_data_crawl.api import HfDatasetApi, HfResponse
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
    store.write_raw(snapshot_date=snapshot, page_index=1, records=records, meta={})
    store.build_bronze(snapshot)
    store.build_silver(snapshot)
    store.update_state(snapshot)


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    seed(instance, DAY1, [record("a", "sha-a1"), record("b", "sha-b1")])
    return instance


def reconciler(transport: FakeCursorTransport, store: DatasetStore) -> Reconciler:
    return Reconciler(api=HfDatasetApi(transport=transport), store=store)


def status_of(store: DatasetStore, dataset_id: str) -> str:
    frame = store.read_current().filter(pl.col("id") == dataset_id)
    return frame.row(0, named=True)["status"]


def test_下架集被标记但观测史保留(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a2"), record("c", "sha-c1")]])

    summary = reconciler(transport, store).run(DAY2, limit=100)

    assert summary.stopped_reason == "end"
    assert summary.disappeared == 1
    assert summary.new_ids == 1
    assert summary.sha_changed == 1
    assert status_of(store, "acme/b") == "disappeared"
    assert store.read_observations().filter(pl.col("id") == "acme/b").height == 1


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
    store.mark_status({"acme/b": "renamed"})
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


# ---------------------------------------------------------------- 续跑一致性


class ProbeRecordingTransport(FakeCursorTransport):
    """记下哪些 id 被送去单库探测。"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.probed: list[str] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        if "blobs" in params:
            self.probed.append(url.split("/api/datasets/", 1)[-1])
        return super().get(url, params, headers)


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
    first = ProbeRecordingTransport(pages)
    reconciler(first, store).run(DAY2, limit=100, max_pages=1)

    second = ProbeRecordingTransport(pages)
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
