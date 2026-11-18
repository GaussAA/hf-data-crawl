"""采集类命令经 `main` 的端到端行为：成功留痕、失败留痕、抓取期间不持锁。"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.acquire.hf.api import HfResponse
from hf_data_crawl.acquire.sizes import SIZE_SCHEMA, StagingWriter, staged_shards
from hf_data_crawl.ops.cli import main
from hf_data_crawl.pipeline import raw, state
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.lock import exclusive
from hf_data_crawl.platform.runs import read_runs
from hf_data_crawl.platform.store import DatasetStore

DAY = date(2026, 11, 4)
DAY_ARG = DAY.isoformat()


def record(index: int, *, with_card: bool = False) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "sha": f"sha-{index}",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "downloads": 100 - index,
        "downloadsAllTime": 200,
        "likes": 1,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en"],
    }
    if with_card:
        payload["cardData"] = {"pretty_name": f"数据集 {index}", "license": "mit"}
    return payload


class PagedTransport:
    """按游标回放列表接口的假传输；末页不带 Link。"""

    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages = pages
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls.append(dict(params))
        cursor = params.get("cursor")
        index = 0 if cursor is None else int(str(cursor).removeprefix("c")) - 1
        response_headers = {"RateLimit": '"api";r=400;t=180'}
        if index + 1 < len(self.pages):
            link = f"https://huggingface.co/api/datasets?cursor=c{index + 2}"
            response_headers["Link"] = f'<{link}>; rel="next"'
        return HfResponse(
            status=200, headers=response_headers, body=json.dumps(self.pages[index]).encode()
        )


class SizeTransport:
    """规模接口的假传输：任何数据集都回一份有效规模。"""

    def __init__(self) -> None:
        self.calls: list[str] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls.append(params["dataset"])
        body = {
            "size": {
                "dataset": {"num_rows": 7, "num_bytes_original_files": 10},
                "configs": [{"config": "default", "num_columns": 2}],
            }
        }
        return HfResponse(status=200, headers={}, body=json.dumps(body).encode())


@pytest.fixture()
def root(tmp_path: Path) -> Path:
    return tmp_path / "data"


def run(root: Path, *argv: str, transport: Any) -> int:
    return main(["--data-dir", str(root), *argv], transport=transport)


def test_collect走完整条路径并留下成功记录(root: Path) -> None:
    transport = PagedTransport([[record(1), record(2)], [record(3)]])

    code = run(root, "collect", "--date", DAY_ARG, "--refresh-top", "0", transport=transport)

    store = DatasetStore(root)
    runs = read_runs(store)
    assert code == 0
    assert len(transport.calls) == 2
    assert [item["status"] for item in runs] == ["success"]
    assert state.read_current(store).height == 3


def test_reconcile对同一批数据不报差异且记成功(root: Path) -> None:
    store = DatasetStore(root)
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1), record(2)], meta={})
    derive_day(store, DAY)
    transport = PagedTransport([[record(1), record(2)]])

    code = run(root, "reconcile", "--date", DAY_ARG, transport=transport)

    runs = read_runs(store)
    assert code == 0
    assert [item["status"] for item in runs] == ["success"]
    assert state.read_current(store)["status"].to_list() == ["active", "active"]


def test_enrich补齐卡片并落表(root: Path) -> None:
    transport = PagedTransport([[record(1, with_card=True), record(2, with_card=True)]])

    code = run(root, "enrich", "--date", DAY_ARG, "--top", "2", transport=transport)

    store = DatasetStore(root)
    assert code == 0
    assert [item["status"] for item in read_runs(store)] == ["success"]
    assert store.read("carddata").height == 2


def test_fetch_sizes的尾批在flush_every不足时仍然落库(root: Path) -> None:
    """`flush` 只在攒满一批时触发；不足一批的尾巴全靠收尾那次合并。"""
    store = DatasetStore(root)
    raw.write(
        store, snapshot_date=DAY, page_index=1, records=[record(1), record(2), record(3)], meta={}
    )
    derive_day(store, DAY)
    transport = SizeTransport()

    code = run(
        root,
        "fetch-sizes",
        "--date",
        DAY_ARG,
        "--top",
        "3",
        "--workers",
        "2",
        "--flush-every",
        "1000",
        transport=transport,
    )

    assert code == 0
    assert sorted(transport.calls) == ["acme/ds-1", "acme/ds-2", "acme/ds-3"]
    assert store.read("size").height == 3
    assert [item["status"] for item in read_runs(store)] == ["success"]


@pytest.mark.parametrize("command", ["collect", "reconcile", "enrich", "fetch-sizes"])
def test_日期写错记成失败而不是抛到外面(root: Path, command: str) -> None:
    transport = PagedTransport([[record(1)]])

    code = run(root, command, "--date", "不是日期", transport=transport)

    runs = read_runs(DatasetStore(root))
    assert code == 1
    assert [item["status"] for item in runs] == ["failed"]
    assert "ValueError" in runs[0]["reason"]
    assert transport.calls == [], "日期解析失败时不该发出任何请求"


def _stage_sizes(root: Path, ids: list[str]) -> None:
    """模拟上一轮中断留下的暂存分片。"""
    store = DatasetStore(root)
    rows = [
        {name: None for name in SIZE_SCHEMA}
        | {"id": dataset_id, "snapshot_date": DAY, "status": "ok"}
        for dataset_id in ids
    ]
    StagingWriter(store, DAY).write(pl.DataFrame(rows, schema=SIZE_SCHEMA))


def test_fetch_sizes抓取期间不持锁(root: Path) -> None:
    """全量抓取跑三小时，整段持锁会让当天的 collect 拿不到锁而失败。"""
    store = DatasetStore(root)
    raw.write(
        store, snapshot_date=DAY, page_index=1, records=[record(1), record(2), record(3)], meta={}
    )
    derive_day(store, DAY)

    class ProbeTransport(SizeTransport):
        def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
            # 抓取期间若还持有数据目录锁，这里会抛 LockBusy。
            with exclusive(root, "probe"):
                pass
            return super().get(url, params, headers)

    code = run(
        root,
        "fetch-sizes",
        "--date",
        DAY_ARG,
        "--top",
        "3",
        "--workers",
        "1",
        transport=ProbeTransport(),
    )

    assert code == 0
    assert store.read("size").height == 3
    assert staged_shards(store) == [], "成功合并后不应留下分片"


def test_残留分片会被续抓并合入正式表(root: Path) -> None:
    """中断后已抓到的批次不该重烧请求；分片在下一轮收尾时并入正式表。"""
    store = DatasetStore(root)
    raw.write(
        store, snapshot_date=DAY, page_index=1, records=[record(1), record(2), record(3)], meta={}
    )
    derive_day(store, DAY)
    _stage_sizes(root, ["acme/ds-1", "acme/ds-2"])
    transport = SizeTransport()

    code = run(
        root,
        "fetch-sizes",
        "--date",
        DAY_ARG,
        "--top",
        "3",
        "--workers",
        "1",
        transport=transport,
    )

    assert code == 0
    assert transport.calls == ["acme/ds-3"], "残留分片里的 id 应跳过"
    assert sorted(store.read("size")["id"].to_list()) == ["acme/ds-1", "acme/ds-2", "acme/ds-3"]
    assert staged_shards(store) == []


def _put_sizes(root: Path, rows: list[dict[str, Any]]) -> None:
    """往正式表写几行已有规模；未给的列由 schema 补 None。"""
    frame = pl.DataFrame(
        [{name: None for name in SIZE_SCHEMA} | row for row in rows], schema=SIZE_SCHEMA
    )
    DatasetStore(root).write("size", frame)


def test_changed顺带补上新库的规模缺口(root: Path) -> None:
    """新库在 `size` 表里没有行，而 `stale_ids` 是 inner join，看不见它们。

    原先 `--changed` 把候选集整个换成陈旧集合，新库只能等周日全量——实测中位 2 天、
    超 7 天的有 1,322 个。缺口必须和刷新在同一趟里补。
    """
    store = DatasetStore(root)
    raw.write(
        store, snapshot_date=DAY, page_index=1, records=[record(1), record(2), record(3)], meta={}
    )
    derive_day(store, DAY)
    # record() 的 lastModified 是 11-01。ds-1 抓在它之前 → 陈旧；
    # ds-2 抓在它之后 → 新鲜；ds-3 没有行 → 缺口。
    _put_sizes(
        root,
        [
            {
                "id": "acme/ds-1",
                "snapshot_date": date(2026, 11, 1),
                "fetched_at": datetime(2026, 10, 30, tzinfo=UTC),
                "status": "ok",
            },
            {
                "id": "acme/ds-2",
                "snapshot_date": DAY,
                "fetched_at": datetime(2026, 11, 5, tzinfo=UTC),
                "status": "ok",
            },
        ],
    )
    transport = SizeTransport()

    code = run(
        root,
        "fetch-sizes",
        "--date",
        DAY_ARG,
        "--changed",
        "10",
        "--workers",
        "1",
        transport=transport,
    )

    assert code == 0
    assert sorted(transport.calls) == ["acme/ds-1", "acme/ds-3"], "陈旧与新库缺口都该被抓"


def test_changed在没有陈旧时也不跳过缺口(root: Path) -> None:
    """原先 `if not refresh: return` 会把缺口一并跳过——没有陈旧≠没有欠账。"""
    store = DatasetStore(root)
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1), record(2)], meta={})
    derive_day(store, DAY)
    transport = SizeTransport()

    code = run(
        root,
        "fetch-sizes",
        "--date",
        DAY_ARG,
        "--changed",
        "10",
        "--workers",
        "1",
        transport=transport,
    )

    assert code == 0
    assert sorted(transport.calls) == ["acme/ds-1", "acme/ds-2"]


def test_fetch_sizes只抓点名的ids(root: Path) -> None:
    """长尾没进头部的库，靠 `--ids` 单独补：名单即范围，`--top` 不参与截断。"""
    store = DatasetStore(root)
    raw.write(
        store, snapshot_date=DAY, page_index=1, records=[record(1), record(2), record(3)], meta={}
    )
    derive_day(store, DAY)
    transport = SizeTransport()

    code = run(
        root,
        "fetch-sizes",
        "--date",
        DAY_ARG,
        "--top",
        "1",  # 若按 top 截断就只会抓 ds-1，点名后这个数应当被忽略
        "--ids",
        "acme/ds-1,acme/ds-3",
        "--workers",
        "1",
        transport=transport,
    )

    assert code == 0
    assert sorted(transport.calls) == ["acme/ds-1", "acme/ds-3"]
    assert sorted(store.read("size")["id"].to_list()) == ["acme/ds-1", "acme/ds-3"]


def test_fetch_sizes点名一个都不存在时失败而不是静默跑空(root: Path) -> None:
    """「跑完了 0 行、状态 success」比报错更容易被当成正常结果。"""
    store = DatasetStore(root)
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1)], meta={})
    derive_day(store, DAY)
    transport = SizeTransport()

    code = run(
        root, "fetch-sizes", "--date", DAY_ARG, "--ids", "acme/not-here", transport=transport
    )

    assert code == 1
    assert transport.calls == []
    assert [item["status"] for item in read_runs(store)] == ["failed"]


def test_fetch_sizes的ids超过上限直接拒绝(root: Path) -> None:
    """`--ids` 是补几条的手工活；没上限时一条手滑就能把全站请求一遍。"""
    store = DatasetStore(root)
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1)], meta={})
    derive_day(store, DAY)
    transport = SizeTransport()
    too_many = ",".join(f"acme/ds-{index}" for index in range(1, 202))

    code = run(root, "fetch-sizes", "--date", DAY_ARG, "--ids", too_many, transport=transport)

    assert code == 1
    assert transport.calls == []


def test_合并拿不到锁时保留分片供下轮继续(root: Path) -> None:
    """合并是短锁；抓不到就如实失败，但抓到的数据不能丢。"""
    store = DatasetStore(root)
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1)], meta={})
    derive_day(store, DAY)

    with exclusive(root, "collect"):
        code = run(
            root,
            "fetch-sizes",
            "--date",
            DAY_ARG,
            "--top",
            "1",
            "--workers",
            "1",
            transport=SizeTransport(),
        )

    assert code == 1
    assert staged_shards(store), "拿不到锁时应把分片留着"
    assert store.read("size").height == 0
    runs = read_runs(store)
    assert [item["status"] for item in runs] == ["failed"]
    assert "LockBusy" in runs[0]["reason"]

    # 下一轮锁空闲时续抓：不再请求任何 id，直接合并残留分片。
    transport = SizeTransport()
    code = run(
        root,
        "fetch-sizes",
        "--date",
        DAY_ARG,
        "--top",
        "1",
        "--workers",
        "1",
        transport=transport,
    )

    assert code == 0
    assert transport.calls == []
    assert store.read("size").height == 1
    assert staged_shards(store) == []


def test_补跑过去日期后后缀重放把底库带回最新(root: Path) -> None:
    """`collect --date 过去` 只派生那一天，而 `current` 是「最新一行的物化」。

    不重放后缀的话，`current` 的标量列会整体退回那天，卡片表还会被旧批次整行
    覆盖——「补了一天数据」的代价变成「底库退回那一天」。
    """
    store = DatasetStore(root)
    first, later = date(2026, 11, 4), date(2026, 11, 5)
    raw.write(
        store,
        snapshot_date=first,
        page_index=1,
        records=[record(1)],
        meta={},
        written_at="2026-11-04T00:00:00.000000+00:00",
    )
    derive_day(store, first)
    newer = {**record(1), "downloads": 999}
    raw.write(
        store,
        snapshot_date=later,
        page_index=1,
        records=[newer],
        meta={},
        written_at="2026-11-05T00:00:00.000000+00:00",
    )
    derive_day(store, later)

    def downloads() -> int:
        frame = state.read_current(store)
        return int(frame.filter(pl.col("id") == "acme/ds-1")["downloads"].item())

    assert downloads() == 999

    code = run(
        root,
        "collect",
        "--date",
        first.isoformat(),
        "--refresh-top",
        "0",
        transport=PagedTransport([[record(1)]]),
    )

    assert code == 0
    assert downloads() == 999, "补跑过去日期后 current 不能回退到那一天"


def test_按下载量取头部的默认名次只有一个来源() -> None:
    """CLI 的 `--top` 默认值与切片的默认值必须是同一个常量。

    两处各写 20_000 时改一边不报错：CLI 传的是它自己那个数，切片默认值只在直接
    调用时生效，两者悄悄分叉而没有任何测试会红。三个命令共用这一个名次，
    所以它属于 acquire，不属于命令行。
    """
    import inspect

    from hf_data_crawl.acquire import DEFAULT_TOP
    from hf_data_crawl.acquire.enrich import Enricher
    from hf_data_crawl.acquire.sizes import SizeFetcher
    from hf_data_crawl.ops.parsers import build_parser

    parser = build_parser()
    for command in ("enrich", "fetch-sizes", "fetch-readme"):
        args = parser.parse_args(["--data-dir", "unused", command])
        assert args.top == DEFAULT_TOP, f"{command} 的 --top 默认值与 DEFAULT_TOP 不一致"

    for run in (Enricher.run, SizeFetcher.run):
        assert inspect.signature(run).parameters["top"].default == DEFAULT_TOP
