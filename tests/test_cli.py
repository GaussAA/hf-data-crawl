"""命令行整条路径的行为测试：参数 → 注入的传输 → 落盘 → 运行记录 → 退出码。

`main(..., transport=...)` 是唯一的注入缝。这里不联网，只验四条联网命令各自
走完后：退出码对不对、`runs/` 里记的是不是成败一致、数据表里有没有行。
假传输各自在本文件里写小的，不依赖别的测试文件。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl import raw, state
from hf_data_crawl.api import HfResponse
from hf_data_crawl.cli import main
from hf_data_crawl.derive import derive_day
from hf_data_crawl.lock import exclusive
from hf_data_crawl.runs import read_runs
from hf_data_crawl.sizes import SIZE_SCHEMA, StagingWriter, staged_shards
from hf_data_crawl.store import DatasetStore

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

    code = run(root, "collect", "--date", DAY_ARG, transport=transport)

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


# ---------------------------------------------------------------- 规模补齐：锁外抓取


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


def test_查询结果含冻结行要出提示(capsys: pytest.CaptureFixture[str]) -> None:
    """改名后的旧 id 永远不再更新；不提示，它的旧值会被当成今天的读数。"""
    from hf_data_crawl.cli import _warn_frozen_rows

    _warn_frozen_rows(
        pl.DataFrame(
            {
                "id": ["a/1", "a/2", "a/3"],
                "downloads": [10, 20, 30],
                "status": ["active", "renamed", "disappeared"],
            }
        )
    )

    out = capsys.readouterr().out
    assert "2 行状态不是 active" in out


def test_查询结果全在架时不打扰(capsys: pytest.CaptureFixture[str]) -> None:
    from hf_data_crawl.cli import _warn_frozen_rows

    _warn_frozen_rows(pl.DataFrame({"id": ["a/1"], "status": ["active"]}))

    assert capsys.readouterr().out == ""


def test_结果里没有状态列就不作声(capsys: pytest.CaptureFixture[str]) -> None:
    """多数查询不投影 status；没有这一列就无从判断，别凭 id 猜。"""
    from hf_data_crawl.cli import _warn_frozen_rows

    _warn_frozen_rows(pl.DataFrame({"id": ["a/1"], "downloads": [10]}))

    assert capsys.readouterr().out == ""
