"""文件清单汇总（tree 接口）的契约测试。

除了汇总口径本身，重点钉住四件容易静默出错的事：

1. 失败分类——永久缺失要落库、临时失败不落库，混了会白烧预算或丢可救记录；
2. 截断语义——tree 按页给，没取完必须标 `truncated`，不许拿下限冒充全库真值；
3. 断点续跑与边跑边落库——长任务中断不能把成果全丢；
4. 宽视图接入——新表不能以「与原列撞名」为由悄悄少列，也不能把行数放大。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.acquire.files import (
    EMPTY_PAGE_GRACE,
    TREE_LIMIT,
    FileFetcher,
    HfFilesApi,
    next_cursor,
)
from hf_data_crawl.cli import WRITE_COMMANDS, main
from hf_data_crawl.gold import GoldCatalog, wide_column_names
from hf_data_crawl.hf.api import HfResponse, TransportError
from hf_data_crawl.pipeline import raw, state
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.runs import read_runs
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import FILES_SCHEMA, Origin, table

DAY = date(2026, 11, 7)


@dataclass(frozen=True)
class Page:
    """一页 tree 响应；带 `next_cursor` 时附上 `Link: rel="next"` 响应头。"""

    entries: list[dict[str, Any]]
    next_cursor: str | None = None

    @property
    def headers(self) -> dict[str, str]:
        if self.next_cursor is None:
            return {}
        url = (
            "https://huggingface.co/api/datasets/acme/x/tree/main"
            f"?recursive=true&expand=false&limit=1000&cursor={self.next_cursor}"
        )
        return {"Link": f'<{url}>; rel="next"'}


@dataclass(frozen=True)
class RateLimited:
    """带 `retry-after` 响应头的 429。"""

    retry_after: float


def tree(entries: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """默认一页：5 个文件 + 1 个目录。目录必须被排除在 file_count 之外。"""
    return (
        entries
        if entries is not None
        else [
            {"type": "file", "path": "README.md", "size": 120},
            {"type": "file", "path": "data/train.parquet", "size": 800},
            {"type": "file", "path": "data/test.parquet", "size": 200},
            {"type": "file", "path": "meta.csv", "size": 50},
            {"type": "file", "path": "LICENSE", "size": None},
            {"type": "directory", "path": "data", "size": 0},
        ]
    )


def file_entry(path: str, size: int | None = 10) -> dict[str, Any]:
    return {"type": "file", "path": path, "size": size}


class FakeFilesTransport:
    """按 dataset id 回放预设 tree 响应，并记录调用次数与请求参数。

    `tuple` 表示「按调用次序回放」的响应序列（元素可为 Page / 状态码 / 异常）；
    单个 `list` 或 `Page` 表示每次都返回同一页；`int` 是状态码，`dict` 是畸形响应。
    """

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[str] = []
        self.params: list[dict[str, Any]] = []
        self.urls: list[str] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        dataset_id = url.split("/api/datasets/", 1)[-1].removesuffix("/tree/main")
        self.calls.append(dataset_id)
        self.params.append(dict(params))
        self.urls.append(url)
        value = self.responses.get(dataset_id, 404)
        if isinstance(value, tuple):
            index = min(self.calls.count(dataset_id) - 1, len(value) - 1)
            value = value[index]
        if isinstance(value, Exception):
            raise value
        if isinstance(value, RateLimited):
            return HfResponse(
                status=429,
                headers={"retry-after": str(value.retry_after)},
                body=b'{"error":"rate limited"}',
            )
        if isinstance(value, int):
            return HfResponse(status=value, headers={}, body=b"{}")
        if isinstance(value, Page):
            return HfResponse(
                status=200,
                headers=value.headers,
                body=json.dumps(value.entries, ensure_ascii=False).encode("utf-8"),
            )
        return HfResponse(
            status=200, headers={}, body=json.dumps(value, ensure_ascii=False).encode("utf-8")
        )


def api(transport: FakeFilesTransport, *, max_pages: int = 1) -> HfFilesApi:
    return HfFilesApi(
        transport=transport, max_retries=2, max_pages=max_pages, sleeper=lambda _: None
    )


def fetcher(transport: FakeFilesTransport) -> FileFetcher:
    return FileFetcher(api=api(transport), workers=2)


def seed(store: DatasetStore, count: int = 3) -> None:
    records = [
        {
            "id": f"acme/ds-{index}",
            "author": "acme",
            "lastModified": "2026-11-01T00:00:00.000Z",
            "createdAt": "2025-01-01T00:00:00.000Z",
            "sha": f"sha-{index}",
            "downloads": index * 100,
            "downloadsAllTime": index * 1000,
            "likes": index,
            "gated": False,
            "private": False,
            "description": "d",
            "tags": ["license:mit"],
        }
        for index in range(1, count + 1)
    ]
    raw.write(store, snapshot_date=DAY, page_index=1, records=records, meta={})
    derive_day(store, DAY)


# ---------------------------------------------------------------- 汇总口径


def test_汇总出文件数体积格式与README() -> None:
    transport = FakeFilesTransport({"acme/x": tree()})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok"
    row = result.row
    assert row["file_count"] == 5, "directory 条目不能计入文件数"
    assert row["total_bytes"] == 1170
    assert row["has_readme"] is True
    assert row["truncated"] is False
    assert json.loads(row["formats"]) == {"(none)": 1, "csv": 1, "md": 1, "parquet": 2}
    assert set(row) == set(FILES_SCHEMA)
    assert "storage_bytes" not in FILES_SCHEMA, "tree 拿不到 usedStorage，不留空壳列"
    assert result.payload_bytes > 0


def test_请求走tree且参数固定() -> None:
    transport = FakeFilesTransport({"acme/x": tree()})

    api(transport).fetch("acme/x", DAY)

    assert transport.urls[0].endswith("/api/datasets/acme/x/tree/main")
    assert transport.params[0] == {
        "recursive": "true",
        "expand": "false",
        "limit": str(TREE_LIMIT),
    }


def test_体积全缺失时留空不拿0冒充() -> None:
    transport = FakeFilesTransport({"acme/x": tree([file_entry("a.parquet", None)])})

    row = api(transport).fetch("acme/x", DAY).row

    assert row["total_bytes"] is None
    assert row["has_readme"] is False
    assert json.loads(row["formats"]) == {"parquet": 1}


def test_空仓库算成功不是失败() -> None:
    transport = FakeFilesTransport({"acme/x": []})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok"
    assert result.row["file_count"] == 0
    assert result.row["total_bytes"] is None
    assert result.row["truncated"] is False


def test_缺type字段时按文件处理() -> None:
    transport = FakeFilesTransport({"acme/x": tree([{"path": "a.bin", "size": 7}])})

    assert api(transport).fetch("acme/x", DAY).row["file_count"] == 1


# ---------------------------------------------------------------- 截断语义


def test_有next但默认只取一页时标truncated() -> None:
    transport = FakeFilesTransport({"acme/x": Page(tree(), next_cursor="p2")})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok"
    assert result.row["truncated"] is True
    assert result.row["file_count"] == 5, "标了截断就说明这只是下限"
    assert len(transport.calls) == 1


def test_多页拼接且末页无next时不标truncated() -> None:
    transport = FakeFilesTransport(
        {
            "acme/x": (
                Page(tree([file_entry("a.bin")]), "p2"),
                Page(tree([file_entry("b.csv")]), "p3"),
                Page(tree([file_entry("c.md")])),
            )
        }
    )

    result = api(transport, max_pages=3).fetch("acme/x", DAY)

    assert result.status == "ok"
    assert result.row["truncated"] is False
    assert result.row["file_count"] == 3
    assert result.row["total_bytes"] == 30
    assert [p.get("cursor") for p in transport.params] == [None, "p2", "p3"]


def test_用尽页数上限仍标truncated() -> None:
    transport = FakeFilesTransport(
        {
            "acme/x": (
                Page(tree([file_entry("a.bin")]), "p2"),
                Page(tree([file_entry("b.csv")]), "p3"),
            )
        }
    )

    result = api(transport, max_pages=2).fetch("acme/x", DAY)

    assert result.row["truncated"] is True
    assert result.row["file_count"] == 2
    assert len(transport.calls) == 2, "不该越界请求第三页"


def test_满页却没给next也标truncated() -> None:
    """上游哪天悄悄去掉 Link 头，也不能把恰好 1000 条当成完整清单。"""
    page = [file_entry(f"f{i}.bin") for i in range(TREE_LIMIT)]
    transport = FakeFilesTransport({"acme/x": page})

    result = api(transport).fetch("acme/x", DAY)

    assert result.row["truncated"] is True
    assert result.row["file_count"] == TREE_LIMIT


def test_next游标能解析URL编码且只认next() -> None:
    link = '<https://huggingface.co/api/datasets/x/tree/main?limit=1000&cursor=ZXlK%3D>; rel="next"'
    assert next_cursor({"Link": link}) == "ZXlK="
    assert next_cursor({"link": '<https://x/tree?cursor=abc>; rel="prev"'}) is None
    assert next_cursor({}) is None
    assert next_cursor({"Link": ""}) is None


# ---------------------------------------------------------------- 失败分类


def test_404记为永久缺失并落库() -> None:
    transport = FakeFilesTransport({"acme/x": 404})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "unavailable"
    assert result.row["id"] == "acme/x"
    assert result.row["status"] == "unavailable"
    assert result.row["file_count"] is None


def test_临时失败不落库() -> None:
    transport = FakeFilesTransport({"acme/x": TransportError("断流")})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "error"
    assert result.row == {}
    assert len(transport.calls) == 3


def test_429退避后重试成功() -> None:
    transport = FakeFilesTransport({"acme/x": (429, tree())})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok"
    assert result.row["file_count"] == 5


def test_429按上游RetryAfter退避后重试成功() -> None:
    delays: list[float] = []
    transport = FakeFilesTransport({"acme/x": (RateLimited(7.0), tree())})
    client = HfFilesApi(transport=transport, max_retries=1, sleeper=delays.append)

    result = client.fetch("acme/x", DAY)

    assert result.status == "ok"
    assert result.row["file_count"] == 5
    assert delays == [7.0], "固定 2 秒退避会在窗口重置前重试，白烧唯一一次机会"


def test_429无RetryAfter时回退指数退避() -> None:
    delays: list[float] = []
    transport = FakeFilesTransport({"acme/x": (429, tree())})
    client = HfFilesApi(
        transport=transport, max_retries=1, retry_base_delay=3.0, sleeper=delays.append
    )

    assert client.fetch("acme/x", DAY).status == "ok"
    assert delays == [3.0]


def test_RetryAfter脏值不会挂死() -> None:
    transport = FakeFilesTransport(
        {"acme/x": (RateLimited(float("nan")), RateLimited(1e9), tree())}
    )
    delays: list[float] = []
    client = HfFilesApi(transport=transport, max_retries=2, sleeper=delays.append)

    # nan 落回指数退避；1e9 被 60 秒上限夹住，两者都不会让进程睡死。
    assert client.fetch("acme/x", DAY).status == "ok"
    assert all(delay <= 60.0 for delay in delays)


def test_200却不是数组算临时失败() -> None:
    transport = FakeFilesTransport({"acme/x": {"id": "acme/x"}})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "error"
    assert result.payload_bytes > 0


def test_第二页失败时整库记临时失败() -> None:
    transport = FakeFilesTransport({"acme/x": (Page(tree(), "p2"), TransportError("第二页断流"))})

    result = api(transport, max_pages=2).fetch("acme/x", DAY)

    assert result.status == "error"
    assert result.row == {}, "半截清单不能当成完整结果落库"


# ---------------------------------------------------------------- 调度与续跑


def test_已处理的id默认跳过且可重试不可用() -> None:
    transport = FakeFilesTransport({f"acme/ds-{i}": tree() for i in range(1, 4)})
    ids = [f"acme/ds-{i}" for i in range(1, 4)]
    existing = pl.DataFrame(
        {
            "id": ["acme/ds-1", "acme/ds-2"],
            "status": ["ok", "unavailable"],
        },
        schema={"id": pl.Utf8, "status": pl.Utf8},
    )

    summary, frame = fetcher(transport).run(DAY, dataset_ids=ids, existing=existing)

    assert summary.skipped == 2
    assert frame.height == 1
    assert transport.calls == ["acme/ds-3"]

    retry_summary, retry_frame = fetcher(transport).run(
        DAY, dataset_ids=ids, existing=existing, retry_unavailable=True
    )

    assert retry_summary.skipped == 1
    assert retry_frame.height == 2


def test_边跑边落库且终态进度必发() -> None:
    transport = FakeFilesTransport({f"acme/ds-{i}": tree() for i in range(1, 6)})
    ids = [f"acme/ds-{i}" for i in range(1, 6)]
    flushed: list[int] = []
    progress: list[tuple[int, int]] = []
    runner = FileFetcher(
        api=api(transport), workers=2, progress=lambda done, total: progress.append((done, total))
    )

    summary, frame = runner.run(
        DAY,
        dataset_ids=ids,
        flush=lambda batch: flushed.append(batch.height),
        flush_every=2,
    )

    assert frame.height == 5
    assert sum(flushed) >= 4, "中断时最多只该损失一个批次"
    assert progress[-1] == (5, 5), "终态进度必须发，不能停在 96%"
    assert summary.downloaded_bytes > 0


def test_零任务时不回调进度也不除零() -> None:
    transport = FakeFilesTransport({})
    progress: list[tuple[int, int]] = []
    runner = FileFetcher(
        api=api(transport), workers=2, progress=lambda done, total: progress.append((done, total))
    )

    summary, frame = runner.run(DAY, dataset_ids=[])

    assert frame.height == 0
    assert progress == []
    assert summary.requested == 0


def test_刷新截断只重抓截断行() -> None:
    """截断行若也按「已处理」跳过，就永远是下限，永远补不成完整清单。"""
    transport = FakeFilesTransport({f"acme/ds-{i}": tree() for i in (1, 2, 3)})
    ids = ["acme/ds-1", "acme/ds-2", "acme/ds-3"]
    existing = pl.DataFrame(
        {
            "id": ids,
            "status": ["ok", "ok", "ok"],
            "truncated": [True, False, True],
        },
        schema={"id": pl.Utf8, "status": pl.Utf8, "truncated": pl.Boolean},
    )

    summary, frame = fetcher(transport).run(
        DAY, dataset_ids=ids, existing=existing, refresh_truncated=True
    )

    assert sorted(transport.calls) == ["acme/ds-1", "acme/ds-3"]
    assert summary.refreshed == 2
    assert summary.skipped == 1, "完整清单不该被重抓"
    assert frame.height == 2


def test_默认不重抓截断行() -> None:
    """不显式刷新时保持断点续跑语义：跑过的就跳过，别偷偷重烧流量。"""
    transport = FakeFilesTransport({"acme/ds-1": tree()})
    existing = pl.DataFrame(
        {"id": ["acme/ds-1"], "status": ["ok"], "truncated": [True]},
        schema={"id": pl.Utf8, "status": pl.Utf8, "truncated": pl.Boolean},
    )

    summary, frame = fetcher(transport).run(DAY, dataset_ids=["acme/ds-1"], existing=existing)

    assert transport.calls == []
    assert summary.skipped == 1
    assert summary.refreshed == 0
    assert frame.height == 0


def test_旧表缺truncated列时当作未截断() -> None:
    """升级前的产物没有该列，不能因此抛错，也不能把全部历史行重抓一遍。"""
    transport = FakeFilesTransport({"acme/ds-1": tree()})
    existing = pl.DataFrame(
        {"id": ["acme/ds-1"], "status": ["ok"]}, schema={"id": pl.Utf8, "status": pl.Utf8}
    )

    summary, frame = fetcher(transport).run(
        DAY, dataset_ids=["acme/ds-1"], existing=existing, refresh_truncated=True
    )

    assert transport.calls == []
    assert summary.refreshed == 0
    assert frame.height == 0


def test_首页全是目录时继续翻页直到见到文件() -> None:
    """目录排在文件前，头几页没文件不代表空仓库——不能报成 0 个文件。"""
    transport = FakeFilesTransport(
        {
            "acme/x": (
                Page([{"type": "directory", "path": "a", "size": 0}], "p2"),
                Page([{"type": "directory", "path": "a/b", "size": 0}], "p3"),
                Page([file_entry("a/b/c.parquet")]),
            )
        }
    )

    result = api(transport).fetch("acme/x", DAY)

    assert result.row["file_count"] == 1, "有文件就不能报 0"
    assert result.row["truncated"] is False, "翻到了末页，清单已完整"
    assert len(transport.calls) == 3, "max_pages=1 时靠宽限翻到见到文件为止"


def test_全程只有目录时撞兜底上限即停() -> None:
    """病态仓库（一直给目录、一直有 next）不能无限翻，且四个指标留空不冒充 0。"""
    transport = FakeFilesTransport(
        {"acme/x": (Page([{"type": "directory", "path": "a", "size": 0}], "p2"),)}
    )

    result = api(transport).fetch("acme/x", DAY)

    assert result.row["file_count"] is None, "没见到文件 ≠ 空仓库，不能报 0"
    assert result.row["has_readme"] is None, "也不能断言没有 README"
    assert result.row["formats"] is None
    assert result.row["total_bytes"] is None
    assert result.row["truncated"] is True
    assert len(transport.calls) == 1 + EMPTY_PAGE_GRACE, "撞上限即停，不许无限翻"


def test_空仓库不因宽限多翻() -> None:
    """真·空仓库没有 next，第一次就该收工，别白烧宽限页。"""
    transport = FakeFilesTransport({"acme/x": []})

    result = api(transport).fetch("acme/x", DAY)

    assert result.row["truncated"] is False
    assert len(transport.calls) == 1


# ---------------------------------------------------------------- 登记与视图


def test_文件表登记为NETWORK且重建不清() -> None:
    assert table("files").origin is Origin.NETWORK
    assert table("files").path == "enrich/files.parquet"


def test_宽视图收进files的列且不放大行数(store_with_data: DatasetStore) -> None:
    store = store_with_data
    store.upsert(
        "files",
        pl.DataFrame(
            [
                {
                    "id": "acme/ds-1",
                    "url": "https://huggingface.co/datasets/acme/ds-1",
                    "snapshot_date": DAY,
                    "status": "ok",
                    "file_count": 3,
                    "total_bytes": 900,
                    "formats": '{"parquet": 3}',
                    "has_readme": True,
                    "truncated": False,
                }
            ],
            schema=FILES_SCHEMA,
        ),
    )

    catalog = GoldCatalog(store)
    try:
        columns = catalog.run("SELECT * FROM datasets LIMIT 0").columns
        rows = int(catalog.run("SELECT count(*) AS n FROM datasets")["n"].item())
    finally:
        catalog.close()

    assert set(columns) == set(wide_column_names())
    assert {"files_status", "file_formats", "file_count", "has_readme", "truncated"} <= set(columns)
    # 改名列必须真的在静态清单里：旧判重逻辑会拿原列名与 current.status 比较，
    # 让 files_status 连改名的机会都没有，而静态清单与视图会一起少列、双双不报错。
    assert {"files_status", "file_formats"} <= set(wide_column_names())
    assert "storage_bytes" not in columns
    assert rows == state.read_current(store).height


def test_fetch_files属于写命令() -> None:
    assert "fetch-files" in WRITE_COMMANDS


# ---------------------------------------------------------------- CLI 端到端


def test_命令行补采成功并留下运行摘要(tmp_path: Path) -> None:
    root = tmp_path / "data"
    store = DatasetStore(root)
    seed(store, 3)
    transport = FakeFilesTransport({f"acme/ds-{i}": tree() for i in range(1, 4)})

    code = main(["--data-dir", str(root), "fetch-files", "--top", "3"], transport=transport)

    assert code == 0
    assert store.read("files").height == 3
    assert store.read("files")["file_count"].to_list() == [5, 5, 5]
    assert store.read("files")["truncated"].to_list() == [False, False, False]
    assert read_runs(store)[-1]["status"] == "success"


def test_命令行可显式加大页数(tmp_path: Path) -> None:
    root = tmp_path / "data"
    store = DatasetStore(root)
    seed(store, 1)
    transport = FakeFilesTransport(
        {
            "acme/ds-1": (
                Page(tree([file_entry("a.bin")]), "p2"),
                Page(tree([file_entry("b.bin")])),
            )
        }
    )

    code = main(
        ["--data-dir", str(root), "fetch-files", "--top", "1", "--max-pages", "2"],
        transport=transport,
    )

    assert code == 0
    assert store.read("files")["file_count"].to_list() == [2]


def test_命令行刷新截断需配合多页(tmp_path: Path) -> None:
    """一页上限时重抓只是把「截断」重新确认一遍，白烧请求——故直接拒绝。"""
    root = tmp_path / "data"
    store = DatasetStore(root)
    seed(store, 1)
    transport = FakeFilesTransport({"acme/ds-1": tree()})

    code = main(
        ["--data-dir", str(root), "fetch-files", "--top", "1", "--refresh-truncated"],
        transport=transport,
    )

    assert code == 1
    assert transport.calls == [], "被拒时一个请求都不该发"
    assert read_runs(store)[-1]["status"] == "failed"
    assert "max-pages" in (read_runs(store)[-1]["reason"] or "")


def test_命令行刷新后截断消失(tmp_path: Path) -> None:
    """端到端：原来截断的行重抓两页后，truncated 应翻成 False 且文件数变全。"""
    root = tmp_path / "data"
    store = DatasetStore(root)
    seed(store, 1)
    store.upsert(
        "files",
        pl.DataFrame(
            [
                {
                    "id": "acme/ds-1",
                    "url": "https://huggingface.co/datasets/acme/ds-1",
                    "snapshot_date": DAY,
                    "status": "ok",
                    "file_count": 1,
                    "total_bytes": 10,
                    "formats": '{"bin": 1}',
                    "has_readme": False,
                    "truncated": True,
                }
            ],
            schema=FILES_SCHEMA,
        ),
    )
    transport = FakeFilesTransport(
        {
            "acme/ds-1": (
                Page(tree([file_entry("a.bin")]), "p2"),
                Page(tree([file_entry("b.bin")])),
            )
        }
    )

    code = main(
        [
            "--data-dir",
            str(root),
            "fetch-files",
            "--top",
            "1",
            "--max-pages",
            "2",
            "--refresh-truncated",
        ],
        transport=transport,
    )

    stored = store.read("files")
    assert code == 0
    assert stored["truncated"].to_list() == [False]
    assert stored["file_count"].to_list() == [2]
    assert stored.height == 1, "刷新是同 id 覆盖，不能把行数放大"


def test_无采集数据时命令失败且留痕(tmp_path: Path) -> None:
    root = tmp_path / "empty-data"
    root.mkdir()

    code = main(["--data-dir", str(root), "fetch-files"], transport=FakeFilesTransport({}))

    assert code == 1
    assert read_runs(DatasetStore(root))[-1]["status"] == "failed"


@pytest.fixture()
def store_with_data(tmp_path: Path) -> DatasetStore:
    store = DatasetStore(tmp_path / "data")
    seed(store)
    return store
