"""S4 · collect 子命令（日增量）的行为测试。

验的是外部行为：跑完落了几行、什么时候停、停了之后能不能续上。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from hf_data_crawl.api import HfDatasetApi, HfResponse
from hf_data_crawl.collect import Collector
from hf_data_crawl.store import DatasetStore

DAY1 = date(2026, 11, 1)
DAY2 = date(2026, 11, 2)


class FakeCursorTransport:
    """按游标回放的假 transport，行为对齐真接口的 cursor 分页。"""

    def __init__(self, pages: list[list[dict[str, Any]]], rate_remaining: int = 400) -> None:
        self.pages = pages
        self.rate_remaining = rate_remaining
        self.calls: list[dict[str, Any]] = []
        base = "https://huggingface.co/api/datasets"
        self._links = [f"{base}?cursor=c{index + 2}" for index in range(len(pages) - 1)] + [None]
        self._map = {
            parse_qs(urlparse(link).query)["cursor"][0]: index + 1
            for index, link in enumerate(self._links)
            if link is not None
        }

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls.append(dict(params))
        cursor = params.get("cursor")
        index = 0 if cursor is None else self._map[str(cursor)]
        link = self._links[index]
        response_headers = {"RateLimit": f'"api";r={self.rate_remaining};t=180'}
        if link is not None:
            response_headers["Link"] = f'<{link}>; rel="next"'
        import json

        return HfResponse(
            status=200,
            headers=response_headers,
            body=json.dumps(self.pages[index]).encode(),
        )


def page(prefix: str, count: int, downloads: int = 100) -> list[dict[str, Any]]:
    return [
        {
            "id": f"{prefix}/ds-{index}",
            "author": prefix,
            "lastModified": "2026-11-01T00:00:00.000Z",
            "createdAt": "2025-01-01T00:00:00.000Z",
            "sha": f"sha-{prefix}-{index}",
            "downloads": downloads,
            "downloadsAllTime": downloads * 2,
            "likes": 1,
            "gated": False,
            "private": False,
            "description": "d",
            "tags": ["language:en", "license:mit"],
        }
        for index in range(count)
    ]


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def collector(transport: FakeCursorTransport, store: DatasetStore, **kwargs: Any) -> Collector:
    return Collector(api=HfDatasetApi(transport=transport), store=store, **kwargs)


def test_空库首轮跑通并产出当前态(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 3), page("b", 2)])

    summary = collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    assert summary.stopped_reason == "end"
    assert summary.records == 5
    assert store.read_current().height == 5
    assert store.read_observations().height == 5


def test_次轮无变更在连续N页后提前停止(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 3), page("b", 2)])
    collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    again = FakeCursorTransport([page("a", 3), page("b", 2), page("c", 2)])
    summary = collector(again, store).run(DAY2, limit=100, pages_without_new=2)

    assert summary.stopped_reason == "watermark"
    assert summary.new_ids == 0
    assert len(again.calls) == 2
    assert store.read_current().height == 5


def test_有界运行保留游标且续跑不重复(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 2), page("b", 2), page("c", 2)])
    runner = collector(transport, store)

    first = runner.run(DAY1, limit=100, max_pages=1, pages_without_new=9)
    assert first.stopped_reason == "max_pages"
    assert first.records == 2

    second = runner.run(DAY1, limit=100, pages_without_new=9)

    assert second.records == 4  # 只抓剩余两页，不重抓已处理的第一页
    assert store.read_current().height == 6
    assert store.read_current()["id"].n_unique() == 6


def test_正常结束后清除游标(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 2)])
    checkpoint_path = store.state_dir / "collect-checkpoint.json"

    collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    assert checkpoint_path.exists()
    _, cursor = open(checkpoint_path, encoding="utf-8").read().split(":", 1)
    assert cursor.strip().rstrip("}") == "null"


def test_dry_run只探一页且不落盘(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 3), page("b", 3)])

    summary = collector(transport, store).run(DAY1, limit=100, dry_run=True)

    assert summary.stopped_reason == "dry_run"
    assert summary.preview is not None and len(summary.preview) == 3
    assert len(transport.calls) == 1
    assert not store.raw_dir.exists()
    assert not store.state_dir.exists()


def test_摘要配额余量与响应头一致(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 1)], rate_remaining=123)

    summary = collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    assert summary.rate_remaining == 123


def test_默认lean档不请求cardData(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 1)])

    collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    expand = transport.calls[0]["expand[]"]
    assert "cardData" not in expand
    assert "downloadsAllTime" in expand


class SortAwareTransport:
    """按 sort 参数分流回放的假 transport，用于验证顶部池刷新。"""

    def __init__(self, pages_by_sort: dict[str, list[list[dict[str, Any]]]]) -> None:
        self._pages = pages_by_sort
        self._index = {key: 0 for key in pages_by_sort}
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        import json

        self.calls.append(dict(params))
        sort = str(params["sort"])
        pages = self._pages[sort]
        index = self._index[sort]
        self._index[sort] = index + 1
        response_headers = {"RateLimit": '"api";r=450;t=180'}
        if index + 1 < len(pages):
            response_headers["Link"] = (
                f'<https://huggingface.co/api/datasets?sort={sort}&cursor={sort}-{index + 2}>; rel="next"'
            )
        return HfResponse(
            status=200,
            headers=response_headers,
            body=json.dumps(pages[index]).encode(),
        )


def test_顶部刷新按条数换算页数并单独落盘(store: DatasetStore) -> None:
    transport = SortAwareTransport(
        {
            "lastModified": [page("a", 2)],
            "downloads": [page("t", 1), page("u", 1), page("v", 1)],
        }
    )

    summary = collector(transport, store).run(DAY1, limit=2, refresh_top=5, pages_without_new=5)

    assert summary.top_pages == 3  # ceil(5 / 2)
    assert summary.refreshed == 3
    assert len(list(store.raw_dir.rglob("top-*.json.gz"))) == 3
    assert store.read_current().height == 5


def test_未指定顶部刷新时只扫一趟(store: DatasetStore) -> None:
    transport = SortAwareTransport({"lastModified": [page("a", 2)]})

    summary = collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    assert summary.top_pages == 0
    assert summary.refreshed == 0
    assert {call["sort"] for call in transport.calls} == {"lastModified"}


def test_顶部刷新使用下载量排序(store: DatasetStore) -> None:
    transport = SortAwareTransport({"lastModified": [page("a", 1)], "downloads": [page("t", 1)]})

    collector(transport, store).run(DAY1, limit=100, refresh_top=100, pages_without_new=5)

    downloads_calls = [call for call in transport.calls if call["sort"] == "downloads"]
    assert downloads_calls
    assert downloads_calls[0]["direction"] == "-1"
