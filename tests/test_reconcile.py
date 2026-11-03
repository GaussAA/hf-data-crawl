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
    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages = pages
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
        response_headers = {"RateLimit": '"api";r=480;t=180'}
        if link is not None:
            response_headers["Link"] = f'<{link}>; rel="next"'
        import json

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
    reconciler(
        FakeCursorTransport([[record("a", "sha-a2"), record("c", "sha-c1")]]), store
    ).run(DAY2, limit=100)
    assert status_of(store, "acme/b") == "disappeared"

    summary = reconciler(
        FakeCursorTransport([[record("a", "sha-a2"), record("b", "sha-b1"), record("c", "sha-c1")]]),
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


def test_全量扫描使用createdAt稳定序(store: DatasetStore) -> None:
    transport = FakeCursorTransport([[record("a", "sha-a1")]])

    reconciler(transport, store).run(DAY2, limit=100)

    assert transport.calls[0]["sort"] == "createdAt"
    assert transport.calls[0]["direction"] == "-1"
