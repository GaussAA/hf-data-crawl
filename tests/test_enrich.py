"""B · enrich 补齐 cardData 的行为测试。

卡片结构在 HF 上并不统一，抽取必须容忍缺字段；重复补齐以最新一次为准。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.enrich import Enricher, scan_changed_cards
from hf_data_crawl.hf.api import HfDatasetApi, HfResponse
from hf_data_crawl.hf.watermark import Watermark
from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.store import DatasetStore

DAY = date(2026, 11, 4)


class SeqTransport:
    """按顺序回放页面的假 transport（游标只用于终止）。"""

    def __init__(self, pages: list[list[dict[str, Any]]]) -> None:
        self.pages = pages
        self.calls: list[dict[str, Any]] = []
        self._index = 0

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls.append(dict(params))
        index = self._index
        self._index = index + 1
        headers_out = {"RateLimit": '"api";r=480;t=180'}
        if index + 1 < len(self.pages):
            headers_out["Link"] = '<https://huggingface.co/api/datasets?cursor=next>; rel="next"'
        page = self.pages[min(index, len(self.pages) - 1)]
        return HfResponse(status=200, headers=headers_out, body=json.dumps(page).encode())


def record(
    index: int,
    *,
    pretty_name: str | None = None,
    with_card: bool = True,
    license_value: Any = "mit",
    sha: str | None = None,
    last_modified: str = "2026-11-01T00:00:00.000Z",
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "sha": sha or f"sha-{index}",
        "lastModified": last_modified,
        "createdAt": "2025-01-01T00:00:00.000Z",
        "downloads": 100,
        "downloadsAllTime": 200,
        "likes": 1,
        "tags": ["language:en"],
    }
    if with_card:
        payload["cardData"] = {
            "pretty_name": pretty_name or f"数据集 {index}",
            "license": license_value,
            "paperswithcode_id": f"pwc-{index}",
            "arxiv": "2401.00001",
            "citation": "@misc{demo}",
            "description": "卡片描述",
        }
    return payload


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def test_补齐卡片字段并落表(store: DatasetStore) -> None:
    transport = SeqTransport([[record(1), record(2)]])

    summary = Enricher(api=HfDatasetApi(transport=transport), store=store).run(DAY, top=2)

    table = store.read("carddata")
    assert summary.with_card == 2
    assert table.height == 2
    row = table.filter(pl.col("id") == "acme/ds-1").row(0, named=True)
    assert row["pretty_name"] == "数据集 1"
    assert row["license_name"] == "mit"
    assert row["paperswithcode_id"] == "pwc-1"
    assert row["arxiv"] == "2401.00001"


def test_无卡片的记录被计数但不入表(store: DatasetStore) -> None:
    transport = SeqTransport([[record(1, with_card=False), record(2)]])

    summary = Enricher(api=HfDatasetApi(transport=transport), store=store).run(DAY, top=2)

    assert summary.with_card == 1
    assert store.read("carddata").height == 1


def test_许可证为列表时拼成字符串(store: DatasetStore) -> None:
    transport = SeqTransport([[record(1, license_value=["mit", "apache-2.0"])]])

    Enricher(api=HfDatasetApi(transport=transport), store=store).run(DAY, top=1)

    assert store.read("carddata").row(0, named=True)["license_name"] == "mit,apache-2.0"


def test_重复补齐以最新一次为准(store: DatasetStore) -> None:
    Enricher(
        api=HfDatasetApi(transport=SeqTransport([[record(1, pretty_name="旧名")]])),
        store=store,
    ).run(DAY, top=1)

    Enricher(
        api=HfDatasetApi(transport=SeqTransport([[record(1, pretty_name="新名")]])),
        store=store,
    ).run(DAY, top=1)

    table = store.read("carddata")
    assert table.height == 1
    assert table.row(0, named=True)["pretty_name"] == "新名"


def test_按下载量取头部并请求cardData(store: DatasetStore) -> None:
    transport = SeqTransport([[record(1)]])

    Enricher(api=HfDatasetApi(transport=transport), store=store).run(DAY, top=1000, limit=500)

    call = transport.calls[0]
    assert call["sort"] == "downloads"
    assert "cardData" in call["expand[]"]


def test_未补齐时datasets视图无卡片列值但可用(store: DatasetStore) -> None:
    from hf_data_crawl.gold import GoldCatalog

    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1, with_card=False)], meta={})
    derive_day(store, DAY)

    catalog = GoldCatalog(store)
    rows = catalog.run("SELECT id, pretty_name FROM datasets")

    assert rows.height == 1
    assert rows.row(0, named=True)["pretty_name"] is None


def test_补齐后datasets视图可直接查到卡片字段(store: DatasetStore) -> None:
    from hf_data_crawl.gold import GoldCatalog

    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1, with_card=False)], meta={})
    derive_day(store, DAY)
    Enricher(
        api=HfDatasetApi(transport=SeqTransport([[record(1, pretty_name="正式名")]])),
        store=store,
    ).run(DAY, top=1)

    catalog = GoldCatalog(store)
    row = catalog.run("SELECT id, pretty_name, license_name FROM datasets").row(0, named=True)

    assert row["pretty_name"] == "正式名"
    assert row["license_name"] == "mit"


def test_卡片行记下抓取时的仓库sha(store: DatasetStore) -> None:
    """没有这一列，「卡片落后于仓库」就无从判定——snapshot_date 只说何时抓的。"""
    transport = SeqTransport([[record(1, sha="abc123")]])

    Enricher(api=HfDatasetApi(transport=transport), store=store).run(DAY, top=1)

    assert store.read("carddata").row(0, named=True)["source_sha"] == "abc123"


def test_变化集卡片扫到水位线即停(store: DatasetStore) -> None:
    """与日增量共用同一条边界：越过旧数据就停，不重扫历史。"""
    transport = SeqTransport(
        [
            [record(1, last_modified="2026-11-05T00:00:00.000Z")],
            [record(2, last_modified="2026-10-01T00:00:00.000Z")],
            [record(3, last_modified="2026-09-01T00:00:00.000Z")],
        ]
    )

    summary = scan_changed_cards(
        HfDatasetApi(transport=transport),
        store,
        DAY,
        baseline=Watermark("2026-11-02T00:00:00Z"),
    )

    # 越界那一页已经落盘（ScanRun 先写后判），所以是 2 页而不是 1 页；
    # 多抓一页不亏——它可能正是一个新库，而少抓一页会漏掉边界上的更新。
    assert summary.stopped_reason == "watermark_timestamp"
    assert summary.pages == 2
    assert transport.calls[0]["sort"] == "lastModified"
    assert "cardData" in transport.calls[0]["expand[]"]


def test_变化集卡片写成card批次而非part(store: DatasetStore) -> None:
    """卡片记录不进 Bronze 去重池，否则 Bronze 行数会随补抓节奏膨胀。"""
    transport = SeqTransport([[record(1)]])

    scan_changed_cards(
        HfDatasetApi(transport=transport),
        store,
        DAY,
        baseline=Watermark("2026-11-01T00:00:00Z"),
    )

    names = [path.name for path in store.raw_dir.rglob("*.json.gz")]
    assert names and all(name.startswith("card-") for name in names)
