"""按需补卡片的契约：只问点名的库、取不到不报错、写进表里的行与批量扫描同构。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from hf_data_crawl.card import ON_DEMAND_LIMIT, backfill
from hf_data_crawl.store import DatasetStore

SNAPSHOT = date(2026, 11, 9)


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def test_只写入真正有卡片的行(store: DatasetStore) -> None:
    records: dict[str, dict[str, object] | None] = {
        "acme/with-card": {
            "id": "acme/with-card",
            "sha": "s1",
            "cardData": {"pretty_name": "展示名", "license": "mit"},
        },
        "acme/empty-card": {"id": "acme/empty-card", "sha": "s2"},
        "acme/gone": None,
    }
    asked: list[str] = []

    def fetch(dataset_id: str) -> dict[str, object] | None:
        asked.append(dataset_id)
        return records[dataset_id]

    summary = backfill(
        store,
        ["acme/with-card", "acme/empty-card", "acme/gone"],
        SNAPSHOT,
        fetch=fetch,
    )

    assert asked == ["acme/with-card", "acme/empty-card", "acme/gone"]
    assert summary.requested == 3
    assert summary.fetched == 2  # 取不到的放过，不算失败
    assert summary.with_card == 1
    assert summary.rows == 1

    frame = store.read("carddata")
    assert frame.height == 1
    assert frame["id"][0] == "acme/with-card"
    assert frame["pretty_name"][0] == "展示名"
    assert frame["license_name"][0] == "mit"
    # 与批量扫描写入的行同构：日后判断卡片对应哪个仓库版本靠它。
    assert frame["source_sha"][0] == "s1"


def test_重复id只问一次(store: DatasetStore) -> None:
    asked: list[str] = []

    def fetch(dataset_id: str) -> dict[str, object] | None:
        asked.append(dataset_id)
        return {"id": dataset_id, "sha": "s", "cardData": {"license": "mit"}}

    backfill(store, ["a/x", "a/x", "a/x"], SNAPSHOT, fetch=fetch)

    assert asked == ["a/x"]


def test_上限挡住无界抓取(store: DatasetStore) -> None:
    """命中几千个缺卡片的库时，逐条问会把交付拖成几十分钟——宁可少补。"""
    ids = [f"a/{index}" for index in range(ON_DEMAND_LIMIT + 50)]
    asked: list[str] = []

    def fetch(dataset_id: str) -> dict[str, object] | None:
        asked.append(dataset_id)
        return {"id": dataset_id, "sha": "s", "cardData": {"license": "mit"}}

    summary = backfill(store, ids, SNAPSHOT, fetch=fetch)

    assert len(asked) == ON_DEMAND_LIMIT
    assert summary.requested == ON_DEMAND_LIMIT


def test_空列表不碰磁盘(store: DatasetStore) -> None:
    def fetch(_dataset_id: str) -> dict[str, object] | None:
        raise AssertionError("不该被调用")

    summary = backfill(store, [], SNAPSHOT, fetch=fetch)

    assert summary == summary.__class__(requested=0, fetched=0, with_card=0, rows=0)
