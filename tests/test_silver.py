"""S3 · Silver 派生与状态表（current / observations）的行为测试。

关键不变式：
- 指标未变不追加观测；指标一变就追加且旧行保留。
- 同日重跑，观测不重复。
- first_seen 一旦定下就不许改，last_seen 每轮更新。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.store import DatasetStore

DAY1 = date(2026, 11, 1)
DAY2 = date(2026, 11, 2)
DAY3 = date(2026, 11, 3)


def record(
    index: int,
    *,
    downloads: int = 100,
    likes: int = 5,
    tags: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-03-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 3,
        "likes": likes,
        "gated": False,
        "private": False,
        "description": "演示用",
        "tags": tags
        if tags is not None
        else [
            "task_categories:text-classification",
            "language:en",
            "language:zh",
            "size_categories:n<1K",
            "license:mit",
            "format:csv",
        ],
    }


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def seed(store: DatasetStore, snapshot: date, records: list[dict[str, Any]]) -> None:
    store.write_raw(snapshot_date=snapshot, page_index=1, records=records, meta={})
    store.build_bronze(snapshot)
    store.build_silver(snapshot)


def observations(store: DatasetStore) -> pl.DataFrame:
    return store.read_observations()


def test_silver把标签拆成可join的维度(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1)])

    row = store.read_silver(DAY1).row(0, named=True)

    assert row["task_categories"] == ["text-classification"]
    assert row["languages"] == ["en", "zh"]
    assert row["size_categories"] == ["n<1K"]
    assert row["licenses"] == ["mit"]
    assert row["libraries"] == []
    assert "format:csv" in row["tags"]


def test_silver保留原始json与指标字段(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, downloads=42)])

    row = store.read_silver(DAY1).row(0, named=True)

    assert row["downloads"] == 42
    assert row["downloads_all_time"] == 126
    assert row["raw"].startswith("{")
    assert '"id":"acme/ds-1"' in row["raw"].replace(" ", "")


def test_首次运行每个数据集产生一条观测(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1), record(2)])

    result = store.update_state(DAY1)

    assert result.appended == 2
    assert observations(store).height == 2


def test_指标未变时不追加观测(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, downloads=100)])
    store.update_state(DAY1)

    seed(store, DAY2, [record(1, downloads=100)])
    result = store.update_state(DAY2)

    assert result.appended == 0
    assert observations(store).height == 1


def test_指标变化时追加且保留旧行(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, downloads=100)])
    store.update_state(DAY1)
    seed(store, DAY2, [record(1, downloads=100)])
    store.update_state(DAY2)

    seed(store, DAY3, [record(1, downloads=180)])
    result = store.update_state(DAY3)

    rows = observations(store).sort("snapshot_date")
    assert result.appended == 1
    assert rows.height == 2
    assert rows["downloads"].to_list() == [100, 180]
    assert rows["snapshot_date"].to_list() == [DAY1, DAY3]


def test_同一天重跑观测不重复(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, downloads=100)])

    store.update_state(DAY1)
    again = store.update_state(DAY1)

    assert again.appended == 0
    assert observations(store).height == 1


def test_current每个数据集仅一行(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1), record(2)])
    store.update_state(DAY1)
    seed(store, DAY2, [record(1), record(2), record(3)])
    store.update_state(DAY2)

    current = store.read_current()

    assert current.height == 3
    assert current["id"].n_unique() == 3


def test_first_seen不随轮次改变且last_seen更新(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1)])
    store.update_state(DAY1)
    seed(store, DAY2, [record(1)])
    store.update_state(DAY2)

    row = store.read_current().row(0, named=True)

    assert row["first_seen_at"] == DAY1
    assert row["last_seen_at"] == DAY2


def test_本轮未出现的数据集在current中保持原状(store: DatasetStore) -> None:
    """下架判定归 reconcile 管，状态更新不得擅自删行或改状态。"""
    seed(store, DAY1, [record(1), record(2)])
    store.update_state(DAY1)
    seed(store, DAY2, [record(1)])
    store.update_state(DAY2)

    current = store.read_current()

    assert current.height == 2
    kept = current.filter(pl.col("id") == "acme/ds-2").row(0, named=True)
    assert kept["last_seen_at"] == DAY1
