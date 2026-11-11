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

from hf_data_crawl import bronze, raw, silver, state
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
    raw.write(store, snapshot_date=snapshot, page_index=1, records=records, meta={})
    bronze.build(store, snapshot)
    silver.build(store, snapshot)


def observations(store: DatasetStore) -> pl.DataFrame:
    return store.read_all("observations")


def test_silver把标签拆成可join的维度(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1)])

    row = store.read("silver", DAY1).row(0, named=True)

    assert row["task_categories"] == ["text-classification"]
    assert row["languages"] == ["en", "zh"]
    assert row["size_categories"] == ["n<1K"]
    assert row["licenses"] == ["mit"]
    assert row["libraries"] == []
    assert "format:csv" in row["tags"]


def test_silver不存指标也不存原始json(store: DatasetStore) -> None:
    """指标归 observations，整行 JSON 只在 Bronze；Silver 只记属性。"""
    seed(store, DAY1, [record(1, downloads=42)])

    frame = store.read("silver", DAY1)

    assert frame.height == 1
    for column in ("downloads", "downloads_all_time", "likes", "raw"):
        assert column not in frame.columns


def test_属性没变的数据集不再追加silver行(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1), record(2)])
    seed(store, DAY2, [record(1, downloads=999), record(2)])

    assert store.read("silver", DAY2).height == 0
    assert store.read_all("silver").height == 2


def test_属性变了只追加变化的那一行(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1), record(2)])
    seed(store, DAY2, [record(1), record(2, tags=["license:apache-2.0"])])

    day2 = store.read("silver", DAY2)

    assert day2["id"].to_list() == ["acme/ds-2"]
    assert day2.row(0, named=True)["licenses"] == ["apache-2.0"]
    assert store.read_all("silver").height == 3


def test_新增数据集追加首行属性(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1)])
    seed(store, DAY2, [record(1), record(2)])

    assert store.read("silver", DAY2)["id"].to_list() == ["acme/ds-2"]


def test_同日重跑silver不重复(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1)])
    seed(store, DAY2, [record(1, tags=["license:mit", "language:fr"])])
    first = store.read_all("silver").height

    silver.build(store, DAY2)

    assert store.read_all("silver").height == first


def test_silver_latest取每个id最近的属性(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, tags=["license:mit"])])
    seed(store, DAY2, [record(1, tags=["license:gpl"])])

    latest = silver.latest(store).row(0, named=True)

    assert latest["licenses"] == ["gpl"]
    assert silver.latest(store, before=DAY2).row(0, named=True)["licenses"] == ["mit"]


def test_首次运行每个数据集产生一条观测(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1), record(2)])

    result = state.update(store, DAY1)

    assert result.appended == 2
    assert observations(store).height == 2


def test_指标未变时不追加观测(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, downloads=100)])
    state.update(store, DAY1)

    seed(store, DAY2, [record(1, downloads=100)])
    result = state.update(store, DAY2)

    assert result.appended == 0
    assert observations(store).height == 1


def test_指标变化时追加且保留旧行(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, downloads=100)])
    state.update(store, DAY1)
    seed(store, DAY2, [record(1, downloads=100)])
    state.update(store, DAY2)

    seed(store, DAY3, [record(1, downloads=180)])
    result = state.update(store, DAY3)

    rows = observations(store).sort("snapshot_date")
    assert result.appended == 1
    assert rows.height == 2
    assert rows["downloads"].to_list() == [100, 180]
    assert rows["snapshot_date"].to_list() == [DAY1, DAY3]


def test_同一天重跑观测不重复(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, downloads=100)])

    state.update(store, DAY1)
    again = state.update(store, DAY1)

    assert again.appended == 0
    assert observations(store).height == 1


def test_current每个数据集仅一行(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1), record(2)])
    state.update(store, DAY1)
    seed(store, DAY2, [record(1), record(2), record(3)])
    state.update(store, DAY2)

    current = state.read_current(store)

    assert current.height == 3
    assert current["id"].n_unique() == 3


def test_first_seen不随轮次改变且last_seen更新(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1)])
    state.update(store, DAY1)
    seed(store, DAY2, [record(1)])
    state.update(store, DAY2)

    row = state.read_current(store).row(0, named=True)

    assert row["first_seen_at"] == DAY1
    assert row["last_seen_at"] == DAY2


def test_本轮未出现的数据集在current中保持原状(store: DatasetStore) -> None:
    """下架判定归 reconcile 管，状态更新不得擅自删行或改状态。"""
    seed(store, DAY1, [record(1), record(2)])
    state.update(store, DAY1)
    seed(store, DAY2, [record(1)])
    state.update(store, DAY2)

    current = state.read_current(store)

    assert current.height == 2
    kept = current.filter(pl.col("id") == "acme/ds-2").row(0, named=True)
    assert kept["last_seen_at"] == DAY1


def test_高频维度modality与format不再丢在原始tags里(store: DatasetStore) -> None:
    """这两个前缀合计占全站标签 21.3%，此前只留在 tags 原列无法结构化查询。"""
    seed(
        store,
        DAY1,
        [
            record(
                1,
                tags=[
                    "modality:text",
                    "modality:image",
                    "format:parquet",
                    "region:us",
                    "arxiv:2401.00001",
                    "doi:10.1000/xyz",
                    "task_ids:question-answering",
                    "multilinguality:multilingual",
                ],
            )
        ],
    )

    row = store.read("silver", DAY1).row(0, named=True)

    assert row["modalities"] == ["text", "image"]
    assert row["formats"] == ["parquet"]
    assert row["regions"] == ["us"]
    assert row["arxiv_ids"] == ["2401.00001"]
    assert row["dois"] == ["10.1000/xyz"]
    assert row["task_ids"] == ["question-answering"]
    assert row["multilinguality"] == ["multilingual"]


def test_无前缀标签单独成列(store: DatasetStore) -> None:
    """旧式模态标记（text/image/audio/tabular）没有前缀，占全部标签 13.7%。"""
    seed(store, DAY1, [record(1, tags=["text", "image", "language:en"])])

    row = store.read("silver", DAY1).row(0, named=True)

    assert row["plain_tags"] == ["text", "image"]
    assert row["languages"] == ["en"]


def test_未知前缀只留在原始tags不进任何维度(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, tags=["某个自定义前缀:值", "language:en"])])

    row = store.read("silver", DAY1).row(0, named=True)

    assert "某个自定义前缀:值" in row["tags"]
    assert row["languages"] == ["en"]
    assert row["plain_tags"] == []


def test_同一维度内重复标签只保留一次(store: DatasetStore) -> None:
    seed(store, DAY1, [record(1, tags=["modality:text", "modality:text", "modality:image"])])

    assert store.read("silver", DAY1).row(0, named=True)["modalities"] == ["text", "image"]
