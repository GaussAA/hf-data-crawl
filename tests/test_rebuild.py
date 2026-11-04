"""S6 · 从 Raw 重建派生层的行为测试。

核心不变式：重建结果必须与「逐日正常跑」逐字节等价，否则重建就等于造了一份新数据。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl.rebuild import discover_raw_dates, rebuild_from_raw
from hf_data_crawl.store import DatasetStore

DAY1 = date(2026, 11, 1)
DAY2 = date(2026, 11, 2)


def record(index: int, downloads: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 2,
        "likes": 1,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en", "license:mit", "task_categories:text-classification"],
    }


def seed(store: DatasetStore, snapshot: date, records: list[dict[str, Any]]) -> None:
    store.write_raw(snapshot_date=snapshot, page_index=1, records=records, meta={})
    store.build_bronze(snapshot)
    store.build_silver(snapshot)
    store.update_state(snapshot)


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    seed(instance, DAY1, [record(1, 100), record(2, 50)])
    seed(instance, DAY2, [record(1, 180), record(2, 50)])
    return instance


def test_发现的日期按升序排列(store: DatasetStore) -> None:
    assert discover_raw_dates(store) == [DAY1, DAY2]


def test_重建后的current与原结果完全一致(store: DatasetStore) -> None:
    expected_current = store.read_current()
    expected_obs = store.read_observations()

    summary = rebuild_from_raw(store)

    assert summary.dates == [DAY1, DAY2]
    assert store.read_current().equals(expected_current)
    assert store.read_observations().equals(expected_obs)


def test_派生层被删光也能重建(store: DatasetStore) -> None:
    import shutil

    for derived in (store.bronze_dir, store.silver_dir, store.state_dir):
        shutil.rmtree(derived)

    summary = rebuild_from_raw(store)

    assert summary.current_rows == 2
    assert store.read_silver(DAY2).height == 2
    # 观测只有 ds-1 在 DAY2 变了，因此两条
    assert store.read_observations().height == 3


def test_无raw时返回空结果且不抛错(tmp_path: Path) -> None:
    summary = rebuild_from_raw(DatasetStore(tmp_path / "empty"))

    assert summary.dates == []
    assert summary.current_rows == 0


def test_不规范的raw目录名被忽略(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    (store.raw_dir / "dt=garbage").mkdir(parents=True)

    assert discover_raw_dates(store) == []


def test_重建不清除规模表(store: DatasetStore) -> None:
    """规模表来自 datasets-server，raw 里没有对应数据，删掉就白丢一轮抓取。

    2026-11-04 实测踩过：build 把 enrich 整个删掉，两万条规模数据（约 13 分钟）
    无声消失，页面上的「规模覆盖」直接归零。
    """
    import polars as pl

    from hf_data_crawl.sizes import SIZE_SCHEMA

    store.merge_sizes(
        pl.DataFrame(
            [
                {
                    "id": "acme/ds-1",
                    "snapshot_date": DAY2,
                    "status": "ok",
                    "num_rows": 42,
                    "estimated_num_rows": None,
                    "num_bytes_original": 10,
                    "num_bytes_parquet": 9,
                    "num_bytes_memory": 8,
                    "config_count": 1,
                    "num_columns_max": 3,
                    "configs": None,
                }
            ],
            schema=SIZE_SCHEMA,
        )
    )

    rebuild_from_raw(store)

    kept = store.read_sizes()
    assert kept.height == 1
    assert kept.row(0, named=True)["num_rows"] == 42


def test_重建会重算卡片表(store: DatasetStore) -> None:
    """卡片表可由 raw 重算，规则一改就该整体重来，旧的必须被清掉。"""
    store.write_raw(
        snapshot_date=DAY2,
        page_index=1,
        records=[{"id": "acme/ds-1", "cardData": {"pretty_name": "正式名"}}],
        meta={},
        label="card",
    )

    rebuild_from_raw(store)

    assert store.read_enrich().height == 1
    assert store.read_enrich().row(0, named=True)["pretty_name"] == "正式名"
