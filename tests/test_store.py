"""S2 · Raw 落盘与幂等 Bronze 的行为测试。

只验外部可见行为：写了几次、落几个文件、重建出几行、字段能否还原。
不断言内部文件命名细节之外的实现方式。
"""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.store import DatasetStore

SNAPSHOT = date(2026, 11, 3)


def record(index: int, downloads: int = 100) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-03-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 3,
        "likes": 7,
        "gated": False,
        "private": False,
        "description": "演示用",
        "tags": ["task_categories:text-classification", "language:en", "license:mit"],
    }


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def test_同页写两次只留一个文件且内容不变(store: DatasetStore) -> None:
    first = store.write_raw(
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1), record(2)],
        meta={"cursor": None, "sort": "createdAt"},
    )
    second = store.write_raw(
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1), record(2)],
        meta={"cursor": None, "sort": "createdAt"},
    )

    assert first == second
    assert len(list(store.raw_dir.rglob("*.json.gz"))) == 1


def test_raw文件可独立解压并保留元信息(store: DatasetStore) -> None:
    path = store.write_raw(
        snapshot_date=SNAPSHOT,
        page_index=3,
        records=[record(9)],
        meta={"cursor": "abc==", "sort": "lastModified"},
    )

    payload = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))

    assert payload["meta"]["cursor"] == "abc=="
    assert payload["meta"]["page_index"] == 3
    assert [r["id"] for r in payload["records"]] == ["acme/ds-9"]


def test_同页内容变化时不覆盖旧文件(store: DatasetStore) -> None:
    """同一页码两次内容不同，说明不是同一批数据，覆盖会静默丢数据。"""
    store.write_raw(snapshot_date=SNAPSHOT, page_index=1, records=[record(1)], meta={})
    store.write_raw(snapshot_date=SNAPSHOT, page_index=1, records=[record(2)], meta={})

    assert len(list(store.raw_dir.rglob("*.json.gz"))) == 2


def test_从raw重建bronze行数与记录数一致(store: DatasetStore) -> None:
    store.write_raw(snapshot_date=SNAPSHOT, page_index=1, records=[record(1), record(2)], meta={})
    store.write_raw(snapshot_date=SNAPSHOT, page_index=2, records=[record(3)], meta={})

    written = store.build_bronze(SNAPSHOT)
    bronzes = store.read_bronze(SNAPSHOT)

    assert written == 3
    assert bronzes.height == 3
    assert sorted(bronzes["id"].to_list()) == ["acme/ds-1", "acme/ds-2", "acme/ds-3"]


def test_bronze同日重建不产生重复行(store: DatasetStore) -> None:
    store.write_raw(snapshot_date=SNAPSHOT, page_index=1, records=[record(1), record(2)], meta={})
    store.write_raw(snapshot_date=SNAPSHOT, page_index=1, records=[record(2), record(3)], meta={})

    store.build_bronze(SNAPSHOT)
    store.build_bronze(SNAPSHOT)

    bronzes = store.read_bronze(SNAPSHOT)

    assert bronzes.height == 3
    assert bronzes["id"].n_unique() == 3


def test_bronze保留原始JSON且不改写内容(store: DatasetStore) -> None:
    original = record(1, downloads=42)
    store.write_raw(snapshot_date=SNAPSHOT, page_index=1, records=[original], meta={})

    store.build_bronze(SNAPSHOT)
    row = store.read_bronze(SNAPSHOT).row(0, named=True)

    restored = json.loads(row["raw"])
    assert restored == original
    assert row["downloads"] == 42
    assert row["downloads_all_time"] == 126
    assert row["snapshot_date"] == SNAPSHOT


def test_缺少可选字段的记录仍能入bronze(store: DatasetStore) -> None:
    minimal = {"id": "acme/bare", "lastModified": "2026-11-02T00:00:00.000Z"}
    store.write_raw(snapshot_date=SNAPSHOT, page_index=1, records=[minimal], meta={})

    store.build_bronze(SNAPSHOT)
    row = store.read_bronze(SNAPSHOT).row(0, named=True)

    assert row["id"] == "acme/bare"
    assert row["downloads"] is None
    assert row["tags"] == []


def test_空目录重建返回零且不产出空分区(store: DatasetStore) -> None:
    assert store.build_bronze(SNAPSHOT) == 0
    assert not store.bronze_dir.exists() or store.read_bronze(SNAPSHOT).height == 0


def test_bronze被polars直接可读(store: DatasetStore) -> None:
    store.write_raw(snapshot_date=SNAPSHOT, page_index=1, records=[record(1)], meta={})
    store.build_bronze(SNAPSHOT)

    path = next(store.bronze_dir.rglob("*.parquet"))
    frame = pl.read_parquet(path)

    assert frame.height == 1


def test_同页多次写入以后写者为准(store: DatasetStore) -> None:
    """同 id 去重的基本语义：最后写入的那份胜出。

    「不依赖文件名字典序」「不依赖 mtime」这两条更强的保证由
    tests/test_raw_order.py 负责；此处只守住最基础的一条。
    """
    store.write_raw(
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1) | {"mainSize": 111}],
        meta={},
        written_at="2026-11-04T09:00:00.000000+00:00",
    )
    store.write_raw(
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1) | {"mainSize": 222}],
        meta={},
        written_at="2026-11-04T10:00:00.000000+00:00",
    )

    store.build_bronze(SNAPSHOT)

    assert store.read_bronze(SNAPSHOT).row(0, named=True)["main_size"] == 222


def test_后写入的新字段不会被先写入的空值覆盖(store: DatasetStore) -> None:
    """先写的记录没有 mainSize，后写的有时，结果必须取后写的。"""
    import os
    import time

    store.write_raw(
        snapshot_date=SNAPSHOT, page_index=1, records=[record(1)], meta={}, label="part"
    )
    store.write_raw(
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1) | {"mainSize": 999}],
        meta={},
        label="top",
    )

    # 让 part 的 mtime 更早，模拟真实时序（collect 先跑增量、再跑顶部刷新）。
    now = time.time()
    for path in (store.raw_dir / f"dt={SNAPSHOT.isoformat()}").glob("part-*.json.gz"):
        os.utime(path, (now - 10, now - 10))
    for path in (store.raw_dir / f"dt={SNAPSHOT.isoformat()}").glob("top-*.json.gz"):
        os.utime(path, (now, now))

    store.build_bronze(SNAPSHOT)

    assert store.read_bronze(SNAPSHOT).row(0, named=True)["main_size"] == 999
