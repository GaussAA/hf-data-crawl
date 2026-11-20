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

from hf_data_crawl.pipeline import bronze, raw
from hf_data_crawl.platform.store import DatasetStore

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
    first = raw.write(
        store,
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1), record(2)],
        meta={"cursor": None, "sort": "createdAt"},
    )
    second = raw.write(
        store,
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1), record(2)],
        meta={"cursor": None, "sort": "createdAt"},
    )

    assert first == second
    assert len(list(store.raw_dir.rglob(f"*{raw.SUFFIX}"))) == 1


def test_raw文件可独立解压并保留元信息(store: DatasetStore) -> None:
    path = raw.write(
        store,
        snapshot_date=SNAPSHOT,
        page_index=3,
        records=[record(9)],
        meta={"cursor": "abc==", "sort": "lastModified"},
    )

    payload = json.loads(raw.unpack(path.read_bytes(), name=path.name).decode("utf-8"))

    assert payload["meta"]["cursor"] == "abc=="
    assert payload["meta"]["page_index"] == 3
    assert [r["id"] for r in payload["records"]] == ["acme/ds-9"]


def test_同页内容变化时不覆盖旧文件(store: DatasetStore) -> None:
    """同一页码两次内容不同，说明不是同一批数据，覆盖会静默丢数据。"""
    raw.write(store, snapshot_date=SNAPSHOT, page_index=1, records=[record(1)], meta={})
    raw.write(store, snapshot_date=SNAPSHOT, page_index=1, records=[record(2)], meta={})

    assert len(list(store.raw_dir.rglob(f"*{raw.SUFFIX}"))) == 2


def test_从raw重建bronze行数与记录数一致(store: DatasetStore) -> None:
    raw.write(store, snapshot_date=SNAPSHOT, page_index=1, records=[record(1), record(2)], meta={})
    raw.write(store, snapshot_date=SNAPSHOT, page_index=2, records=[record(3)], meta={})

    written = bronze.build(store, SNAPSHOT)
    bronzes = store.read("bronze", SNAPSHOT)

    assert written == 3
    assert bronzes.height == 3
    assert sorted(bronzes["id"].to_list()) == ["acme/ds-1", "acme/ds-2", "acme/ds-3"]


def test_bronze同日重建不产生重复行(store: DatasetStore) -> None:
    raw.write(store, snapshot_date=SNAPSHOT, page_index=1, records=[record(1), record(2)], meta={})
    raw.write(store, snapshot_date=SNAPSHOT, page_index=1, records=[record(2), record(3)], meta={})

    bronze.build(store, SNAPSHOT)
    bronze.build(store, SNAPSHOT)

    bronzes = store.read("bronze", SNAPSHOT)

    assert bronzes.height == 3
    assert bronzes["id"].n_unique() == 3


def test_bronze保留原始JSON且不改写内容(store: DatasetStore) -> None:
    original = record(1, downloads=42)
    raw.write(store, snapshot_date=SNAPSHOT, page_index=1, records=[original], meta={})

    bronze.build(store, SNAPSHOT)
    row = store.read("bronze", SNAPSHOT).row(0, named=True)

    restored = json.loads(row["raw"])
    assert restored == original
    assert row["downloads"] == 42
    assert row["downloads_all_time"] == 126
    assert row["snapshot_date"] == SNAPSHOT


def test_缺少可选字段的记录仍能入bronze(store: DatasetStore) -> None:
    minimal = {"id": "acme/bare", "lastModified": "2026-11-02T00:00:00.000Z"}
    raw.write(store, snapshot_date=SNAPSHOT, page_index=1, records=[minimal], meta={})

    bronze.build(store, SNAPSHOT)
    row = store.read("bronze", SNAPSHOT).row(0, named=True)

    assert row["id"] == "acme/bare"
    assert row["downloads"] is None
    assert row["tags"] == []


def test_空目录重建返回零且不产出空分区(store: DatasetStore) -> None:
    assert bronze.build(store, SNAPSHOT) == 0
    assert not store.has("bronze", SNAPSHOT)


def test_bronze被polars直接可读(store: DatasetStore) -> None:
    raw.write(store, snapshot_date=SNAPSHOT, page_index=1, records=[record(1)], meta={})
    bronze.build(store, SNAPSHOT)

    frame = pl.read_parquet(store.path("bronze", SNAPSHOT))

    assert frame.height == 1


def test_同页多次写入以后写者为准(store: DatasetStore) -> None:
    """同 id 去重的基本语义：最后写入的那份胜出。

    「不依赖文件名字典序」「不依赖 mtime」这两条更强的保证由
    tests/pipeline/test_raw_order.py 负责；此处只守住最基础的一条。
    """
    raw.write(
        store,
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1) | {"mainSize": 111}],
        meta={},
        written_at="2026-11-04T09:00:00.000000+00:00",
    )
    raw.write(
        store,
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1) | {"mainSize": 222}],
        meta={},
        written_at="2026-11-04T10:00:00.000000+00:00",
    )

    bronze.build(store, SNAPSHOT)

    assert store.read("bronze", SNAPSHOT).row(0, named=True)["main_size"] == 222


def test_后写入的新字段不会被先写入的空值覆盖(store: DatasetStore) -> None:
    """先写的记录没有 mainSize，后写的有时，结果必须取后写的。"""
    import os
    import time

    raw.write(
        store, snapshot_date=SNAPSHOT, page_index=1, records=[record(1)], meta={}, label="part"
    )
    raw.write(
        store,
        snapshot_date=SNAPSHOT,
        page_index=1,
        records=[record(1) | {"mainSize": 999}],
        meta={},
        label="top",
    )

    # 让 part 的 mtime 更早，模拟真实时序（collect 先跑增量、再跑顶部刷新）。
    now = time.time()
    for path in (store.raw_dir / f"dt={SNAPSHOT.isoformat()}").glob(f"part-*{raw.SUFFIX}"):
        os.utime(path, (now - 10, now - 10))
    for path in (store.raw_dir / f"dt={SNAPSHOT.isoformat()}").glob(f"top-*{raw.SUFFIX}"):
        os.utime(path, (now, now))

    bronze.build(store, SNAPSHOT)

    assert store.read("bronze", SNAPSHOT).row(0, named=True)["main_size"] == 999


def test_历史gzip分块与新zstd分块混存时都能读(store: DatasetStore) -> None:
    """压缩格式换代之后，历史分块必须照读——它们不重写，指纹才不动。

    2026-11-16 把 Raw 的压缩从 gzip（默认 level 9，压一页要 61 ms）换成 zstd
    level 3（4.7 ms，体积还小 5%）。已落的 10419 个 `.json.gz` 一律不碰：文件名
    参与 `lineage` 指纹，重写一个就等于宣告那天派生作废。兼容只能落在读取端，
    所以这里同时造新旧两种分块，钉住「同一天混存也能读全」。
    """
    partition = raw.partition_dir(store, SNAPSHOT)
    partition.mkdir(parents=True)
    legacy_payload = {
        "meta": {"written_at": "2026-11-03T10:00:00+00:00"},
        "records": [record(1)],
    }
    legacy = partition / "part-legacy-000001-abcd1234.json.gz"
    legacy.write_bytes(gzip.compress(json.dumps(legacy_payload).encode("utf-8")))

    fresh = raw.write(store, snapshot_date=SNAPSHOT, page_index=2, records=[record(2)])

    assert fresh.name.endswith(raw.SUFFIX)
    assert legacy.exists(), "历史分块不该被改写或删除，删掉就动了指纹"

    # 单文件读：按后缀分派解压，不靠试错
    assert raw.load_batches(legacy)[0].records == [record(1)]

    # 整天读：两种后缀都要被 batch_files 收进来
    names = {path.name for path in raw.batch_files(store, SNAPSHOT)}
    assert names == {legacy.name, fresh.name}
    assert sum(len(raw.load_batches(path)) for path in raw.batch_files(store, SNAPSHOT)) == 2

    # 混存的一天照样能派生，且两个 id 都在
    bronze.build(store, SNAPSHOT)
    assert set(store.read("bronze", SNAPSHOT)["id"].to_list()) == {"acme/ds-1", "acme/ds-2"}
