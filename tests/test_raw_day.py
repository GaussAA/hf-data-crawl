"""Raw 日模型：把「哪天有 Raw、哪天可重放」收成一处判据。

五种形态各有一条用例：普通 part/top、仅归档分片、仅 card 补抓、已 compact
进冷区、空 `dt=` 目录。判据只看文件形态，不看目录名。
"""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import lineage, raw, retention
from hf_data_crawl.store import DatasetStore

DAY = date(2026, 11, 7)


def record(index: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "sha": f"sha-{index}",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "downloads": 100,
        "downloadsAllTime": 200,
        "likes": 1,
        "tags": ["language:en"],
    }


def write_archive(store: DatasetStore, snapshot_date: date, label: str) -> Path:
    """按归档命名落一个分片；测试只关心文件名形态。"""
    partition = raw.partition_dir(store, snapshot_date)
    partition.mkdir(parents=True, exist_ok=True)
    path = partition / f"{label}-archive-000001-abcd1234.json.gz"
    payload = {"meta": {"archived": True}, "records": [record(1)]}
    path.write_bytes(gzip.compress(json.dumps(payload).encode("utf-8")))
    return path


def write_manifest(store: DatasetStore, dates: list[date]) -> None:
    path = retention.manifest_path(store)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"compressed_dates": sorted(day.isoformat() for day in dates)}),
        encoding="utf-8",
    )


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def test_normal_part_day_is_replayable(store: DatasetStore) -> None:
    """普通 part/top 分块：可重放、采过、未归档、未压缩。"""
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1)], meta={})
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(2)], meta={}, label="top")

    day = raw.describe_day(store, DAY)

    assert day.replayable and day.observed
    assert not day.archived and not day.compacted
    assert len(day.bronze) == 2
    assert day.supplements == ()
    assert day.files == day.bronze


def test_archive_only_day_is_replayable(store: DatasetStore) -> None:
    """仅归档分片：行为不变，但从「碰巧被 glob 匹配」变为接口承诺。"""
    write_archive(store, DAY, "part")
    write_archive(store, DAY, "top")

    day = raw.describe_day(store, DAY)

    assert day.replayable and day.observed and day.archived
    assert len(day.bronze) == 2
    assert day.supplements == ()


def test_card_only_day_is_replayable(store: DatasetStore) -> None:
    """仅 card 补抓批次：语义变化的唯一来源，从此算可重放。"""
    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[record(1)],
        meta={},
        label=raw.CARD_LABEL,
    )

    day = raw.describe_day(store, DAY)

    assert day.replayable and day.observed
    assert not day.archived
    assert day.bronze == ()
    assert len(day.supplements) == 1
    assert day.files == day.supplements


def test_compacted_day_is_observed_but_not_replayable(store: DatasetStore) -> None:
    """冷区日只可发现：采过，但主目录没有可重放的分块。"""
    write_manifest(store, [DAY])

    day = raw.describe_day(store, DAY)

    assert day.observed and day.compacted
    assert not day.replayable
    assert day.files == ()


def test_empty_partition_is_neither_observed_nor_replayable(store: DatasetStore) -> None:
    """空 `dt=` 目录只是 compact 留下的壳，不算采过。"""
    raw.partition_dir(store, DAY).mkdir(parents=True)

    day = raw.describe_day(store, DAY)

    assert not day.observed and not day.replayable
    assert raw.observed_days(store) == []


def test_observed_days_unions_main_directory_and_cold_manifest(store: DatasetStore) -> None:
    """采过的日期 = 主目录有分块的日子 ∪ 冷区清单，按日期升序。"""
    earlier = date(2026, 11, 5)
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1)], meta={})
    write_manifest(store, [earlier])

    days = raw.observed_days(store)

    assert [day.snapshot_date for day in days] == [earlier, DAY]
    assert [day.compacted for day in days] == [True, False]


def test_replayable_days_excludes_compacted(store: DatasetStore) -> None:
    """可重放集合不包含只能从冷区发现的日子。"""
    earlier = date(2026, 11, 5)
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1)], meta={})
    write_manifest(store, [earlier])

    assert [day.snapshot_date for day in raw.replayable_days(store)] == [DAY]


def test_replayable_matches_lineage_fingerprint(store: DatasetStore) -> None:
    """`RawDay.replayable` 与 `lineage.source_fingerprint` 必须恒等。"""
    archive_day = date(2026, 11, 4)
    card_day = date(2026, 11, 6)
    write_archive(store, archive_day, "part")
    raw.write(
        store,
        snapshot_date=card_day,
        page_index=1,
        records=[record(1)],
        meta={},
        label=raw.CARD_LABEL,
    )

    for day in (archive_day, card_day):
        replayable = raw.describe_day(store, day).replayable
        assert replayable == (lineage.source_fingerprint(store, day) is not None)
