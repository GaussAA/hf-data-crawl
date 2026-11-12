"""Dataset 状态：词表、探测映射、重建留存协议。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.acquire.reconcile import STATUS_BY_VERDICT
from hf_data_crawl.pipeline import raw, state
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.pipeline.rebuild import restore, retain, snapshot_path
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.vocab import INACTIVE, Status

DAY = date(2026, 11, 1)


def record(index: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": 10,
        "downloadsAllTime": 20,
        "likes": 1,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en"],
    }


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    raw.write(instance, snapshot_date=DAY, page_index=1, records=[record(1), record(2)], meta={})
    derive_day(instance, DAY)
    return instance


def status_of(store: DatasetStore, dataset_id: str) -> str:
    frame = state.read_current(store).filter(pl.col("id") == dataset_id)
    return frame.row(0, named=True)["status"]


# ---------------------------------------------------------------- 词表与映射


def test_词表恰好四值() -> None:
    assert {status.value for status in Status} == {
        "active",
        "disappeared",
        "restricted",
        "renamed",
    }


def test_非在架集合不含active() -> None:
    assert Status.ACTIVE not in INACTIVE
    assert INACTIVE == {Status.DISAPPEARED, Status.RESTRICTED, Status.RENAMED}


def test_探测结论映射到状态() -> None:
    assert STATUS_BY_VERDICT == {
        "deleted": Status.DISAPPEARED,
        "restricted": Status.RESTRICTED,
        "renamed": Status.RENAMED,
        "present": Status.ACTIVE,
    }


def test_unknown结论不在映射内表示先别动() -> None:
    assert "unknown" not in STATUS_BY_VERDICT


def test_mark_status拒绝词表外的值(store: DatasetStore) -> None:
    with pytest.raises(ValueError, match="gone"):
        state.mark_status(store, {"acme/ds-1": "gone"})

    assert status_of(store, "acme/ds-1") == "active"


# ---------------------------------------------------------------- 留存协议


def test_无失效项时不写快照(store: DatasetStore) -> None:
    assert retain(store) == {}
    assert not snapshot_path(store).exists()


def test_留存只含非在架的行并写下快照(store: DatasetStore) -> None:
    state.mark_status(store, {"acme/ds-2": "restricted"})

    retained = retain(store)

    assert retained == {"acme/ds-2": "restricted"}
    assert json.loads(snapshot_path(store).read_text(encoding="utf-8")) == retained


def test_快照存在时以快照为准而非current(store: DatasetStore) -> None:
    """快照存在说明上次重建被打断，此刻 current 可能已被清空或重算。"""
    snapshot_path(store).parent.mkdir(parents=True, exist_ok=True)
    snapshot_path(store).write_text(json.dumps({"acme/ds-1": "renamed"}), encoding="utf-8")

    assert retain(store) == {"acme/ds-1": "renamed"}


def test_损坏的快照回退读current(store: DatasetStore) -> None:
    state.mark_status(store, {"acme/ds-2": "restricted"})
    snapshot_path(store).parent.mkdir(parents=True, exist_ok=True)
    snapshot_path(store).write_text("{半截", encoding="utf-8")

    assert retain(store) == {"acme/ds-2": "restricted"}


def test_快照里词表之外的值被丢弃而不是打断重建(store: DatasetStore) -> None:
    snapshot_path(store).parent.mkdir(parents=True, exist_ok=True)
    snapshot_path(store).write_text(
        json.dumps({"acme/ds-1": "gone", "acme/ds-2": "renamed"}), encoding="utf-8"
    )

    assert retain(store) == {"acme/ds-2": "renamed"}


def test_写回后状态生效且快照被删除(store: DatasetStore) -> None:
    state.mark_status(store, {"acme/ds-2": "restricted"})
    retained = retain(store)
    state.mark_status(store, {"acme/ds-2": "active"})  # 模拟 state 被重算后全部在架

    restore(store, retained)

    assert status_of(store, "acme/ds-2") == "restricted"
    assert not snapshot_path(store).exists()


def test_没有留存时写回也会清掉残留快照(store: DatasetStore) -> None:
    snapshot_path(store).parent.mkdir(parents=True, exist_ok=True)
    snapshot_path(store).write_text("{}", encoding="utf-8")

    restore(store, {})

    assert not snapshot_path(store).exists()
