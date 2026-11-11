"""对账会话的行为测试：游标、已见集合、基线同键同生命周期。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import raw
from hf_data_crawl.derive import derive_day
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.session import (
    BASELINE_FILENAME,
    SEEN_FILENAME,
    ReconcileSession,
    diff_sets,
)

DAY = date(2026, 11, 1)
CHECKPOINT = "reconcile-checkpoint.json"


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


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    raw.write(
        instance,
        snapshot_date=DAY,
        page_index=1,
        records=[record("a", "sha-a1"), record("b", "sha-b1")],
        meta={},
    )
    derive_day(instance, DAY)
    return instance


# ---------------------------------------------------------------- 集合差（纯函数）


def test_集合差分出四类() -> None:
    baseline = {
        "a": ("sha-a1", "active"),
        "b": ("sha-b1", "active"),
        "c": ("sha-c1", "restricted"),
        "d": ("sha-d1", "active"),
    }
    seen = {"a": "sha-a2", "c": "sha-c1", "e": "sha-e1", "b": "sha-b1"}

    result = diff_sets(baseline, seen)

    assert result.new == ["e"]
    assert result.sha_changed == ["a"]
    assert result.reactivated == ["c"]
    assert result.missing == ["d"]


def test_空基线时见到的全是新增() -> None:
    result = diff_sets({}, {"a": "s", "b": None})

    assert result.new == ["a", "b"]
    assert result.missing == []


# ---------------------------------------------------------------- open 的三种情形


def test_无游标时新建会话并固定基线(store: DatasetStore) -> None:
    session = ReconcileSession.open(store, checkpoint_filename=CHECKPOINT)

    assert session.resumed is False
    assert session.discarded is False
    assert (store.state_dir / BASELINE_FILENAME).exists()
    assert session.diff().missing == ["acme/a", "acme/b"]


def test_续跑沿用开始时的基线而非现读current(store: DatasetStore) -> None:
    first = ReconcileSession.open(store, checkpoint_filename=CHECKPOINT)
    first.remember([record("a", "sha-a1")])
    first.checkpoint.save("c2")
    # 有界停止那一轮会改写 current：这里模拟成多了一个库
    raw.write(store, snapshot_date=DAY, page_index=2, records=[record("c", "sha-c1")], meta={})
    derive_day(store, DAY)

    resumed = ReconcileSession.open(store, checkpoint_filename=CHECKPOINT)

    assert resumed.resumed is True
    assert resumed.status_before("acme/c") is None
    assert resumed.diff().missing == ["acme/b"]


def test_有游标但无基线时丢弃旧会话重来(store: DatasetStore) -> None:
    (store.state_dir).mkdir(parents=True, exist_ok=True)
    (store.state_dir / CHECKPOINT).write_text('{"cursor": "old"}', encoding="utf-8")

    session = ReconcileSession.open(store, checkpoint_filename=CHECKPOINT)

    assert session.discarded is True
    assert session.resumed is False
    assert session.checkpoint.cursor is None


def test_启动时清掉旧格式的按日期命名文件(store: DatasetStore) -> None:
    store.state_dir.mkdir(parents=True, exist_ok=True)
    legacy = store.state_dir / "reconcile-seen-2026-10-30.csv"
    legacy.write_text("id,sha\n", encoding="utf-8")

    ReconcileSession.open(store, checkpoint_filename=CHECKPOINT)

    assert not legacy.exists()


# ---------------------------------------------------------------- 生命周期


def test_close一起删除三样东西(store: DatasetStore) -> None:
    session = ReconcileSession.open(store, checkpoint_filename=CHECKPOINT)
    session.remember([record("a", "sha-a1")])
    session.checkpoint.save("c2")

    session.close()

    assert session.checkpoint.cursor is None
    assert not (store.state_dir / BASELINE_FILENAME).exists()
    assert not (store.state_dir / SEEN_FILENAME).exists()


def test_重复追加同一页无害(store: DatasetStore) -> None:
    session = ReconcileSession.open(store, checkpoint_filename=CHECKPOINT)

    session.remember([record("a", "sha-a1")])
    session.remember([record("a", "sha-a1")])

    assert session.seen() == {"acme/a": "sha-a1"}
