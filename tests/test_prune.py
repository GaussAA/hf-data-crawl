"""备份清理的契约：只动 backup/、按目录名判龄、预演不动磁盘。

为什么值得单独测：这是全项目唯一做删除的地方。判龄判错会删掉刚备份的分区
（回滚依据没了），路径判错会伤到权威层（数据没了）。
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from hf_data_crawl.prune import PruneTarget, apply, plan, total_size
from hf_data_crawl.store import DatasetStore

NOW = datetime(2026, 11, 9, 20, 0)


def make_backup_partition(store: DatasetStore, day: date, items: int = 2) -> Path:
    target = store.root / "backup" / "raw" / f"dt={day.isoformat()}"
    target.mkdir(parents=True, exist_ok=True)
    for index in range(items):
        (target / f"part-{index:06d}.json.gz").write_bytes(b"x" * 100)
    return target


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    """每个用例独立根目录：清理是删除操作，用例之间绝不能共享状态。"""
    return DatasetStore(tmp_path / "data")


def test_只清超过保留期的分区(store: DatasetStore) -> None:
    old = date(2026, 10, 30)
    fresh = date(2026, 11, 8)
    make_backup_partition(store, old)
    make_backup_partition(store, fresh)

    targets = plan(store, now=NOW, keep_days=7)

    assert [item.path.name for item in targets] == [f"dt={old.isoformat()}"]


def test_按目录名判龄而不是修改时间(store: DatasetStore) -> None:
    """mtime 会被同步盘、备份工具整体改掉；目录名是搬运时刻写下的，不会说谎。"""
    old = date(2026, 10, 30)
    path = make_backup_partition(store, old)
    recent = (NOW - timedelta(hours=1)).timestamp()
    os.utime(path, (recent, recent))

    targets = plan(store, now=NOW, keep_days=7)

    assert [item.path.name for item in targets] == [f"dt={old.isoformat()}"]


def test_不认识的分区名不当成清理对象(store: DatasetStore) -> None:
    root = store.root / "backup" / "raw"
    (root / "dt=garbage").mkdir(parents=True)
    (root / "keep-me").mkdir(parents=True)

    assert plan(store, now=NOW, keep_days=7) == ()


def test_目录不存在时什么都不做(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "nowhere")

    assert plan(store, now=NOW) == ()
    assert total_size(store) == 0


def test_清理只删目标并返回释放量(store: DatasetStore) -> None:
    old = date(2026, 10, 30)
    fresh = date(2026, 11, 8)
    gone = make_backup_partition(store, old)
    kept = make_backup_partition(store, fresh)

    report = apply(store, plan(store, now=NOW, keep_days=7))

    assert report.count == 1
    assert report.bytes_freed == 200
    assert not gone.exists()
    assert kept.exists()


def test_拒绝清理备份之外的路径(store: DatasetStore) -> None:
    """一次路径拼接错误就可能是删掉 data/raw：宁可少删，不可错删。"""
    make_backup_partition(store, date(2026, 10, 30))
    victim = store.root / "raw" / "dt=2026-10-01"
    victim.mkdir(parents=True)

    with pytest.raises(ValueError, match="backup 之外"):
        apply(store, (PruneTarget("backup/raw", victim, 0),))
    assert victim.exists()


def test_旧基线按龄一并清理(store: DatasetStore) -> None:
    cold = store.root / "backup" / "cold"
    cold.mkdir(parents=True)
    stale = cold / "baseline-000001-aaaaaaaa.json.gz"
    stale.write_bytes(b"y" * 50)
    long_ago = (NOW - timedelta(days=30)).timestamp()
    os.utime(stale, (long_ago, long_ago))
    fresh = cold / "baseline-000002-bbbbbbbb.json.gz"
    fresh.write_bytes(b"z" * 50)

    targets = plan(store, now=NOW, keep_days=7)

    assert [item.path.name for item in targets] == [stale.name]
