"""备份清理的契约：只动 backup/、按目录名判龄、预演不动磁盘。

为什么值得单独测：这是全项目唯一做删除的地方。判龄判错会删掉刚备份的分区
（回滚依据没了），路径判错会伤到权威层（数据没了）。
"""

from __future__ import annotations

import os
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from hf_data_crawl.ops.prune import PruneTarget, apply, plan, total_size
from hf_data_crawl.platform.store import DatasetStore

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


def test_compact_当晚搬入的备份当晚不被删(store: DatasetStore) -> None:
    """compact 只压 < today-7 的分区，搬入那一刻数据日期就已经 8 天旧。

    判龄若也用 7 天，同一晚 03:10 的 prune 会把它当过期清掉——备份的实际保留期
    只剩 2.6 小时（00:30 搬入 → 03:10 删除），「留 7 天给 build --full 回滚」
    就是一句空话。默认阈值必须把 compact 的那个窗口算进去。
    """
    tonight = datetime(2026, 11, 16, 3, 10)
    moved_in = date(2026, 11, 8)  # compact 只压早于 11-09 的分区
    make_backup_partition(store, moved_in)

    assert plan(store, now=tonight) == ()


def test_默认阈值下真正过期的备份仍会清理(store: DatasetStore) -> None:
    """放宽阈值不等于永不清理：早于保留期的备份照旧释放磁盘。"""
    tonight = datetime(2026, 11, 16, 3, 10)
    expired = date(2026, 10, 31)
    make_backup_partition(store, expired)

    targets = plan(store, now=tonight)

    assert [item.path.name for item in targets] == [f"dt={expired.isoformat()}"]


def make_generation(
    store: DatasetStore, index: int, pieces: int = 2, *, at: datetime
) -> list[Path]:
    """造一代旧基线：同一时刻写出的 `pieces` 个分片。"""
    cold = store.root / "backup" / "cold"
    cold.mkdir(parents=True, exist_ok=True)
    stamp = at.timestamp()
    created: list[Path] = []
    for piece in range(1, pieces + 1):
        path = cold / f"baseline-{piece:06d}-{index:02x}{piece:02x}abcd.json.zst"
        path.write_bytes(b"y" * 50)
        os.utime(path, (stamp, stamp))
        created.append(path)
    return created


def test_旧基线按代保留_更早的整代清理(store: DatasetStore) -> None:
    """旧基线是「整条冷区基线的上一版」，一代 150 MB，回滚只需最近两代。

    按天判龄会让 14 份几乎相同的快照并存（实测 1.58 GB），故改按代计数。
    """
    base = datetime(2026, 11, 1, 0, 30)
    generations = [
        make_generation(store, index, at=base + timedelta(days=index)) for index in range(4)
    ]

    targets = plan(store, now=NOW, keep_generations=2)

    assert {item.path.name for item in targets} == {
        path.name for generation in generations[:2] for path in generation
    }
    assert {item.kind for item in targets} == {"backup/cold"}


def test_一整代同去同留_不留残缺基线(store: DatasetStore) -> None:
    """半份基线既不能回滚也占着磁盘，比不删更糟：按代删，不按片删。"""
    base = datetime(2026, 11, 1, 0, 30)
    big = make_generation(store, 0, pieces=3, at=base)
    small = make_generation(store, 1, pieces=2, at=base + timedelta(days=1))

    targets = plan(store, now=NOW, keep_generations=1)

    assert {item.path.name for item in targets} == {path.name for path in big}
    assert all(path.exists() for path in small)


def test_yzst_基线同样计入代际(store: DatasetStore) -> None:
    """`prune` 曾只匹配 `*.gz`，新格式 `.zst` 的旧基线永远漏网（1.58 GB 的来历）。"""
    base = datetime(2026, 11, 1, 0, 30)
    old = make_generation(store, 0, at=base)
    new = make_generation(store, 1, at=base + timedelta(days=1))
    # 混入一份历史 `.gz` 基线，两代都要被认出来
    legacy = store.root / "backup" / "cold" / "baseline-000009-legacy1.json.gz"
    legacy.write_bytes(b"z" * 50)
    os.utime(legacy, (base.timestamp(), base.timestamp()))

    targets = plan(store, now=NOW, keep_generations=1)

    assert {item.path.name for item in targets} == {path.name for path in old} | {legacy.name}
    assert all(path.exists() for path in new)


def test_代数不足保留数时一份不删(store: DatasetStore) -> None:
    base = datetime(2026, 11, 1, 0, 30)
    make_generation(store, 0, at=base)
    make_generation(store, 1, at=base + timedelta(days=1))

    assert plan(store, now=NOW) == ()


def test_不清理开关下旧基线全留(store: DatasetStore) -> None:
    """基线可疑时的一键急救：只清 `backup/raw`，`backup/cold` 一片不动。"""
    base = datetime(2026, 11, 1, 0, 30)
    for index in range(3):
        make_generation(store, index, at=base + timedelta(days=index))

    assert plan(store, now=NOW, keep_generations=None) == ()


def test_保留代数为负时拒绝(store: DatasetStore) -> None:
    base = datetime(2026, 11, 1, 0, 30)
    make_generation(store, 0, at=base)

    with pytest.raises(ValueError, match="不能为负"):
        plan(store, now=NOW, keep_generations=-1)
