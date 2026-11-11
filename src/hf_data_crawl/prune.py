"""备份清理：把「只移不删」的备份真正释放成磁盘。

为什么单独成一步
----------------
`compact` / `archive` 刻意只移不删——删除不可逆，让它在数据搬运的同一步里顺手
发生，出事就没有回滚依据。但「永不删除」等于把磁盘交给时间：每天约 330 MB 的
原始分块进 `backup/raw/`，一年上百 GB，而它们早已被冷区基线（按 id 去重，体量
恒定）取代。所以销毁必须单独一步、单独上锁、单独写日志：搬运与销毁分开，谁出事
都能定位。

边界
----
只动 `backup/` 子树。权威层（`raw` 热区、`silver`、`observations`、`current`、
`cold` 基线）一律不碰——清理是「把副本释放掉」，不是「删历史」。

阈值
----
`BACKUP_PRUNE_AFTER_DAYS`（7 天）来自 `retention.py`，与 `compact` 的冷区门槛
同一个常量：压缩过 7 天的分区才会进 backup，留 7 天是给 `build --full` 留出
足够的重放窗口与回滚余量。两处若各写一个数，迟早会出现「刚备份就被删」或
「该删的永远留着」。
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from .platform.retention import BACKUP_PRUNE_AFTER_DAYS

if TYPE_CHECKING:
    from .platform.store import DatasetStore

#: 允许清理的子树。写死成常量而不是参数：清理命令一旦能指向别处，
#: 一次笔误就可能是删掉 `data/raw`。
BACKUP_SUBDIR = ("backup",)


@dataclass(frozen=True)
class PruneTarget:
    """一处待清理的备份。`kind` 只用于打印，便于人看懂删的是什么。"""

    kind: str
    path: Path
    bytes: int


@dataclass(frozen=True)
class PruneReport:
    removed: tuple[PruneTarget, ...]
    bytes_freed: int
    dry_run: bool

    @property
    def count(self) -> int:
        return len(self.removed)


def _tree_bytes(path: Path) -> int:
    """目录或文件的字节数。单个文件读 stat，目录逐个累加。"""
    if path.is_file():
        return path.stat().st_size
    total = 0
    for item in path.rglob("*"):
        if item.is_file():
            total += item.stat().st_size
    return total


def _partition_date(path: Path) -> date | None:
    """从 `dt=YYYY-MM-DD` 解析日期；不是这个形状就返回 None（不当成可清理对象）。"""
    name = path.name
    if not name.startswith("dt="):
        return None
    try:
        return date.fromisoformat(name[3:])
    except ValueError:
        return None


def _backup_root(store: DatasetStore) -> Path:
    return store.root.joinpath(*BACKUP_SUBDIR)


def plan(
    store: DatasetStore,
    *,
    now: datetime | None = None,
    keep_days: int = BACKUP_PRUNE_AFTER_DAYS,
) -> tuple[PruneTarget, ...]:
    """列出该清的备份；纯计算，不碰磁盘。

    两类对象：
    - `backup/raw/dt=<日期>`：按**目录名**判龄。用名字而不是 mtime——mtime 会被
      同步盘、备份工具整体改掉，而目录名是搬运时刻写下的，不会说谎。
    - `backup/cold/*.gz`：旧基线没有日期可读，只能按 mtime；它们是「上一版基线」，
      留够同样的天数即可回滚。
    """
    moment = now or datetime.now()
    cutoff = (moment - timedelta(days=keep_days)).date()
    root = _backup_root(store)
    if not root.is_dir():
        return ()

    targets: list[PruneTarget] = []

    raw_root = root / "raw"
    if raw_root.is_dir():
        for path in sorted(raw_root.glob("dt=*")):
            day = _partition_date(path)
            if day is None or day >= cutoff:
                continue
            targets.append(PruneTarget("backup/raw", path, _tree_bytes(path)))

    cold_root = root / "cold"
    if cold_root.is_dir():
        limit = moment - timedelta(days=keep_days)
        for path in sorted(cold_root.glob("*.gz")):
            modified = datetime.fromtimestamp(path.stat().st_mtime)
            if modified >= limit:
                continue
            targets.append(PruneTarget("backup/cold", path, _tree_bytes(path)))

    return tuple(targets)


def apply(store: DatasetStore, targets: tuple[PruneTarget, ...]) -> PruneReport:
    """执行清理。每一处都先验路径确实在 backup 子树内，再删。

    这道校验不是形式主义：清理命令是全项目唯一做删除的地方，一次路径拼接错误
    就可能指向 `data/raw`。宁可少删，不可错删。
    """
    root = _backup_root(store).resolve()
    freed = 0
    removed: list[PruneTarget] = []
    for target in targets:
        resolved = target.path.resolve()
        if not resolved.is_relative_to(root):
            raise ValueError(f"拒绝清理 backup 之外的路径：{resolved}")
        if target.path.is_dir():
            shutil.rmtree(target.path)
        else:
            target.path.unlink(missing_ok=True)
        freed += target.bytes
        removed.append(target)
    return PruneReport(removed=tuple(removed), bytes_freed=freed, dry_run=False)


def total_size(store: DatasetStore) -> int:
    """`backup/` 当前占用，供体检引用。"""
    root = _backup_root(store)
    return _tree_bytes(root) if root.is_dir() else 0
