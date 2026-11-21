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

阈值：`backup/raw` 按天，`backup/cold` 按代
-------------------------------------------
两类备份的代价差 100 倍，不能共用一个数字。

`backup/raw`（`BACKUP_KEEP_DAYS`，来自 `retention.py`）= 「想保留的天数」+
「compact 的冷区窗口」。为什么不是「想保留的天数」本身：备份目录名记的是
**数据日期**，而 `compact` 只压早于 `today - REPLAY_WINDOW_DAYS` 的分区，所以
一份备份落地那一刻数据日期就已经 8 天旧了。判龄若只取 7 天，同一晚 00:30 刚
搬进 `backup/raw/` 的分区会被 03:10 的 `prune` 立刻删掉——备份只活 2.6 小时，
`build --full` 的回滚依据等于没有。
（2026-11-16 审计实测：`prune --dry-run` 直接命中当晚搬入的 `dt=<today-8>`。）
这类备份**每天新增一份、每份只值一天**，按天判龄是对的。

`backup/cold` 是另一回事：它装的是**整条冷区基线的上一版**，一次 `compact`
搬入一整代（实测 104–114 片、约 150–325 MB），而基线本身是「全量去重后的
当前态」——旧代的价值只在于「当前基线坏了，退回上一版」。留 14 天意味着留
14 份几乎相同的 150 MB 快照，实测 998 片 / 1.58 GB，且每天再涨 175 MB
（2026-11-21 体积审计）。回滚只需要上一两代，故改按**代**计数。

判「代」用的是写出时刻的后向聚类：同代分片由一次 `compact` 连续写出，不同代
之间至少隔一天。实测 11-11~11-20 九代——代内最大间隔 6.9 秒、代间最小间隔
23 小时，取 1 小时为界，两侧各留三个数量级余量。误聚的唯一方向是「把两代并成
一代」，那只会**少删**，不会误删。

`backup/raw` 的判龄阈值 `prune.plan` 与 `doctor._backup_backlog` 共用，各写一个
数迟早出现「刚备份就被删」或「该删的永远留着」。
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from ..platform.retention import (
    BACKUP_KEEP_DAYS,
    BASELINE_PREFIX,
    COLD_BASELINE_GENERATIONS,
    COLD_GENERATION_GAP_SECONDS,
)

if TYPE_CHECKING:
    from ..platform.store import DatasetStore

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


def _cold_generations(paths: list[Path]) -> list[list[Path]]:
    """把旧基线按写出时刻聚成「代」，返回从旧到新。

    相邻两片间隔超过 `COLD_GENERATION_GAP_SECONDS` 即视为换了一代。聚类只可能
    把两代并成一代（少删），不会把一代劈成两半（留下一份残缺基线、删掉另一半
    反而更糟）——除非外部工具把 mtime 全压平，那种情况下全归一代、一份不删。
    """
    ordered = sorted(paths, key=lambda item: (item.stat().st_mtime, item.name))
    generations: list[list[Path]] = []
    previous: float | None = None
    for path in ordered:
        moment = path.stat().st_mtime
        if previous is None or moment - previous > COLD_GENERATION_GAP_SECONDS:
            generations.append([])
        generations[-1].append(path)
        previous = moment
    return generations


def plan(
    store: DatasetStore,
    *,
    now: datetime | None = None,
    keep_days: int = BACKUP_KEEP_DAYS,
    keep_generations: int | None = COLD_BASELINE_GENERATIONS,
) -> tuple[PruneTarget, ...]:
    """列出该清的备份；纯计算，不碰磁盘。

    两类对象、两套判据：
    - `backup/raw/dt=<日期>`：按**目录名**判龄，超过 `keep_days` 即清。用名字
      而不是 mtime——mtime 会被同步盘、备份工具整体改掉，而目录名是搬运时刻
      写下的，不会说谎。
    - `backup/cold/baseline-*`：按**代**保留最近 `keep_generations` 代，更早的
      整代清掉。它们没有日期可读，mtime 也只剩「是哪个整批」这一层信息，所以
      不用来判龄而只用来分代。`None` 表示一代都不清（基线可疑时的急救开关）。
    """
    if keep_generations is not None and keep_generations < 0:
        raise ValueError(f"keep_generations 不能为负：{keep_generations}")

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
    if cold_root.is_dir() and keep_generations is not None:
        generations = _cold_generations(sorted(cold_root.glob(f"{BASELINE_PREFIX}*")))
        stale = generations[: max(len(generations) - keep_generations, 0)]
        for generation in stale:
            for path in generation:
                targets.append(PruneTarget("backup/cold", path, _tree_bytes(path)))

    return tuple(targets)


def apply(store: DatasetStore, targets: tuple[PruneTarget, ...]) -> PruneReport:
    """执行清理。每一处都先验路径确实在 backup 子树内，再删。

    这道校验不是形式主义：清理命令是全项目唯一做删除的地方，一次路径拼接错误
    就可能指向 `data/raw`。宁可少删，不可错删。
    """
    root = _backup_root(store).resolve()
    # 先全部校验、再全部删。混着做的时候，第 N 个路径越界会让前 N-1 个已经删掉，
    # 而异常只说「拒绝清理」，调用方无从知道删到了哪一步——报告里的数字也会是空的。
    for target in targets:
        resolved = target.path.resolve()
        if not resolved.is_relative_to(root):
            raise ValueError(f"拒绝清理 backup 之外的路径：{resolved}")

    freed = 0
    removed: list[PruneTarget] = []
    for target in targets:
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
