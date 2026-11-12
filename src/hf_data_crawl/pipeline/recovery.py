"""派生崩溃恢复：发现超时的派生标记，清掉残留并从该日起后缀重放。

标记的写与清是 `marker` 里的底层原语，`derive` / `rebuild` 都要用；恢复则要
调 `rebuild` 做重放。两件事原先放在同一个模块（当时叫 `deriving`），于是
`derive → deriving → rebuild → derive` 成环，靠函数内延迟导入勉强躲过。拆开后
依赖只剩一个方向：`recovery → rebuild → derive → marker`。

原理、窗口与不覆盖的场景见 `marker` 模块文档串。
"""

from __future__ import annotations

from datetime import UTC, date, datetime
from typing import TYPE_CHECKING

from . import marker
from .rebuild import rebuild_from_raw, replay_suffix

if TYPE_CHECKING:
    from ..platform.store import DatasetStore


def check_and_recover(store: DatasetStore) -> list[date]:
    """检查所有派生标记，恢复未完成的派生。返回已恢复的日期列表。"""
    if not store.state_dir.exists():
        return []

    recovered: list[date] = []
    now = datetime.now(UTC)

    for path in store.state_dir.glob("deriving-*.flag"):
        flag = marker.load_flag(path)
        if flag is None:
            # 损坏的标记文件，直接删除
            path.unlink(missing_ok=True)
            continue

        # 检查是否超过阈值
        try:
            started = datetime.fromisoformat(flag.started_at)
        except ValueError:
            # 时间戳格式错误，视为损坏
            path.unlink(missing_ok=True)
            continue

        elapsed = (now - started).total_seconds()
        if elapsed > marker.STALE_THRESHOLD_SECONDS:
            # 超过阈值：先清掉残留的派生层，再视 Raw 是否还在决定是否重派生。
            marker.clear_day(store, flag.snapshot_date)
            path.unlink(missing_ok=True)
            if marker.has_raw(store, flag.snapshot_date):
                _rederive(store, flag.snapshot_date)
            recovered.append(flag.snapshot_date)

    return recovered


def _rederive(store: DatasetStore, snapshot_date: date) -> None:
    """清理后真正重派生：从该日起**后缀重放**，而不是只重算这一天。

    只重算一天会把 `current` 覆盖成那一天的值：`state._rebuild_current` 用当日帧
    替换同名 id 的全部标量列与 `last_seen_at`。若崩溃那天不是最新 Raw 日，
    「自愈」的结果是当前态整体回退——比不修更坏。后缀重放（见
    `rebuild.replay_suffix`）保证最后一天是最新日，`current` 收敛到正确值。

    `derive_day()` 会在开头重新打标记；这里吞掉异常，让新标记在下次启动时
    再次触发恢复。一个坏日期不应阻塞其余写命令。
    """
    try:
        rebuild_from_raw(store, days=replay_suffix(store, snapshot_date))
    except Exception:
        # 新标记已由 derive_day 开头写入，保留它等下一次超时重试。
        pass
