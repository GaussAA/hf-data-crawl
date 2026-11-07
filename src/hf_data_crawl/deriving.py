"""派生原子性保护：检测和恢复未完成的派生。

为什么需要它
------------
`derive_day()` 依次更新 bronze → silver → observations → current 四个文件。
如果在中途崩溃（如 observations 写完、current 写入前），会导致：
- observations 已有新数据
- current 还是旧状态

下次启动时，`state.update()` 会重新读取 bronze 并再次追加 observations，
虽然同日重跑是幂等的（会先删除当天的 observations），但如果崩溃后
**第二天**才重启，就会把昨天的 bronze 再推一次观测（因为不会删第二天的分区）。

解决方案
--------
在 `derive_day()` 开头写一个"派生中标记文件"（如 `state/deriving-YYYY-MM-DD.flag`），
包含开始时间戳。派生成功后删除。

启动时检查：
1. 如果发现标记文件且超过阈值（如 10 分钟），说明上次派生可能未完成
2. 删除该日的**所有派生层分区**（bronze/silver/observations）
3. 删除标记文件
4. 该日 Raw 仍存在时从该日起**后缀重放**（不是只重算这一天——只算一天会让
   `current` 回退到那天的值，见 `_rederive`）；Raw 已被归档清理时只清理派生层，
   不动全局的 `current`

这样：
- 复杂度低（只多写一个小文件）
- 性能开销小（只在启动时检查一次）
- 能检测并自愈大部分不一致

不覆盖的场景
-----------
如果派生完成、标记删除前崩溃，下次启动看不到标记文件，不会清理。
但这个窗口极窄（只有"删除文件"这一步），且影响有限（派生已完成，只是多了个标记文件）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .atomic import write_text

if TYPE_CHECKING:
    from .store import DatasetStore

#: 超过这个时长的标记文件被视为"可能未完成"，单位：秒
STALE_THRESHOLD_SECONDS = 600  # 10 分钟


@dataclass(frozen=True)
class DerivingFlag:
    """派生中标记。"""

    snapshot_date: date
    started_at: str  # ISO 8601 UTC 时间戳


def flag_path(store: DatasetStore, snapshot_date: date) -> Path:
    """标记文件路径。"""
    return store.state_dir / f"deriving-{snapshot_date.isoformat()}.flag"


def mark_deriving(store: DatasetStore, snapshot_date: date) -> None:
    """标记开始派生某一天。"""
    flag = DerivingFlag(
        snapshot_date=snapshot_date,
        started_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    content = json.dumps(
        {"snapshot_date": flag.snapshot_date.isoformat(), "started_at": flag.started_at},
        ensure_ascii=False,
    )
    write_text(flag_path(store, snapshot_date), content)


def clear_deriving(store: DatasetStore, snapshot_date: date) -> None:
    """清除派生标记（派生完成）。"""
    flag_path(store, snapshot_date).unlink(missing_ok=True)


def check_and_recover(store: DatasetStore) -> list[date]:
    """检查所有派生标记，恢复未完成的派生。返回已恢复的日期列表。"""
    if not store.state_dir.exists():
        return []

    recovered: list[date] = []
    now = datetime.now(UTC)

    for path in store.state_dir.glob("deriving-*.flag"):
        flag = _load_flag(path)
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
        if elapsed > STALE_THRESHOLD_SECONDS:
            # 超过阈值：先清掉残留的派生层，再视 Raw 是否还在决定是否重派生。
            _clean_derived_partitions(store, flag.snapshot_date)
            path.unlink(missing_ok=True)
            if _has_raw(store, flag.snapshot_date):
                _rederive(store, flag.snapshot_date)
            recovered.append(flag.snapshot_date)

    return recovered


def _load_flag(path: Path) -> DerivingFlag | None:
    """加载标记文件；损坏返回 None。"""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        snapshot_date = date.fromisoformat(payload["snapshot_date"])
        started_at = payload["started_at"]
        return DerivingFlag(snapshot_date=snapshot_date, started_at=started_at)
    except (json.JSONDecodeError, KeyError, ValueError, OSError):
        return None


def _clean_derived_partitions(store: DatasetStore, snapshot_date: date) -> None:
    """删除某一天的所有派生层分区（bronze/silver/observations）。

    不删 current（它是全局的、不按日分区）；清理后由 `_rederive` 依据 Raw
    重建，Raw 已不存在时 current 保持原状。
    """
    for name in ["bronze", "silver", "observations"]:
        partition = store.path(name, snapshot_date)
        partition.unlink(missing_ok=True)


def _has_raw(store: DatasetStore, snapshot_date: date) -> bool:
    """该日是否还有可重建派生层的 Raw。

    只看 Bronze 消费的 `part` / `top`：只剩卡片批次时重派生不出任何标量数据，
    却会触发 `bronze.prune` 删掉旧分区，反而制造不一致。
    """
    from .raw import bronze_files

    return bool(bronze_files(store, snapshot_date))


def _rederive(store: DatasetStore, snapshot_date: date) -> None:
    """清理后真正重派生：从该日起**后缀重放**，而不是只重算这一天。

    只重算一天会把 `current` 覆盖成那一天的值：`state._rebuild_current` 用当日帧
    替换同名 id 的全部标量列与 `last_seen_at`。若崩溃那天不是最新 Raw 日，
    「自愈」的结果是当前态整体回退——比不修更坏。后缀重放（见
    `rebuild.replay_suffix`）保证最后一天是最新日，`current` 收敛到正确值。

    `derive_day()` 会在开头重新打标记；这里吞掉异常，让新标记在下次启动时
    再次触发恢复。一个坏日期不应阻塞其余写命令。
    """
    from .rebuild import rebuild_from_raw, replay_suffix

    try:
        rebuild_from_raw(store, days=replay_suffix(store, snapshot_date))
    except Exception:
        # 新标记已由 derive_day 开头写入，保留它等下一次超时重试。
        pass
