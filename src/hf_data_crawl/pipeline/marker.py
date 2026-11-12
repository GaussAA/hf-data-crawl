"""派生原子性保护：标记文件原语（恢复流程见 `recovery.py`）。

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
在 `derive_day()` 开头写一个"派生中标记文件"（如 `state/marker-YYYY-MM-DD.marker`），
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

from ..platform.atomic import write_text

if TYPE_CHECKING:
    from ..platform.store import DatasetStore

#: 超过这个时长的标记文件被视为"可能未完成"，单位：秒
STALE_THRESHOLD_SECONDS = 600  # 10 分钟


@dataclass(frozen=True)
class Marker:
    """派生中标记。"""

    snapshot_date: date
    started_at: str  # ISO 8601 UTC 时间戳


def marker_path(store: DatasetStore, snapshot_date: date) -> Path:
    """标记文件路径。"""
    return store.state_dir / f"marker-{snapshot_date.isoformat()}.marker"


def write_marker(store: DatasetStore, snapshot_date: date) -> None:
    """标记开始派生某一天。"""
    marker = Marker(
        snapshot_date=snapshot_date,
        started_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )
    content = json.dumps(
        {"snapshot_date": marker.snapshot_date.isoformat(), "started_at": marker.started_at},
        ensure_ascii=False,
    )
    write_text(marker_path(store, snapshot_date), content)


def clear_marker(store: DatasetStore, snapshot_date: date) -> None:
    """清除派生标记（派生完成）。"""
    marker_path(store, snapshot_date).unlink(missing_ok=True)


def load_marker(path: Path) -> Marker | None:
    """加载标记文件；损坏返回 None。"""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        snapshot_date = date.fromisoformat(payload["snapshot_date"])
        started_at = payload["started_at"]
        return Marker(snapshot_date=snapshot_date, started_at=started_at)
    except (json.JSONDecodeError, KeyError, ValueError, OSError):
        return None


def clear_day(store: DatasetStore, snapshot_date: date) -> None:
    """删除某一天的所有派生层分区（`tables.DERIVED_DAY_TABLES`）。

    不删 current（它是全局的、不按日分区）；清理后由 `_rederive` 依据 Raw
    重建，Raw 已不存在时 current 保持原状。层清单只在 `tables` 登记一处，
    新增派生层时不会漏掉这里。
    """
    from ..platform.tables import DERIVED_DAY_TABLES

    for name in DERIVED_DAY_TABLES:
        store.path(name, snapshot_date).unlink(missing_ok=True)


def has_raw(store: DatasetStore, snapshot_date: date) -> bool:
    """该日是否还有可重放的 Raw（`RawDay.replayable`）。

    包含仅 card 补抓批次的日子：这类日子会通过 `bronze.day_frame` 的补位进入
    current 与 observations，恢复时跳过它才会制造不一致。空目录、只剩冷区清单
    的日子仍不算——主目录没有分块，重放不出任何东西。
    """
    from .raw import describe_day

    return describe_day(store, snapshot_date).replayable
