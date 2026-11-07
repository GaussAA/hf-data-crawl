"""推导一天：把某天的 Raw 依次派生成 Bronze、Silver、卡片表与运行态。

四步有硬性的先后依赖：Silver 读 Bronze，运行态读 Silver。顺序写错不会报错，
只会读到空表、静默返回 0——所以「按什么顺序、跑哪几步」只在这里写一次，
`collect` / `reconcile` / `rebuild` 与测试 fixture 都调它，而不是各抄一遍。

原子性保护
----------
派生过程中如果崩溃，可能导致部分文件已更新、部分未更新的不一致状态。
通过"派生中标记文件"机制检测和恢复：
- 开始派生前写标记文件
- 派生完成后删除标记文件
- 启动时检查标记文件，恢复未完成的派生
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from . import bronze, card, silver, state
from .deriving import clear_deriving, mark_deriving

if TYPE_CHECKING:
    from .store import DatasetStore


@dataclass(frozen=True)
class DayResult:
    """推导一天的结果。"""

    bronze_rows: int
    silver_rows: int
    state: state.StateUpdate
    #: 未跑卡片这一步时为 None，而不是用 0 冒充「跑了但没有」。
    card_rows: int | None = None
    config_rows: int | None = None


def derive_day(store: DatasetStore, snapshot_date: date, *, enrich: bool = False) -> DayResult:
    """按固定顺序推导一天：bronze → silver →（enrich 时）card → state。

    空日不提前返回：每一步读到空表自会返回 0，而 `state.update` 在空 Silver 时
    仍会给出现有的当前态行数，调用方的摘要要用到它。

    原子性保护：开始前标记、完成后清除。如果中途崩溃，下次启动时
    `check_and_recover()` 会检测到标记文件并清理该日的派生层。
    """
    # 标记开始派生
    mark_deriving(store, snapshot_date)

    try:
        bronze_rows = bronze.build(store, snapshot_date)
        silver_rows = silver.build(store, snapshot_date)
        card_rows: int | None = None
        config_rows: int | None = None
        if enrich:
            card_rows, config_rows = card.build(store, snapshot_date)
        update = state.update(store, snapshot_date)
        # 运行态已经读完当天的 Bronze，旧分区不再有读者，且随时能从 Raw 重建。
        bronze.prune(store)

        result = DayResult(
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            state=update,
            card_rows=card_rows,
            config_rows=config_rows,
        )

        # 派生成功，清除标记
        clear_deriving(store, snapshot_date)
        return result
    except Exception:
        # 派生失败，保留标记（供下次启动恢复）
        raise
