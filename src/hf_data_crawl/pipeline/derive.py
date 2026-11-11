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

派生凭证
--------
成功之后落一条戳记（见 `lineage`），记下这次是照着哪一版 Raw 跑的。标记文件回答
「派生是不是跑到一半崩了」，戳记回答「跑完的那一次基于什么输入」——Raw 被重写后，
只有后者能证明派生有没有跟上。失败路径不写戳记，旧凭证自然与新的 Raw 指纹不符，
体检据此报出「派生滞后」。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from . import bronze, card, lineage, silver, state
from .deriving import clear_deriving, mark_deriving

if TYPE_CHECKING:
    from ..platform.store import DatasetStore


@dataclass(frozen=True)
class DayResult:
    """推导一天的结果。"""

    bronze_rows: int
    silver_rows: int
    state: state.StateUpdate
    #: 未跑卡片这一步时为 None，而不是用 0 冒充「跑了但没有」。
    card_rows: int | None = None
    config_rows: int | None = None


@dataclass(frozen=True)
class ReplayCarry:
    """一次重放内跨日顺推的历史态。

    逐日重放时，每天都把全部 silver / observations / current 读一遍，代价随天数
    平方增长。重放日期升序，前一天的锚点就是第二天的输入，把两份锚点留在内存里
    顺推即可；变化判定仍然只看「严格早于该日的最后一行」，口径没有变化。
    """

    silver: silver.Latest
    state: state.Carry

    @classmethod
    def load(cls, store: DatasetStore, *, before: date) -> ReplayCarry:
        return cls(
            silver=silver.Latest.load(store, before=before),
            state=state.Carry.load(store, before=before),
        )


def derive_day(
    store: DatasetStore,
    snapshot_date: date,
    *,
    enrich: bool = False,
    carry: ReplayCarry | None = None,
) -> DayResult:
    """按固定顺序推导一天：bronze → silver →（enrich 时）card → state。

    空日不提前返回：每一步读到空表自会返回 0，而 `state.update` 在空 Silver 时
    仍会给出现有的当前态行数，调用方的摘要要用到它。

    `carry` 由重放循环传入时，silver 与 state 的历史锚点从上一日顺推；单日
    派生（`collect` / `reconcile`）不传，仍按该日重读一次。

    原子性保护：开始前标记、完成后清除。如果中途崩溃，下次启动时
    `check_and_recover()` 会检测到标记文件并清理该日的派生层。

    凭证：成功路径在 `clear_deriving` 之前写戳记。写入顺序很重要——戳记先落、
    标记后清，中途崩掉时戳记可能已经写了而派生标记还在，下次恢复会重派一遍并
    覆盖戳记；反过来（先清标记后写戳记）则会出现「没有标记、也没有凭证」的
    空档，体检看到的就是「这天从没派生过」，与「派生过但没记下」无法区分。
    """
    # 标记开始派生
    mark_deriving(store, snapshot_date)

    try:
        bronze_rows = bronze.build(store, snapshot_date)
        # 当日帧只读一次，交给 silver 与 state 共用：两次调用会把补抓批次
        # （11-07 有 1,350 个卡片文件）完整解压两遍，而两处要的是同一份帧。
        today = bronze.day_frame(store, snapshot_date)
        silver_rows = silver.build(
            store,
            snapshot_date,
            frame=today,
            latest_state=carry.silver if carry is not None else None,
        )
        card_rows: int | None = None
        config_rows: int | None = None
        if enrich:
            card_rows, config_rows = card.build(store, snapshot_date)
        update = state.update(
            store,
            snapshot_date,
            frame=today,
            carry=carry.state if carry is not None else None,
        )
        lineage.stamp(
            store,
            snapshot_date,
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            observations=store.read("observations", snapshot_date).height,
        )
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
