"""运行态：观测流水、当前态，以及状态标记。

当天的 Bronze 是该日的标量快照（含指标）；这里把它折叠成两样东西：
- **观测流水**：指标变了才追加一行，不是每天一行；
- **当前态**：每个数据集一行，带首见 / 末见日期与状态。
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from ..platform.tables import BRONZE_SCHEMA, CURRENT_SCHEMA
from ..platform.vocab import Status
from . import bronze, state

if TYPE_CHECKING:
    from ..platform.store import DatasetStore

#: current 中由当天 Bronze 直接带过来、不需要额外计算的列。
#:
#: 由 schema 求交而非手抄：往 Bronze 加一个标量列、同时在 CURRENT_SCHEMA 里声明，
#: 这里自动跟上。手抄的版本会漏——而漏掉的表现是「视图里查得到、物化表里是
#: null」，不报错，只是同一数据集在两个视图里的字段完整度不同。
#:
#: 取 Bronze 而非 Silver：Silver 只记属性变更、且不含指标，当天的完整标量状态
#: 只有 Bronze 有。
CARRIED_COLUMNS: tuple[str, ...] = tuple(
    column for column in BRONZE_SCHEMA if column in CURRENT_SCHEMA
)

#: current 里由本地计算、不来自 Bronze 的列。
COMPUTED_COLUMNS: frozenset[str] = frozenset({"first_seen_at", "last_seen_at", "status"})

METRIC_COLUMNS: tuple[str, ...] = ("downloads", "downloads_all_time", "likes")


def read_current(store: DatasetStore) -> pl.DataFrame:
    """读 current 表，旧表的 status 缺失时补 active（沿用历史语义）。"""
    frame = store.read("current")
    if frame.height > 0 and frame["status"].null_count():
        frame = frame.with_columns(pl.col("status").fill_null(Status.ACTIVE.value))
    return frame


def _previous_metrics(history: pl.DataFrame) -> pl.DataFrame:
    """按日取每个 id 的最后一条观测，作为次日的变化锚点。"""
    return (
        history.sort("snapshot_date")
        .group_by("id", maintain_order=True)
        .agg([pl.col(column).last().alias(f"{column}_prev") for column in METRIC_COLUMNS])
    )


@dataclass
class Carry:
    """重放期间顺推的运行态：每 id 的指标锚点 + 当前态。

    `update` 原本每次都要读全部观测流水（求锚点）和全部当前态。全量重放 N 天
    等于 N 次全表扫描，且观测流水本身随天数增长，总代价是 O(N²)。同一轮重放的
    日期升序，锚点只在当日有新观测时被替换，因此在内存里顺推即可，判定口径不变。

    调用方只在「已清掉重放日分区」的重放循环里使用它（`rebuild` 即如此），
    这样当日已有的观测行数为 0，`appended` 与逐日重读的计数一致。
    """

    previous: pl.DataFrame
    current: pl.DataFrame

    @classmethod
    def load(cls, store: DatasetStore, *, before: date) -> Carry:
        history = store.read_all("observations")
        return cls(
            previous=_previous_metrics(history.filter(pl.col("snapshot_date") < before)),
            current=state.read_current(store),
        )

    def advance(self, observations: pl.DataFrame) -> None:
        """用当日新观测替换同名 id 的锚点；没有新观测的日子锚点不动。"""
        if observations.height == 0:
            return
        fresh = observations.select(
            "id",
            *[pl.col(column).alias(f"{column}_prev") for column in METRIC_COLUMNS],
        )
        self.previous = pl.concat(
            [self.previous.join(fresh.select("id"), on="id", how="anti"), fresh],
            how="vertical_relaxed",
        )


@dataclass(frozen=True)
class StateUpdate:
    """一次状态更新的结果，供运行摘要使用。"""

    appended: int
    current_rows: int


def mark_status(store: DatasetStore, updates: Mapping[str, str]) -> int:
    """按 id 批量改状态，返回实际发生变化的行数。只改状态，不动观测史。

    状态不再只有「在架/下架」两极：`restricted` 表示转为私有、`renamed` 表示
    改名（原 id 失效但库还在）。把它们一律记成 disappeared，会让下架数虚高，
    也让改名后的库在下轮变成「新增」而丢掉原有观测史。

    取值必须在 `Status` 词表内，未知值直接报错而不是静默落库。
    """
    if not updates:
        return 0
    known = {status.value for status in Status}
    unknown = sorted({value for value in updates.values() if value not in known})
    if unknown:
        raise ValueError(f"未知的状态值: {unknown}，可选：{sorted(known)}")
    current = state.read_current(store)
    if current.height == 0:
        return 0

    patch = pl.DataFrame(
        {"id": list(updates), "next_status": list(updates.values())},
        schema={"id": pl.Utf8, "next_status": pl.Utf8},
    )
    updated = (
        current.join(patch, on="id", how="left")
        .with_columns(pl.coalesce(["next_status", "status"]).alias("status"))
        .drop("next_status")
    )
    changed = int((updated["status"] != current["status"]).sum())
    if changed:
        store.write("current", updated)
    return changed


def update(
    store: DatasetStore,
    snapshot_date: date,
    *,
    frame: pl.DataFrame | None = None,
    carry: Carry | None = None,
) -> StateUpdate:
    """按当日帧更新观测流水与当前态。

    观测只在该日指标与「该日之前的最后一条观测」不同时追加；
    同日重跑先重建该日分区，故结果与一次跑完一致。

    当日帧取自 `bronze.day_frame`：Bronze 分区加上只在补抓批次里出现的库。补位
    放在读时而不写进 Bronze，协议见该函数；`silver` 用的是同一份帧，属性、当前态、
    观测三者才不会各说各话。`frame` 由 `derive_day` 传入时直接复用，省掉第二次读。

    重放循环传入 `carry` 时，指标锚点与当前态从上一日顺推，不再每天重读全部
    流水；此时重放日的旧分区已被清除，`before` 恒为 0。
    """
    today_frame = frame if frame is not None else bronze.day_frame(store, snapshot_date)
    if today_frame.height == 0:
        current_rows = (
            carry.current.height if carry is not None else state.read_current(store).height
        )
        return StateUpdate(appended=0, current_rows=current_rows)

    if carry is not None:
        before = 0
        previous = carry.previous
        prior_current = carry.current
    else:
        existing = store.read_all("observations")
        before = existing.filter(pl.col("snapshot_date") == snapshot_date).height
        previous = _previous_metrics(existing.filter(pl.col("snapshot_date") < snapshot_date))
        prior_current = None

    joined = today_frame.select(["id", "url", *METRIC_COLUMNS]).join(previous, on="id", how="left")

    changed = pl.col(f"{METRIC_COLUMNS[0]}_prev").is_null()
    for column in METRIC_COLUMNS:
        changed = changed | (pl.col(column).fill_null(-1) != pl.col(f"{column}_prev").fill_null(-1))

    new_observations = joined.filter(changed).select(
        [
            pl.col("id"),
            pl.col("url"),
            pl.lit(snapshot_date, dtype=pl.Date).alias("snapshot_date"),
            *[pl.col(column) for column in METRIC_COLUMNS],
        ]
    )

    if new_observations.height == 0:
        store.path("observations", snapshot_date).unlink(missing_ok=True)
    else:
        store.write("observations", new_observations, snapshot_date)

    current = _rebuild_current(store, snapshot_date, today_frame, prior=prior_current)
    store.write("current", current)
    if carry is not None:
        carry.advance(new_observations)
        carry.current = current

    return StateUpdate(
        appended=max(0, new_observations.height - before),
        current_rows=current.height,
    )


def _rebuild_current(
    store: DatasetStore,
    snapshot_date: date,
    bronze: pl.DataFrame,
    *,
    prior: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """勾稽当前态：本轮出现的取当日 Bronze，未出现的沿用原行，不擅自改状态。

    `first_seen_at` 直接沿用当前态，而不是每天重扫全部观测流水：一个 id 的第一条
    观测必定落在它首次被看见的那天，所以 current 里记下的首见日就是流水的 min；
    只有当日新出现的 id 才需要补当日日期。`prior` 由重放循环传入时省掉读盘。
    """
    carried_columns = [*CARRIED_COLUMNS, "last_seen_at", "status"]
    previous = prior if prior is not None else state.read_current(store)
    today_ids = bronze["id"].to_list()
    carried = previous.filter(~pl.col("id").is_in(today_ids)).select(carried_columns)

    today = bronze.select(list(CARRIED_COLUMNS)).with_columns(
        pl.lit(snapshot_date, dtype=pl.Date).alias("last_seen_at"),
        pl.lit(Status.ACTIVE.value).alias("status"),
    )

    first_seen = previous.select("id", pl.col("first_seen_at").alias("__first_seen"))

    combined = pl.concat(
        [carried.select(carried_columns), today.select(carried_columns)],
        how="vertical_relaxed",
    )
    return (
        combined.join(first_seen, on="id", how="left")
        .with_columns(
            pl.col("__first_seen")
            .fill_null(pl.lit(snapshot_date, dtype=pl.Date))
            .alias("first_seen_at")
        )
        .select(list(CURRENT_SCHEMA))
        .sort("id")
    )
