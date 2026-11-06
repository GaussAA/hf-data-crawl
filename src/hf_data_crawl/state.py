"""运行态：观测流水、当前态，以及状态标记。

当天的 Bronze 是该日的标量快照（含指标）；这里把它折叠成两样东西：
- **观测流水**：指标变了才追加一行，不是每天一行；
- **当前态**：每个数据集一行，带首见 / 末见日期与状态。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from .status import Status
from .tables import BRONZE_SCHEMA, CURRENT_SCHEMA

if TYPE_CHECKING:
    from .store import DatasetStore

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


@dataclass(frozen=True)
class StateUpdate:
    """一次状态更新的结果，供运行摘要使用。"""

    appended: int
    current_rows: int


def mark_status(store: DatasetStore, updates: dict[str, str]) -> int:
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
    current = store.read_current()
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


def update(store: DatasetStore, snapshot_date: date) -> StateUpdate:
    """按当日 Bronze 更新观测流水与当前态。

    观测只在该日指标与「该日之前的最后一条观测」不同时追加；
    同日重跑先重建该日分区，故结果与一次跑完一致。
    """
    today_frame = store.read_bronze(snapshot_date)
    if today_frame.height == 0:
        return StateUpdate(appended=0, current_rows=store.read_current().height)

    existing = store.read_observations()
    before = existing.filter(pl.col("snapshot_date") == snapshot_date).height

    previous = (
        existing.filter(pl.col("snapshot_date") < snapshot_date)
        .sort("snapshot_date")
        .group_by("id", maintain_order=True)
        .agg([pl.col(column).last().alias(f"{column}_prev") for column in METRIC_COLUMNS])
    )
    joined = today_frame.select(["id", *METRIC_COLUMNS]).join(previous, on="id", how="left")

    changed = pl.col(f"{METRIC_COLUMNS[0]}_prev").is_null()
    for column in METRIC_COLUMNS:
        changed = changed | (pl.col(column).fill_null(-1) != pl.col(f"{column}_prev").fill_null(-1))

    new_observations = joined.filter(changed).select(
        [
            pl.col("id"),
            pl.lit(snapshot_date, dtype=pl.Date).alias("snapshot_date"),
            *[pl.col(column) for column in METRIC_COLUMNS],
        ]
    )

    if new_observations.height == 0:
        store.path("observations", snapshot_date).unlink(missing_ok=True)
    else:
        store.write("observations", new_observations, snapshot_date)

    current = _rebuild_current(store, snapshot_date, today_frame)
    store.write("current", current)

    return StateUpdate(
        appended=max(0, new_observations.height - before),
        current_rows=current.height,
    )


def _rebuild_current(
    store: DatasetStore, snapshot_date: date, bronze: pl.DataFrame
) -> pl.DataFrame:
    """勾稽当前态：本轮出现的取当日 Bronze，未出现的沿用原行，不擅自改状态。"""
    carried_columns = [*CARRIED_COLUMNS, "last_seen_at", "status"]
    previous = store.read_current()
    today_ids = bronze["id"].to_list()
    carried = previous.filter(~pl.col("id").is_in(today_ids)).select(carried_columns)

    today = bronze.select(list(CARRIED_COLUMNS)).with_columns(
        pl.lit(snapshot_date, dtype=pl.Date).alias("last_seen_at"),
        pl.lit(Status.ACTIVE.value).alias("status"),
    )

    first_seen = (
        store.read_observations()
        .group_by("id")
        .agg(pl.col("snapshot_date").min().alias("first_seen_at"))
    )

    combined = pl.concat(
        [carried.select(carried_columns), today.select(carried_columns)],
        how="vertical_relaxed",
    )
    return (
        combined.join(first_seen, on="id", how="left")
        .with_columns(pl.col("first_seen_at").fill_null(pl.lit(snapshot_date, dtype=pl.Date)))
        .select(list(CURRENT_SCHEMA))
        .sort("id")
    )
