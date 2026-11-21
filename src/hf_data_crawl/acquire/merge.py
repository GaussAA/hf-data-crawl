"""富化表的收尾合并：读残留分片 → 保护旧值 → 并入正式表 → 清分片。

四条抓取命令（`fetch-sizes` / `fetch-readme` / `fetch-files` / `fetch-preview`）的
收尾原先各写一遍，形状完全相同，差别只有四处：

| 命令 | 保护旧值 | 写入方式 | 锁名 |
|---|---|---|---|
| `fetch-sizes` | 挡 `unavailable` 与 `error` | `upsert`（整表读入） | `fetch-sizes:merge` |
| `fetch-readme` | 只挡 `error` | `merge_by_id`（流式） | `fetch-readme:merge` |
| `fetch-files` | 无 | `upsert` | `fetch-files:merge` |
| `fetch-preview` | 无 | `upsert` | `fetch-preview:merge` |

而「保护旧值」的那两份函数是逐字相同的复制品——只有「哪些状态算降级」不同。
改一份不会让另一份报错，只会让它们悄悄分叉：这正是本仓库反复付学费的那类缺陷。

这里只放**形状**：`MergeSpec` 声明一段收尾怎么做，`merge_staged` 照做。各表
具体的声明留在各自的模块里（它们知道自己的分片目录与 schema）。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

import polars as pl

from ..platform.lock import exclusive
from ..platform.tables import PolarsType
from .batch import clear_staged, read_staged

if TYPE_CHECKING:
    from ..platform.store import DatasetStore


@dataclass(frozen=True)
class MergeSpec:
    """一段收尾合并的声明。

    `degraded` 是「这次没拿到结论」的状态集合：新行命中它们、而旧行是 `ok` 时，
    旧行原样保留。空集合表示这张表不做保护（`files` / `preview` 的 `unavailable`
    本身就带信息，且旧行不会因此丢字段）。

    `previous_columns` 只给大表用：`readme` 的判定只需 `id` 与 `status`，把正文
    （全量 2.8 GB）一起读进内存会在收尾时 OOM（2026-11-18 实测）。

    `streaming` 走 `store.merge_by_id`：不把旧表读进内存、不排序。只对单文件大表
    成立，分区表会被它直接拒绝。
    """

    table: str
    staging_dir: str
    schema: dict[str, PolarsType]
    lock: str
    degraded: frozenset[str] = frozenset()
    previous_columns: tuple[str, ...] | None = None
    streaming: bool = False


def keep_ok_rows(
    previous: pl.DataFrame, fresh: pl.DataFrame, *, degraded: frozenset[str]
) -> pl.DataFrame:
    """不让「这次没拿到结论」覆盖「已经拿到」的旧行。

    判据只看两件事：旧行是不是 `ok`、新行是不是落在 `degraded` 里。旧行从未成功
    过的照常被新结论替换（`unavailable` / `absent` / `denied` 都是真实结论，也是
    永久结论），已经拿到值的保持原样——`snapshot_date` 因此诚实地表示「这个值是
    哪一天取得的」，而不是「哪一天问过」。

    两张表此前各写一份，函数体逐字相同：

    - `sizes.protect_ok_rows` 挡 `status != "ok"`——一次瞬时 500（`/size` 用它表示
      「不支持」）会把 `num_rows` / `configs` 一起抹掉，此后该 id 被长期跳过；
    - `readme.keep_previous_ok` 只挡 `error`——`absent` / `denied` 是明确结论，
      可以覆盖（README 可能真被删了或转私有），而一次瞬时故障不该抹掉几 KB 正文。
    """
    if previous.height == 0 or fresh.height == 0 or not degraded:
        return fresh
    protected = previous.filter(pl.col("status") == "ok").select("id")
    if protected.height == 0:
        return fresh
    spoiled = fresh.filter(pl.col("status").is_in(sorted(degraded))).join(
        protected, on="id", how="semi"
    )
    if spoiled.height == 0:
        return fresh
    return fresh.join(spoiled.select("id"), on="id", how="anti")


def merge_staged(store: DatasetStore, spec: MergeSpec, lock_wait: float) -> int:
    """短锁内把本轮结果与残留分片一次并入正式表，返回并入的行数。

    短锁是这套设计的要害：长耗时抓取全程不持锁，只在收尾这几秒独占数据目录。
    抓不到锁就抛 `LockBusy`——分片原样留着，下一轮据此续抓，不丢已抓到的东西。

    分片**只在并入成功之后**才删：先删后并，中途失败就等于把这批结果扔了。
    """
    with exclusive(store.root, spec.lock, lock_wait):
        merged, paths = read_staged(store, spec.staging_dir, spec.schema)
        if merged.height:
            fresh = merged
            if spec.degraded:
                previous = store.read(spec.table, columns=spec.previous_columns)
                fresh = keep_ok_rows(previous, fresh, degraded=spec.degraded)
            if spec.streaming:
                store.merge_by_id(spec.table, fresh)
            else:
                store.upsert(spec.table, fresh)
        clear_staged(store, paths, spec.staging_dir)
    return merged.height
