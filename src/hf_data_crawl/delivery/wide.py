"""默认交付底库导出：把 `gold.datasets` 视图按字段契约裁成一份可重建的 parquet。

定位
----
这是**只读投影的物化快照**，不是事实源，也不是状态表。它同时是「默认交付
字段」的物化：`fields.DEFAULT_COLUMNS` 是唯一契约，导出物与查询交付都按它投列，
`doctor` 勾稽两者不漂移。视图本身仍保留全部 72 列供内部查询用。

- 不登记进 `tables.TABLES`，不参与 `state.update` / `rebuild` / `compact` 的状态机；
- 随时可删、随时可由 `export-wide` 重建，删它不影响任何派生；
- 口径唯一来源是 `gold.datasets` 视图，这里绝不重新拼一次 join。

为什么不扩 current
------------------
`current` 只有 19 列，但语义字段早已在 `silver` / `carddata` / `size` 里，
`gold.datasets` 视图已把它们 LEFT JOIN 好（当前 65 列，全库 112 万行）。
把它们搬进 `current` 有三处代价：

1. 每轮 `state.update` 都要重写 112 万行全量表；
2. 每轮实际只扫约 22 万行，其余沿用 previous，而 `_align_frame` 对旧表缺列
   一律**补 null 而非重算**，于是约九成行的新字段会长期为空，直到各自被扫到
   ——与 2026-11-04 `private` 整列静默变空同类；
3. `silver` 本就是「同 id 取最新一条即当前属性」，再存一份就出现双真相源。

单独导出则状态表保持轻，分析者拿到单文件宽表，两者兼得。

安全边界
--------
`GoldCatalog` 的连接关闭了外部访问，只留数据目录这一处例外（视图要读
parquet），且不再对外暴露连接本身：导出只经 `copy_wide()` 这一条受控通道
流式 COPY，不必把 112 万行读进内存；`copy_wide()` 与这里各验一次目标确实
在数据目录内：万一将来有人放宽连接配置，`--out` 也不该写到别处。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from ..platform.store import DatasetStore
from ..query.gold import GoldCatalog, wide_column_names
from .fields import DEFAULT_COLUMNS, FALLBACKS, contract_errors, projection_sql

#: 默认产物路径，相对 `store.root`。放在 `exports/` 而非登记表管理的目录，
#: 是为了让「它不是一张表」在目录结构上一眼可见。
DEFAULT_RELATIVE = Path("exports") / "datasets.parquet"


class WideExportMismatch(RuntimeError):
    """导出结果不满足契约（视图列、静态清单、行数三者对不上），已撤销临时文件。"""


@dataclass(frozen=True)
class WideExportSummary:
    """一次导出的结果，供运行摘要使用。"""

    path: Path
    rows: int
    columns: int
    bytes: int


def default_target(store: DatasetStore) -> Path:
    return store.root / DEFAULT_RELATIVE


def _assert_inside(root: Path, target: Path) -> None:
    """目标必须落在数据目录内；用 resolve 消掉 `..` 与符号链接再判。"""
    if not target.resolve().is_relative_to(root.resolve()):
        raise ValueError(f"导出目标必须在数据目录内：{target}（数据目录 {root}）")


def _current_rows(store: DatasetStore) -> int:
    """基准行数。scan + count 只读 parquet 元数据，不把整表读进内存。"""
    return int(pl.scan_parquet(store.path("current")).select(pl.len()).collect().item())


def export_wide(store: DatasetStore, target: Path | None = None) -> WideExportSummary:
    """按默认交付字段契约，把 `datasets` 视图流式写成 parquet。

    投的是契约列而非 `SELECT *`：这份文件是给人当**默认参考底库**用的，不是
    管道全景。视图仍保留全部 72 列供内部查询，两者不必相同。

    先在同目录写 `.tmp`，再做四项校验（视图/静态清单、契约自洽、导出列集、
    行数），任一不符就抛 `WideExportMismatch` 并在 `finally` 里撤销——绝不让
    半截或错口径的文件顶替上一版。校验在替换**之前**做，是这里最关键的顺序。
    """
    destination = Path(target) if target is not None else default_target(store)
    _assert_inside(store.root, destination)

    catalog = GoldCatalog(store)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".tmp")
    temp.unlink(missing_ok=True)

    try:
        expected = wide_column_names()
        view_columns = tuple(catalog.run("SELECT * FROM datasets LIMIT 0").columns)
        if view_columns != expected:
            raise WideExportMismatch(
                f"视图实际列与静态清单不一致（{len(view_columns)} vs {len(expected)}），"
                "多半是改了 _wide_view_sql 却没同步 wide_columns()"
            )

        problems = contract_errors()
        if problems:
            raise WideExportMismatch(f"默认交付字段契约不自洽：{'；'.join(problems)}")
        needed = (*DEFAULT_COLUMNS, *FALLBACKS.values())
        outside = [column for column in needed if column not in view_columns]
        if outside:
            raise WideExportMismatch(f"契约列或卡片兜底列在视图里找不到：{outside}")

        catalog.copy_wide(temp, projection=projection_sql())

        frame = pl.scan_parquet(temp)
        actual = tuple(frame.collect_schema().names())
        if actual != DEFAULT_COLUMNS:
            raise WideExportMismatch(
                f"导出文件列与默认交付契约不一致：缺 {sorted(set(DEFAULT_COLUMNS) - set(actual))}，"
                f"多 {sorted(set(actual) - set(DEFAULT_COLUMNS))}"
            )

        rows = int(frame.select(pl.len()).collect().item())
        baseline = _current_rows(store)
        if rows != baseline:
            raise WideExportMismatch(
                f"导出行数 {rows} 与 current 基准 {baseline} 不等："
                "附表有重复 id，LEFT JOIN 把行放大了"
            )

        size = temp.stat().st_size
        os.replace(temp, destination)
        return WideExportSummary(destination, rows, len(actual), size)
    finally:
        temp.unlink(missing_ok=True)
        catalog.close()
