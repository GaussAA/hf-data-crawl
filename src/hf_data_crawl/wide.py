"""宽表导出：把 `gold.datasets` 视图物化成一份可重建的 parquet。

定位
----
这是**只读投影的物化快照**，不是事实源，也不是状态表：

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
parquet）。导出借此流式 COPY，不必把 112 万行读进内存；这里再验一次目标
确实在数据目录内：万一将来有人放宽连接配置，`--out` 也不该写到别处。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from .gold import GoldCatalog, wide_column_names
from .store import DatasetStore

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
    """把 `datasets` 视图流式写成 parquet，校验通过后才原子替换目标。

    先在同目录写 `.tmp`，再对临时文件做三项校验，任一不符就抛
    `WideExportMismatch` 并在 `finally` 里撤销——绝不让半截或错口径的文件
    顶替上一版。校验在替换**之前**做，是这里最关键的顺序。
    """
    destination = Path(target) if target is not None else default_target(store)
    _assert_inside(store.root, destination)

    catalog = GoldCatalog(store)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".tmp")
    temp.unlink(missing_ok=True)

    try:
        connection = catalog.connection
        expected = wide_column_names()
        view_columns = tuple(
            row[0] for row in connection.execute("DESCRIBE SELECT * FROM datasets").fetchall()
        )
        if view_columns != expected:
            raise WideExportMismatch(
                f"视图实际列与静态清单不一致（{len(view_columns)} vs {len(expected)}），"
                "多半是改了 _wide_view_sql 却没同步 wide_columns()"
            )

        escaped = temp.resolve().as_posix().replace("'", "''")
        connection.execute(f"COPY (SELECT * FROM datasets) TO '{escaped}' (FORMAT PARQUET)")

        frame = pl.scan_parquet(temp)
        actual = tuple(frame.collect_schema().names())
        if actual != expected:
            raise WideExportMismatch(
                f"导出文件列与视图不一致：缺 {sorted(set(expected) - set(actual))}，"
                f"多 {sorted(set(actual) - set(expected))}"
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
