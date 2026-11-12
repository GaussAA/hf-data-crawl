"""`query` 切片的命令处理函数，参数解析与分派在 ops/cli。"""

from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from ..platform.runs import Outcome
from ..platform.store import DatasetStore
from .gold import GoldCatalog, GoldNotReady, UnsafeQuery
from .report import Reporter


def run_query(args: argparse.Namespace, store: DatasetStore) -> int:
    try:
        catalog = GoldCatalog(store)
    except GoldNotReady as error:
        print(error)
        return 1

    try:
        frame = (
            catalog.run(args.sql) if args.sql else catalog.board(args.board or "growth", args.limit)
        )
    except UnsafeQuery as error:
        print(error)
        return 1
    finally:
        catalog.close()

    if frame.height == 0:
        print("查询无结果：可能尚无足够的观测点（增速榜需至少两天的数据）。")
        return 0

    print(frame)
    _warn_frozen_rows(frame)
    return 0


def _warn_frozen_rows(frame: pl.DataFrame) -> None:
    """结果带 status 且有非在架行时，明说这些行的值已冻结。

    不改数据、不过滤——用不用这些行是判断，该由人来做。但不说一声，那个「下载量
    28 万」会被当成今天的读数，而它可能停在四天前；改名后的旧 id 更是永远不再更新。
    """
    from ..platform.vocab import INACTIVE

    if "status" not in frame.columns:
        return
    frozen = frame.filter(pl.col("status").is_in([status.value for status in INACTIVE]))
    if frozen.height == 0:
        return
    print(
        f"注意：其中 {frozen.height} 行状态不是 active（改名 / 下架 / 转私有），"
        "这些行的指标停在最后一次看到时的值，不是现势值。"
    )


def run_report(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    try:
        catalog = GoldCatalog(store)
    except GoldNotReady as error:
        print(error)
        return Outcome({"error": str(error)}, code=1, reason=str(error))

    try:
        target = Path(args.out) if args.out else store.root / "reports" / "dashboard.html"
        summary = Reporter(catalog).write(target, top=args.top)
    finally:
        catalog.close()

    print(
        f"榜单页已生成：{summary.path}"
        f"（{summary.bytes / 1024:.0f} KB · {summary.datasets:,} 个数据集）"
    )
    return Outcome(summary)
