"""命令行入口：collect / reconcile / build / query。

除 query（只读）外，每条命令都把运行摘要写入 runs/，失败也写并带原因。
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path
from typing import Any

from .api import MAX_LIMIT, HfDatasetApi
from .collect import CollectSummary, Collector
from .gold import BOARDS, GoldCatalog, GoldNotReady
from .rebuild import RebuildSummary, rebuild_from_raw
from .reconcile import ReconcileSummary, Reconciler
from .runs import write_run
from .store import DatasetStore
from .transport import HttpxTransport


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hf-data-crawl",
        description="Hugging Face 数据集元信息底座",
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("HF_DATA_DIR", "data"),
        help="数据根目录（默认取 HF_DATA_DIR，否则 ./data）",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    collect = subparsers.add_parser("collect", help="日增量采集")
    collect.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    collect.add_argument("--limit", type=int, default=MAX_LIMIT, help="每页条数，上限 1000")
    collect.add_argument("--max-pages", type=int, default=None, help="本轮最多抓几页（有界试跑）")
    collect.add_argument(
        "--pages-without-new",
        type=int,
        default=3,
        help="连续多少页无新记录即判定到达水位线",
    )
    collect.add_argument("--dry-run", action="store_true", help="只探一页并打印字段，不落盘")
    collect.add_argument(
        "--refresh-top",
        type=int,
        default=0,
        help="额外按下载量补扫前 N 名（0 表示不补），用于让增速榜每天有新读数",
    )

    reconcile = subparsers.add_parser("reconcile", help="全量对账（按 createdAt 稳定序）")
    reconcile.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    reconcile.add_argument("--limit", type=int, default=MAX_LIMIT, help="每页条数，上限 1000")
    reconcile.add_argument("--max-pages", type=int, default=None, help="本轮最多抓几页（未扫完不判下架）")

    query = subparsers.add_parser("query", help="查询 Gold 视图或执行任意 SQL")
    query.add_argument("--board", default=None, choices=sorted(BOARDS), help="内置榜单")
    query.add_argument("--sql", default=None, help="任意只读 SQL")
    query.add_argument("--limit", type=int, default=50, help="榜单条数上限")

    subparsers.add_parser("build", help="从 Raw 重建全部派生层（不联网）")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = DatasetStore(Path(args.data_dir))

    if args.command == "collect":
        return _execute(store, "collect", lambda: _collect(args, store))
    if args.command == "reconcile":
        return _execute(store, "reconcile", lambda: _reconcile(args, store))
    if args.command == "build":
        return _execute(store, "build", lambda: _build(store))
    if args.command == "query":
        return _query(args, store)

    return 1


def _execute(
    store: DatasetStore,
    command: str,
    action: Callable[[], tuple[int, Any]],
) -> int:
    started = datetime.now()
    try:
        code, payload = action()
    except Exception as error:
        write_run(
            store,
            command,
            started,
            "failed",
            {"error": f"{type(error).__name__}: {error}"},
        )
        print(f"{command} 失败：{type(error).__name__}: {error}")
        return 1

    write_run(store, command, started, "success", payload)
    return code


def _collect(args: argparse.Namespace, store: DatasetStore) -> tuple[int, Any]:
    snapshot = date.fromisoformat(args.date) if args.date else date.today()
    collector = Collector(
        api=HfDatasetApi(transport=HttpxTransport()),
        store=store,
        pages_without_new=args.pages_without_new,
    )
    summary = collector.run(
        snapshot,
        limit=args.limit,
        max_pages=args.max_pages,
        pages_without_new=args.pages_without_new,
        refresh_top=args.refresh_top,
        dry_run=args.dry_run,
    )
    _print_collect(summary)
    return 0, summary


def _reconcile(args: argparse.Namespace, store: DatasetStore) -> tuple[int, Any]:
    snapshot = date.fromisoformat(args.date) if args.date else date.today()
    reconciler = Reconciler(api=HfDatasetApi(transport=HttpxTransport()), store=store)
    summary = reconciler.run(snapshot, limit=args.limit, max_pages=args.max_pages)
    _print_reconcile(summary)
    return 0, summary


def _build(store: DatasetStore) -> tuple[int, Any]:
    summary = rebuild_from_raw(store)
    _print_build(summary)
    return 0, summary


def _query(args: argparse.Namespace, store: DatasetStore) -> int:
    try:
        catalog = GoldCatalog(store)
    except GoldNotReady as error:
        print(error)
        return 1

    try:
        frame = (
            catalog.run(args.sql)
            if args.sql
            else catalog.board(args.board or "growth", args.limit)
        )
    finally:
        catalog.close()

    if frame.height == 0:
        print("查询无结果：可能尚无足够的观测点（增速榜需至少两天的数据）。")
        return 0

    print(frame)
    return 0


def _print_collect(summary: CollectSummary) -> None:
    if summary.stopped_reason == "dry_run":
        print(f"dry-run：探得 {len(summary.preview or [])} 条样本，配额余量 {summary.rate_remaining}")
        for record in summary.preview or []:
            print(f"  {record.get('id')}  字段={sorted(record.keys())}")
        return

    print(
        f"{summary.snapshot_date} 采集完成："
        f"{summary.pages} 页 / {summary.records} 条 / 耗时 {summary.elapsed_seconds:.1f}s"
    )
    print(
        f"  新增 {summary.new_ids} · 跳过坏记录 {summary.skipped} · "
        f"bronze {summary.bronze_rows} 行 · silver {summary.silver_rows} 行 · "
        f"观测 +{summary.appended_observations} · current {summary.current_rows} 行"
    )
    print(f"  配额余量 {summary.rate_remaining} · 停止原因 {summary.stopped_reason}")
    if summary.top_pages:
        print(f"  顶部池刷新 {summary.top_pages} 页 / {summary.refreshed} 条")


def _print_reconcile(summary: ReconcileSummary) -> None:
    print(
        f"{summary.snapshot_date} 对账完成："
        f"{summary.pages} 页 / {summary.records} 条 / 耗时 {summary.elapsed_seconds:.1f}s"
    )
    print(
        f"  新增 {summary.new_ids} · 下架 {summary.disappeared} · "
        f"sha 变更 {summary.sha_changed} · 恢复 {summary.reactivated}"
    )
    print(f"  差异清单 {summary.report_path} · 停止原因 {summary.stopped_reason}")


def _print_build(summary: RebuildSummary) -> None:
    if not summary.dates:
        print("raw 下没有任何分区，无可重建。")
        return

    print(
        f"重建完成：{len(summary.dates)} 个日期"
        f"（{summary.dates[0]} ~ {summary.dates[-1]}），全程未联网"
    )
    print(
        f"  bronze {summary.bronze_rows} 行 · silver {summary.silver_rows} 行 · "
        f"观测 {summary.observations} 行 · current {summary.current_rows} 行"
    )
