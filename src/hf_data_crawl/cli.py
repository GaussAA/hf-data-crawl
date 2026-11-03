"""命令行入口。

子命令按切片逐步接入：collect（S4）→ reconcile（S5）→ build（S6）→ query（S7）。
"""

from __future__ import annotations

import argparse
import os
from datetime import date
from pathlib import Path

from .api import MAX_LIMIT, HfDatasetApi
from .collect import CollectSummary, Collector
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

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    store = DatasetStore(Path(args.data_dir))

    if args.command == "collect":
        return _run_collect(args, store)

    return 1


def _run_collect(args: argparse.Namespace, store: DatasetStore) -> int:
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
        dry_run=args.dry_run,
    )
    _print_summary(summary)
    return 0


def _print_summary(summary: CollectSummary) -> None:
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
