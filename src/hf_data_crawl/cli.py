"""命令行入口：collect / reconcile / build / query。

除 query（只读）外，每条命令都把运行摘要写入 runs/，失败也写并带原因。
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

from .api import MAX_LIMIT, HfDatasetApi, Transport
from .collect import Collector, CollectSummary
from .doctor import diagnose, sync_alert
from .enrich import Enricher, EnrichSummary
from .gold import BOARDS, GoldCatalog, GoldNotReady
from .lock import exclusive
from .rebuild import RebuildSummary, rebuild_from_raw
from .reconcile import Reconciler, ReconcileSummary
from .report import Reporter
from .runs import Outcome, read_runs, write_run
from .sizes import HfSizeApi, SizeFetcher, SizeSummary, tail
from .store import DatasetStore
from .transport import HttpxTransport

#: 会改写数据目录的命令，执行期间持有独占锁。report 只写自己的 HTML（原子替换），
#: query 只读，二者不需要。
WRITE_COMMANDS = frozenset({"collect", "reconcile", "build", "enrich", "fetch-sizes"})


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

    collect.add_argument(
        "--skip-if-done",
        action="store_true",
        help="该快照日已有一次完整采集就直接跳过（计划任务补跑用，避免同一天重复采）",
    )

    reconcile = subparsers.add_parser("reconcile", help="全量对账（按 createdAt 稳定序）")
    reconcile.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    reconcile.add_argument("--limit", type=int, default=MAX_LIMIT, help="每页条数，上限 1000")
    reconcile.add_argument(
        "--max-pages", type=int, default=None, help="本轮最多抓几页（未扫完不判下架）"
    )

    query = subparsers.add_parser("query", help="查询 Gold 视图或执行任意 SQL")
    query.add_argument("--board", default=None, choices=sorted(BOARDS), help="内置榜单")
    query.add_argument("--sql", default=None, help="任意只读 SQL")
    query.add_argument("--limit", type=int, default=50, help="榜单条数上限")

    subparsers.add_parser("build", help="从 Raw 重建全部派生层（不联网）")

    enrich = subparsers.add_parser("enrich", help="按下载量取头部，补抓 cardData")
    enrich.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    enrich.add_argument("--top", type=int, default=20_000, help="补抓前多少名")
    enrich.add_argument("--limit", type=int, default=MAX_LIMIT, help="每页条数，上限 1000")

    sizes = subparsers.add_parser("fetch-sizes", help="经 datasets-server 补规模与列数")
    sizes.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    sizes.add_argument("--top", type=int, default=20_000, help="按下载量取前多少名")
    sizes.add_argument("--workers", type=int, default=16, help="并发数，实测 16 稳定在 22 req/s")
    sizes.add_argument(
        "--retry-unavailable",
        action="store_true",
        help="重试上次标记为不可用的数据集（默认跳过，因为它们多为永久缺失）",
    )
    sizes.add_argument(
        "--flush-every",
        type=int,
        default=5000,
        help="每处理多少条就落一次库，中断时最多损失这一批",
    )

    subparsers.add_parser("doctor", help="体检：任务是否按时、Raw 是否断日、有无残留文件")

    report = subparsers.add_parser("report", help="生成静态榜单页（不联网）")
    report.add_argument("--out", default=None, help="输出路径，默认 data/reports/dashboard.html")
    report.add_argument("--top", type=int, default=20, help="每张榜单条数")

    return parser


def main(argv: list[str] | None = None, *, transport: Transport | None = None) -> int:
    """命令行入口。`transport` 是唯一的注入缝：测试传假实现，生产不传。"""
    args = build_parser().parse_args(argv)
    store = DatasetStore(Path(args.data_dir))

    if args.command == "collect":
        return _execute(store, "collect", lambda: _collect(args, store, transport))
    if args.command == "reconcile":
        return _execute(store, "reconcile", lambda: _reconcile(args, store, transport))
    if args.command == "build":
        return _execute(store, "build", lambda: _build(store))
    if args.command == "enrich":
        return _execute(store, "enrich", lambda: _enrich(args, store, transport))
    if args.command == "fetch-sizes":
        return _execute(store, "fetch-sizes", lambda: _fetch_sizes(args, store, transport))
    if args.command == "report":
        return _execute(store, "report", lambda: _report(args, store))
    if args.command == "query":
        return _query(args, store)
    if args.command == "doctor":
        return _doctor(store)

    return 1


def _execute(
    store: DatasetStore,
    command: str,
    action: Callable[[], Outcome],
) -> int:
    """执行一条命令并留下唯一一份运行记录。

    成败只在这里判定一次：异常与业务失败都收敛成 `Outcome`，然后走同一条
    `write_run`。此前异常路径写 failed、正常路径无条件写 success，于是
    「退出码 1 但记录成功」成为可能。
    """
    started = datetime.now()
    try:
        if command in WRITE_COMMANDS:
            # 写命令互斥：拿不到锁就如实失败并留下 runs 记录，而不是并发写坏 current。
            with exclusive(store.root, command):
                outcome = action()
        else:
            outcome = action()
    except Exception as error:
        reason = f"{type(error).__name__}: {error}"
        outcome = Outcome(payload={"error": reason}, code=1, reason=reason)
        print(f"{command} 失败：{reason}")

    write_run(store, command, started, outcome)
    if outcome.failed and outcome.reason:
        print(f"{command} 未完成：{outcome.reason}")
    return outcome.code


def _open_transport(transport: Transport | None) -> Transport:
    """联网命令取传输的唯一入口：没注入才构造真实的 httpx 实现。"""
    return transport if transport is not None else HttpxTransport()


def _snapshot(args: argparse.Namespace) -> date:
    """快照日期：给了就按 ISO 解析（写错抛 ValueError，由 `_execute` 记成失败），否则今天。"""
    return date.fromisoformat(args.date) if args.date else date.today()


def _collected_today(store: DatasetStore, snapshot: date) -> bool:
    """该快照日是否已有一次完整采集（走到末页或触发水位线，且在这一天内完成）。"""
    for item in read_runs(store):
        if item.get("command") != "collect" or item.get("status") != "success":
            continue
        payload = item.get("payload") or {}
        if payload.get("snapshot_date") != snapshot.isoformat():
            continue
        if payload.get("stopped_reason") in {"end", "watermark"}:
            return True
    return False


def _collect(args: argparse.Namespace, store: DatasetStore, transport: Transport | None) -> Outcome:
    snapshot = _snapshot(args)
    if args.skip_if_done and not args.dry_run and _collected_today(store, snapshot):
        print(f"{snapshot} 已完整采集过，跳过。")
        return Outcome({"snapshot_date": snapshot.isoformat(), "stopped_reason": "already_done"})
    collector = Collector(
        api=HfDatasetApi(transport=_open_transport(transport)),
        store=store,
        pages_without_new=args.pages_without_new,
    )
    summary = collector.run(
        snapshot,
        limit=args.limit,
        max_pages=args.max_pages,
        refresh_top=args.refresh_top,
        dry_run=args.dry_run,
    )
    _print_collect(summary)
    return Outcome(summary)


def _reconcile(
    args: argparse.Namespace, store: DatasetStore, transport: Transport | None
) -> Outcome:
    snapshot = _snapshot(args)
    reconciler = Reconciler(api=HfDatasetApi(transport=_open_transport(transport)), store=store)
    summary = reconciler.run(snapshot, limit=args.limit, max_pages=args.max_pages)
    _print_reconcile(summary)
    return Outcome(summary)


def _build(store: DatasetStore) -> Outcome:
    summary = rebuild_from_raw(store)
    _print_build(summary)
    return Outcome(summary)


def _enrich(args: argparse.Namespace, store: DatasetStore, transport: Transport | None) -> Outcome:
    snapshot = _snapshot(args)
    enricher = Enricher(api=HfDatasetApi(transport=_open_transport(transport)), store=store)
    summary = enricher.run(snapshot, top=args.top, limit=args.limit)
    _print_enrich(summary)
    return Outcome(summary)


def _fetch_sizes(
    args: argparse.Namespace, store: DatasetStore, transport: Transport | None
) -> Outcome:
    snapshot = _snapshot(args)
    current = store.read_current()
    if current.height == 0:
        print("尚无采集数据，请先运行 `hf-data-crawl collect`。")
        return Outcome({"error": "empty"}, code=1, reason="尚无采集数据，请先运行 collect")

    ordered = current.sort("downloads", descending=True, nulls_last=True)
    fetcher = SizeFetcher(
        api=HfSizeApi(transport=_open_transport(transport)),
        workers=args.workers,
        progress=_size_progress,
    )
    summary, frame = fetcher.run(
        snapshot,
        dataset_ids=ordered["id"].to_list(),
        top=max(1, args.top),
        retry_unavailable=args.retry_unavailable,
        existing=store.read_sizes(),
        # 边跑边落库：十万条要跑一小时，不能把一小时的成果押在最后一次写入上。
        flush=store.merge_sizes,
        flush_every=args.flush_every,
    )
    # 进度条走的是 \r 原地刷新，收尾必须换行，否则摘要会贴在同一行上。
    print()
    if frame.height:
        store.merge_sizes(frame)
    _print_sizes(summary, store.read_sizes().height)
    return Outcome(summary)


def _size_progress(done: int, total: int) -> None:
    """终端里原地刷新，非终端（日志、CI）按十等分打印。

    直接输出 `\\r` 会把日志冲成几百行甚至糊成一行——取决于接收方是否解释回车。
    进度是给人看的，先判断给谁看。
    """
    step = max(1, total // 10)
    if not sys.stdout.isatty():
        if done % step and done != total:
            return
        print(f"  补齐规模 {done}/{total}（{tail(done, total)}%）", flush=True)
        return
    print(f"\r  补齐规模 {done}/{total}（{tail(done, total)}%）", end="", flush=True)


def _print_sizes(summary: SizeSummary, table_rows: int) -> None:
    print(
        f"{summary.snapshot_date} 规模补齐：候选 {summary.requested} 条"
        f" / 耗时 {summary.elapsed_seconds:.1f}s"
    )
    print(
        f"  成功 {summary.fetched} · 永久缺失 {summary.unavailable} · "
        f"临时失败 {summary.failed} · 已存跳过 {summary.skipped}"
    )
    print(f"  规模表累计 {table_rows} 行（临时失败不入库，下次自然重试）")


def _query(args: argparse.Namespace, store: DatasetStore) -> int:
    try:
        catalog = GoldCatalog(store)
    except GoldNotReady as error:
        print(error)
        return 1

    try:
        frame = (
            catalog.run(args.sql) if args.sql else catalog.board(args.board or "growth", args.limit)
        )
    finally:
        catalog.close()

    if frame.height == 0:
        print("查询无结果：可能尚无足够的观测点（增速榜需至少两天的数据）。")
        return 0

    print(frame)
    return 0


def _doctor(store: DatasetStore) -> int:
    """只读体检。有问题退出码为 1，并写 ALERT.txt；全部正常则删掉它。"""
    now = datetime.now()
    findings = diagnose(store, now)
    sync_alert(store, findings, now)
    for item in findings:
        print(f"{'[!]' if item.problem else '[ok]'} {item.text}")
    return 1 if any(item.problem for item in findings) else 0


def _report(args: argparse.Namespace, store: DatasetStore) -> Outcome:
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


def _print_collect(summary: CollectSummary) -> None:
    if summary.stopped_reason == "dry_run":
        print(
            f"dry-run：探得 {len(summary.preview or [])} 条样本，配额余量 {summary.rate_remaining}"
        )
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
    tag = "（续跑，页数为本轮，差异计数为整个会话）" if summary.resumed else ""
    if summary.discarded_stale_session:
        print("发现旧版本留下的对账会话（缺基线，无法还原），已丢弃并从头开始。")
    print(
        f"{summary.snapshot_date} 对账完成{tag}："
        f"{summary.pages} 页 / {summary.records} 条 / 耗时 {summary.elapsed_seconds:.1f}s"
    )
    print(
        f"  新增 {summary.new_ids} · 删除 {summary.disappeared} · "
        f"转私有 {summary.restricted} · 改名 {summary.renamed} · "
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
    if summary.card_rows or summary.config_rows:
        print(
            f"  卡片 {summary.card_rows} 行 · config 明细 {summary.config_rows} 行"
            f"（已按新规则从 raw 重算）"
        )


def _print_enrich(summary: EnrichSummary) -> None:
    print(
        f"{summary.snapshot_date} 卡片补齐：{summary.pages} 页 / {summary.records} 条"
        f" / 耗时 {summary.elapsed_seconds:.1f}s"
    )
    print(
        f"  含卡片 {summary.with_card} 条 · 其中带结构明细 {summary.with_structure} 条"
        f" · 卡片表累计 {summary.table_rows} 行 · config 明细 {summary.config_rows} 行"
    )
