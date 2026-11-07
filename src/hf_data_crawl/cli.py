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
from .deriving import check_and_recover
from .doctor import diagnose, sync_alert
from .enrich import Enricher, EnrichSummary
from .env import load_env_file
from .files import FileFetcher, FileSummary, HfFilesApi
from .gold import BOARDS, GoldCatalog, GoldNotReady, UnsafeQuery
from .lock import LockBusy, exclusive
from .rebuild import RebuildSummary, rebuild_from_raw
from .reconcile import Reconciler, ReconcileSummary
from .report import Reporter
from .retention import REPLAY_WINDOW_DAYS
from .runs import COLLECT_COMPLETE_REASONS, Outcome, read_runs, write_run
from .sizes import HfSizeApi, SizeFetcher, SizeSummary, tail
from .store import DatasetStore
from .transport import HttpxTransport

#: 会改写数据目录的命令，执行期间持有独占锁。query 只读且单次查询很短，
#: 不在此列；report/docs 不改数据，但要同时读多张表，与写命令并发会读到
#: 新旧混合的快照，因此也纳入锁集合。
WRITE_COMMANDS = frozenset(
    {
        "collect",
        "reconcile",
        "build",
        "enrich",
        "fetch-sizes",
        "fetch-files",
        "archive",
        "compact",
        "export-wide",
    }
)

#: 只读一批表、需要一致快照的生成命令。它们与写命令共用同一把独占锁——
#: 只有互斥才能保证读到的是某一刻的完整快照；Windows 的文件锁不提供共享模式。
SNAPSHOT_COMMANDS = frozenset({"report", "docs"})

#: 需要持锁的命令全集。
LOCKED_COMMANDS = WRITE_COMMANDS | SNAPSHOT_COMMANDS

#: 生成命令默认等锁时长（秒）。它们通常由计划任务或人工临时触发，撞上采集
#: 时干等一会儿比直接失败有用；等不到仍会如实报错。可用 `--lock-wait` 覆盖。
SNAPSHOT_LOCK_WAIT = 60.0


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
    parser.add_argument(
        "--lock-wait",
        type=float,
        default=None,
        help=(
            "拿不到数据目录锁时最多等几秒；默认写命令 0（立刻失败），"
            f"report/docs 等 {SNAPSHOT_LOCK_WAIT:g} 秒"
        ),
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

    build = subparsers.add_parser("build", help="重建派生层（默认只重放热区，不联网）")
    build.add_argument(
        "--full",
        action="store_true",
        help="清空全部派生层并按天重放 data/raw 里所有日期（冷区权威层已损坏时的逃生舱）",
    )

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

    files = subparsers.add_parser(
        "fetch-files", help="按下载量取头部，补文件清单汇总（只补 top N，不逐库全量）"
    )
    files.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    files.add_argument("--top", type=int, default=1_000, help="按下载量取前多少名（默认 1000）")
    files.add_argument(
        "--workers", type=int, default=8, help="并发数，tree 每页虽轻仍是外网请求，保守取 8"
    )
    files.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="单库请求超时秒数；tree 实测 0.4-0.7s，超时即记临时失败",
    )
    files.add_argument(
        "--max-retries",
        type=int,
        default=1,
        help="单库重试次数；429 按上游 Retry-After 退避，默认只试一次",
    )
    files.add_argument(
        "--max-pages",
        type=int,
        default=1,
        help="每个库最多取几页 tree（每页 1000 条）；默认 1 页，未取完会标 truncated",
    )
    files.add_argument(
        "--retry-unavailable",
        action="store_true",
        help="重试上次标记为不可用的数据集（默认跳过）",
    )
    files.add_argument(
        "--refresh-truncated",
        action="store_true",
        help="重抓上次标了截断的库（须配合 --max-pages>1，否则只是重新确认截断）",
    )
    files.add_argument(
        "--flush-every",
        type=int,
        default=100,
        help="每处理多少条就落一次库，中断时最多损失这一批",
    )

    subparsers.add_parser("doctor", help="体检：任务是否按时、Raw 是否断日、有无残留文件")

    archive = subparsers.add_parser(
        "archive", help="归档早于 N 天的 Raw 分区：合并小分块并去重，原文件移入 backup/raw"
    )
    archive.add_argument(
        "--older-than-days", type=int, default=30, help="只归档早于这么多天的日期（默认 30）"
    )
    archive.add_argument("--date", default=None, help="只归档指定日期 YYYY-MM-DD（忽略天数门槛）")

    compact = subparsers.add_parser(
        "compact",
        help="压缩冷区 Raw：跨天去重成一份基线，原分块移入 backup/raw 不删除",
    )
    compact.add_argument(
        "--older-than-days",
        type=int,
        default=REPLAY_WINDOW_DAYS,
        help=f"只压缩早于这么多天的日期（默认 {REPLAY_WINDOW_DAYS}，与 build 热区窗口一致）",
    )
    compact.add_argument("--date", default=None, help="只压缩指定日期 YYYY-MM-DD（忽略天数门槛）")

    report = subparsers.add_parser("report", help="生成静态榜单页（不联网）")
    report.add_argument("--out", default=None, help="输出路径，默认 data/reports/dashboard.html")
    report.add_argument("--top", type=int, default=20, help="每张榜单条数")

    docs = subparsers.add_parser("docs", help="生成 data 目录说明（结构·文件·字段含义）")
    docs.add_argument("--out", default=None, help="输出路径，默认 docs/DATA.md")

    wide = subparsers.add_parser(
        "export-wide",
        help="把 gold.datasets 宽视图导出成单文件 parquet（可重建，不参与状态更新）",
    )
    wide.add_argument(
        "--out", default=None, help="输出路径，默认 data/exports/datasets.parquet（须在数据目录内）"
    )

    return parser


def main(argv: list[str] | None = None, *, transport: Transport | None = None) -> int:
    """命令行入口。`transport` 是唯一的注入缝：测试传假实现，生产不传。"""
    args = build_parser().parse_args(argv)
    store = DatasetStore(Path(args.data_dir))
    # 未显式给 --lock-wait 时按命令取默认值：写命令立即失败，生成命令先等一会儿。
    lock_wait = args.lock_wait
    if lock_wait is None:
        lock_wait = SNAPSHOT_LOCK_WAIT if args.command in SNAPSHOT_COMMANDS else 0.0

    # 启动时自动检查和恢复未完成的派生
    # 只有写命令才做：恢复本身会改派生层，只读快照命令（report/docs）不应顺手写数据。
    if args.command in WRITE_COMMANDS:
        recovered = check_and_recover(store)
        if recovered:
            print(f"⚠️  检测到 {len(recovered)} 个未完成的派生，已自动恢复：{recovered}")
            print("   提示：残留派生层已清理；有 Raw 的日期已重新派生，无 Raw 的仅清理")

    if args.command == "collect":
        return _execute(store, "collect", lambda: _collect(args, store, transport), lock_wait)
    if args.command == "reconcile":
        return _execute(store, "reconcile", lambda: _reconcile(args, store, transport), lock_wait)
    if args.command == "build":
        return _execute(store, "build", lambda: _build(args, store), lock_wait)
    if args.command == "archive":
        return _execute(store, "archive", lambda: _archive(args, store), lock_wait)
    if args.command == "compact":
        return _execute(store, "compact", lambda: _compact(args, store), lock_wait)
    if args.command == "enrich":
        return _execute(store, "enrich", lambda: _enrich(args, store, transport), lock_wait)
    if args.command == "fetch-sizes":
        return _execute(
            store, "fetch-sizes", lambda: _fetch_sizes(args, store, transport), lock_wait
        )
    if args.command == "fetch-files":
        return _execute(
            store, "fetch-files", lambda: _fetch_files(args, store, transport), lock_wait
        )
    if args.command == "report":
        return _execute(store, "report", lambda: _report(args, store), lock_wait)
    if args.command == "export-wide":
        return _execute(store, "export-wide", lambda: _export_wide(args, store), lock_wait)
    if args.command == "query":
        return _query(args, store)
    if args.command == "doctor":
        return _doctor(store)
    if args.command == "docs":
        # docs 不写 runs/，但同样要在一致快照上生成；拿不到锁就明确失败。
        try:
            with exclusive(store.root, "docs", lock_wait):
                return _write_docs(args, store)
        except LockBusy as error:
            print(f"docs 失败：{type(error).__name__}: {error}")
            return 1

    return 1


def _execute(
    store: DatasetStore,
    command: str,
    action: Callable[[], Outcome],
    lock_wait: float = 0.0,
) -> int:
    """执行一条命令并留下唯一一份运行记录。

    成败只在这里判定一次：异常与业务失败都收敛成 `Outcome`，然后走同一条
    `write_run`。此前异常路径写 failed、正常路径无条件写 success，于是
    「退出码 1 但记录成功」成为可能。
    """
    started = datetime.now()
    try:
        if command in LOCKED_COMMANDS:
            # 写命令互斥，生成命令则等一个完整快照：拿不到锁就如实失败并留下
            # runs 记录，而不是并发读到新旧混合的表。
            with exclusive(store.root, command, lock_wait):
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
    """联网命令取传输的唯一入口：没注入才读本机凭据并构造真实的 httpx 实现。

    本机凭据（`HF_TOKEN` / `HF_PROXY`）只在**真要联网**时加载，不在 `main`
    开头统一读盘：测试注入假传输时便不会碰磁盘上的 `.env`，本机令牌与代理
    也就不会污染用例。
    """
    if transport is not None:
        return transport
    load_env_file()
    return HttpxTransport()


def _files_transport(transport: Transport | None, timeout: float) -> Transport:
    """`fetch-files` 需要自定义超时，故单独留一个同样先加载凭据的入口。"""
    if transport is not None:
        return transport
    load_env_file()
    return HttpxTransport(timeout=timeout)


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
        if payload.get("stopped_reason") in COLLECT_COMPLETE_REASONS:
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


def _build(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    summary = rebuild_from_raw(store, window_days=None if args.full else REPLAY_WINDOW_DAYS)
    _print_build(summary)
    return Outcome(summary)


def _archive(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """归档符合条件的日期。某天验证失败就记为失败，但不影响已完成的其他日期。"""
    from datetime import timedelta

    from .archive import ArchiveMismatch, archive_day
    from .rebuild import discover_raw_dates

    if args.date:
        days = [date.fromisoformat(args.date)]
    else:
        cutoff = date.today() - timedelta(days=args.older_than_days)
        days = [day for day in discover_raw_dates(store) if day < cutoff]

    done: list[dict[str, object]] = []
    failures: list[str] = []
    for day in days:
        try:
            result = archive_day(store, day)
        except ArchiveMismatch as error:
            failures.append(str(error))
            print(f"  [!] {error}")
            continue
        if result is None:
            continue
        done.append(
            {
                "date": day.isoformat(),
                "files": f"{result.source_files}→{result.shards}",
                "records": f"{result.records_before}→{result.records_after}",
                "bytes": f"{result.bytes_before}→{result.bytes_after}",
            }
        )
        print(
            f"{day} 归档：{result.source_files} 个文件→{result.shards} 个分片，"
            f"记录 {result.records_before}→{result.records_after}，"
            f"{result.bytes_before / 1e6:.1f}MB→{result.bytes_after / 1e6:.1f}MB，"
            f"原文件在 {result.backup_dir}"
        )
    if not days:
        print("没有符合条件的日期。")
    payload = {"archived": done, "failures": failures}
    if failures:
        return Outcome(payload, code=1, reason=f"{len(failures)} 个日期归档验证失败")
    return Outcome(payload)


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
        progress=_progress_line,
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


def _fetch_files(
    args: argparse.Namespace, store: DatasetStore, transport: Transport | None
) -> Outcome:
    snapshot = _snapshot(args)
    if args.refresh_truncated and args.max_pages <= 1:
        # 页数还是一页时重抓，只会把「截断」原样重新确认一遍，白烧请求与配额。
        message = "--refresh-truncated 需配合 --max-pages>1；否则只是重新确认截断，白烧请求"
        print(f"失败：{message}")
        return Outcome({"error": "max-pages"}, code=1, reason=message)

    current = store.read_current()
    if current.height == 0:
        print("尚无采集数据，请先运行 `hf-data-crawl collect`。")
        return Outcome({"error": "empty"}, code=1, reason="尚无采集数据，请先运行 collect")

    ordered = current.sort("downloads", descending=True, nulls_last=True)
    fetcher = FileFetcher(
        api=HfFilesApi(
            transport=_files_transport(transport, args.timeout),
            max_retries=args.max_retries,
            max_pages=args.max_pages,
        ),
        workers=args.workers,
        progress=lambda done, total: _progress_line(done, total, "补齐文件清单"),
    )
    summary, frame = fetcher.run(
        snapshot,
        dataset_ids=ordered["id"].to_list(),
        top=max(1, args.top),
        retry_unavailable=args.retry_unavailable,
        refresh_truncated=args.refresh_truncated,
        existing=store.read_files(),
        flush=store.merge_files,
        flush_every=args.flush_every,
    )
    print()
    if frame.height:
        store.merge_files(frame)
    _print_files(summary, store.read_files().height)
    return Outcome(summary)


def _progress_line(done: int, total: int, label: str = "补齐规模") -> None:
    """终端里原地刷新，非终端（日志、CI）按十等分打印。

    直接输出 `\\r` 会把日志冲成几百行甚至糊成一行——取决于接收方是否解释回车。
    进度是给人看的，先判断给谁看。
    """
    step = max(1, total // 10)
    if not sys.stdout.isatty():
        if done % step and done != total:
            return
        print(f"  {label} {done}/{total}（{tail(done, total)}%）", flush=True)
        return
    print(f"\r  {label} {done}/{total}（{tail(done, total)}%）", end="", flush=True)


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


def _print_files(summary: FileSummary, table_rows: int) -> None:
    print(
        f"{summary.snapshot_date} 文件清单补齐：候选 {summary.requested} 条"
        f" / 耗时 {summary.elapsed_seconds:.1f}s"
        f" / 下载 {summary.downloaded_bytes / 1e6:.1f}MB"
    )
    print(
        f"  成功 {summary.fetched}（其中刷新截断 {summary.refreshed}） · "
        f"永久缺失 {summary.unavailable} · "
        f"临时失败 {summary.failed} · 已存跳过 {summary.skipped}"
    )
    print(f"  汇总表累计 {table_rows} 行（临时失败不入库，下次自然重试）")


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
    except UnsafeQuery as error:
        print(error)
        return 1
    finally:
        catalog.close()

    if frame.height == 0:
        print("查询无结果：可能尚无足够的观测点（增速榜需至少两天的数据）。")
        return 0

    print(frame)
    return 0


def _write_docs(args: argparse.Namespace, store: DatasetStore) -> int:
    """生成 data 目录说明。纯本地动作，不联网。

    缺字段说明时退出码为 1：文档不完整等于没写，应当在生成那一刻就看见。
    """
    from .datadoc import missing_docs, write

    target = Path(args.out) if args.out else Path("docs") / "DATA.md"
    path = write(store.root, target)
    gaps = missing_docs()
    print(f"数据说明已生成：{path}")
    if gaps:
        preview = "、".join(f"{name}.{column}" for name, column in gaps[:6])
        print(f"  [!] {len(gaps)} 个字段缺说明：{preview}{' …' if len(gaps) > 6 else ''}")
        print("      补在 src/hf_data_crawl/fielddocs.py 里")
        return 1
    return 0


def _doctor(store: DatasetStore) -> int:
    """只读体检。有问题退出码为 1，并写 ALERT.txt；全部正常则删掉它。"""
    now = datetime.now()
    findings = diagnose(store, now)
    sync_alert(store, findings, now)
    for item in findings:
        print(f"{'[!]' if item.problem else '[ok]'} {item.text}")
    return 1 if any(item.problem for item in findings) else 0


def _export_wide(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """导出宽表。失败由 `_execute` 统一记进 runs/，不留半截文件。"""
    from .wide import export_wide

    target = Path(args.out) if args.out else None
    summary = export_wide(store, target)
    print(
        f"宽表已导出：{summary.path}（{summary.columns} 列 · "
        f"{summary.rows:,} 行 · {summary.bytes / 1e6:.1f}MB）"
    )
    print("  可重建产物：不参与状态更新，删掉后重跑本命令即可")
    return Outcome(summary)


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


def _compact(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """压缩冷区日期。一次合并全部目标日期与已有基线，避免逐日重写基线。"""
    from datetime import timedelta

    from .compact import CompactMismatch, compact
    from .rebuild import discover_raw_dates

    if args.date:
        days = [date.fromisoformat(args.date)]
    else:
        cutoff = date.today() - timedelta(days=args.older_than_days)
        days = [day for day in discover_raw_dates(store) if day < cutoff]
    if not days:
        print("没有符合条件的冷区日期。")
        return Outcome({"compacted": [], "failures": []})

    try:
        result = compact(store, days)
    except CompactMismatch as error:
        print(f"  [!] {error}")
        return Outcome({"compacted": [], "failures": [str(error)]}, code=1, reason=str(error))
    if result is None:
        print("这些日期在 data/raw 里没有分块，无需压缩。")
        return Outcome({"compacted": [], "failures": []})

    print(
        f"冷区压缩完成：{len(result.dates)} 个日期 · {result.source_files} 个分块"
        f"→ {result.baselines} 个基线分片，记录 "
        f"{result.records_before}→{result.records_after}，"
        f"{result.bytes_before / 1e6:.1f}MB→{result.bytes_after / 1e6:.1f}MB"
    )
    print("原分块在 data/backup/raw/dt=<日期>/，未删除；回滚见 docs/operations/RECOVERY.md")
    return Outcome(
        {
            "dates": [day.isoformat() for day in result.dates],
            "files": f"{result.source_files}→{result.baselines}",
            "records": f"{result.records_before}→{result.records_after}",
            "bytes": f"{result.bytes_before}→{result.bytes_after}",
            "backup_files": result.backup_files,
        }
    )


def _print_build(summary: RebuildSummary) -> None:
    if not summary.dates:
        print("raw 下没有任何分区，无可重建。")
        return

    mode = {
        "window": f"热区重放（近 {REPLAY_WINDOW_DAYS} 天，冷区权威层保留）",
        "full": "全量重放（显式 --full）",
        "recovery": "全量重放（冷区权威层缺失，自动回退）",
    }[summary.mode]
    print(
        f"重建完成：{len(summary.dates)} 个日期"
        f"（{summary.dates[0]} ~ {summary.dates[-1]}），全程未联网 · {mode}"
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
