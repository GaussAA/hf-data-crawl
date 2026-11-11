"""命令行入口：collect / reconcile / build / query。

除 query（只读）外，每条命令都把运行摘要写入 runs/，失败也写并带原因。
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from collections.abc import Callable
from datetime import date, datetime
from pathlib import Path

import polars as pl

from . import compare, specdiff
from .acquire.collect import Collector, CollectSummary
from .acquire.enrich import CARD_PAGES_CAP, Enricher, EnrichSummary
from .acquire.files import FileFetcher, FileSummary, HfFilesApi
from .acquire.reconcile import PROBE_DAILY_LIMIT, Reconciler, ReconcileSummary
from .acquire.sizes import (
    HfSizeApi,
    SizeFetcher,
    SizeSummary,
    StagingWriter,
    clear_staged,
    read_staged,
    tail,
)
from .backfill import FillError, header_change, header_of
from .delivery import DeliveryBlocked, DeliveryResult, Preview, deliver, preview, run_due
from .doctor import diagnose, sync_alert
from .health import record as record_health
from .hf.api import MAX_LIMIT, HfDatasetApi, Transport
from .hf.transport import HttpxTransport
from .method import METHOD_DIR, METHOD_DOC, METHOD_INDEX, problems
from .method import sync as sync_method
from .metrics import lookup
from .pipeline import state
from .pipeline.deriving import check_and_recover
from .pipeline.lineage import restamp
from .pipeline.rebuild import RebuildSummary, divergent_since, rebuild_from_raw, replay_suffix
from .platform.env import load_env_file
from .platform.lock import LockBusy, exclusive
from .platform.retention import BACKUP_PRUNE_AFTER_DAYS, REPLAY_WINDOW_DAYS
from .platform.runs import COLLECT_COMPLETE_REASONS, Outcome, read_runs, write_run
from .platform.store import DatasetStore
from .query.gold import BOARDS, GoldCatalog, GoldNotReady, UnsafeQuery
from .query.report import Reporter
from .requirement import (
    DELIVERIES_DIR,
    HISTORY_DIR,
    INPUT_DIR,
    MANIFEST_NAME,
    SPEC_NAME,
    Cadence,
    Kind,
    Mode,
    Spec,
    SpecError,
    compile_sql,
    directory_name,
    history_versions,
    resolve_dir,
    skeleton,
    spec_digest,
    version_conflict,
)
from .requirement import load as load_spec

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
        "prune",
        "export-wide",
    }
)

#: 只读一批表、需要一致快照的生成命令。它们与写命令共用同一把独占锁——
#: 只有互斥才能保证读到的是某一刻的完整快照；Windows 的文件锁不提供共享模式。
SNAPSHOT_COMMANDS = frozenset({"report", "docs"})

#: 需要持锁的命令全集。
LOCKED_COMMANDS = WRITE_COMMANDS | SNAPSHOT_COMMANDS

#: 自己安排锁的命令：长耗时的网络抓取在锁外做，只把「并入正式表」包在短锁里。
#: 它们仍在 WRITE_COMMANDS 里（启动检查/恢复未完成的派生照旧），但不走
#: `_execute` 的整段锁。原因见 `sizes.StagingWriter`：全量抓取要跑三小时，
#: 整段持锁会直接让当天的 collect 等锁失败（2026-11-07 实际事故）。
SELF_LOCKED_COMMANDS = frozenset({"fetch-sizes"})

#: 生成命令默认等锁时长（秒）。它们通常由计划任务或人工临时触发，撞上采集
#: 时干等一会儿比直接失败有用；等不到仍会如实报错。可用 `--lock-wait` 覆盖。
SNAPSHOT_LOCK_WAIT = 60.0

#: 体检等锁时长（秒）。体检是人工排障入口，撞上写命令时宁可降级为「非快照」
#: 也不要卡住；等不到锁会照常体检，并把跨表检查降级为提示。
DOCTOR_LOCK_WAIT = 2.0


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
        "--requests-dir",
        default=os.environ.get("HF_REQUESTS_DIR", "requests"),
        help="需求目录（默认取 HF_REQUESTS_DIR，否则 ./requests）；与数据目录分开",
    )
    parser.add_argument(
        "--lock-wait",
        type=float,
        default=None,
        help=(
            "拿不到数据目录锁时最多等几秒；默认写命令 0（立刻失败），"
            f"report/docs 等 {SNAPSHOT_LOCK_WAIT:g} 秒，doctor 等 {DOCTOR_LOCK_WAIT:g} 秒后"
            "降级为非快照"
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
        "--cards",
        action="store_true",
        help="随日增量补抓卡片：同一批「今天变过的库」再扫一趟，多带 cardData（约 +17 分钟）",
    )
    collect.add_argument(
        "--card-pages",
        type=int,
        default=None,
        help="变化集卡片扫描的页数上限，默认 200；防止水位线异常时把全站拉完",
    )

    collect.add_argument(
        "--skip-if-done",
        action="store_true",
        help="该快照日已有一次完整采集就直接跳过（计划任务补跑用，避免同一天重复采）",
    )

    request = subparsers.add_parser("request", help="需求与交付管理：需求定义、口径、交付档案")
    request_sub = request.add_subparsers(dest="request_action", required=True)
    request_new = request_sub.add_parser("new", help="新建一份需求，生成带注释的 spec 骨架")
    request_new.add_argument("id", help="需求号，形如 REQ-0001")
    request_new.add_argument("--title", required=True, help="一句话说明要什么")
    request_new.add_argument("--requester", required=True, help="谁提的——交付单要能回答「给谁」")
    request_new.add_argument("--purpose", default="", help="拿去做什么，会影响口径取舍")
    request_new.add_argument(
        "--cadence",
        default=Cadence.ONCE.value,
        choices=[member.value for member in Cadence],
        help="交付节奏：once 只跑一次 / daily / weekly / monthly / manual 只等人跑",
    )
    request_new.add_argument(
        "--kind",
        default=Kind.QUERY.value,
        choices=[member.value for member in Kind],
        help="交付形态：query 出数据表 / backfill 回填给定表格；形态只换口径体与执行体",
    )
    request_new.add_argument(
        "--source",
        default="",
        help="kind=backfill 必填：对方给的原始表格路径（必须已存在，且引擎绝不改动它）",
    )
    request_sub.add_parser("list", help="列出全部需求与交付概况")
    request_show = request_sub.add_parser("show", help="查看一份需求的口径、历史版本与交付台账")
    request_show.add_argument("id", help="需求号")
    request_run = request_sub.add_parser("run", help="按当前口径交付一次，写出三件套")
    request_run.add_argument("id", help="需求号")
    request_run.add_argument(
        "--force",
        action="store_true",
        help="一次性需求（once）交付过之后再交一份；不加则拒绝，防误重复交付",
    )
    request_run.add_argument(
        "--dry-run",
        action="store_true",
        help="只算不写：报行数与逐列覆盖，不建交付档案、不受护栏阻断",
    )
    request_sub.add_parser("run-due", help="跑所有到期的需求（计划任务用）")
    request_method = request_sub.add_parser(
        "method", help="校验过程记录 method/，并按目录实际内容重算脚本索引"
    )
    request_method.add_argument("id", help="需求号")
    request_method.add_argument(
        "--check",
        action="store_true",
        help="只判不写：列出不合规之处，有问题则退出码非零（CI / 交付前自检用）",
    )
    request_input = request_sub.add_parser(
        "input", help="收一版新输入表：按 input/v<n>/ 归档，并报出与当前表头的字段差异"
    )
    request_input.add_argument("id", help="需求号")
    request_input.add_argument("--add", required=True, metavar="文件", help="对方这一轮给的原始表")
    request_input.add_argument("--sheet", default=None, help="表名，不写沿用 spec 或取第一个")
    request_input.add_argument(
        "--header-row", type=int, default=None, help="列名所在行，默认沿用 spec"
    )
    request_diff = request_sub.add_parser("diff", help="比对两版口径的差异：默认最近两版")
    request_diff.add_argument("id", help="需求号")
    request_diff.add_argument(
        "old", nargs="?", default=None, metavar="旧版", help="如 v1 或 1；默认倒数第二版"
    )
    request_diff.add_argument(
        "new", nargs="?", default=None, metavar="新版", help="如 v2 或 2；默认最新一版"
    )
    request_compare = request_sub.add_parser("compare", help="逐列比对两次交付：默认最近两次")
    request_compare.add_argument("id", help="需求号")
    request_compare.add_argument(
        "left", nargs="?", default=None, metavar="旧时刻", help="交付目录名，默认倒数第二次"
    )
    request_compare.add_argument(
        "right", nargs="?", default=None, metavar="新时刻", help="交付目录名，默认最新一次"
    )

    reconcile = subparsers.add_parser("reconcile", help="全量对账（按 createdAt 稳定序）")
    reconcile.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    reconcile.add_argument("--limit", type=int, default=MAX_LIMIT, help="每页条数，上限 1000")
    reconcile.add_argument(
        "--max-pages", type=int, default=None, help="本轮最多抓几页（未扫完不判下架）"
    )
    reconcile.add_argument(
        "--probe-limit",
        type=int,
        default=PROBE_DAILY_LIMIT,
        help=f"本轮最多探测多少个「没扫到」的库，其余顺延（默认 {PROBE_DAILY_LIMIT}）",
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
    build.add_argument(
        "--stale",
        action="store_true",
        help=(
            "只重放「派生与 Raw 不符」的日期及其之后（按派生戳记判定）；"
            "Raw 被重写过而派生没跟上时用这个，比 --full 快得多"
        ),
    )

    enrich = subparsers.add_parser("enrich", help="按下载量取头部，补抓 cardData")
    enrich.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    enrich.add_argument("--top", type=int, default=20_000, help="补抓前多少名")
    enrich.add_argument(
        "--ids",
        default=None,
        help="按需补：只补这些 id（逗号分隔，上限 200）；给了它就不做批量扫描",
    )
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

    doctor = subparsers.add_parser("doctor", help="体检：任务是否按时、Raw 是否断日、有无残留文件")
    # 未显式给时沿用全局默认；SUPPRESS 避免把全局 --lock-wait 覆盖回 None。
    doctor.add_argument(
        "--lock-wait",
        type=float,
        default=argparse.SUPPRESS,
        help=f"等锁秒数（默认 {DOCTOR_LOCK_WAIT:g}）；超时降级为非快照，照常体检",
    )

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

    pruning = subparsers.add_parser(
        "prune",
        help="清理备份：把 backup/ 里超过保留期的分片真正删掉（只动 backup/）",
    )
    pruning.add_argument(
        "--dry-run",
        action="store_true",
        help=f"只算不删：列出会清理哪些、能释放多少（阈值 {BACKUP_PRUNE_AFTER_DAYS} 天）",
    )

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
    # 未显式给 --lock-wait 时按命令取默认值：写命令立即失败，生成命令先等一会儿；
    # 体检只等 2 秒，等不到就降级为非快照而不是阻塞排障。
    lock_wait = args.lock_wait
    if lock_wait is None:
        if args.command == "doctor":
            lock_wait = DOCTOR_LOCK_WAIT
        elif args.command in SNAPSHOT_COMMANDS:
            lock_wait = SNAPSHOT_LOCK_WAIT
        else:
            lock_wait = 0.0

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
    if args.command == "prune":
        return _execute(store, "prune", lambda: _prune(args, store), lock_wait)
    if args.command == "enrich":
        return _execute(store, "enrich", lambda: _enrich(args, store, transport), lock_wait)
    if args.command == "fetch-sizes":
        return _execute(
            store,
            "fetch-sizes",
            lambda: _fetch_sizes(args, store, transport, lock_wait),
            lock_wait,
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
    if args.command == "request":
        # new / list / show 只读写 requests/，不碰数据目录，不参与数据锁；
        # run 要读多张表出一个一致快照，与 report / docs 同规矩——先等一会儿锁。
        if args.request_action in {"run", "run-due"}:
            wait = SNAPSHOT_LOCK_WAIT if args.lock_wait is None else args.lock_wait
            try:
                with exclusive(store.root, "request-run", wait):
                    if args.request_action == "run":
                        return _request_run(args, store)
                    return _request_run_due(args, store)
            except LockBusy as error:
                print(f"交付失败：{error}")
                return 1
        return _request(args)
    if args.command == "doctor":
        return _doctor_locked(store, lock_wait, requests_root=Path(args.requests_dir))
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
        if command in LOCKED_COMMANDS and command not in SELF_LOCKED_COMMANDS:
            # 写命令互斥，生成命令则等一个完整快照：拿不到锁就如实失败并留下
            # runs 记录，而不是并发读到新旧混合的表。自己排锁的命令（fetch-sizes）
            # 由动作内部只锁真正写表的短段。
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
        cards=args.cards,
        card_max_pages=CARD_PAGES_CAP if args.card_pages is None else args.card_pages,
        dry_run=args.dry_run,
    )
    _print_collect(summary)
    return Outcome(summary)


def _reconcile(
    args: argparse.Namespace, store: DatasetStore, transport: Transport | None
) -> Outcome:
    snapshot = _snapshot(args)
    reconciler = Reconciler(
        api=HfDatasetApi(transport=_open_transport(transport)),
        store=store,
        probe_limit=args.probe_limit,
    )
    summary = reconciler.run(snapshot, limit=args.limit, max_pages=args.max_pages)
    _print_reconcile(summary)
    return Outcome(summary)


def _build(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    if args.full and args.stale:
        print("失败：--full 与 --stale 互斥；全量重放不需要挑日期。")
        return Outcome({"error": "flags"}, code=1, reason="--full 与 --stale 不能同时给")

    if args.stale:
        since = divergent_since(store)
        if since is None:
            print("派生层与 Raw 已对齐（逐日戳记一致），无需重建。")
            return Outcome({"snapshot_date": None, "mode": "already-aligned", "days": 0})
        days = replay_suffix(store, since)
        # 说清楚为什么不是只修那一天：后缀闭合是正确性要求，不是保守做法。
        print(f"检出派生滞后：自 {since} 起重放 {len(days)} 天（含其后所有有 Raw 的日子）")
        summary = rebuild_from_raw(store, days=days)
    else:
        summary = rebuild_from_raw(store, window_days=None if args.full else REPLAY_WINDOW_DAYS)
    _print_build(summary)
    return Outcome(summary)


def _archive(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """归档符合条件的日期。某天验证失败就记为失败，但不影响已完成的其他日期。"""
    from datetime import timedelta

    from .archive import ArchiveMismatch, archive_day
    from .pipeline.rebuild import discover_raw_dates

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
        # 归档在移走原件之前校验过「前后 Bronze 逐行相等」，派生结果仍然有效，
        # 只是 Raw 分块换了一批文件名与 mtime。不刷新指纹，下轮体检会把这次
        # 等价改写报成「派生滞后」——假警报比漏报更能训练人忽略告警。
        restamp(store, day)
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
    api = HfDatasetApi(transport=_open_transport(transport))

    if args.ids:
        # 按需补：只问点名的这几个库。全量扫一遍要十几小时，而实际被交付命中的
        # 常常只有几十个；逐条问一次，换到的是默认面那两列不再空着。
        from .pipeline.card import backfill

        ids = [item.strip() for item in args.ids.split(",") if item.strip()]
        if not ids:
            print("--ids 里没有有效的数据集 id。")
            return Outcome({"requested": 0, "rows": 0}, code=1, reason="空 id 列表")
        summary = backfill(store, ids, snapshot, fetch=api.record)
        print(
            f"按需补卡片：点名 {summary.requested} 个 · 取到 {summary.fetched} 个 · "
            f"有卡片 {summary.with_card} 个 · 写入 {summary.rows} 行"
        )
        if summary.with_card == 0:
            print("  这些库的卡片本身为空（多数长尾上传即如此），不是抓取失败。")
        return Outcome(
            {
                "requested": summary.requested,
                "fetched": summary.fetched,
                "with_card": summary.with_card,
                "rows": summary.rows,
            }
        )

    enricher = Enricher(api=api, store=store)
    summary_run = enricher.run(snapshot, top=args.top, limit=args.limit)
    _print_enrich(summary_run)
    return Outcome(summary_run)


def _fetch_sizes(
    args: argparse.Namespace,
    store: DatasetStore,
    transport: Transport | None,
    lock_wait: float = 0.0,
) -> Outcome:
    """锁外抓取、锁内合并。

    全量抓取要三小时，整段持锁会挡住当天的 collect（2026-11-07 实际事故）。
    现在只有两处短锁：收尾合并正式表时独占，合并本身是把新批 upsert 进
    `enrich/size.parquet`，与原来的逐批 `merge_sizes` 语义相同（同 id 以新值为准）。
    抓取期间的结果写进 `data/.staging/sizes/` 的原子分片，不碰任何正式表；
    中断后分片还在，下一轮据此续抓。
    """
    snapshot = _snapshot(args)
    current = state.read_current(store)
    if current.height == 0:
        print("尚无采集数据，请先运行 `hf-data-crawl collect`。")
        return Outcome({"error": "empty"}, code=1, reason="尚无采集数据，请先运行 collect")

    # 残留分片同样是「已处理」的 id：上一轮中断前抓到的结果不该再请求一遍。
    existing = store.read("size")
    staged, _ = read_staged(store)
    if staged.height:
        existing = pl.concat([existing, staged], how="vertical_relaxed")

    ordered = current.sort("downloads", descending=True, nulls_last=True)
    fetcher = SizeFetcher(
        api=HfSizeApi(transport=_open_transport(transport)),
        workers=args.workers,
        progress=_progress_line,
    )
    writer = StagingWriter(store, snapshot)
    summary, frame = fetcher.run(
        snapshot,
        dataset_ids=ordered["id"].to_list(),
        top=max(1, args.top),
        retry_unavailable=args.retry_unavailable,
        existing=existing,
        # 边跑边落暂存分片：中断时最多损失一个批次，但写入发生在锁外。
        flush=writer.write,
        flush_every=args.flush_every,
    )
    # `flush` 只在攒满一批时触发；不足一批的尾巴必须补写进分片，否则合并只读
    # 分片时会漏掉它们。`frame` 的行序与写入序一致，尾巴从已写行数处切。
    if frame.height > writer.flushed:
        writer.write(frame.slice(writer.flushed, frame.height - writer.flushed))
    # 进度条走的是 \r 原地刷新，收尾必须换行，否则摘要会贴在同一行上。
    print()

    # 短锁：只在这段独占数据目录，把本轮结果与残留分片一次并入正式表。
    # 抓不到锁就失败，分片原样留着，下一轮继续（不丢已抓到的数据）。
    with exclusive(store.root, "fetch-sizes:merge", lock_wait):
        merged, paths = read_staged(store)
        if merged.height:
            store.upsert("size", merged)
        clear_staged(store, paths)

    _print_sizes(summary, store.read("size").height)
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

    current = state.read_current(store)
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
        existing=store.read("files"),
        flush=lambda frame: store.upsert("files", frame),
        flush_every=args.flush_every,
    )
    print()
    if frame.height:
        store.upsert("files", frame)
    _print_files(summary, store.read("files").height)
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
    _warn_frozen_rows(frame)
    return 0


def _warn_frozen_rows(frame: pl.DataFrame) -> None:
    """结果带 status 且有非在架行时，明说这些行的值已冻结。

    不改数据、不过滤——用不用这些行是判断，该由人来做。但不说一声，那个「下载量
    28 万」会被当成今天的读数，而它可能停在四天前；改名后的旧 id 更是永远不再更新。
    """
    from .platform.vocab import INACTIVE

    if "status" not in frame.columns:
        return
    frozen = frame.filter(pl.col("status").is_in([status.value for status in INACTIVE]))
    if frozen.height == 0:
        return
    print(
        f"注意：其中 {frozen.height} 行状态不是 active（改名 / 下架 / 转私有），"
        "这些行的指标停在最后一次看到时的值，不是现势值。"
    )


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


def _doctor_locked(store: DatasetStore, wait: float, *, requests_root: Path | None = None) -> int:
    """体检优先在一致快照上跑；等不到写锁就降级，不阻塞人工排障。

    与 report/docs 不同：那两个命令拿不到锁直接失败，因为产出的文件必须基于
    一致快照；体检的价值在于「现在就告诉我大致哪里不对」，标注非快照比拒绝
    执行更有用。
    """
    try:
        with exclusive(store.root, "doctor", wait):
            return _doctor(store, snapshot=True, requests_root=requests_root)
    except LockBusy:
        return _doctor(store, snapshot=False, requests_root=requests_root)


def _doctor(store: DatasetStore, *, snapshot: bool, requests_root: Path | None = None) -> int:
    """体检：有问题退出码为 1，写 ALERT.txt，并把结论追加进体检流水。

    流水（`state/health-history.jsonl`）与 `runs/` 分工不同：`runs/` 记「命令成没
    成功」，而体检退出码 1 是「有告警」——记进 runs 会让「近 7 天无失败运行」失真。
    没有流水时，「断日从哪天开始」这类问题只能靠人回忆。

    `snapshot=False` 时跨表检查已由 `diagnose` 降级为提示，退出码与 `ALERT.txt`
    只反映不依赖一致快照的问题。
    """
    now = datetime.now()
    if snapshot:
        print("快照：一致（已持有写锁）")
    else:
        print("快照：不可用（写锁被占用，未取得锁）；跨表读数可能跨时点，跨表检查已降级")
    findings = diagnose(store, now, snapshot=snapshot, requests_root=requests_root)
    sync_alert(store, findings, now, snapshot=snapshot)
    try:
        record_health(store, findings, now, snapshot=snapshot)
    except OSError as error:
        # 记不下流水不该让体检本身变成失败：结论仍按 findings 判定。
        print(f"[!] 体检历史写入失败：{error}")
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
    if summary.card_pages:
        print(f"  变化集卡片 {summary.card_pages} 页 / {summary.card_records} 条")
    elif summary.card_reason == "no_watermark":
        print("  变化集卡片：无水位线（首轮），跳过——那是全量补齐，不是增量")


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
    if summary.probe_probed or summary.probe_deferred:
        tail = f" · 顺延 {summary.probe_deferred}（下一轮继续）" if summary.probe_deferred else ""
        failed = f"，失败 {summary.probe_failed}" if summary.probe_failed else ""
        print(f"  探测复核 {summary.probe_probed} 个{failed}{tail}")


def _compact(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """压缩冷区日期。一次合并全部目标日期与已有基线，避免逐日重写基线。"""
    from datetime import timedelta

    from .compact import CompactMismatch, compact
    from .pipeline.rebuild import discover_raw_dates

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


def _prune(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """清理备份。搬运与销毁分开：这里只销毁，且只碰 `backup/`。"""
    from .prune import apply, plan, total_size

    targets = plan(store)
    before = total_size(store)
    if not targets:
        print(f"备份没有需要清理的分区（阈值 {BACKUP_PRUNE_AFTER_DAYS} 天）：{before / 1e6:.1f} MB")
        return Outcome({"removed": [], "freed": 0, "backup_mb": round(before / 1e6, 1)})

    reclaimable = sum(item.bytes for item in targets)
    if args.dry_run:
        for item in targets:
            print(f"  将清理 {item.kind}/{item.path.name}  {item.bytes / 1e6:.1f} MB")
        print(
            f"预演：{len(targets)} 处可清理 · 释放 {reclaimable / 1e6:.1f} MB"
            f"（当前 backup {before / 1e6:.1f} MB，未动磁盘）"
        )
        return Outcome(
            {
                "removed": [],
                "freed": 0,
                "reclaimable": reclaimable,
                "dry_run": True,
            }
        )

    report = apply(store, targets)
    after = total_size(store)
    for item in report.removed:
        print(f"  已清理 {item.kind}/{item.path.name}  {item.bytes / 1e6:.1f} MB")
    print(
        f"清理完成：{report.count} 处 · 释放 {report.bytes_freed / 1e6:.1f} MB"
        f"（backup {before / 1e6:.1f} → {after / 1e6:.1f} MB）"
    )
    return Outcome(
        {
            "removed": [item.path.name for item in report.removed],
            "freed": report.bytes_freed,
            "backup_mb": round(after / 1e6, 1),
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
        "target": "定点重放（派生滞后的日期及其后，冷区权威层保留）",
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


# ---------------------------------------------------------------- 需求管理


def deliveries_of(request_dir: Path) -> list[Path]:
    """一次需求的交付目录，按名字升序——名字是时间戳，字典序即时间序。

    以 `.` 开头的是原子写留下的 `.tmp-*` 暂存目录，它**不是**一次交付：算进台账
    会让「once 已交付过」凭空成立，把真正需要的那份挡在门外。
    """
    base = request_dir / DELIVERIES_DIR
    if not base.is_dir():
        return []
    return sorted(
        path for path in base.iterdir() if path.is_dir() and not path.name.startswith(".")
    )


def criteria_text(spec: Spec) -> str:
    """把口径说成一句话，供 list 打印。形态不同，但都只报「这次要什么」。"""
    if spec.kind is Kind.BACKFILL:
        fill = spec.fill
        if fill is None:
            return "回填表格（fill 段待补）"
        return (
            f"回填 {fill.source.path.name}：{len(fill.columns)} 列，标识列 {fill.source.key_column}"
        )
    if spec.mode is Mode.SQL:
        return "手写 SQL（见 spec.yaml）"
    return " 且 ".join(f"{item.metric} {item.op} {item.value}" for item in spec.criteria)


def manifest_summary(directory: Path) -> str:
    """交付目录的一行摘要。读不到就留空——台账缺一份不该让 show 整体失败。"""
    path = directory / MANIFEST_NAME
    if not path.exists():
        return "  （缺 manifest）"
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "  （manifest 读不出来）"
    rows = (body.get("result") or {}).get("rows")
    status = (body.get("health") or {}).get("status", "?")
    return f"  {rows} 行 · {status}"


def _request(args: argparse.Namespace) -> int:
    """需求管理里不写数据的几个动作，都不持数据目录的锁。"""
    root = Path(args.requests_dir)
    if args.request_action == "new":
        return _request_new(args, root)
    if args.request_action == "list":
        return _request_list(root)
    if args.request_action == "method":
        return _request_method(root, args.id, check=args.check)
    if args.request_action == "input":
        return _request_input(root, args)
    if args.request_action == "diff":
        return _request_diff(root, args)
    if args.request_action == "compare":
        return _request_compare(root, args)
    return _request_show(root, args.id)


def _brief(text: str, limit: int = 42) -> str:
    flat = text.replace("\r", " ").replace("\n", " ").strip()
    if not flat:
        return "（空）"
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _request_compare(root: Path, args: argparse.Namespace) -> int:
    """逐列比对两次交付。默认最近两次——「刚改完这版动了什么」最常问的就是这一对。"""
    try:
        request_dir = resolve_dir(root, args.id)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    done = deliveries_of(request_dir)
    if args.left and args.right:
        left, right = args.left, args.right
    elif len(done) < 2:
        print(f"{args.id} 只有 {len(done)} 次交付，没有可比对象。")
        return 1
    else:
        left, right = done[-2].name, done[-1].name

    base = request_dir / DELIVERIES_DIR
    for name in (left, right):
        if not (base / name).is_dir():
            existing = "、".join(path.name for path in done) or "无"
            print(f"失败：没有交付 {name}（现有：{existing}）")
            return 1

    try:
        result = compare.compare_deliveries(base / left, base / right)
    except compare.CompareError as error:
        print(f"失败：{error}")
        return 1

    print(f"{args.id} 交付对比（{result.kind}）：{result.left} → {result.right}")
    print(
        f"  行数：{result.left_rows:,} → {result.right_rows:,}"
        f"（共同 {result.common_rows:,} · 新增 {len(result.added)} · 消失 {len(result.removed)}）"
    )
    for label, items in (("新增", result.added), ("消失", result.removed)):
        if items:
            head = "、".join(items[:6])
            tail = f" 等 {len(items)} 条" if len(items) > 6 else ""
            print(f"  {label}：{head}{tail}")
    if result.left_only_columns:
        print(f"  仅旧版有的列：{'、'.join(result.left_only_columns)}")
    if result.right_only_columns:
        print(f"  仅新版有的列：{'、'.join(result.right_only_columns)}")

    changed = [item for item in result.columns if item.changed]
    if not changed:
        print(f"  共同列逐格相同（共 {len(result.columns)} 列）。")
        return 0
    print(f"  改值 {result.changed_cells:,} 格，涉及 {len(changed)} 列：")
    for item in changed:
        print(f"    {item.name}  {item.changed:,} 格")
        for key, before, after in item.samples:
            print(f"      {key}：{_brief(before)} → {_brief(after)}")
    return 0


def _version_of(text: str) -> int:
    """`v2` 与 `2` 都认。认不出来就报错——猜一个版本号会让人看错两版。"""
    value = text.strip().lower().removeprefix("v")
    if not value.isdigit():
        raise ValueError(f"版本号应形如 v2 或 2，实得 {text!r}")
    return int(value)


def _request_diff(root: Path, args: argparse.Namespace) -> int:
    """比对两版口径。默认最近两版——那是「刚改了什么」最常被问的一对。"""
    try:
        request_dir = resolve_dir(root, args.id)
    except SpecError as error:
        print(f"失败：{error}")
        return 1
    versions = history_versions(request_dir / SPEC_NAME)
    try:
        if args.old and args.new:
            left, right = _version_of(args.old), _version_of(args.new)
        elif len(versions) < 2:
            print(f"{args.id} 只有 {len(versions)} 版快照，没有可比对象。")
            print("  改过口径并交付一次之后，才会留下下一版快照。")
            return 1
        else:
            left, right = versions[-2], versions[-1]
    except ValueError as error:
        print(f"失败：{error}")
        return 1

    for version in (left, right):
        if version not in versions:
            existing = "、".join(f"v{item}" for item in versions) or "无"
            print(f"失败：没有 v{version} 的快照（现有：{existing}）")
            return 1

    try:
        # 历史快照躺在 history/ 下，相对路径必须按需求目录解析。
        old = load_spec(request_dir / HISTORY_DIR / f"v{left}.yaml", base_dir=request_dir)
        new = load_spec(request_dir / HISTORY_DIR / f"v{right}.yaml", base_dir=request_dir)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    changes = specdiff.compare(old, new)
    print(f"{args.id} 口径 v{left} → v{right}")
    print()
    if changes:
        for line in specdiff.table(changes):
            print(f"  {line}")
        print()
        print(f"共 {len(changes)} 处差异（只比决定产物的部分）。")
    else:
        print("  口径未变：条件、排序、投影、护栏、回填规则逐项相同。")
    notes = specdiff.side_notes(old, new)
    if notes:
        print()
        print("  另（不影响产物，也不进口径指纹）：")
        for line in notes:
            print(f"  {line}")
    return 0


def _next_input_version(request_dir: Path) -> int:
    """下一版输入表的序号。顶层还有快照时，那一版就算 v1。"""
    base = request_dir / INPUT_DIR
    if not base.is_dir():
        return 1
    versions = [
        int(path.name[1:])
        for path in base.iterdir()
        if path.is_dir() and path.name.startswith("v") and path.name[1:].isdigit()
    ]
    if versions:
        return max(versions) + 1
    return 2 if any(path.is_file() for path in base.iterdir()) else 1


def _print_header_change(old: tuple[str, ...], new: tuple[str, ...]) -> None:
    """报字段差异。人看的是「哪几列不一样」，不是「怎么挪最省」。"""
    if not old:
        print(f"  新表共 {len(new)} 列（读不到当前表头，无从比对）")
        return
    change = header_change(old, new)
    if not (change["added"] or change["removed"] or change["reordered"]):
        print(f"  表头与当前完全一致（{len(new)} 列）")
        return
    if change["added"]:
        print(f"  新增 {len(change['added'])} 列：{'、'.join(change['added'])}")
    if change["removed"]:
        print(f"  少了 {len(change['removed'])} 列：{'、'.join(change['removed'])}")
    if change["reordered"]:
        print("  共同列的次序变了（按列名定位不受影响，但人眼看会串行）")


def _request_input(root: Path, args: argparse.Namespace) -> int:
    """把新一版输入收进需求目录，并报出字段差异。

    只搬文件、只报告，**不改 spec**：改 `fill.source.path` 等于换口径，得升版本、
    得在 METHOD.md 里记一句为什么换。这个判断不能交给命令替人做——它是人的活，
    命令只负责把「新表和旧表差在哪」摆到明面上。
    """
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1
    fill = spec.fill
    if fill is None:
        print(f"失败：{spec.id} 是 {spec.kind.value} 型需求，没有输入表可换。")
        return 1

    given = Path(args.add).expanduser()
    if not given.is_file():
        print(f"失败：新输入不存在或不是文件：{given}")
        return 1

    sheet = args.sheet or fill.source.sheet
    header_row = args.header_row or fill.source.header_row
    try:
        new_header = header_of(given, sheet=sheet, header_row=header_row)
    except (FillError, OSError) as error:
        print(f"失败：读不出新表表头——{error}")
        return 1
    if fill.source.key_column not in new_header:
        print(
            f"失败：新表第 {header_row} 行没有标识列 {fill.source.key_column!r}，接进来也没法用。"
        )
        print(f"  现有列：{'、'.join(new_header[:10])}{'…' if len(new_header) > 10 else ''}")
        return 1

    target = request_dir / INPUT_DIR / f"v{_next_input_version(request_dir)}" / given.name
    if target.exists():
        print(f"失败：{target} 已存在，不覆盖。")
        return 1
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(given, target)

    try:
        old_header = header_of(
            fill.source.path, sheet=fill.source.sheet, header_row=fill.source.header_row
        )
    except (FillError, OSError):
        old_header = ()
        print("  [!] 当前输入表读不出表头（文件或 sheet 有问题），只报新表结构")

    relative = target.relative_to(request_dir).as_posix()
    print(f"已收进 {relative}（{given.stat().st_size:,} 字节，原件未改动）")
    _print_header_change(old_header, new_header)

    missing = [item.name for item in fill.columns if item.name not in new_header]
    if missing:
        print(
            f"  [!] spec 声明了 {len(missing)} 列、新表里没有：{'、'.join(missing[:8])}"
            f"{'…' if len(missing) > 8 else ''}"
        )
        print("      直接交付会失败；要么补回这些列，要么把对应规则改成 manual 或删掉")

    print()
    print("下一步（换输入等于换口径，这几步不代劳）：")
    print(f"  1. spec.yaml：fill.source.path 改成 {relative}")
    print(f"  2. spec.yaml：version 改成 {spec.version + 1}")
    print("  3. method/METHOD.md：记一句换了哪版表、字段怎么变、为什么换")
    print(f"  4. 先 `request run {spec.id} --dry-run` 看逐列覆盖，再正式交付")
    return 0


def _request_method(root: Path, request_id: str, *, check: bool) -> int:
    """过程记录的入口：缺骨架就补，脚本有增删改就重算指纹。

    索引只钉「哪个脚本、指纹多少」，不替人编用途——所以新增脚本后第一次重算会
    报「待补」，那是有意的：机器猜得出 sha256，猜不出这个脚本为什么存在。
    """
    try:
        request_dir = resolve_dir(root, request_id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    if not check:
        report = sync_method(
            request_dir, request_id=spec.id, title=spec.title, kind=spec.kind.value
        )
        if report.doc_created:
            print(f"已补过程记录骨架：{request_dir / METHOD_DIR / METHOD_DOC}")
            print("  填完删掉 `<!-- 待填：` 那一行，否则交付会被拦下")
        for name in report.added:
            print(f"  新登记：{name}（用途待补，请回 METHOD.md 与索引里写清楚）")
        for name in report.refreshed:
            print(f"  指纹更新：{name}")
        for name in report.dropped:
            print(f"  已移除：{name}（文件不在 method/scripts/ 了）")
        if report.written and not (report.added or report.refreshed or report.dropped):
            print(f"  已生成脚本索引：{METHOD_DIR}/{METHOD_INDEX}")
        if not report.changed and not report.written:
            print(f"{spec.id} 方法索引已是最新，无需改动")

    found = problems(request_dir)
    if found:
        print(f"\n{spec.id} 过程记录不合规（{len(found)} 处）：")
        for text in found:
            print(f"  - {text}")
        return 1
    print(f"{spec.id} 过程记录合规：{METHOD_DIR}/{METHOD_DOC} + 脚本索引一致")
    return 0


def _request_new(args: argparse.Namespace, root: Path) -> int:
    """建需求：目录语义化，外部输入表当场复制进目录，之后与外部再无瓜葛。"""
    kind = Kind(args.kind)
    folder = root / directory_name(args.id, args.title, kind)
    # 目录名带语义后缀，判重不能只看同名目录，得看编号前缀——否则同一个
    # REQ-0002 换个标题就能再建一份，编号的唯一性就废了。
    taken = (
        [
            path
            for path in root.iterdir()
            if path.is_dir() and (path.name == args.id or path.name.startswith(f"{args.id}-"))
        ]
        if root.is_dir()
        else []
    )
    if taken or folder.exists():
        names = "、".join(path.name for path in taken) or folder.name
        print(
            f"失败：需求号 {args.id} 已有目录（{names}）。要改口径请直接编辑它并把 version 加一。"
        )
        return 1

    given: Path | None = None
    if kind is Kind.BACKFILL:
        if not args.source:
            print("失败：kind=backfill 需要 --source 指向对方给的原始表格（文件须已存在）。")
            return 1
        given = Path(args.source).expanduser()
        if not given.is_file():
            print(f"失败：输入表不存在或不是文件：{given}")
            return 1

    folder.mkdir(parents=True)
    snapshot: Path | None = None
    try:
        if given is not None:
            # 取材一次，取完即断：外部文件之后被删被改，都影响不到这份需求。
            snapshot = folder / INPUT_DIR / given.name
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(given, snapshot)
        target = folder / SPEC_NAME
        target.write_text(
            skeleton(
                request_id=args.id,
                title=args.title,
                requester=args.requester,
                purpose=args.purpose,
                cadence=Cadence(args.cadence),
                kind=kind,
                source_path=given.name if given else "",
            ),
            encoding="utf-8",
        )
        # 建完立刻按真规矩读一遍：不合法就整目录收走，好过等下次 run 才发现。
        load_spec(target)
        # 过程记录一并建好骨架：新需求从第一天起就有 method/ 这个坑位，
        # 不必等人想起来该记「怎么做的」——想不起来，是这类记录最常见的死法。
        sync_method(folder, request_id=args.id, title=args.title, kind=kind.value)
    except (SpecError, OSError) as error:
        shutil.rmtree(folder, ignore_errors=True)
        print(f"失败：{error}")
        return 1

    print(f"已建需求：{folder}（{kind.value}）")
    if snapshot is not None:
        print(f"  输入快照：{snapshot}（原件未改动；此后本需求不再依赖外部文件）")
        print(
            "  下一步：核对 fill.source 的 sheet / header_row / key_column 与逐列规则，"
            "改完把 version 加一，先跑 `request run <id> --dry-run`"
        )
    else:
        print("  下一步：编辑 criteria 与 output（metric 只写词表词条），再跑 `request run`")
    print(
        f"  过程记录：{METHOD_DIR}/{METHOD_DOC} 已建骨架（含 `<!-- 待填：` 标记）。"
        "按步骤写清做法、删掉那行标记，交付才会放行"
    )
    return 0


def _request_list(root: Path) -> int:
    specs = sorted(root.glob(f"*/{SPEC_NAME}"))
    if not specs:
        print(f"{root} 下还没有需求。")
        print("  用 `request new REQ-0001 --title 标题 --requester 姓名` 建第一个。")
        return 0
    for path in specs:
        try:
            spec = load_spec(path)
        except SpecError as error:
            # 一个坏 spec 不该挡住其余需求：列出来标红，让人去修。
            print(f"[!] {path.parent.name}：{error}")
            continue
        runs = deliveries_of(path.parent)
        last = runs[-1].name if runs else "从未交付"
        print(f"{spec.id}  {spec.title}")
        print(
            f"  {spec.kind.value} · {spec.cadence.value} · v{spec.version} · "
            f"交付 {len(runs)} 次 · 最后 {last}"
        )
        print(f"  目录：{path.parent.name}")
        print(f"  口径：{criteria_text(spec)}")
    return 0


def _show_fill(spec: Spec) -> None:
    """打印回填口径体。与「编译后的 SQL」并列，同属「这次到底怎么算」。"""
    fill = spec.fill
    if fill is None:
        print("口径：（fill 段缺失，spec 校验应已拦下）")
        return
    sheet = fill.source.sheet or "首个 sheet"
    print("口径：")
    print(
        f"  输入快照：{fill.source.relative}（需求目录内；{sheet}，"
        f"表头第 {fill.source.header_row} 行，标识列 {fill.source.key_column}）"
    )
    print(f"  产物：deliveries/<时刻>/{fill.output_name}；只写在需求目录内")
    print(
        f"  声明 {len(fill.columns)} 列 · overwrite={fill.overwrite} · "
        f"overrides={fill.overrides or '无'}"
    )
    for item in fill.columns:
        origin = {
            "field": f"底库 `{item.field}`" + (f" · {item.fmt}" if item.fmt else ""),
            "template": f"模板 `{item.template}`",
            "const": f"常量 `{item.const}`",
            "manual": f"人工判读：{item.reason}",
        }[item.kind]
        print(f"    {item.name}  ← {origin}")
    print()


def _request_show(root: Path, request_id: str) -> int:
    try:
        path = resolve_dir(root, request_id) / SPEC_NAME
        spec = load_spec(path)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    print(f"{spec.id}  {spec.title}")
    print(f"  提出人：{spec.requester}")
    if spec.purpose:
        print(f"  用途：{spec.purpose}")
    print(f"  形态：{spec.kind.value} · 节奏：{spec.cadence.value} · 版本：v{spec.version}")
    limit = spec.guard.max_rows if spec.guard.max_rows is not None else "不限"
    print(f"  护栏：{spec.guard.min_rows} ≤ 行数 ≤ {limit}")
    print(
        f"  标疑：与上次相比行数变化超 {spec.alert.row_delta_pct:g}% "
        f"且超 {spec.alert.row_delta_abs} 行"
    )
    print()

    if spec.kind is Kind.BACKFILL:
        _show_fill(spec)
    else:
        if spec.criteria:
            print("口径：")
            for index, item in enumerate(spec.criteria, 1):
                metric = lookup(item.metric)
                print(f"  {index}. {item.metric} {item.op} {item.value}")
                print(f"     落成列 {metric.field} —— {metric.definition}")
            print()
        print("编译后的 SQL：")
        for line in compile_sql(spec).splitlines():
            print(f"  {line}")
        print()

    versions = history_versions(path)
    if versions:
        print(f"历史版本：{'、'.join(f'v{version}' for version in versions)}")
    else:
        print("历史版本：（尚未快照，首次交付时自动留存）")

    print(f"口径指纹：{spec_digest(path)[:16]}…（只算口径内容，改注释与标题不算）")
    conflict = version_conflict(path, spec.version)
    if conflict:
        print(f"  [!] {conflict}")
        print("      在改好并升版之前，下一次交付会被拦下。")

    print(f"过程记录：{METHOD_DIR}/{METHOD_DOC}")
    found = problems(path.parent)
    if found:
        print(f"  不合规 {len(found)} 处，交付会被拦下（`request method {spec.id}` 可看详情）：")
        for text in found:
            print(f"    - {text}")
    else:
        print("  合规：步骤齐备，脚本索引与文件指纹一致")
    runs = deliveries_of(path.parent)
    print(f"交付台账：{len(runs)} 次")
    for run in runs:
        print(f"  {run.name}{manifest_summary(run)}")
    return 0


def _request_run(args: argparse.Namespace, store: DatasetStore) -> int:
    """交付一次。一次性需求交付过就不再自动重复，加 --force 才重交。"""
    root = Path(args.requests_dir)
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    if args.dry_run:
        # 预演不写盘、也不占「once 已交付」的名额——它本来就是给人先看一眼的。
        try:
            looked = preview(store, root, args.id)
        except (DeliveryBlocked, SpecError) as error:
            print(f"预演失败：{error}")
            return 1
        _print_preview(looked)
        return 0

    done = deliveries_of(request_dir)
    if spec.cadence is Cadence.ONCE and done and not args.force:
        print(f"{spec.id} 是一次性需求，已于 {done[-1].name} 交付过。")
        print("  确实要再交一份（比如对方要求重出），加 --force。")
        return 1

    try:
        result = deliver(store, root, args.id)
    except (DeliveryBlocked, SpecError) as error:
        print(f"交付失败：{error}")
        return 1
    _print_delivery(result)
    return 0


def _print_preview(result: Preview) -> None:
    print(f"预演 {result.request_id}（{result.kind.value}）· {result.rows:,} 行 · 未写盘")
    for line in result.lines:
        print(f"  {line}")


def _print_delivery(result: DeliveryResult) -> None:
    print(f"已交付 {result.request_id}（{result.kind.value}） → {result.directory}")
    print(f"  {result.rows:,} 行 · {len(result.columns)} 列 · 口径 v{result.spec_version}")
    for item in result.files:
        digest = str(item["sha256"])[:16]
        print(f"  {item['name']}  {item['bytes']:,} 字节  sha256 {digest}…")
    health = result.health
    if health.status == "suspect":
        print(
            f"  [!] 行数突变：{health.baseline_rows:,} → {result.rows:,}"
            f"（{health.delta_pct:+.1f}%），已标疑，README 顶部有警告"
        )
    elif health.status == "first":
        print("  首次交付，无基线可比")
    elif health.note:
        print(f"  {health.note}")


def _request_run_due(args: argparse.Namespace, store: DatasetStore) -> int:
    """跑所有到期的需求。一个失败不挡别的，但整体退出码会反映出来。"""
    root = Path(args.requests_dir)
    if not root.is_dir():
        print(f"{root} 下还没有需求，无需交付。")
        return 0

    try:
        report = run_due(store, root)
    except DeliveryBlocked as error:
        # 底库本身不成立（current 缺失）：这不是某一条需求的问题，整体失败。
        print(f"交付失败：{error}")
        return 1

    for result in report.delivered:
        mark = "  [!] 已标疑" if result.health.status == "suspect" else ""
        print(f"  {result.request_id}  已交付 {result.rows:,} 行 → {result.directory.name}{mark}")
    for request_id, reason in report.skipped:
        print(f"  {request_id}  跳过：{reason}")
    for request_id, reason in report.failed:
        print(f"  [!] {request_id}  失败：{reason}")

    print(
        f"共 {len(report.delivered)} 交付 / {len(report.skipped)} 跳过 / {len(report.failed)} 失败"
    )
    return 1 if report.failed else 0
