"""命令行入口：collect / reconcile / build / query。

除 query（只读）外，每条命令都把运行摘要写入 runs/，失败也写并带原因。
"""

from __future__ import annotations

import argparse
import os
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from ..acquire import MAX_LIMIT, PROBE_DAILY_LIMIT
from ..acquire.commands import (
    run_collect,
    run_enrich,
    run_fetch_files,
    run_fetch_sizes,
    run_reconcile,
)
from ..delivery import Cadence, Kind
from ..delivery.commands import (
    run_export_wide,
    run_request,
    run_request_run,
    run_request_run_due,
)
from ..pipeline.commands import run_build
from ..pipeline.recovery import check_and_recover
from ..platform import (
    BACKUP_PRUNE_AFTER_DAYS,
    REPLAY_WINDOW_DAYS,
    DatasetStore,
    LockBusy,
    Outcome,
    exclusive,
    write_run,
)
from ..query import BOARDS
from ..query.commands import run_query, run_report
from .commands import run_archive, run_compact, run_docs, run_doctor, run_prune

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


def main(argv: list[str] | None = None, *, transport: Any = None) -> int:
    """命令行入口。

    Args:
        argv: 命令行参数,None 时使用 sys.argv[1:]
        transport: 网络传输抽象,仅供测试注入假实现。生产环境传 None,
            由各命令内部创建真实传输。类型标注为 Any 以避免 CLI 层耦合
            acquire 切片的内部抽象。
    """
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
        return _execute(store, "collect", lambda: run_collect(args, store, transport), lock_wait)
    if args.command == "reconcile":
        return _execute(
            store, "reconcile", lambda: run_reconcile(args, store, transport), lock_wait
        )
    if args.command == "build":
        return _execute(store, "build", lambda: run_build(args, store), lock_wait)
    if args.command == "archive":
        return _execute(store, "archive", lambda: run_archive(args, store), lock_wait)
    if args.command == "compact":
        return _execute(store, "compact", lambda: run_compact(args, store), lock_wait)
    if args.command == "prune":
        return _execute(store, "prune", lambda: run_prune(args, store), lock_wait)
    if args.command == "enrich":
        return _execute(store, "enrich", lambda: run_enrich(args, store, transport), lock_wait)
    if args.command == "fetch-sizes":
        return _execute(
            store,
            "fetch-sizes",
            lambda: run_fetch_sizes(args, store, transport, lock_wait),
            lock_wait,
        )
    if args.command == "fetch-files":
        return _execute(
            store, "fetch-files", lambda: run_fetch_files(args, store, transport), lock_wait
        )
    if args.command == "report":
        return _execute(store, "report", lambda: run_report(args, store), lock_wait)
    if args.command == "export-wide":
        return _execute(store, "export-wide", lambda: run_export_wide(args, store), lock_wait)
    if args.command == "query":
        return run_query(args, store)
    if args.command == "request":
        # new / list / show 只读写 requests/，不碰数据目录，不参与数据锁；
        # run 要读多张表出一个一致快照，与 report / docs 同规矩——先等一会儿锁。
        if args.request_action in {"run", "run-due"}:
            wait = SNAPSHOT_LOCK_WAIT if args.lock_wait is None else args.lock_wait
            try:
                with exclusive(store.root, "request-run", wait):
                    if args.request_action == "run":
                        return run_request_run(args, store)
                    return run_request_run_due(args, store)
            except LockBusy as error:
                print(f"交付失败：{error}")
                return 1
        return run_request(args)
    if args.command == "doctor":
        return run_doctor(store, lock_wait, requests_root=Path(args.requests_dir))
    if args.command == "docs":
        # docs 不写 runs/，但同样要在一致快照上生成；拿不到锁就明确失败。
        try:
            with exclusive(store.root, "docs", lock_wait):
                return run_docs(args, store)
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


# ---------------------------------------------------------------- 需求管理
