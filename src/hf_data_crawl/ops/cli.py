"""命令行入口：收集各子命令的参数并分派到各切片的公开处理函数。

本模块是唯一的组合根：独占锁、派生崩溃恢复与运行台账都在这里统一施加。
写命令与 report/docs 经 `_execute` 写 runs/（失败也写并带原因）；
query、doctor 与 request 直接返回，不写台账。
"""

from __future__ import annotations

import argparse
import os
import traceback
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from ..acquire import DEFAULT_PREVIEW_RATE, DEFAULT_TOP, MAX_LIMIT, PREVIEW_TOP, PROBE_DAILY_LIMIT
from ..acquire.commands import (
    run_collect,
    run_enrich,
    run_fetch_files,
    run_fetch_preview,
    run_fetch_readme,
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
from ..pipeline.commands import run_bronze_view, run_build
from ..pipeline.recovery import check_and_recover
from ..platform import (
    ARCHIVE_AFTER_DAYS,
    BACKUP_KEEP_DAYS,
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
        "fetch-readme",
        "fetch-preview",
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
#: 需求交付也要独占：它读多张表出一个一致快照，且要写 `history/`、`deliveries/`。
#: 与写命令放同一把锁，而不是自己另开一把——「同一时刻只有一个进程在动底库或档案」。
REQUEST_COMMANDS = frozenset({"request-run", "request-run-due"})

#: 拿到锁的命令：写命令互斥，生成命令等一个完整快照。需求交付同属此列。
LOCKED_COMMANDS = WRITE_COMMANDS | SNAPSHOT_COMMANDS | REQUEST_COMMANDS

#: 自己安排锁的命令：长耗时的网络抓取在锁外做，只把「并入正式表」包在短锁里。
#: 它们仍在 WRITE_COMMANDS 里（启动检查/恢复未完成的派生照旧），但不走
#: `_execute` 的整段锁。原因见 `sizes.StagingWriter`：全量抓取要跑三小时，
#: 整段持锁会直接让当天的 collect 等锁失败（2026-11-07 实际事故）。
SELF_LOCKED_COMMANDS = frozenset({"fetch-sizes", "fetch-readme", "fetch-preview"})

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
        default=None,
        help="连续多少页无新记录即判定到达水位线。默认 3（--preset 可覆盖）",
    )
    collect.add_argument("--dry-run", action="store_true", help="只探一页并打印字段，不落盘")
    collect.add_argument(
        "--preset",
        choices=["default", "aggressive", "maximum", "minimal", "conservative"],
        default=None,
        help=(
            "使用预设配置，只填未显式指定的参数（--refresh-top 等显式值不会被覆盖）。"
            "可选: default(推荐), aggressive(高新鲜度), maximum(需异步), minimal(测试), conservative(旧行为)"
        ),
    )
    collect.add_argument(
        "--refresh-top",
        type=int,
        default=None,
        help=(
            "额外按下载量补扫前 N 名（0 表示不补），用于让增速榜每天有新读数。"
            "默认 5000（--preset 可覆盖）"
        ),
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
    request_run.add_argument(
        "--preview",
        action="store_true",
        help="算一遍并把产物写进 <需求目录>/scratch/preview/，可打开看，但不新增交付",
    )
    request_sub.add_parser("run-due", help="跑所有到期的需求（计划任务用）")
    request_usage = request_sub.add_parser(
        "usage", help="反查一个词条/库列被哪些需求用到：改定义前先看影响面"
    )
    request_usage.add_argument("term", help="词条名（如 下载量）或库列名（如 downloads）")
    request_prune = request_sub.add_parser(
        "prune", help="清掉旧交付的产物字节：档案与 sha256 都留着，只回收磁盘"
    )
    request_prune.add_argument("id", nargs="?", default=None, help="需求号；不写则全部")
    request_prune.add_argument(
        "--keep", type=int, default=1, help="保留最近几条的产物（默认 1；当前有效那条永远保留）"
    )
    request_prune.add_argument(
        "--dry-run", action="store_true", help="只报会清掉什么、能省多少，不动磁盘"
    )
    request_close = request_sub.add_parser(
        "close", help="关掉一份需求：不再跑 run / run-due，台账与产物一份不动"
    )
    request_close.add_argument("id", help="需求号")
    request_close.add_argument("--reason", required=True, help="为什么关（必填，写给人看）")
    request_reopen = request_sub.add_parser("reopen", help="恢复一份已关闭的需求")
    request_reopen.add_argument("id", help="需求号")
    request_index = request_sub.add_parser(
        "index", help="重写 deliveries/INDEX.md：列出每条交付的性质、可复现性与当前有效"
    )
    request_index.add_argument("id", nargs="?", default=None, help="需求号；不写则全部重写")
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
        help=(
            "清空全部派生层并按天重放 data/raw 里所有日期（冷区权威层已损坏时的逃生舱）；"
            "若已有日期被 compact 压入冷区则直接拒绝，见 --allow-cold-loss"
        ),
    )
    build.add_argument(
        "--allow-cold-loss",
        action="store_true",
        help=(
            "明知冷区日期的 Raw 已不在主目录、其 silver/observations 无法重放，仍然全量重放。"
            "不加这个开关时 --full 会拒绝执行，免得把冷区历史静默清零"
        ),
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
    enrich.add_argument("--top", type=int, default=DEFAULT_TOP, help="补抓前多少名")
    enrich.add_argument(
        "--ids",
        default=None,
        help="按需补：只补这些 id（逗号分隔，上限 200）；给了它就不做批量扫描",
    )
    enrich.add_argument("--limit", type=int, default=MAX_LIMIT, help="每页条数，上限 1000")

    sizes = subparsers.add_parser("fetch-sizes", help="经 datasets-server 补规模与列数")
    sizes.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    sizes.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help="按下载量取前多少名；0 表示全量（按 current 实际行数，随底库增长自动适配）",
    )
    sizes.add_argument(
        "--changed",
        type=int,
        default=0,
        help="规模随内容刷新：重取 lastModified 晚于抓取日的库，最多 N 个（0 表示不做）",
    )
    sizes.add_argument(
        "--ids",
        default=None,
        help="按需补：只补这些 id（逗号分隔，上限 200，忽略 --top）；交付点名的长尾只补几条",
    )
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
        "--ids",
        default=None,
        help="按需补：只补这些 id（逗号分隔，上限 200，忽略 --top）",
    )
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

    readme = subparsers.add_parser(
        "fetch-readme", help="按 id / top / 全量抓 README 全文（独立表，不进默认导出）"
    )
    readme.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    readme.add_argument(
        "--top",
        type=int,
        default=DEFAULT_TOP,
        help="按下载量取前多少名；0 表示全量（按 current 实际行数，随底库增长自动适配）",
    )
    readme.add_argument("--ids", default=None, help="只抓这些 id（逗号分隔），忽略 --top")
    readme.add_argument(
        "--changed",
        type=int,
        default=0,
        help="README 随内容刷新：重取 commit 已变的库，最多 N 个（0 表示不做）",
    )
    readme.add_argument("--workers", type=int, default=12, help="并发数，实测 12 稳定在 19 req/s")
    readme.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="单库请求超时秒数；README 中位 2.5 KB，超时即记临时失败",
    )
    readme.add_argument(
        "--rate",
        type=float,
        default=15.0,
        help="每秒请求数上限（HF 解析器配额 5000 次 / 5 分钟 ≈ 16.7/s，默认留余量取 15；0 不限速）",
    )
    readme.add_argument(
        "--max-retries",
        type=int,
        default=2,
        help="单库重试次数；429 按上游 Retry-After 退避",
    )
    readme.add_argument(
        "--retry-denied",
        action="store_true",
        help="重试上次 401/403 的库（受门禁的库带令牌也可能读到）",
    )
    readme.add_argument(
        "--flush-every",
        type=int,
        default=2_000,
        help="每处理多少条就写一个暂存分片，中断时最多损失这一批",
    )

    preview = subparsers.add_parser(
        "fetch-preview", help="抓数据集 viewer 的前几行当样例（独立表，不进默认导出）"
    )
    preview.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    preview.add_argument(
        "--top", type=int, default=PREVIEW_TOP, help="按下载量取前多少名（默认 10000）"
    )
    preview.add_argument("--ids", default=None, help="只抓这些 id（逗号分隔），忽略 --top")
    preview.add_argument("--rows", type=int, default=3, help="每个库取几行（默认 3，上限 100）")
    preview.add_argument("--config", default="default", help="viewer 的 config 名（默认 default）")
    preview.add_argument("--split", default="train", help="viewer 的 split 名（默认 train）")
    preview.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_PREVIEW_RATE,
        help="每秒放行几个请求（默认 4）。撞 429 会整轮中止——它按 IP 封，继续打只会更久",
    )
    preview.add_argument("--workers", type=int, default=8, help="并发数；吞吐由 --rate 决定")
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
        "--older-than-days",
        type=int,
        default=ARCHIVE_AFTER_DAYS,
        help=f"只归档早于这么多天的日期（默认 {ARCHIVE_AFTER_DAYS}）",
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
        help=f"只算不删：列出会清理哪些、能释放多少（阈值 {BACKUP_KEEP_DAYS} 天）",
    )

    report = subparsers.add_parser("report", help="生成静态榜单页（不联网）")
    report.add_argument("--out", default=None, help="输出路径，默认 data/reports/dashboard.html")
    report.add_argument("--top", type=int, default=20, help="每张榜单条数")

    # 只读、不落盘、不加锁：给「回看某一天」用。Bronze 只保留最新一天，
    # 历史分区的早被 prune 删掉，而 build 会写正式目录并顺手 prune。
    bronze = subparsers.add_parser(
        "bronze",
        help="按日期从 Raw 现算 Bronze 并打印（只读、不落盘、可导出；仅限 Raw 里还在的日期）",
    )
    bronze.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    bronze.add_argument("--id", default=None, help="只看这一个数据集（打印关键列）")
    bronze.add_argument("--limit", type=int, default=5, help="打印前 N 行，默认 5")
    bronze.add_argument("--out", default=None, help="把整表写到这个路径（.parquet 或 .csv）")

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
    # 恢复会写 current / lineage / 各派生分区，必须与命令本体共用同一把锁——否则
    # 两个写命令各自在锁外重放，后写静默盖掉先写（此前是全项目唯一绕过锁的写路径）。
    # 这段在 `_execute` 之外，故 `LockBusy` 要自己兜住，不能让命令无痕崩溃。
    if args.command in WRITE_COMMANDS:
        try:
            with exclusive(store.root, "recovery", lock_wait):
                recovered = check_and_recover(store)
        except LockBusy:
            # 别的写命令正持锁，它启动时已经做过恢复；本轮跳过即可。
            recovered = []
            print("[恢复] 另一个写命令正持有数据锁，本轮跳过启动恢复")
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
    if args.command == "fetch-readme":
        return _execute(
            store,
            "fetch-readme",
            lambda: run_fetch_readme(args, store, transport, lock_wait),
            lock_wait,
        )
    if args.command == "fetch-preview":
        return _execute(
            store,
            "fetch-preview",
            lambda: run_fetch_preview(args, store, transport, lock_wait),
            lock_wait,
        )
    if args.command == "report":
        return _execute(store, "report", lambda: run_report(args, store), lock_wait)
    if args.command == "export-wide":
        return _execute(store, "export-wide", lambda: run_export_wide(args, store), lock_wait)
    if args.command == "query":
        return run_query(args, store)
    if args.command == "bronze":
        # 只读 Raw、不落盘、不改派生层：与 query 同待遇，不进数据锁、不写 runs 台账
        # （「看一眼某天」不该挤掉采集记录的位置）。
        return run_bronze_view(args, store).code
    if args.command == "request":
        # new / list / show 只读写 requests/，不碰数据目录，不参与数据锁；
        # run 要读多张表出一个一致快照，与 report / docs 同规矩——先等一会儿锁。
        if args.request_action in {"run", "run-due"}:
            wait = SNAPSHOT_LOCK_WAIT if args.lock_wait is None else args.lock_wait
            # 预演与预览不记账：它们不产出交付，写进 runs/ 只会让「最近的失败运行」
            # 混进一堆草稿。真正那次交付次次留痕，失败也留——需求侧的失败原先只活在
            # 终端里，体检的「近 7 天失败」根本看不见（ADR-0003 审计发现）。
            if getattr(args, "dry_run", False) or getattr(args, "preview", False):
                try:
                    with exclusive(store.root, "request-run", wait):
                        return run_request_run(args, store).code
                except LockBusy as error:
                    print(f"交付失败：{error}")
                    return 1
            command = "request-run" if args.request_action == "run" else "request-run-due"
            return _execute(store, command, lambda: _request_outcome(args, store), wait)
        return run_request(args)
    if args.command == "doctor":
        return run_doctor(store, lock_wait, requests_root=Path(args.requests_dir))
    if args.command == "docs":
        # 与 report 同路：加锁与落台账都交给 `_execute`。缺字段说明时退出码为 1，
        # 失败因此也会留在 runs/ 里——原先这里自己开一次锁并直接返回，等于在
        # `_execute` 之外又造了一条「有加锁、没台账」的路径。
        return _execute(store, "docs", lambda: run_docs(args, store), lock_wait)

    return 1


def _request_outcome(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """把「交付一次 / 跑一批」收敛成一条运行记录。

    原因用 `Attempt.reason`：那是「被哪道闸门拦住」的完整一句话。台账是机读面，
    写「见上方说明」等于没写——几天后再看，终端早滚没了。
    """
    if args.request_action == "run":
        attempt = run_request_run(args, store)
    else:
        attempt = run_request_run_due(args, store)
    return Outcome(payload={"exit": attempt.code}, code=attempt.code, reason=attempt.reason)


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
    except LockBusy as error:
        # 锁被占用是**预期状态**，不是程序错误：不打堆栈，也不该与真正的崩溃
        # 长一个样。计划任务里它意味着「另一个写命令还在跑」，人要读的是谁占着、
        # 该等还是该查，而不是一段 traceback。
        # reason 仍带 `LockBusy: ` 前缀：台账是机读面，类型名让「锁冲突」与
        # 「真崩溃」在 `doctor._recent_failures` 里能分开统计。
        reason = f"{type(error).__name__}: {error}"
        outcome = Outcome(payload={"error": reason}, code=1, reason=reason)
    except Exception as error:
        reason = f"{type(error).__name__}: {error}"
        outcome = Outcome(payload={"error": reason}, code=1, reason=reason)
        # catch-all 是台账一致性的需要，不等于可以吞掉现场：业务失败走的是
        # Outcome(code=1) 正常返回，能到这里的多半是未预期异常（含编程错误）。
        # 完整堆栈只进 stderr，由 scripts/_common.ps1 收进 logs/，不写进 runs/
        # 的 payload——台账保持机读，不夹带本机路径。
        traceback.print_exc()

    write_run(store, command, started, outcome)
    # 失败只在这里说一次。原先 except 分支也打印一句，同一条原因出现两遍
    # （「失败：…」+「未完成：…」），而两处措辞还不一样。
    # 需求交付自己已经把原因打得更细，台账那句不再重复一遍。
    if outcome.failed and outcome.reason and command not in REQUEST_COMMANDS:
        print(f"{command} 未完成：{outcome.reason}")
    return outcome.code


# ---------------------------------------------------------------- 需求管理
