"""子命令参数表：只声明「命令行长什么样」，不做任何执行。

从 `cli` 拆出来，是因为它一度占掉组合根六成篇幅（492 / 785 行）。参数定义与
「谁加锁、谁记账」是两件事：前者随功能增长，后者只在事故后收紧。混在一起时，
每加一个开关都要在近五百行里找位置，而真正该被 review 的锁策略反而看不见。

这里只用 argparse 与各切片公开的常量；任何执行逻辑都不许进来。
"""

from __future__ import annotations

import argparse
import os

from ..acquire import (
    DEFAULT_PREVIEW_RATE,
    DEFAULT_TOP,
    MAX_LIMIT,
    PREVIEW_TOP,
    PROBE_DAILY_LIMIT,
)
from ..delivery import Cadence, Kind
from ..platform import ARCHIVE_AFTER_DAYS, BACKUP_KEEP_DAYS, REPLAY_WINDOW_DAYS
from ..query import BOARDS
from .refresh import DEFAULT_BUDGET_HOURS, DEFAULT_LAUNCH_HOURS

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
    collect.add_argument(
        "--max-pages",
        type=int,
        default=None,
        help="本轮最多抓几页（有界试跑）；0 表示不设上限，扫到末页",
    )
    collect.add_argument(
        "--pages-without-new",
        type=int,
        default=None,
        help="连续多少页无新记录即判定到达水位线。默认 3（--preset 可覆盖）",
    )
    collect.add_argument(
        "--full-scan",
        action="store_true",
        help="扫到末页、忽略水位线（每日全站采集用，约 1,140 页、12–20 分钟）",
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
        help=(
            "变化集卡片扫描的页数上限，默认 200（防止水位线异常时把全站拉完）；"
            "0 表示不设上限，刷新链用它把变化 ∪ 缺口全刷"
        ),
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
        help=(
            "kind=backfill 必填：对方给的原始表格路径（必须已存在，且引擎绝不改动它；"
            "受理时会读出第一页的 sheet 名写进骨架）"
        ),
    )
    request_new.add_argument(
        "--header-row",
        type=int,
        default=None,
        help="kind=backfill：列名在第几行（骨子里默认 2；多数表第 1 行就是列名）",
    )
    request_new.add_argument(
        "--sample",
        action="store_true",
        help="标记为开发用样例需求：台账与体检照常覆盖，但不计入影响面统计",
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
    request_sample = request_sub.add_parser(
        "sample", help="标记/取消样例需求：不计入影响面，其余照常"
    )
    request_sample.add_argument("id", help="需求号")
    request_sample.add_argument("--reason", default="", help="样例说明；默认「开发与验证用」")
    request_sample.add_argument("--undo", action="store_true", help="取消样例标记")
    request_handoff = request_sub.add_parser(
        "handoff", help="记一笔「哪一版给了谁」：只追加，不改档案"
    )
    request_handoff.add_argument("id", help="需求号")
    request_handoff.add_argument("--to", required=True, help="给了谁（必填）")
    request_handoff.add_argument(
        "--delivery", default=None, help="哪一次交付（目录名）；默认最新一次"
    )
    request_handoff.add_argument("--note", default="", help="备注：渠道、附件、对方的反馈等")
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
        "--max-pages",
        type=int,
        default=None,
        help="本轮最多抓几页（未扫完不判下架）；0 表示不设上限，一轮扫完全站",
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
        help=(
            "规模随内容刷新：重取 lastModified 晚于抓取日的库，最多 N 个（0 表示不做）；"
            "同一趟顺带补新库缺口（在架但没有规模行，按下载量最多 2 万个）"
        ),
    )
    sizes.add_argument(
        "--changed-all",
        action="store_true",
        help="刷新链用：陈旧集合全部重取，不设名额（与 --changed 的 N 互斥，优先此开关）",
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
    readme.add_argument(
        "--changed-all",
        action="store_true",
        help="刷新链用：陈旧与缺口全部取，不设名额（与 --changed 的 N 互斥，优先此开关）",
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
        help=(
            "重试上次 401/403 的库。只在你确实已获批访问那批库时才有意义："
            "全库 denied 中 99.75% 是 gated（manual 需作者审批、auto 需账号接受条款），"
            "2026-11-18 带令牌抽 500 条重试零命中，不要指望它能把缺口补上"
        ),
    )
    readme.add_argument(
        "--flush-every",
        type=int,
        default=2_000,
        help="每处理多少条就写一个暂存分片，中断时最多损失这一批",
    )

    parse_readme = subparsers.add_parser(
        "parse-readme",
        help="从已抓到的 README 正文抽 Homepage / Paper / Point of Contact（纯本地，不发请求）",
    )
    parse_readme.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")

    preview = subparsers.add_parser(
        "fetch-preview", help="抓数据集 viewer 的前几行当样例（独立表，不进默认导出）"
    )
    preview.add_argument("--date", default=None, help="快照日期 YYYY-MM-DD，默认今天")
    preview.add_argument(
        "--top", type=int, default=PREVIEW_TOP, help="按下载量取前多少名（默认 10000）"
    )
    preview.add_argument("--ids", default=None, help="只抓这些 id（逗号分隔），忽略 --top")
    preview.add_argument("--rows", type=int, default=3, help="每个库取几行（默认 3，上限 100）")
    preview.add_argument(
        "--config",
        default="auto",
        help="viewer 的 config 名（默认 auto：先查 /splits 取真实的那个；多数库不叫 default）",
    )
    preview.add_argument("--split", default="train", help="viewer 的 split 名（默认 train）")
    preview.add_argument(
        "--rate",
        type=float,
        default=DEFAULT_PREVIEW_RATE,
        help="每秒放行几个请求（默认 4）；额度按 IP 算，越小越稳",
    )
    preview.add_argument(
        "--throttle-wait",
        type=float,
        default=0.0,
        help="撞 429 后睡多少秒再继续（默认 0 = 整轮中止）。"
        "这个接口一个窗口只够几十条，设成 600 才能在一次进程里跨窗口补齐",
    )
    preview.add_argument("--workers", type=int, default=8, help="并发数；吞吐由 --rate 决定")
    preview.add_argument(
        "--flush-every",
        type=int,
        default=200,
        help="每攒多少条落一次暂存分片（默认 200）。中断后重跑据此续抓，不丢已抓到的",
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
    docs.add_argument("--out", default=None, help="输出路径，默认 data/DATA.md")

    wide = subparsers.add_parser(
        "export-wide",
        help="把 gold.datasets 宽视图导出成单文件 parquet（可重建，不参与状态更新）",
    )
    wide.add_argument(
        "--out", default=None, help="输出路径，默认 data/exports/datasets.parquet（须在数据目录内）"
    )

    refresh = subparsers.add_parser(
        "refresh", help="刷新鲜：每日 00:30 起跑完全部例行（串并混合，参 ADR-0008）"
    )
    refresh.add_argument(
        "--budget-hours",
        type=float,
        default=DEFAULT_BUDGET_HOURS,
        help=f"整条链的时间预算（小时，默认 {DEFAULT_BUDGET_HOURS}）；超时只停止未启动的步骤",
    )
    refresh.add_argument(
        "--launch-hours",
        type=float,
        default=DEFAULT_LAUNCH_HOURS,
        help=f"超过这个时长不再启动新抓取（默认 {DEFAULT_LAUNCH_HOURS} 小时）",
    )
    refresh.add_argument(
        "--only",
        default=None,
        help="只跑点名的步骤（逗号分隔），手动补跑用：不看依赖、不读今天已完成的台账、"
        "不套时间边界。可选值见 python -m hf_data_crawl.ops.refresh 的 STEP_NAMES",
    )

    refresh.add_argument(
        "--strict",
        action="store_true",
        help="手动补跑时若前置今天没有成功记录就拒绝（默认只提示）",
    )

    return parser
