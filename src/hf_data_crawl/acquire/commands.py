"""`acquire` 切片的命令处理函数，参数解析与分派在 ops/cli。"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import polars as pl
import pyarrow.parquet as pq

from ..pipeline import readme_clean, readme_fields, rebuild, state
from ..platform.env import load_env_file
from ..platform.lock import exclusive
from ..platform.presets import get_preset
from ..platform.runs import COLLECT_COMPLETE_REASONS, Outcome, read_runs
from ..platform.store import DatasetStore
from ..platform.tables import FILES_SCHEMA, README_FIELDS_SCHEMA, README_SCHEMA
from .batch import (
    STALL_HEARTBEAT_SECONDS,
)
from .batch import (
    StagingWriter as StagingDirWriter,
)
from .batch import (
    clear_staged as clear_staged_dir,
)
from .batch import (
    read_staged as read_staged_dir,
)
from .collect import Collector, CollectSummary
from .enrich import CARD_PAGES_CAP, Enricher, EnrichSummary
from .files import FILES_STAGING_DIR, FileFetcher, FileSummary, HfFilesApi
from .hf.api import HfDatasetApi, Transport
from .hf.transport import HttpxTransport
from .preview import HfPreviewApi, PreviewFetcher, PreviewSummary
from .readme import (
    README_STAGING_DIR,
    HfReadmeApi,
    ReadmeFetcher,
    ReadmeSummary,
    keep_previous_ok,
    missing_readme_ids,
    stale_readme_ids,
)
from .reconcile import PROBE_DEGRADE_AFTER, Reconciler, ReconcileSummary
from .sizes import (
    GAP_BUDGET,
    HfSizeApi,
    SizeFetcher,
    SizeSummary,
    StagingWriter,
    clear_staged,
    protect_ok_rows,
    read_staged,
    stale_ids,
    tail,
)


def _snapshot(args: argparse.Namespace) -> date:
    """快照日期：给了就按 ISO 解析（写错抛 ValueError，由 `_execute` 记成失败），否则今天。"""
    return date.fromisoformat(args.date) if args.date else date.today()


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


def _readme_transport(transport: Transport | None, timeout: float) -> Transport:
    """`fetch-readme` 是十几小时的长跑：关掉空闲连接复用。

    代理侧会把空闲连接单方面丢掉，而 httpx 仍当它可用，下一次复用就卡在 TLS
    握手上直到超时——实测 10 个连接同时变坏后吞吐从 14/s 掉到 0.33/s，进程重启
    即恢复。每条请求各开各的连接后，坏连接最多影响一条请求。
    """
    if transport is not None:
        return transport
    load_env_file()
    return HttpxTransport(timeout=timeout, max_keepalive=0)


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


#: 未给 `--refresh-top` 时补扫的头部数量。给这个默认值是为了让增速榜每天有新读数：
#: 只靠 `lastModified` 水位线的话，下载量涨了但文件没变的库永远拿不到第二个观测点。
DEFAULT_REFRESH_TOP = 5000

#: 未给 `--pages-without-new` 时，连续多少页无新记录即判定到达水位线。
DEFAULT_PAGES_WITHOUT_NEW = 3


def apply_collect_defaults(args: argparse.Namespace) -> None:
    """把预设与默认值填进 `args`；**只填空位，一个显式参数都不改**。

    早先的写法用「值是否等于默认值」来判断用户有没有指定，于是 `--refresh-top 5000`
    （恰好等于当时的默认值）会被预设悄悄换掉，命令行上还看不出任何提示。argparse
    本身分不清「没给」与「给了默认值」，所以只能让这两个参数没有默认值——`None`
    才是可靠的哨兵。
    """
    preset = get_preset(args.preset) if args.preset else None
    if preset is not None:
        print(f"使用预设: {preset.name} - {preset.description}")
    if args.max_pages is None and preset is not None:
        args.max_pages = preset.max_pages
    if args.refresh_top is None:
        args.refresh_top = preset.refresh_top if preset is not None else DEFAULT_REFRESH_TOP
    if args.pages_without_new is None:
        args.pages_without_new = (
            preset.pages_without_new if preset is not None else DEFAULT_PAGES_WITHOUT_NEW
        )


def run_collect(
    args: argparse.Namespace, store: DatasetStore, transport: Transport | None
) -> Outcome:
    apply_collect_defaults(args)

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
        full_scan=args.full_scan,
        dry_run=args.dry_run,
    )
    _print_collect(summary)
    # 补跑过去日期时，`collector.run` 只派生了那一天，而 `current` 是「最新一行
    # 的物化」——把它写成那天的值等于整体回退，卡片表还会被旧批次整行覆盖。
    # 补完必须把后缀重放到最新（`replay_suffix` 是重放集合唯一合法的构造方式），
    # 否则「补了一天数据」的代价是「底库退回那一天」。
    if not args.dry_run and summary.snapshot_date < date.today():
        days = rebuild.replay_suffix(store, summary.snapshot_date)
        if len(days) > 1:
            print(
                f"补跑 {summary.snapshot_date} 后重放后缀 {len(days)} 天，把底库带回最新"
                "（单日派生会把 current 写成那天的状态）"
            )
            rebuild.rebuild_from_raw(store, since=summary.snapshot_date)
    return Outcome(summary)


def run_reconcile(
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


def run_enrich(
    args: argparse.Namespace, store: DatasetStore, transport: Transport | None
) -> Outcome:
    snapshot = _snapshot(args)
    api = HfDatasetApi(transport=_open_transport(transport))

    if args.ids:
        # 按需补：只问点名的这几个库。全量扫一遍要十几小时，而实际被交付命中的
        # 常常只有几十个；逐条问一次，换到的是默认面那两列不再空着。
        from ..pipeline.card import backfill

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


def resolve_size_top(args_top: int, candidates: int, refresh: set[str]) -> int:
    """把 `--top` 解析成实际的候选截断数。

    `0` 表示全量：取 current 的实际行数，而不是脚本里写死一个会过期的常数——
    底库涨到 120 万时，写死 1122000 会让长尾永远补不到规模，且没有任何报错。
    指定 `--changed` 的刷新集合时这批一个都不能少，`--top` 不参与截断。
    """
    return max(1, candidates if refresh or args_top <= 0 else args_top)


#: `--ids` 一次最多点多少名。这是「补几个库」的手工活，不是批量重跑：
#: 没有上限时，一条手滑的 `--ids` 就能把全站请求一遍。
IDS_LIMIT = 200

#: `parse-readme` 一次读多少条正文。
#:
#: 正文中位 4 KB、均值约 3.6 KB，两万条约占 80 MB：大到能摊薄逐批判开销，又小到
#: 不会把内存推到危险区。此前是整表 `store.read("readme")`——全量 2.8 GB 正文一次
#: 入内存，2026-11-18 在 16 GB 机器上跑到 3.6 GB 就被迫刹车。
PARSE_BATCH_ROWS = 20_000


def split_ids(raw: str) -> list[str]:
    """`--ids` 的逗号串 → 去空白的 id 列表。"""
    return [item.strip() for item in raw.split(",") if item.strip()]


def pick_ids(ordered: pl.DataFrame, raw: str) -> tuple[pl.DataFrame, str]:
    """按 `--ids` 过滤候选，返回 (过滤后的表, 错误消息)。错误消息为空表示没问题。

    名单里查不到的 id 静默丢弃（多打了个空格、或库里已下架），但**一个都没匹配上
    要报错**：那多半是 id 写错，而「跑完了 0 行、状态 success」比报错更容易被当成
    正常结果。
    """
    wanted = split_ids(raw)
    if not wanted or len(wanted) > IDS_LIMIT:
        return ordered, f"--ids 需要 1–{IDS_LIMIT} 个有效 id，实得 {len(wanted)} 个"
    picked = ordered.filter(pl.col("id").is_in(wanted))
    if picked.height == 0:
        return picked, "--ids 里的 id 在 current 里都不存在（底库没有这个库，或已下架）"
    return picked, ""


def run_fetch_sizes(
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
    # 规模是内容的函数：库改了（lastModified 越过抓取日），旧的行数就不再是事实。
    # `--changed N` 只重取这批里的前 N 个（按下载量），不重扫全站——全量那一趟留
    # 给周任务，这里补的是「每天都变」的那一小撮。
    #
    # 同一趟里还要补「新库」：它们在 `size` 表里没有行，而 `stale_ids` 是 inner
    # join，天然把它们排除在陈旧集合之外。原先这里把候选集整个换成陈旧集合，
    # 于是新库只能等周日那趟全量——实测中位 2 天、超 7 天的有 1,322 个，
    # 2026-11 一个月就攒下 8,726 个没有行数的新库。缺口按下载量取前
    # `GAP_BUDGET` 个，防的是某轮全量失败后缺口涨到几十万。
    refresh: set[str] = set()
    if args.changed > 0 or args.changed_all:
        stale = stale_ids(current, store.read("size"))
        ranked = stale.sort("downloads", descending=True, nulls_last=True)
        # `--changed-all`：陈旧集合整个刷，不设名额——刷新链要的是「当天有多少刷多少」。
        picked = ranked if args.changed_all else ranked.head(args.changed)
        refresh = set(picked["id"].to_list())
        changed = ordered.filter(pl.col("id").is_in(sorted(refresh)))
        # `existing` 已含残留分片：上一轮中断前抓到的缺口不该再请求一遍。
        gap = ordered.join(existing.select("id").unique(), on="id", how="anti").head(GAP_BUDGET)
        if not changed.height and not gap.height:
            print("规模无陈旧、也无缺口：没有需要重取的库。")
            return Outcome({"changed": 0, "refreshed": 0, "gap": 0})
        ordered = pl.concat([changed, gap]) if gap.height else changed
        print(
            f"规模随变化刷新：{changed.height} 个库内容已变，"
            f"另补 {gap.height} 个新库缺口（共 {ordered.height} 个）。"
        )

    if args.ids:
        # 按需补：只问点名的库。全量一轮三小时，而交付实际命中的常常只有几十条。
        ordered, problem = pick_ids(ordered, args.ids)
        if problem:
            print(f"失败：{problem}")
            return Outcome({"error": "ids"}, code=1, reason=problem)

    fetcher = SizeFetcher(
        api=HfSizeApi(transport=_open_transport(transport)),
        workers=args.workers,
        progress=_progress_line,
        heartbeat=_stall_line,
    )
    writer = StagingWriter(store, snapshot)
    candidates = ordered["id"].to_list()
    # 点名了就不再按下载量截断：名单本身就是范围，`--top` 只在批量扫描时说话。
    top = len(candidates) if args.ids else resolve_size_top(args.top, len(candidates), refresh)
    summary, frame = fetcher.run(
        snapshot,
        dataset_ids=candidates,
        top=top,
        retry_unavailable=args.retry_unavailable,
        existing=existing,
        refresh=refresh,
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
            # 整行替换的下手对象是「已经拿到数字」的行：一次瞬时 500 不该把它抹空。
            store.upsert("size", protect_ok_rows(store.read("size"), merged))
        clear_staged(store, paths)

    _print_sizes(summary, store.read("size").height)
    return Outcome(summary)


def run_fetch_preview(
    args: argparse.Namespace,
    store: DatasetStore,
    transport: Transport | None,
    lock_wait: float = 0.0,
) -> Outcome:
    """锁外抓取、锁内并入，与 `fetch-sizes` 同一个形状（只是量级小得多）。

    一万个库、8 并发大约十几分钟；整段持锁会挡住当天的 collect（2026-11-07 的事故）。
    故抓取在锁外，只有「并入 preview 表」那一小段独占。这里不设暂存分片：中断最多
    损失一轮十来分钟，重跑即可，不必为它维护一套续抓机制。

    只抓 `size` 表里 `status=ok` 的库——没有 viewer 的库（14.25%）连请求都不该发。
    """
    snapshot = _snapshot(args)
    current = state.read_current(store)
    if current.height == 0:
        print("尚无采集数据，请先运行 `hf-data-crawl collect`。")
        return Outcome({"error": "empty"}, code=1, reason="尚无采集数据，请先运行 collect")

    ordered = current.sort("downloads", descending=True, nulls_last=True)
    if args.ids:
        ordered, problem = pick_ids(ordered, args.ids)
        if problem:
            print(f"失败：{problem}")
            return Outcome({"error": "ids"}, code=1, reason=problem)
        candidates = ordered["id"].to_list()
    else:
        fetchable = set(store.read("size").filter(pl.col("status") == "ok")["id"].to_list())
        candidates = [item for item in ordered["id"].to_list() if item in fetchable][: args.top]

    if not candidates:
        print("没有可抓的库：size 表里没有 status=ok 的记录，先跑 `fetch-sizes`。")
        return Outcome({"fetched": 0})

    # 已经定过的库不再问：`ok` 与 `unavailable` 都是终局，`error` 才是可救的。
    # 有这一条就不用暂存分片，中断后重跑同一命令即可从断点续抓。
    done: set[str] = set()
    existing = store.read("preview")
    if existing.height:
        done = set(existing.filter(pl.col("status").is_in(["ok", "unavailable"]))["id"].to_list())
    pending = [item for item in candidates if item not in done]
    if not pending:
        print(f"无需抓取：{len(candidates)} 个候选都已定局（preview 表 {existing.height} 行）。")
        return Outcome({"fetched": 0})
    if len(pending) < len(candidates):
        print(f"跳过 {len(candidates) - len(pending)} 个已定局的库，本轮抓 {len(pending)} 个")
    candidates = pending

    print(f"取样例：{len(candidates)} 个库 × 最多 {args.rows} 行，匀速 {args.rate} req/s")
    fetcher = PreviewFetcher(
        api=HfPreviewApi(
            transport=_open_transport(transport),
            rows=args.rows,
            config=args.config,
            split=args.split,
            rate=args.rate,
        ),
        workers=args.workers,
        progress=_progress_line,
    )
    summary, frame = fetcher.run(snapshot, candidates)
    # 进度条是 \r 原地刷新，收尾必须换行。
    print()

    if frame.height:
        with exclusive(store.root, "fetch-preview:merge", lock_wait):
            store.upsert("preview", frame)

    _print_preview(summary, store.read("preview").height)
    if summary.aborted:
        message = (
            f"被限流（429）中止：已并入 {frame.height} 行，其余未抓。"
            f"降低 --rate（当前 {args.rate}）或稍后重跑"
        )
        print(f"失败：{message}")
        return Outcome(summary, code=1, reason=message)
    return Outcome(summary)


def _print_preview(summary: PreviewSummary, total_rows: int) -> None:
    print(
        f"样例：取到 {summary.fetched} · 无 viewer/受限 {summary.unavailable} · "
        f"临时失败 {summary.failed} · 截断过 {summary.truncated}；"
        f"preview 表现有 {total_rows} 行（{summary.elapsed_seconds:.0f}s）"
    )


def run_fetch_files(
    args: argparse.Namespace,
    store: DatasetStore,
    transport: Transport | None,
    lock_wait: float = 0.0,
) -> Outcome:
    """锁外抓取、锁内合并：与 `fetch-sizes` / `fetch-readme` 同一套路。

    原先整段持锁（写命令的默认）。刷新链要让它与对账并行，而它每天 2 万条约
    70 分钟——整段占锁会把并行段里其他抓取的合并全堵在锁外（2026-11-19 实测：
    `fetch-files` 持锁 25 分钟，`fetch-readme` 的合并等到 `--lock-wait` 超时）。
    现在结果先落 `.staging/files/` 的原子分片，收尾才用短锁并入 `enrich/files.parquet`。
    """
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
    if args.ids:
        # 与 fetch-sizes / fetch-readme 同一套按需补：名单即范围，`--top` 不参与。
        ordered, problem = pick_ids(ordered, args.ids)
        if problem:
            print(f"失败：{problem}")
            return Outcome({"error": "ids"}, code=1, reason=problem)

    # 残留分片同样是「已处理」的 id：上一轮中断前抓到的结果不该再请求一遍。
    existing = store.read("files")
    staged, _ = read_staged_dir(store, FILES_STAGING_DIR, FILES_SCHEMA)
    if staged.height:
        existing = pl.concat([existing, staged], how="vertical_relaxed")

    fetcher = FileFetcher(
        api=HfFilesApi(
            transport=_files_transport(transport, args.timeout),
            max_retries=args.max_retries,
            max_pages=args.max_pages,
        ),
        workers=args.workers,
        progress=lambda done, total: _progress_line(done, total, "补齐文件清单"),
        heartbeat=lambda done, total: _stall_line(done, total, "补齐文件清单"),
    )
    writer = StagingDirWriter(store, snapshot, FILES_STAGING_DIR)
    summary, frame = fetcher.run(
        snapshot,
        dataset_ids=ordered["id"].to_list(),
        # 点了名就按名单来（名单已过滤成正好这几个），否则照旧按下载量取前 N。
        top=max(1, ordered.height if args.ids else args.top),
        retry_unavailable=args.retry_unavailable,
        refresh_truncated=args.refresh_truncated,
        existing=existing,
        # 边跑边落暂存分片：中断时最多损失一个批次，但写入发生在锁外。
        flush=writer.write,
        flush_every=args.flush_every,
    )
    # `flush` 只在攒满一批时触发；不足一批的尾巴必须补写进分片，否则合并只读
    # 分片时会漏掉它们。
    if frame.height > writer.flushed:
        writer.write(frame.slice(writer.flushed, frame.height - writer.flushed))
    print()

    # 短锁：只在这段独占数据目录，把本轮结果与残留分片一次并入正式表。
    # 抓不到锁就失败，分片原样留着，下一轮继续（不丢已抓到的数据）。
    with exclusive(store.root, "fetch-files:merge", lock_wait):
        merged, paths = read_staged_dir(store, FILES_STAGING_DIR, FILES_SCHEMA)
        if merged.height:
            store.upsert("files", merged)
        clear_staged_dir(store, paths, FILES_STAGING_DIR)
    _print_files(summary, store.read("files").height)
    return Outcome(summary)


def run_fetch_readme(
    args: argparse.Namespace,
    store: DatasetStore,
    transport: Transport | None,
    lock_wait: float = 0.0,
) -> Outcome:
    """锁外抓取、锁内合并：全量要跑十几小时，整段持锁会挡住当天的采集。

    与 `fetch-sizes` 同一套路：结果先写 `.staging/readme/` 的原子分片，
    收尾才用短锁并入 `enrich/readme.parquet`；中断后分片还在，下一轮续抓。
    """
    snapshot = _snapshot(args)
    current = state.read_current(store)
    if current.height == 0:
        print("尚无采集数据，请先运行 `hf-data-crawl collect`。")
        return Outcome({"error": "empty"}, code=1, reason="尚无采集数据，请先运行 collect")

    # 残留分片同样是「已处理」的 id：上一轮中断前抓到的结果不该再请求一遍。
    # 只要 `id` / `status` / `source_sha` 三列：判「跳过谁」「谁陈旧」「谁是缺口」
    # 都用不到正文。整表读入是 365 MB 压缩 / 3 GB 解压——2026-11-19 实测收尾时
    # 峰值 3.8 GB，而这一步只是想知道哪些 id 已有结论。
    existing = store.read("readme", columns=["id", "status", "source_sha"])
    staged, _ = read_staged_dir(store, README_STAGING_DIR, README_SCHEMA)
    if staged.height:
        existing = pl.concat(
            [existing, staged.select("id", "status", "source_sha")], how="vertical_relaxed"
        )

    ordered = current.sort("downloads", descending=True, nulls_last=True)
    # README 是仓库里的一个文件：commit 变了，旧全文就不再是当前版本。
    # `--changed N` 只重取这批里的前 N 个（按下载量），全量那一趟留给人工择机跑。
    refresh: set[str] = set()
    if args.changed > 0:
        # 候选 = 陈旧 ∪ 缺口。只看陈旧会漏掉「从没记录」的新库——`stale_readme_ids`
        # 是 inner join，不在表里的 id 永远进不了候选（2026-11-19 实测卡住 2,945 个）。
        # 与 `fetch-sizes` 的缺口补抓对齐，按下载量分配这一轮的 N 个名额。
        stale = stale_readme_ids(current, existing)
        gap = missing_readme_ids(current, existing)
        ranked = pl.concat(
            [
                stale.select("id", "downloads", "sha"),
                gap.select("id", "downloads", "sha"),
            ]
        ).sort("downloads", descending=True, nulls_last=True)
        # `--changed-all`：陈旧与缺口整个刷，不设名额（刷新链的默认形态）。
        picked = ranked if args.changed_all else ranked.head(args.changed)
        refresh = set(picked["id"].to_list())
        if refresh:
            ordered = ordered.filter(pl.col("id").is_in(sorted(refresh)))
            print(
                f"README 随内容刷新：{len(stale)} 个库 commit 已变、{len(gap)} 个还没有结论，"
                f"本轮取 {len(refresh)} 个（按下载量）。"
            )
        elif staged.height:
            # 上一轮抓完却没来得及合并（撞锁、被杀）：分片在盘上，本轮没有新候选
            # ——分片里的行已算「有结论」，缺口也是 0。但收尾必须走到，否则分片
            # 永远滞留，而每轮又都因为没有候选而提前返回，形成死锁。
            print(f"README 无新候选，但有 {staged.height} 条暂存分片待合并。")
            ordered = ordered.head(0)
        else:
            print("README 既无陈旧也无缺口：所有库都已有结论。")
            return Outcome({"changed": 0, "refreshed": 0})

    if args.ids:
        wanted = [item.strip() for item in args.ids.split(",") if item.strip()]
        ordered = ordered.filter(pl.col("id").is_in(wanted))
        if ordered.height == 0:
            print("--ids 里没有有效的数据集 id。")
            return Outcome({"error": "ids"}, code=1, reason="--ids 无有效 id")
        candidates = ordered["id"].to_list()
        picked = ordered
    elif refresh:
        candidates = ordered["id"].to_list()
        picked = ordered
    else:
        candidates = (
            ordered["id"].to_list()[: args.top] if args.top > 0 else ordered["id"].to_list()
        )
        picked = ordered.head(len(candidates))

    # sha 只给本轮候选：全库 113 万条建 dict 没必要，候选通常几万个。
    shas = dict(zip(picked["id"].to_list(), picked["sha"].to_list()))

    fetcher = ReadmeFetcher(
        api=HfReadmeApi(
            transport=_readme_transport(transport, args.timeout),
            max_retries=args.max_retries,
            rate=args.rate,
        ),
        workers=args.workers,
        progress=lambda done, total: _progress_line(done, total, "补 README"),
        heartbeat=lambda done, total: _stall_line(done, total, "补 README"),
    )
    writer = StagingDirWriter(store, snapshot, README_STAGING_DIR)
    summary, frame = fetcher.run(
        snapshot,
        dataset_ids=candidates,
        shas=shas,
        existing=existing,
        refresh=refresh,
        retry_denied=args.retry_denied,
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
    with exclusive(store.root, "fetch-readme:merge", lock_wait):
        merged, paths = read_staged_dir(store, README_STAGING_DIR, README_SCHEMA)
        if merged.height:
            # 临时失败不许抹掉已拿到的全文；absent / denied 是明确结论，可以覆盖。
            #
            # 只读 `id` 与 `status`：`keep_previous_ok` 的判据就这两列。整表读入会
            # 把正文（全量 2.8 GB）一起搬进内存，收尾峰值十几 GB——2026-11-18 实测
            # 16 GB 机器直接 OOM，只能靠一次性脚本绕路。合并本身也走流式。
            previous = store.read("readme", columns=["id", "status"])
            store.merge_by_id("readme", keep_previous_ok(previous, merged))
        clear_staged_dir(store, paths, README_STAGING_DIR)

    _print_readme(summary, store.read("readme").height)
    return Outcome(summary)


def _print_readme(summary: ReadmeSummary, table_rows: int) -> None:
    print(
        f"{summary.snapshot_date} README 补齐：候选 {summary.requested} 条"
        f" / 耗时 {summary.elapsed_seconds:.1f}s"
        f" / 下载 {summary.downloaded_bytes / 1e6:.1f}MB"
    )
    print(
        f"  成功 {summary.fetched} · 仓库没有 README {summary.absent} · "
        f"受限读不到 {summary.denied} · 临时失败 {summary.failed} · 已存跳过 {summary.skipped}"
    )
    print(f"  全文表累计 {table_rows} 行（临时失败不入库，下次自然重试）")


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


def _stall_line(done: int, total: int, label: str = "补齐规模") -> None:
    """一条都没完成的这段时间，也得说一声。

    它没法替人判断是「大表解析慢」还是「请求挂死了」，但沉默是最坏的一种：
    2026-11-16 那次，日志停在初始一行整整一个半小时，与「老老实实跑得慢」看不出
    任何分别，只能靠比对文件时间才发觉它早就不动了。
    """
    print(
        f"  {label} {done}/{total}：{STALL_HEARTBEAT_SECONDS:.0f} 秒内无一条完成，仍在等待",
        flush=True,
    )


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
    if summary.probe_degraded:
        print(
            f"  探测顽固失败 {summary.probe_degraded} 个（连续 {PROBE_DEGRADE_AFTER} 轮无结论，"
            "见差异清单 probe_degraded 行）"
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


def run_parse_readme(
    args: argparse.Namespace,
    store: DatasetStore,
    transport: Transport | None = None,
    lock_wait: float = 0.0,
) -> Outcome:
    """从已抓到的 README 正文里抽侧栏字段。纯本地，不发一个请求。

    `cardData` 里没有 Homepage / Paper / Point of Contact——它们只写在正文的
    Markdown 列表里（`- **Homepage:** ...`）。`fetch-readme` 已经把全文落到
    `readme` 表，这里只做解析。

    只处理 `status=ok` 的行：`absent`（仓库没有 README）与 `denied`（权限）都
    没有正文可抽。抽不到一律 null，别用空串冒充「有」。
    """
    target = store.path("readme")
    total_records = pq.ParquetFile(target).metadata.num_rows if target.exists() else 0
    if total_records == 0:
        print("readme 表为空，先跑 `hf-data-crawl fetch-readme`。")
        return Outcome({"error": "empty"}, code=1, reason="readme 表为空")

    # 「有多少条 ok」只需要 status 一列。整表读入会把正文（全量 2.8 GB）一起搬进
    # 内存，而这里只想知道要解析多少条——2026-11-18 实测整表读入到 3.6 GB 后被迫刹车。
    statuses = store.read("readme", columns=["status"])
    ok_total = int((statuses["status"] == "ok").sum())
    print(f"解析 {ok_total:,} 条 README 正文（共 {total_records:,} 条记录）")

    # 已有凭证：`(id, source_sha)` 都相同的行，字段就是照这版正文抽出来的，不必重抽。
    # 每周保鲜只重取几千条（2026-11-18 实测 4,781 条），而全量重抽 78 万条要 340 秒
    # ——不跳过的代价是「每次保鲜都白跑一遍全量」。
    existing = _extracted_pairs(store)
    if existing.height:
        print(f"已抽过 {existing.height:,} 条（同 id 同 sha 者跳过）")

    # 逐批读正文：一批只驻留 `PARSE_BATCH_ROWS` 条，抽完即弃。抽出的字段是短字符串，
    # 累积起来只有几十 MB，故帧可以攒到最后一次写。
    parquet = pq.ParquetFile(target)
    frames: list[pl.DataFrame] = []
    skipped = 0
    try:
        for batch in parquet.iter_batches(batch_size=PARSE_BATCH_ROWS):
            chunk = pl.from_arrow(batch)
            assert isinstance(chunk, pl.DataFrame), "record batch 读出来必须是 DataFrame"
            usable = chunk.filter(pl.col("status") == "ok")
            if usable.height == 0:
                continue
            if existing.height:
                before = usable.height
                # anti join 而不是拼字符串键：两列都是 Utf8，拼键要为 78 万行造一个
                # 新列；join 直接用两列做等值匹配，内存与时间都省。
                usable = usable.join(existing, on=["id", "source_sha"], how="anti")
                skipped += before - usable.height
                if usable.height == 0:
                    continue
            rows: list[dict[str, Any]] = [
                {
                    "id": item["id"],
                    "url": item["url"],
                    "snapshot_date": item["snapshot_date"],
                    "source_sha": item["source_sha"],
                    **readme_fields.extract_fields(item.get("text")),
                }
                for item in usable.iter_rows(named=True)
            ]
            frames.append(pl.DataFrame(rows, schema=README_FIELDS_SCHEMA))
    finally:
        # Windows 下不关句柄，后续写盘或替换会报 WinError 5（2026-11-18 实测）。
        parquet.close()

    frame = (
        pl.concat(frames, how="vertical") if frames else pl.DataFrame(schema=README_FIELDS_SCHEMA)
    )
    if frame.height:
        with exclusive(store.root, "parse-readme:write", lock_wait):
            store.upsert("readme_fields", frame, dedupe=["id"])

    total = store.read("readme_fields").height
    _print_readme_fields(frame, total)
    return Outcome({"parsed": frame.height, "skipped": skipped, "table_rows": total})


def run_clean_readme(
    args: argparse.Namespace,
    store: DatasetStore,
    lock_wait: float = 0.0,
) -> Outcome:
    """把 README 正文清洗后物化进 `readme_clean`（可选加速，不进每日链）。

    为什么可选：导出宽表时现场清洗要 13.8 秒，物化后只需读 287 MB；但 `upsert` 是
    整表重写（2026-11-19 实测：改 1 行也要 11.2 秒），每天跑一次只净省 5.5 秒。
    所以它不进刷新鲜——只在「要反复重导出」或「调清洗规则」时手动跑一次；
    `export-wide` 检测到这张表就用它，没有就现场清洗。判据与取舍见 GLOSSARY #137。
    """
    target = store.path("readme")
    if not target.exists():
        print("readme 表为空，先跑 `hf-data-crawl fetch-readme`。")
        return Outcome({"error": "empty"}, code=1, reason="readme 表为空")

    clean_path = store.path("readme_clean")
    sql = readme_clean.incremental_sql(
        _literal_path(target),
        _literal_path(clean_path) if clean_path.exists() else None,
    )
    started = time.time()
    with duckdb.connect() as connection:
        # `execute()` 返回连接本身；要 polars 帧得走 `sql()` 拿 relation。
        frame = connection.sql(sql).pl()
    cleaned = frame.height
    if cleaned:
        with exclusive(store.root, "clean-readme:write", lock_wait):
            store.upsert("readme_clean", frame, dedupe=["id"])
    total = store.read("readme_clean").height if clean_path.exists() else 0
    print(f"清洗 {cleaned:,} 条正文（{time.time() - started:.1f}s）；readme_clean 共 {total:,} 行")
    return Outcome({"cleaned": cleaned, "table_rows": total})


def _literal_path(path: Path) -> str:
    """SQL 字面量里的绝对路径：正斜杠 + 单引号转义。"""
    return path.resolve().as_posix().replace("'", "''")


def _extracted_pairs(store: DatasetStore) -> pl.DataFrame:
    """已抽过的 `(id, source_sha)`：正文没变就不必再抽一遍。

    只读两列（78 万行约 40 MB），不碰正文。凭证为空的行不算「抽过」——空 sha 是
    「没记账」，按它跳过等于让旧行的字段永远停在旧正文上。
    """
    path = store.path("readme_fields")
    if not path.exists():
        return pl.DataFrame(schema={"id": pl.Utf8, "source_sha": pl.Utf8})
    frame = pl.read_parquet(path, columns=["id", "source_sha"])
    return frame.filter(pl.col("source_sha").is_not_null())


def _print_readme_fields(frame: pl.DataFrame, table_rows: int) -> None:
    """报各字段的命中率。

    它们在上游本就稀疏（Homepage 12.4%、Paper 12.7%、Point of Contact 1.0%、
    真 BibTeX 8.6%），所以「百分比」比「条数」更能说明这次解析值不值。
    """
    if frame.height == 0:
        print(f"  没有可解析的正文 · 表累计 {table_rows} 行")
        return
    total = frame.height
    homepage = frame.filter(pl.col("homepage").is_not_null()).height
    paper = frame.filter(pl.col("paper_url").is_not_null()).height
    contact = frame.filter(pl.col("point_of_contact").is_not_null()).height
    citation = frame.filter(pl.col("citation_bibtex").is_not_null()).height
    print(f"  解析 {total:,} 条 · 表累计 {table_rows:,} 行")
    print(
        f"  Homepage {homepage:,} ({homepage / total * 100:.1f}%) · "
        f"Paper {paper:,} ({paper / total * 100:.1f}%) · "
        f"Point of Contact {contact:,} ({contact / total * 100:.1f}%) · "
        f"BibTeX {citation:,} ({citation / total * 100:.1f}%)"
    )
