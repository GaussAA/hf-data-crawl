"""`acquire` 切片的命令处理函数，参数解析与分派在 ops/cli。"""

from __future__ import annotations

import argparse
import sys
from datetime import date

import polars as pl

from ..pipeline import rebuild, state
from ..platform.env import load_env_file
from ..platform.lock import exclusive
from ..platform.presets import get_preset
from ..platform.runs import COLLECT_COMPLETE_REASONS, Outcome, read_runs
from ..platform.store import DatasetStore
from ..platform.tables import README_SCHEMA
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
from .files import FileFetcher, FileSummary, HfFilesApi
from .hf.api import HfDatasetApi, Transport
from .hf.transport import HttpxTransport
from .readme import (
    README_STAGING_DIR,
    HfReadmeApi,
    ReadmeFetcher,
    ReadmeSummary,
    keep_previous_ok,
    stale_readme_ids,
)
from .reconcile import PROBE_DEGRADE_AFTER, Reconciler, ReconcileSummary
from .sizes import (
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
            rebuild.rebuild_from_raw(store, days=days)
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
    refresh: set[str] = set()
    if args.changed > 0:
        stale = stale_ids(current, store.read("size"))
        picked = stale.sort("downloads", descending=True, nulls_last=True).head(args.changed)
        refresh = set(picked["id"].to_list())
        if not refresh:
            print("规模无陈旧：没有内容变更晚于抓取日的库。")
            return Outcome({"changed": 0, "refreshed": 0})
        ordered = ordered.filter(pl.col("id").is_in(sorted(refresh)))
        print(f"规模随变化刷新：{len(refresh)} 个库内容已变，重取其规模。")

    fetcher = SizeFetcher(
        api=HfSizeApi(transport=_open_transport(transport)),
        workers=args.workers,
        progress=_progress_line,
        heartbeat=_stall_line,
    )
    writer = StagingWriter(store, snapshot)
    candidates = ordered["id"].to_list()
    summary, frame = fetcher.run(
        snapshot,
        dataset_ids=candidates,
        top=resolve_size_top(args.top, len(candidates), refresh),
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


def run_fetch_files(
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
        heartbeat=lambda done, total: _stall_line(done, total, "补齐文件清单"),
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
    existing = store.read("readme")
    staged, _ = read_staged_dir(store, README_STAGING_DIR, README_SCHEMA)
    if staged.height:
        existing = pl.concat([existing, staged], how="vertical_relaxed")

    ordered = current.sort("downloads", descending=True, nulls_last=True)
    # README 是仓库里的一个文件：commit 变了，旧全文就不再是当前版本。
    # `--changed N` 只重取这批里的前 N 个（按下载量），全量那一趟留给人工择机跑。
    refresh: set[str] = set()
    if args.changed > 0:
        stale = stale_readme_ids(current, store.read("readme"))
        picked = stale.sort("downloads", descending=True, nulls_last=True).head(args.changed)
        refresh = set(picked["id"].to_list())
        if not refresh:
            print("README 无陈旧：没有 commit 变过的库。")
            return Outcome({"changed": 0, "refreshed": 0})
        ordered = ordered.filter(pl.col("id").is_in(sorted(refresh)))
        print(f"README 随内容刷新：{len(refresh)} 个库 commit 已变，重取其全文。")

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
            transport=_files_transport(transport, args.timeout),
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
            store.upsert("readme", keep_previous_ok(store.read("readme"), merged))
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
