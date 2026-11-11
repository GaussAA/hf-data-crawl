"""日增量采集：串起 API 遍历、Raw 落盘、派生与状态更新。

停止条件有三：走到末页、连续 N 页无新记录（watermark）、达到页数上限（有界试跑）。
其中「有界」不是完成态，必须保住游标以便下次续跑。

卡片随带（`cards=True`）
------------------------
同一批「今天变过的库」再扫一趟，`expand` 多带 `cardData`，写进 `card-*` 批次。
驱动字段是 `lastModified` 而非 sha：HF 没有「给我这批 id 的卡片」的接口，
先算出哪些 sha 变了再去抓并无路可走；而 sha 变必然伴随一次 commit，
`lastModified` 必变，日增量本来就按它排序——变化集现成就在手里，不必另建索引。
`source_sha` 只用来记账与核对（见 `card.extract_card_row`）。

没有水位线时**不跑**这一趟：首轮没有「变化」可言，抓全站是 6.7 GB、14 小时的事，
属一次性补齐，不该混进日常增量。
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

from . import state
from .api import IDENTITY_EXPAND, MAX_LIMIT, HfDatasetApi, Page
from .checkpoint import FileCheckpoint
from .derive import derive_day
from .enrich import CARD_PAGES_CAP, scan_changed_cards
from .runs import COLLECT_COMPLETE_REASONS
from .scan import ScanRun, ScanSpec, ScanSummary
from .store import DatasetStore
from .watermark import WatermarkCheckpoint, _is_reasonable_timestamp, extract_last_modified

#: 身份与指标基座定义在 `api.IDENTITY_EXPAND`（与 expand[] 白名单同处）：
#: 任何写入 Raw 的扫描都必须含它，ScanSpec 在构造期校验。

#: Lean 档：不含 cardData。实测满档约 6 KB/条、带宽 90–170 KB/s，
#: 日增量若拉全量字段，光传输就要数小时，而增量的信息量几乎全在指标字段。
#: 补齐后实测 980 B/条（原 923 B），只多 6.2%，却换来体积与真实私有标记。
LEAN_EXPAND: tuple[str, ...] = IDENTITY_EXPAND

CHECKPOINT_FILENAME = "collect-checkpoint.json"
WATERMARK_FILENAME = "collect-watermark.txt"


@dataclass(frozen=True)
class CollectSummary:
    """一轮采集的产出摘要，供 CLI 打印与后续运行报告使用。"""

    snapshot_date: date
    pages: int
    records: int
    skipped: int
    new_ids: int
    bronze_rows: int
    silver_rows: int
    appended_observations: int
    current_rows: int
    rate_remaining: int | None
    stopped_reason: str
    elapsed_seconds: float
    preview: list[dict[str, Any]] | None = None
    top_pages: int = 0
    refreshed: int = 0
    #: 变化集卡片扫描的页数与记录数；未开启时为 0。
    card_pages: int = 0
    card_records: int = 0
    #: 卡片趟的停止原因；`skipped` 表示没开，`no_watermark` 表示首轮无变化集。
    card_reason: str = "skipped"


class Collector:
    """日增量入口。"""

    def __init__(
        self,
        api: HfDatasetApi,
        store: DatasetStore,
        *,
        pages_without_new: int = 3,
        checkpoint_filename: str = CHECKPOINT_FILENAME,
    ) -> None:
        self._api = api
        self._store = store
        self._pages_without_new = max(1, pages_without_new)
        self._checkpoint_filename = checkpoint_filename

    def run(
        self,
        snapshot_date: date,
        *,
        limit: int = MAX_LIMIT,
        max_pages: int | None = None,
        pages_without_new: int | None = None,
        refresh_top: int = 0,
        cards: bool = False,
        card_max_pages: int | None = CARD_PAGES_CAP,
        dry_run: bool = False,
    ) -> CollectSummary:
        threshold = max(
            1, pages_without_new if pages_without_new is not None else self._pages_without_new
        )
        started = time.monotonic()
        runner = ScanRun(self._api, self._store)

        # 试跑提前返回：此后每条路径都确定要落盘，断点与水线不再有「可能为 None」
        # 这种状态。原先两类运行混在一条函数里，每个使用点都得写一层
        # `if checkpoint is not None` 才能骗过类型检查——而那层兜底恰恰遮住了
        # 「非试跑必有断点」这条不变式：真断了会静默跳过保存，不报错。
        if dry_run:
            preview: list[dict[str, Any]] | None = None

            def capture(page: Page, _index: int) -> str | None:
                nonlocal preview
                preview = page.records[:5]
                return "dry_run"

            scan = runner.run(
                snapshot_date,
                ScanSpec(
                    sort="lastModified",
                    expand=LEAN_EXPAND,
                    mode="dry-run",
                    limit=limit,
                    write=False,
                    on_page=capture,
                ),
            )
            return CollectSummary(
                snapshot_date=snapshot_date,
                pages=0,
                records=0,
                skipped=0,
                new_ids=0,
                bronze_rows=0,
                silver_rows=0,
                appended_observations=0,
                current_rows=0,
                rate_remaining=scan.rate_remaining,
                stopped_reason=scan.stopped_reason,
                elapsed_seconds=time.monotonic() - started,
                preview=preview,
            )

        # 水位线机制：基于时间戳而非「连续 N 页无新 id」。
        base_checkpoint = FileCheckpoint(self._store.state_dir / self._checkpoint_filename)
        checkpoint = WatermarkCheckpoint(
            base_checkpoint, self._store.state_dir / WATERMARK_FILENAME
        )

        known: set[str] = set(state.read_current(self._store)["id"].to_list())

        # 整轮固定用开始时加载的水位线判断边界。若拿本轮已更新的值来比，
        # 跨日更新会被提前截断：第一页之后水位线跳到今天，旧边界反而显得“已越过”。
        baseline = checkpoint.watermark
        baseline_time = baseline.latest_modified
        new_ids = 0
        streak = 0
        current_watermark = baseline

        def track(page: Page, _index: int) -> str | None:
            """双重 watermark：时间戳看边界，新 id 作为后备。"""
            nonlocal new_ids, streak, current_watermark

            timestamps = extract_last_modified(page.records)
            page_new = sum(1 for record in page.records if record["id"] not in known)
            # 新 id 或 lastModified 晚于基线，都说明本页仍有新信息；二者任一成立
            # 就不计入 streak。否则跨日的稀疏更新会被“连续 N 页无新 id”提前截断。
            fresh = page_new + sum(
                1
                for record, timestamp in zip(page.records, timestamps)
                if record["id"] in known
                and baseline_time is not None
                and timestamp
                and _is_reasonable_timestamp(timestamp)
                and timestamp > baseline_time
            )
            new_ids += page_new
            known.update(record["id"] for record in page.records)

            # 列表按 lastModified 倒序：页内最老记录严格早于基线，才说明越过旧边界。
            # 无论是否停止都把本页最新值并入，供完成态统一持久化。
            stale = baseline.is_stale(timestamps)
            current_watermark = current_watermark.update(timestamps)
            if stale:
                return "watermark_timestamp"

            # 传统 watermark：连续 N 页无新 id（保留作为后备）
            streak = 0 if fresh else streak + 1
            return "watermark_no_new" if streak >= threshold else None

        scan = runner.run(
            snapshot_date,
            ScanSpec(
                sort="lastModified",
                expand=LEAN_EXPAND,
                mode="lean",
                limit=limit,
                max_pages=max_pages,
                checkpoint=checkpoint,
                on_page=track,
            ),
        )

        # 顶部池刷新：日增量只覆盖「最近更新的万把条」，增速榜因此长期取不到
        # 第二个观测点；补这一趟，头部集每天都有新读数，成本只有二十来页。
        top = ScanSummary(
            pages=0, records=0, skipped=0, rate_remaining=None, stopped_reason="skipped"
        )
        if refresh_top > 0:
            page_budget = max(1, math.ceil(refresh_top / max(1, limit)))
            top = runner.run(
                snapshot_date,
                ScanSpec(
                    sort="downloads",
                    expand=LEAN_EXPAND,
                    mode="top-refresh",
                    label="top",
                    limit=limit,
                    max_pages=page_budget,
                ),
            )

        # 变化集卡片：同一批「今天变过的库」再扫一趟，多带 cardData。
        # 用本轮开始时的 baseline，不能用刚被本轮推高的新水位线——后者会在第一页
        # 就判定越界，等于什么都没抓。没有水位线（首轮）时不做：那不是增量，是全量。
        card_scan = ScanSummary(
            pages=0, records=0, skipped=0, rate_remaining=None, stopped_reason="skipped"
        )
        card_reason = "skipped"
        if cards:
            if baseline_time is None:
                card_reason = "no_watermark"
            else:
                card_scan = scan_changed_cards(
                    self._api,
                    self._store,
                    snapshot_date,
                    baseline=baseline,
                    limit=limit,
                    max_pages=card_max_pages,
                )
                card_reason = card_scan.stopped_reason

        day = derive_day(self._store, snapshot_date, enrich=cards)
        bronze_rows = day.bronze_rows
        silver_rows = day.silver_rows
        day_state = day.state

        # 完成态才推进水位线：走到末页说明本轮到尾，时间戳/无新 id 说明已越过
        # 旧边界。有界停止（max_pages）要保住游标，水位线也不能前进，否则续跑会
        # 误以为边界已经越过。完成只作废游标，水位线留给下一轮判断新旧边界。
        if scan.stopped_reason in COLLECT_COMPLETE_REASONS:
            checkpoint.save_watermark(current_watermark)
            checkpoint.finish()

        return CollectSummary(
            snapshot_date=snapshot_date,
            pages=scan.pages,
            records=scan.records,
            skipped=scan.skipped,
            new_ids=new_ids,
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            appended_observations=day_state.appended,
            current_rows=day_state.current_rows,
            rate_remaining=scan.rate_remaining,
            stopped_reason=scan.stopped_reason,
            elapsed_seconds=time.monotonic() - started,
            top_pages=top.pages,
            refreshed=top.records,
            card_pages=card_scan.pages,
            card_records=card_scan.records,
            card_reason=card_reason,
        )
