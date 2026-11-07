"""日增量采集：串起 API 遍历、Raw 落盘、派生与状态更新。

停止条件有三：走到末页、连续 N 页无新记录（watermark）、达到页数上限（有界试跑）。
其中「有界」不是完成态，必须保住游标以便下次续跑。
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import date
from typing import Any

from .api import IDENTITY_EXPAND, MAX_LIMIT, HfDatasetApi, Page
from .checkpoint import FileCheckpoint
from .derive import derive_day
from .scan import ScanRun, ScanSpec, ScanSummary
from .store import DatasetStore
from .watermark import WatermarkCheckpoint, extract_last_modified

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

        known: set[str] = set(self._store.read_current()["id"].to_list())

        new_ids = 0
        streak = 0
        current_watermark = checkpoint.watermark

        def track(page: Page, _index: int) -> str | None:
            """双重 watermark：既看新 id、也看时间戳。"""
            nonlocal new_ids, streak, current_watermark

            fresh = sum(1 for record in page.records if record["id"] not in known)
            known.update(record["id"] for record in page.records)
            new_ids += fresh

            # 时间戳 watermark：本页最老记录仍晚于上次最新，说明已越过旧数据边界
            timestamps = extract_last_modified(page.records)
            if current_watermark.is_stale(timestamps):
                return "watermark_timestamp"

            # 更新水位线（即使未触发停止，也要记录本页最新时间戳）
            current_watermark = current_watermark.update(timestamps)
            # 只写水位线：此刻 checkpoint.cursor 还是上一页的，本页的 next_cursor
            # 由 ScanRun 在钩子之后统一保存。旧写法把这个 cursor 传给 save，
            # 首轮它恒为 None，被 save 当成「完成」信号，刚写的水线当场清空。
            checkpoint.save_watermark(current_watermark)

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

        day = derive_day(self._store, snapshot_date)
        bronze_rows = day.bronze_rows
        silver_rows = day.silver_rows
        state = day.state

        # 只有「走到末页」与「触发 watermark」算本轮完成；有界停止要保住游标。
        # 完成只作废游标，水位线留给下一轮判断新旧边界（见 WatermarkCheckpoint.finish）。
        if scan.stopped_reason in {
            "end",
            "watermark_timestamp",
            "watermark_no_new",
        }:
            checkpoint.finish()

        return CollectSummary(
            snapshot_date=snapshot_date,
            pages=scan.pages,
            records=scan.records,
            skipped=scan.skipped,
            new_ids=new_ids,
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            appended_observations=state.appended,
            current_rows=state.current_rows,
            rate_remaining=scan.rate_remaining,
            stopped_reason=scan.stopped_reason,
            elapsed_seconds=time.monotonic() - started,
            top_pages=top.pages,
            refreshed=top.records,
        )
