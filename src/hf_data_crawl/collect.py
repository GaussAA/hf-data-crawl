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

from .api import MAX_LIMIT, HfDatasetApi, Page
from .checkpoint import FileCheckpoint
from .scan import ScanRun, ScanSpec, ScanSummary
from .store import DatasetStore

#: 身份与指标基座：一次请求拿全。含体积（mainSize）、私有与禁用标记、趋势分。
#:
#: 铁律——HF 的 `expand[]` 是**白名单裁剪**而非增补：只要传了它，未列出的字段
#: 一律不返回，连默认响应里本就有的 `private` / `disabled` 都会被裁掉
#: （实测 `expand[]=downloads` 时返回体只剩 `_id/downloads/id`）。
#: 因此本清单必须与 `store.BRONZE_SCHEMA` 的源字段严格同步，
#: 由 `tests/test_schema_contract.py` 强制校验，防止再出现「列声明了但源端不供」
#: 从而整列为空却不报错的静默缺陷（2026-11-04 实测 private 曾 100% 为空）。
IDENTITY_EXPAND: tuple[str, ...] = (
    "author",
    "createdAt",
    "description",
    "disabled",
    "downloads",
    "downloadsAllTime",
    "gated",
    "lastModified",
    "likes",
    "mainSize",
    "private",
    "resourceGroup",
    "sha",
    "tags",
    "trendingScore",
    "xetEnabled",
)

#: Lean 档：不含 cardData。实测满档约 6 KB/条、带宽 90–170 KB/s，
#: 日增量若拉全量字段，光传输就要数小时，而增量的信息量几乎全在指标字段。
#: 补齐后实测 980 B/条（原 923 B），只多 6.2%，却换来体积与真实私有标记。
LEAN_EXPAND: tuple[str, ...] = IDENTITY_EXPAND

CHECKPOINT_FILENAME = "collect-checkpoint.json"


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
        checkpoint = (
            None if dry_run else FileCheckpoint(self._store.state_dir / self._checkpoint_filename)
        )
        known: set[str] = set() if dry_run else set(self._store.read_current()["id"].to_list())

        new_ids = 0
        streak = 0
        preview: list[dict[str, Any]] | None = None

        def track(page: Page, _index: int) -> str | None:
            """维护 watermark：连续 threshold 页没有新 id 就停。"""
            nonlocal new_ids, streak
            fresh = sum(1 for record in page.records if record["id"] not in known)
            known.update(record["id"] for record in page.records)
            new_ids += fresh
            streak = 0 if fresh else streak + 1
            return "watermark" if streak >= threshold else None

        def capture(page: Page, _index: int) -> str | None:
            nonlocal preview
            preview = page.records[:5]
            return "dry_run"

        runner = ScanRun(self._api, self._store)

        if dry_run:
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
        top = ScanSummary(pages=0, records=0, skipped=0, rate_remaining=None, stopped_reason="skipped")
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

        bronze_rows = self._store.build_bronze(snapshot_date)
        silver_rows = self._store.build_silver(snapshot_date)
        state = self._store.update_state(snapshot_date)

        # 只有「走到末页」与「触发 watermark」算本轮完成；有界停止要保住游标。
        if checkpoint is not None and scan.stopped_reason in {"end", "watermark"}:
            checkpoint.save(None)

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
