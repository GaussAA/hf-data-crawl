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

from .api import MAX_LIMIT, HfDatasetApi
from .checkpoint import FileCheckpoint
from .store import DatasetStore

#: Lean 档：不含 cardData。实测 full 档约 6 KB/条、带宽 90–170 KB/s，
#: 日增量若拉全量字段，光传输就要数小时，而增量的信息量几乎全在指标字段。
LEAN_EXPAND: tuple[str, ...] = (
    "author",
    "downloads",
    "downloadsAllTime",
    "likes",
    "gated",
    "sha",
    "createdAt",
    "lastModified",
    "description",
    "trendingScore",
    "tags",
)

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
        threshold = max(1, pages_without_new if pages_without_new is not None else self._pages_without_new)
        started = time.monotonic()
        checkpoint = (
            None
            if dry_run
            else FileCheckpoint(self._store.state_dir / self._checkpoint_filename)
        )
        known: set[str] = set() if dry_run else set(self._store.read_current()["id"].to_list())

        pages = 0
        records = 0
        skipped = 0
        new_ids = 0
        streak = 0
        rate_remaining: int | None = None
        preview: list[dict[str, Any]] | None = None
        stopped_reason = "end"

        for current_page in self._api.scan(
            sort="lastModified",
            direction="-1",
            limit=limit,
            expand=LEAN_EXPAND,
            checkpoint=checkpoint,
        ):
            if current_page.rate_remaining is not None:
                rate_remaining = current_page.rate_remaining

            if dry_run:
                preview = current_page.records[:5]
                stopped_reason = "dry_run"
                break

            pages += 1
            records += len(current_page.records)
            skipped += current_page.skipped

            self._store.write_raw(
                snapshot_date=snapshot_date,
                page_index=pages,
                records=current_page.records,
                meta={"sort": "lastModified", "direction": "-1", "mode": "lean"},
            )

            fresh = sum(1 for record in current_page.records if record["id"] not in known)
            known.update(record["id"] for record in current_page.records)
            new_ids += fresh
            streak = 0 if fresh else streak + 1

            # 游标由本层保存而非依赖生成器恢复时保存：中途 break 时生成器已暂停，
            # 若等它自己落盘就会少记一页，导致续跑重抓上一页。
            if checkpoint is not None:
                checkpoint.save(current_page.next_cursor)

            if streak >= threshold:
                stopped_reason = "watermark"
                break
            if max_pages is not None and pages >= max_pages:
                stopped_reason = "max_pages"
                break

        if dry_run:
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
                rate_remaining=rate_remaining,
                stopped_reason=stopped_reason,
                elapsed_seconds=time.monotonic() - started,
                preview=preview,
            )

        # 顶部池刷新：按全站下载量取前 N 名补扫一遍。
        # 日增量只覆盖「最近更新的万把条」，增速榜因此长期取不到第二个点；
        # 补这一趟，头部集每天都有新读数，而成本只有二十来页。
        top_pages_done = 0
        refreshed = 0
        if refresh_top > 0:
            page_budget = max(1, math.ceil(refresh_top / max(1, limit)))
            for top_index, top_page in enumerate(
                self._api.scan(
                    sort="downloads",
                    direction="-1",
                    limit=limit,
                    expand=LEAN_EXPAND,
                    max_pages=page_budget,
                ),
                1,
            ):
                top_pages_done = top_index
                refreshed += len(top_page.records)
                self._store.write_raw(
                    snapshot_date=snapshot_date,
                    page_index=top_index,
                    records=top_page.records,
                    meta={"sort": "downloads", "direction": "-1", "mode": "top-refresh"},
                    label="top",
                )

        bronze_rows = self._store.build_bronze(snapshot_date)
        silver_rows = self._store.build_silver(snapshot_date)
        state = self._store.update_state(snapshot_date)

        # 只有「走到末页」与「触发 watermark」算本轮完成；有界停止要保住游标。
        if checkpoint is not None and stopped_reason in {"end", "watermark"}:
            checkpoint.save(None)

        return CollectSummary(
            snapshot_date=snapshot_date,
            pages=pages,
            records=records,
            skipped=skipped,
            new_ids=new_ids,
            bronze_rows=bronze_rows,
            silver_rows=silver_rows,
            appended_observations=state.appended,
            current_rows=state.current_rows,
            rate_remaining=rate_remaining,
            stopped_reason=stopped_reason,
            elapsed_seconds=time.monotonic() - started,
            top_pages=top_pages_done,
            refreshed=refreshed,
        )
