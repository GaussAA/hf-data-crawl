"""按需补齐 cardData。

单库详情接口即使带 `blobs=false` 仍可达 1.17MB（含 siblings），逐条补不可行；
唯一可行路径是用列表接口带 `expand[]=cardData` 按下载量取头部 N 名。
因此 enrich 是「按榜单补」而非「按 id 补」。
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import date

from .api import MAX_LIMIT, HfDatasetApi, Page
from .collect import IDENTITY_EXPAND
from .raw import CARD_LABEL
from .scan import ScanRun, ScanSpec
from .store import DatasetStore

#: 比 lean 档多要三样：卡片本体、论文关联、引用文本。
#: 基座沿用 IDENTITY_EXPAND 而非另抄一份：两档字段一旦分叉，同一日若既有 part
#: 又有 card 分区，Bronze 会按文件名去重，缺字段的那份可能胜出，导致整列为空。
ENRICH_EXPAND: tuple[str, ...] = (
    *IDENTITY_EXPAND,
    "cardData",
    "paperswithcode_id",
    "citation",
)


@dataclass(frozen=True)
class EnrichSummary:
    snapshot_date: date
    pages: int
    records: int
    with_card: int
    table_rows: int
    elapsed_seconds: float
    config_rows: int = 0
    with_structure: int = 0


class Enricher:
    """按下载量取头部若干名，补抓卡片字段并入表。"""

    def __init__(self, api: HfDatasetApi, store: DatasetStore) -> None:
        self._api = api
        self._store = store

    def run(
        self, snapshot_date: date, *, top: int = 20_000, limit: int = MAX_LIMIT
    ) -> EnrichSummary:
        started = time.monotonic()
        page_budget = max(1, math.ceil(top / max(1, limit)))

        with_card = 0
        with_structure = 0

        def tally(page: Page, _index: int) -> str | None:
            """统计有卡片的条数，顺带记下多少带结构明细（行数/列结构的唯一来源）。"""
            nonlocal with_card, with_structure
            for record in page.records:
                card = record.get("cardData")
                if not isinstance(card, dict) or not card:
                    continue
                with_card += 1
                if isinstance(card.get("dataset_info"), list) and card["dataset_info"]:
                    with_structure += 1
            return None

        scan = ScanRun(self._api, self._store).run(
            snapshot_date,
            ScanSpec(
                sort="downloads",
                expand=ENRICH_EXPAND,
                mode="enrich",
                label=CARD_LABEL,
                limit=limit,
                max_pages=page_budget,
                on_page=tally,
            ),
        )

        table_rows, config_rows = self._store.build_enrich(snapshot_date)

        return EnrichSummary(
            snapshot_date=snapshot_date,
            pages=scan.pages,
            records=scan.records,
            with_card=with_card,
            table_rows=table_rows,
            elapsed_seconds=time.monotonic() - started,
            config_rows=config_rows,
            with_structure=with_structure,
        )
