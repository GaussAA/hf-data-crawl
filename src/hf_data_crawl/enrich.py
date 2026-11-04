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

from .api import MAX_LIMIT, HfDatasetApi
from .collect import IDENTITY_EXPAND
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


class Enricher:
    """按下载量取头部若干名，补抓卡片字段并入表。"""

    def __init__(self, api: HfDatasetApi, store: DatasetStore) -> None:
        self._api = api
        self._store = store

    def run(self, snapshot_date: date, *, top: int = 20_000, limit: int = MAX_LIMIT) -> EnrichSummary:
        started = time.monotonic()
        page_budget = max(1, math.ceil(top / max(1, limit)))

        pages = 0
        records = 0
        with_card = 0

        for index, page in enumerate(
            self._api.scan(
                sort="downloads",
                direction="-1",
                limit=limit,
                expand=ENRICH_EXPAND,
                max_pages=page_budget,
            ),
            1,
        ):
            pages = index
            records += len(page.records)
            with_card += sum(
                1
                for record in page.records
                if isinstance(record.get("cardData"), dict) and record["cardData"]
            )
            self._store.write_raw(
                snapshot_date=snapshot_date,
                page_index=index,
                records=page.records,
                meta={"sort": "downloads", "direction": "-1", "mode": "enrich"},
                label="card",
            )

        table_rows = self._store.build_enrich(snapshot_date)

        return EnrichSummary(
            snapshot_date=snapshot_date,
            pages=pages,
            records=records,
            with_card=with_card,
            table_rows=table_rows,
            elapsed_seconds=time.monotonic() - started,
        )
