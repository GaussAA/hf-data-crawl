"""按需补齐 cardData。

单库详情接口即使带 `blobs=false` 仍可达 1.17MB（含 siblings），逐条补不可行；
唯一可行路径是用列表接口带 `expand[]=cardData` 取头部 N 名或按最后修改时间扫。

两条路各有分工，不可互相替代：

- `Enricher`（按下载量）：补齐**从没抓过卡片的头部**。sha 多年不变的库永远不会
  进入增量范围，只能靠榜单一次性覆盖。
- `scan_changed_cards`（按 lastModified）：**跟着日增量走**。变化的集合不需要另建
  索引——`collect` 每天本来就按 `sort=lastModified` 扫到水位线为止，那批页就是
  「今天变过的库」。重翻同一批页、`expand` 多带 `cardData` 即可。

为什么不能先按 id 列表抓：列表接口只能 sort + 翻页，不接受 id 过滤；单库详情接口
带 `siblings` 达 1.2MB/条，两万条要上百 GB。
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import date

from ..hf.api import IDENTITY_EXPAND, MAX_LIMIT, HfDatasetApi, Page
from ..hf.scan import ScanRun, ScanSpec, ScanSummary
from ..hf.watermark import Watermark, extract_last_modified
from ..pipeline import card
from ..pipeline.raw import CARD_LABEL
from ..platform.store import DatasetStore

#: 比 lean 档多要卡片本体；论文关联与引用文本已在 `IDENTITY_EXPAND` 基座里，
#: 这里不再重复列出（重复会让请求发出同名的 expand 键）。
#: 基座沿用 IDENTITY_EXPAND 而非另抄一份：两档字段一旦分叉，同一日若既有 part
#: 又有 card 分区，Bronze 按 `written_at` 定序取最后写入者，缺字段的那份可能
#: 胜出导致整列为空。ScanSpec 已在构造期强制「expand 必含基座」。
ENRICH_EXPAND: tuple[str, ...] = (*IDENTITY_EXPAND, "cardData")

#: 变化集卡片扫描的页数上限（20 万条）。防的是水位线缺失或异常时把全站 1117 页
#: 一次拉完（约 6.7 GB、14 小时）。正常情况下它远用不到：日增量只有二十来页。
CARD_PAGES_CAP = 200


def scan_changed_cards(
    api: HfDatasetApi,
    store: DatasetStore,
    snapshot_date: date,
    *,
    baseline: Watermark,
    limit: int = MAX_LIMIT,
    max_pages: int | None = CARD_PAGES_CAP,
) -> ScanSummary:
    """按 `lastModified` 变化集补抓卡片，与日增量共用同一条边界。

    边界由调用方传入**本轮开始时**的水位线，而不是跑完 lean 趟之后的新值：
    后者已被本轮自己的记录推高，`is_stale` 会在第一页就成立，等于什么都没抓。

    写 `card-*` 批次而非 `part`：卡片记录不进 Bronze 去重池（否则 Bronze 的行数
    会随补抓节奏膨胀），但仍属当日真实观测，由状态层补位。
    """

    def stop_at_baseline(page: Page, _index: int) -> str | None:
        return (
            "watermark_timestamp"
            if baseline.is_stale(extract_last_modified(page.records))
            else None
        )

    return ScanRun(api, store).run(
        snapshot_date,
        ScanSpec(
            sort="lastModified",
            expand=ENRICH_EXPAND,
            mode="card-refresh",
            label=CARD_LABEL,
            limit=limit,
            max_pages=max_pages,
            on_page=stop_at_baseline,
        ),
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

        table_rows, config_rows = card.build(self._store, snapshot_date)

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
