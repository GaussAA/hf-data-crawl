"""一轮扫描：翻页 → 落盘 → 钩子 → 存游标 → 判停止。

为什么需要它
------------
这条时序原本有四份实现（日增量、顶部刷新、全量对账、卡片补齐），差异只存在于
代码顺序里——页码从 `pages += 1` 还是 `enumerate(..., 1)` 起、什么时候保存游标、
结束后清不清、`label` 用哪个、跑哪几步派生。于是读「collect 到底做了什么」
必须同时打开四个文件。

更实际的问题是：游标必须在**每页写完盘之后立刻保存**。调用方一旦在下一页
之前 `break`，生成器就停在那里，等它自己落盘会少记一页，续跑时重抓上一页
（`api.scan` 里那句 `checkpoint.save` 在 `yield` 之后才执行）。这条知识原本
靠 collect.py 的一段注释维系，另外三份实现各写各的。

本轮 ScanRun 把时序收到一处，四个调用方只提供策略。
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import date
from typing import Protocol

from .api import IDENTITY_EXPAND, MAX_LIMIT, Page
from .checkpoint import Checkpoint
from .pipeline import raw
from .platform.store import DatasetStore

#: 每页钩子。返回非 None 表示「到此为止」，返回值即停止原因。
#: 钩子在保存游标之前执行，可能因续跑被重复调用，副作用须幂等。
PageHook = Callable[[Page, int], str | None]


class ScanApi(Protocol):
    """ScanRun 只依赖「按排序翻页」这一能力，不依赖具体 HTTP 实现。

    生产实现 HfDatasetApi 与测试替身都按结构满足它，替身无需继承具体类。
    """

    def scan(
        self,
        *,
        sort: str,
        direction: str,
        limit: int,
        expand: Sequence[str],
        checkpoint: Checkpoint | None,
    ) -> Iterator[Page]: ...


@dataclass(frozen=True)
class ScanSpec:
    """一轮扫描的策略。

    差异只有这十个字段，全部是调用方真正需要决定的事；时序本身不在此列。
    """

    sort: str
    expand: tuple[str, ...]
    #: 语义名，落进 raw 的 meta，便于日后看出这一批是怎么扫出来的。
    mode: str
    label: str = "part"
    direction: str = "-1"
    limit: int = MAX_LIMIT
    #: 页数上限；None 表示扫到末页。由上层的 ScanRun 判定而非交给生成器，
    #: 这样才能给出准确的停止原因（生成器只会在到限时静默 return）。
    max_pages: int | None = None
    #: 游标；None 表示这类重扫不需要续跑（顶部刷新、卡片补齐都是幂等的）。
    checkpoint: Checkpoint | None = None
    #: 是否落盘。唯一的 False 用途是 dry-run 预览。
    write: bool = True
    on_page: PageHook | None = None

    def __post_init__(self) -> None:
        """写入 Raw 的扫描不得比 Bronze 基座更窄（见 api.IDENTITY_EXPAND）。"""
        missing = set(IDENTITY_EXPAND) - set(self.expand)
        if missing:
            raise ValueError(
                f"ScanSpec.expand 缺少 Bronze 基座字段 {sorted(missing)}："
                "不同来源共享同一去重池，缺字段的后写分块会覆盖完整记录，整列静默变空"
            )

    @property
    def source(self) -> str:
        """命名空间：由 mode + sort 派生，写进 Raw 文件名与 meta。

        调用方不必手写，也不会与文件名漂移；同一天不同目的的扫描因此各有
        独立的页码空间，归档后仍能从文件名认出「这批是谁扫的」。
        """
        return raw.source_slug(self.mode, self.sort)


@dataclass(frozen=True)
class ScanSummary:
    pages: int
    records: int
    skipped: int
    rate_remaining: int | None
    stopped_reason: str


class ScanRun:
    """翻页扫描的唯一实现。"""

    def __init__(self, api: ScanApi, store: DatasetStore) -> None:
        self.api = api
        self.store = store

    def run(self, snapshot_date: date, spec: ScanSpec) -> ScanSummary:
        pages = 0
        records = 0
        skipped = 0
        rate_remaining: int | None = None
        stopped_reason = "end"

        for page in self.api.scan(
            sort=spec.sort,
            direction=spec.direction,
            limit=spec.limit,
            expand=spec.expand,
            checkpoint=spec.checkpoint,
        ):
            pages += 1
            records += len(page.records)
            skipped += page.skipped
            if page.rate_remaining is not None:
                rate_remaining = page.rate_remaining

            if spec.write:
                raw.write(
                    self.store,
                    snapshot_date=snapshot_date,
                    page_index=pages,
                    records=page.records,
                    meta={
                        "sort": spec.sort,
                        "direction": spec.direction,
                        "mode": spec.mode,
                        "source": spec.source,
                    },
                    label=spec.label,
                    source=spec.source,
                )

            # 顺序：写盘 → 钩子 → 存游标 → 判停止。
            # 钩子在存游标之前：钩子可能带副作用（如对账追加已见集合），若游标先存、
            # 钩子后跑，两者之间被打断，续跑会从下一页开始而这一页的副作用丢了。
            # 反过来最坏只是重做一页的副作用，要求钩子幂等即可。
            hint = spec.on_page(page, pages) if spec.on_page is not None else None

            # 必须在任何 break 之前保存：调用方随时可能在下一页之前停下，
            # 而生成器此刻已暂停，它内部那句 save 不会执行。
            if spec.checkpoint is not None:
                spec.checkpoint.save(page.next_cursor)

            if hint is not None:
                stopped_reason = hint
                break

            if spec.max_pages is not None and pages >= spec.max_pages:
                stopped_reason = "max_pages"
                break

        return ScanSummary(
            pages=pages,
            records=records,
            skipped=skipped,
            rate_remaining=rate_remaining,
            stopped_reason=stopped_reason,
        )
