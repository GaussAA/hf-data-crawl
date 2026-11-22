"""按 id 抓 README 全文。

为什么需要单独一条链路
----------------------
列表接口的 `description` 是 HF 在服务端截断过的摘要（约 500–600 字符，尾部
常带「… See the full description on the dataset page」），`cardData` 只有
YAML 头，正文不在任何一次列表响应里。全文只有一个来源：

    GET https://huggingface.co/datasets/<id>/raw/main/README.md

一次一个库，返回原始 Markdown（含 YAML 头）。

接口脾性（2026-11-17 实测 860 条 + 2026-11-22 复核配额）
----------------------------------------------------------
- 短时并发 12 可达 18.9–19.9 req/s，但**不能一直这么跑**：带令牌实测响应头是
  `RateLimit-Policy: "fixed window";"resolvers";q=3000;w=300`，即 **10 req/s**。
  撞上后 `retry-after` 约 58 秒。第一轮 6000 条只用了 2 分钟就撞线，之后每个
  请求都 429。
  旧注释写的是「5000 次 / 5 分钟 ≈ 16.7 req/s、默认取 15」，比真配额高 50%——
  那份读数来自撞墙前的瞬时吞吐，不是可持续值。现在交给 `pace.SITE_QUOTA` 压平，
  由它按窗口配额放行。
- 所以本模块**主动限速**而不是靠撞 429 后退避。20,000 条约 33 分钟；全量 113 万条
  （**每条都要发一次请求**，404 也占配额）约 31 小时，其中约 70% 能拿到正文。
  想更快只能换 PRO/Team 账号（配额更高），不是改并发能解决的。
- 命中率：200 约 70%、404（仓库没有 README）约 25%、401/403（门禁或资源
  被禁）约 5%。三种空必须分开记账——`absent` 是「作者本来没写」，`denied` 是
  「我们读不到」，`error` 是「这次没抓到」。
- `denied` 别指望带令牌重来一遍就能补上：全量 51,860 条里 join `current.gated`
  后 99.75% 是门禁库（`manual` / `auto`），且 `private` 无一为真——不是私有库，
  是公开但受限。带令牌抽 500 条实测零命中。见 `data/DATA.md` 第 18 条。
- 体积：中位 2.5 KB、均值 4.6 KB、p90 8.6 KB；全量约 3.6 GB 文本。

与 `size` / `files` 一样是 `Origin.NETWORK` 表：raw 里没有副本，删掉就要重抓；
抓取期间写锁外分片，收尾短锁合并（`fetch-readme` 全量要跑十几小时，整段持锁
会挡住当天的采集）。
"""

from __future__ import annotations

import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from ..platform.tables import README_SCHEMA, url_for
from .batch import collect_rows
from .hf.api import HfResponse, Transport, TransportError
from .hf.pace import BUCKET_SITE, SITE_QUOTA_WINDOW_SECONDS, Pacer, RateGate, shared
from .hf.retry import StatusPolicy, delay_seconds, exponential_seconds
from .merge import MergeSpec

#: 站点根（非 API 域）：README 正文在 `huggingface.co/<id>/raw/...` 下。
SITE_BASE_URL = "https://huggingface.co"

#: 默认限速由 `pace.SITE_QUOTA`（实测 `resolvers` 桶 3000 / 300s = 10 req/s）
#: 承担，构造函数不再自带默认速率。`rate` 只作覆盖用：`None` 走桶默认，`0` 完全
#: 不限（测试），`>0` 按该速率折算配额。
DEFAULT_RATE: float | None = None

#: 暂存目录（相对数据根）。与规模表的 `.staging/sizes` 并列，互不干扰。
README_STAGING_DIR = ".staging/readme"

#: 收尾合并的声明。
#:
#: `degraded` 只有 `error`：`absent` / `denied` 是明确结论，可以覆盖旧值（README
#: 可能真被删了或转私有），而一次瞬时故障不该抹掉几 KB 正文——抹掉之后该 id 仍
#: 「有行」，下次照样被跳过，等于永久丢全文。
#:
#: `previous_columns` 与 `streaming` 都是为表体积：正文全量 2.8 GB，整表读入会在
#: 收尾时把峰值推到十几 GB（2026-11-18 实测 16 GB 机器 OOM）。
README_MERGE = MergeSpec(
    table="readme",
    staging_dir=README_STAGING_DIR,
    schema=README_SCHEMA,
    lock="fetch-readme:merge",
    degraded=frozenset({"error"}),
    previous_columns=("id", "status"),
    streaming=True,
)

#: 这张端点的状态分区。三种「空」必须分开记账：`absent` 是作者本来没写，
#: `denied` 是我们读不到，`error` 是这次没抓到（不落库，下轮重试）。
#:
#: `denied` 别指望带令牌重来一遍就能补上：全量 51,860 条里 join `current.gated`
#: 后 99.75% 是门禁库（`manual` / `auto`），且 `private` 无一为真——不是私有库，
#: 是公开但受限。带令牌抽 500 条实测零命中。要重试它得显式 `--retry-denied`。
POLICY = StatusPolicy(
    retryable=frozenset({429, 500, 502, 503, 504}),
    permanent={401: "denied", 403: "denied", 404: "absent"},
)


@dataclass(frozen=True)
class ReadmeResult:
    """单库 README 抓取结果。`payload_bytes` 用于量化真实流量，不落库。"""

    dataset_id: str
    status: str
    row: dict[str, Any]
    payload_bytes: int = 0


@dataclass(frozen=True)
class ReadmeSummary:
    snapshot_date: date
    requested: int
    fetched: int
    absent: int
    denied: int
    failed: int
    skipped: int
    table_rows: int
    downloaded_bytes: int
    elapsed_seconds: float


def _resolve_gate(rate: float | None, pacer: RateGate | None) -> RateGate | None:
    """把「注入的闸 / 速率覆盖 / 桶默认」收成一个判据。

    返回 `None` 表示不限速——只该出现在测试与排障里。
    """
    if pacer is not None:
        return pacer
    if rate == 0:
        return None
    if rate is None:
        return shared(BUCKET_SITE)
    # 显式给了速率就不再挂共享闸：两者叠加会各买一张票，实际速率只有一半。
    return Pacer(
        quota=max(1, round(rate * SITE_QUOTA_WINDOW_SECONDS)),
        window=SITE_QUOTA_WINDOW_SECONDS,
    )


class HfReadmeApi:
    """单库 README 全文查询：列表接口拿不到的那部分正文。"""

    def __init__(
        self,
        transport: Transport,
        *,
        base_url: str = SITE_BASE_URL,
        max_retries: int = 2,
        retry_base_delay: float = 2.0,
        sleeper: Callable[[float], None] | None = None,
        rate: float | None = DEFAULT_RATE,
        pacer: RateGate | None = None,
    ) -> None:
        self._transport = transport
        self._base_url = base_url.rstrip("/")
        self._max_retries = max(0, max_retries)
        self._retry_base_delay = retry_base_delay
        self._sleeper = sleeper or time.sleep
        self._pacer = _resolve_gate(rate, pacer)

    def fetch(
        self,
        dataset_id: str,
        snapshot_date: date,
        source_sha: str | None = None,
    ) -> ReadmeResult:
        """抓一个库的 README；永不抛异常，失败落 `error` 行等下一轮。"""
        for attempt in range(self._max_retries + 1):
            # 每次真正发请求（含重试）都先买一张票：配额按请求数算，不按「条目数」算。
            if self._pacer is not None:
                self._pacer.wait()
            try:
                response = self._transport.get(
                    f"{self._base_url}/datasets/{dataset_id}/raw/main/README.md",
                    {},
                    {"Accept": "text/markdown, text/plain"},
                )
            except TransportError:
                if attempt >= self._max_retries:
                    return _row(dataset_id, snapshot_date, source_sha, "error")
                self._sleeper(exponential_seconds(attempt, base=self._retry_base_delay))
                continue

            if response.status == 200:
                return _success(dataset_id, snapshot_date, source_sha, response)
            terminal = POLICY.terminal(response.status)
            if terminal is not None:
                return _row(dataset_id, snapshot_date, source_sha, terminal)
            if POLICY.retry(response.status) and attempt < self._max_retries:
                self._sleeper(
                    delay_seconds(
                        response.status,
                        response.headers,
                        attempt,
                        base=self._retry_base_delay,
                    )
                )
                continue
            return _row(dataset_id, snapshot_date, source_sha, "error")
        return _row(dataset_id, snapshot_date, source_sha, "error")


def _success(
    dataset_id: str,
    snapshot_date: date,
    source_sha: str | None,
    response: HfResponse,
) -> ReadmeResult:
    # README 约定是 UTF-8；个别历史仓库混入非 UTF-8 字节，替换而不是整条丢掉——
    # 丢一整行等于把「有一份 README」误报成「没有」。
    text = response.body.decode("utf-8", errors="replace")
    return ReadmeResult(
        dataset_id=dataset_id,
        status="ok",
        row={
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "source_sha": source_sha,
            "status": "ok",
            "bytes": len(response.body),
            "text": text,
        },
        payload_bytes=len(response.body),
    )


def _row(
    dataset_id: str,
    snapshot_date: date,
    source_sha: str | None,
    status: str,
) -> ReadmeResult:
    return ReadmeResult(
        dataset_id=dataset_id,
        status=status,
        row={
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "source_sha": source_sha,
            "status": status,
            "bytes": None,
            "text": None,
        },
    )


class ReadmeFetcher:
    """按下载量取头部（或给定 id 列表），并发补齐 README 全文。"""

    def __init__(
        self,
        api: HfReadmeApi,
        *,
        workers: int = 12,
        progress: Callable[[int, int], None] | None = None,
        heartbeat: Callable[[int, int], None] | None = None,
    ) -> None:
        self._api = api
        self._workers = max(1, workers)
        self._progress = progress
        self._heartbeat = heartbeat

    def run(
        self,
        snapshot_date: date,
        *,
        dataset_ids: Iterable[str],
        shas: Mapping[str, str] | None = None,
        existing: pl.DataFrame | None = None,
        refresh: Iterable[str] = (),
        retry_denied: bool = False,
        flush: Callable[[pl.DataFrame], object] | None = None,
        flush_every: int = 2_000,
    ) -> tuple[ReadmeSummary, pl.DataFrame]:
        """返回 (摘要, 结果表)。

        候选顺序由调用方决定（按下载量降序）；`refresh` 里的 id 一律重抓——
        仓库 commit 变了，旧全文就不再是当前版本。`existing` 里 ok / absent
        默认跳过，denied 要 `retry_denied` 才重试，error 下次自然重来。

        `retry_denied` **不是「补齐缺口」的开关**：2026-11-18 实测全库 51,860 条
        denied 中 99.75% 是 gated（`manual` 34,471 需作者逐一审批、`auto` 17,257
        需账号接受条款），带令牌随机抽 500 条重试**零命中**。它只对「你确实已
        获批的那批库」有意义。详见 `data/DATA.md` 第 18 条。
        """
        started = time.monotonic()
        candidates = list(dataset_ids)
        done = _resolved_ids(existing, retry_denied=retry_denied, refresh=refresh)
        pending = [dataset_id for dataset_id in candidates if dataset_id not in done]
        skipped = len(candidates) - len(pending)
        sha_by_id = shas or {}

        counts = {"ok": 0, "absent": 0, "denied": 0, "error": 0}
        downloaded = 0

        def tally(result: ReadmeResult) -> None:
            nonlocal downloaded
            counts[result.status] = counts.get(result.status, 0) + 1
            downloaded += result.payload_bytes

        # 全文一条几 KB、全量上百万条：给了 flush（即真的有落盘去处）就走流式，
        # 不在内存里攒结果；否则全量跑会在收尾时占掉几个 GB。
        stream = flush is not None
        _, frame = collect_rows(
            pending=pending,
            fetch=lambda dataset_id: self._api.fetch(
                dataset_id, snapshot_date, sha_by_id.get(dataset_id)
            ),
            schema=README_SCHEMA,
            workers=self._workers,
            flush=flush,
            flush_every=flush_every,
            progress=self._progress,
            heartbeat=self._heartbeat,
            retain=not stream,
            on_result=tally,
        )

        return (
            ReadmeSummary(
                snapshot_date=snapshot_date,
                requested=len(candidates),
                fetched=counts["ok"],
                absent=counts["absent"],
                denied=counts["denied"],
                failed=counts["error"],
                skipped=skipped,
                # 本轮真正写出的行数。流式模式下 `frame` 是空的，不能拿它的高度冒充
                # 「写了多少」——台账里那个数会变成 0，而磁盘上明明多了几千行。
                table_rows=sum(counts.values()),
                downloaded_bytes=downloaded,
                elapsed_seconds=time.monotonic() - started,
            ),
            frame,
        )


def _resolved_ids(
    existing: pl.DataFrame | None,
    *,
    retry_denied: bool,
    refresh: Iterable[str] = (),
) -> set[str]:
    """已有结论的 id。`error` 不算结论——它下次自然重试。"""
    if existing is None or existing.height == 0:
        return set()
    keep = existing.filter(pl.col("status").is_in(["ok", "absent"]))
    if not retry_denied:
        keep = pl.concat([keep, existing.filter(pl.col("status") == "denied")])
    return set(keep["id"].to_list()) - set(refresh)
