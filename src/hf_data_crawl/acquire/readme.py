"""按 id 抓 README 全文。

为什么需要单独一条链路
----------------------
列表接口的 `description` 是 HF 在服务端截断过的摘要（约 500–600 字符，尾部
常带「… See the full description on the dataset page」），`cardData` 只有
YAML 头，正文不在任何一次列表响应里。全文只有一个来源：

    GET https://huggingface.co/datasets/<id>/raw/main/README.md

一次一个库，返回原始 Markdown（含 YAML 头）。

接口脾性（2026-11-17 实测 860 条 + 限流复现）
---------------------------------------------
- 短时并发 12 可达 18.9–19.9 req/s，但**不能一直这么跑**：HF 的解析器（resolver）
  配额是 **5000 次 / 5 分钟**（带令牌，实测 429 原文如此；约 16.7 req/s），撞上后
  `retry-after` 约 58 秒。第一轮 6000 条只用了 2 分钟就撞线，之后每个请求都 429。
- 所以本模块**主动限速**而不是靠撞 429 后退避：默认 15 req/s，留一点余量。
  20,000 条约 22 分钟；全量 113 万条（**每条都要发一次请求**，404 也占配额）
  约 21 小时，其中约 70% 能拿到正文。想更快只能换 PRO/Team 账号（配额更高），
  不是改并发能解决的。
- 命中率：200 约 70%、404（仓库没有 README）约 25%、401/403（门禁或资源
  被禁）约 5%。三种空必须分开记账——`absent` 是「作者本来没写」，`denied` 是
  「我们读不到」，`error` 是「这次没抓到」。
- `denied` 别指望带令牌重来一遍就能补上：全量 51,860 条里 join `current.gated`
  后 99.75% 是门禁库（`manual` / `auto`），且 `private` 无一为真——不是私有库，
  是公开但受限。带令牌抽 500 条实测零命中。见 `docs/DATA.md` 第 18 条。
- 体积：中位 2.5 KB、均值 4.6 KB、p90 8.6 KB；全量约 3.6 GB 文本。

与 `size` / `files` 一样是 `Origin.NETWORK` 表：raw 里没有副本，删掉就要重抓；
抓取期间写锁外分片，收尾短锁合并（`fetch-readme` 全量要跑十几小时，整段持锁
会挡住当天的采集）。
"""

from __future__ import annotations

import math
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from ..platform.tables import README_SCHEMA, url_for
from .batch import collect_rows
from .hf.api import HfResponse, Transport, TransportError

DEFAULT_BASE_URL = "https://huggingface.co"

#: 默认限速：HF 解析器配额约 5000 次 / 5 分钟（≈16.7 req/s），留一点余量。
DEFAULT_RATE = 15.0

#: 暂存目录（相对数据根）。与规模表的 `.staging/sizes` 并列，互不干扰。
README_STAGING_DIR = ".staging/readme"

#: 仓库没有 README：重试无意义，落 `absent`。
ABSENT_STATUS: frozenset[int] = frozenset({404})

#: 门禁或资源被禁：匿名读不到，带令牌也基本仍被拒（`manual` gated、`auto` gated
#: 未接受条款、仓库被 disabled）。2026-11-18 实测：500 条带令牌重试全 403 零命中，
#: 而同一令牌读 `ok` 组建库全部 200——是数据属性，不是凭据问题。
DENIED_STATUS: frozenset[int] = frozenset({401, 403})

#: 限流与网关抖动：值得退避重试。
RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})


class RateLimiter:
    """按固定间隔放行的匀速器：多线程共享，串行地给每个请求「买票」。

    为什么不用「撞 429 再退避」：配额窗口是 5 分钟，一次撞线后 12 个线程会各自
    睡满 `retry-after`（实测约 58 秒），醒来仍贴着上限跑，于是反复撞线——实测
    吞吐会掉到 0.2 req/s。主动匀速反而更快：15 req/s 跑 2 万条约 22 分钟。

    `clock` / `sleep` 可注入，便于测试「间隔真的被拉开」。
    """

    def __init__(
        self,
        per_second: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._interval = (1.0 / per_second) if per_second and per_second > 0 else 0.0
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next = 0.0

    @property
    def enabled(self) -> bool:
        return self._interval > 0

    def acquire(self) -> None:
        """取一个放行名额；必要时睡到下一个名额的时间点。"""
        if self._interval <= 0:
            return
        with self._lock:
            now = self._clock()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self._interval
        if delay > 0:
            self._sleep(delay)


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


class HfReadmeApi:
    """单库 README 全文查询：列表接口拿不到的那部分正文。"""

    def __init__(
        self,
        transport: Transport,
        *,
        base_url: str = DEFAULT_BASE_URL,
        max_retries: int = 2,
        retry_base_delay: float = 2.0,
        sleeper: Callable[[float], None] | None = None,
        rate: float = DEFAULT_RATE,
    ) -> None:
        self._transport = transport
        self._base_url = base_url.rstrip("/")
        self._max_retries = max(0, max_retries)
        self._retry_base_delay = retry_base_delay
        self._sleeper = sleeper or time.sleep
        self._limiter = RateLimiter(rate, sleep=self._sleeper)

    def fetch(
        self,
        dataset_id: str,
        snapshot_date: date,
        source_sha: str | None = None,
    ) -> ReadmeResult:
        """抓一个库的 README；永不抛异常，失败落 `error` 行等下一轮。"""
        for attempt in range(self._max_retries + 1):
            # 每次真正发请求（含重试）都先买一张票：配额按请求数算，不按「条目数」算。
            self._limiter.acquire()
            try:
                response = self._transport.get(
                    f"{self._base_url}/datasets/{dataset_id}/raw/main/README.md",
                    {},
                    {"Accept": "text/markdown, text/plain"},
                )
            except TransportError:
                if attempt >= self._max_retries:
                    return _row(dataset_id, snapshot_date, source_sha, "error")
                self._sleeper(self._backoff(attempt))
                continue

            if response.status == 200:
                return _success(dataset_id, snapshot_date, source_sha, response)
            if response.status in DENIED_STATUS:
                return _row(dataset_id, snapshot_date, source_sha, "denied")
            if response.status in ABSENT_STATUS:
                return _row(dataset_id, snapshot_date, source_sha, "absent")
            if response.status in RETRYABLE_STATUS and attempt < self._max_retries:
                self._sleeper(self._delay_for(response, attempt))
                continue
            return _row(dataset_id, snapshot_date, source_sha, "error")
        return _row(dataset_id, snapshot_date, source_sha, "error")

    def _delay_for(self, response: HfResponse, attempt: int) -> float:
        """429 优先听上游的 `Retry-After`；没给或给脏值才用指数退避。

        与 `files` 同一理由：固定退避可能在窗口重置前就重试，白烧次数。
        合法值夹在 60 秒内，防止上游脏值把进程睡死。
        """
        if response.status == 429:
            raw = next(
                (value for key, value in response.headers.items() if key.lower() == "retry-after"),
                None,
            )
            if raw is not None:
                try:
                    seconds = float(raw)
                except ValueError:
                    pass
                else:
                    if math.isfinite(seconds) and seconds >= 0:
                        return min(seconds, 60.0)
        return self._backoff(attempt)

    def _backoff(self, attempt: int) -> float:
        return min(self._retry_base_delay * (2**attempt), 30.0)


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
        获批的那批库」有意义。详见 `docs/DATA.md` 第 18 条。
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


def stale_readme_ids(current: pl.DataFrame, existing: pl.DataFrame) -> pl.DataFrame:
    """内容已变的库：当前 `sha` 与抓取时的 `source_sha` 不一致。

    为什么按 sha 而不是 `lastModified`：README 是仓库里的一个文件，它属于哪个
    版本由 commit 决定；`lastModified` 会被无关改动（比如上传数据文件）推高，
    按它重抓等于每次有人传文件就白烧一次请求。旧行没有 `source_sha`（加列之前
    抓的）一律算陈旧，升级后第一轮会补一次。

    只挑 `ok` / `absent`：`error` 下一轮自然重试，`denied` 要显式带令牌重试。
    """
    empty = current.head(0)
    if current.height == 0 or existing.height == 0:
        return empty
    joined = current.join(
        existing.select(
            "id",
            pl.col("source_sha").alias("readme_sha"),
            # 必须改名：`current` 自己也有 `status`（active / disappeared），
            # 不改的话过滤条件取到的是底库在架状态，判据恒不成立、永远「无陈旧」。
            pl.col("status").alias("readme_status"),
        ),
        on="id",
        how="inner",
    )
    return joined.filter(
        pl.col("readme_status").is_in(["ok", "absent"])
        & (pl.col("readme_sha").is_null() | (pl.col("readme_sha") != pl.col("sha")))
    )


def keep_previous_ok(previous: pl.DataFrame, fresh: pl.DataFrame) -> pl.DataFrame:
    """不让临时失败覆盖已拿到的全文。

    `absent` / `denied` 是明确结论，可以覆盖旧值（README 可能真被删了或转私有）；
    `error` 不是结论——一次瞬时 500 就把几 KB 的正文抹掉，下次还会因为「有行」
    被跳过，等于永久丢全文。只挡这一种。
    """
    if previous.height == 0 or fresh.height == 0:
        return fresh
    protected = previous.filter(pl.col("status") == "ok").select("id")
    if protected.height == 0:
        return fresh
    degraded = fresh.filter(pl.col("status") == "error").join(protected, on="id", how="semi")
    if degraded.height == 0:
        return fresh
    return fresh.join(degraded.select("id"), on="id", how="anti")
