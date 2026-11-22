"""按下载量取头部，补数据集的文件清单汇总（B 案轻量试点）。

为什么先用详情接口、再用 /tree/main 兜底
------------------------------------------
2026-11-07 的判断是「列表接口即便带 `expand[]=siblings` 或 `full=true` 也不返回
文件清单，`siblings` 只出现在 `/api/datasets/{id}` 详情里；而详情响应呈极端长尾
——两个头部库分别 13 MB / 44s 与 25 MB / 76s，补 1000 个会反复超时」，于是改用
`/tree/main?recursive=true&expand=false`（每页 1000 条、110–230 KB）。

2026-11-22 复测，两件事都变了：

1. **详情的耗时不再是问题**：`?blobs=true` 下最慢的样本（2.8 万文件的 fineweb）
   6.9 MB / 2.2 秒，10 万文件的 Event6D 23.9 MB / 6.1 秒——不再是 44–76 秒。
2. **详情一次就能拿全，而 tree 要翻几十页**：同一个 fineweb，详情 1 次请求 6.9 MB，
   tree 要 29 次、约 8.7 MB。文件越多，差距越大。

故改为**详情优先**：`GET /api/datasets/{id}?blobs=true` 的 `siblings` 给出
`rfilename` + `size` + `lfs`（含 `sha256`），一次请求拿全。`tree` 保留为兜底路径
——详情返回的形态一旦不是预期的 JSON 对象，就退回去分页，宁可多花几十次请求，
也不要因为上游改了字段就把整库记成失败。

`siblings` 有一个**硬上限 10 万条**（实测：两个文件数超过十万的库都正好停在
100,000），故超过十万的库仍标 `truncated`。这比 tree 的「一页 1,000 条」高两个
数量级，但**不是无限**，别把它当成「一定能拿全」。

口径边界（务必看清）
--------------------
tree 兜底路径按页返回，`Link: rel="next"` 表示还有后续页。默认只取第一页，命中的
库以 `truncated=True` 标记「清单没取完」——此时 `file_count` / `total_bytes` /
`formats` / `has_readme` 全是**这一页的下限**，不是全库真值。要拿完整清单须显式
加大 `--max-pages`。只统计 `type=file` 的条目，目录不进文件数。满页却没给出
next 也一律标成截断：宁可承认是下限，也不假装完整。

`recursive` 按路径排序，**目录排在文件前**，嵌套深的仓库头几页可能一个文件都
没有。这种「0 个文件」只是「这几页没看到文件」，不是空仓库，故在尚无文件时
额外允许翻 `EMPTY_PAGE_GRACE` 页；一旦见到文件就严格按 `max_pages` 收工。

匿名配额是 500 请求 / 300 秒固定窗口，429 会带 `retry-after`（窗口剩余秒数）；
退避必须听上游，固定 2 秒会在窗口重置前就重试、白烧次数。需要更高配额请带
`HF_TOKEN` 登录，或分批跑让断点续跑接力。

与 `size` 表的关系
------------------
两张表都是 `Origin.NETWORK`：raw 里没有对应数据，删掉就要重抓，故 `rebuild`
一律保留（由 `tables.TABLES` 登记保证）。并发调度、边跑边落库与进度共用
`batch.collect_rows`，不另写一份循环。
"""

from __future__ import annotations

import json
import re
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import date
from pathlib import PurePosixPath
from typing import Any, Literal
from urllib.parse import parse_qs, urlsplit

import orjson
import polars as pl

from ..platform.coerce import as_int
from ..platform.tables import FILES_SCHEMA, url_for
from .batch import collect_rows
from .hf.api import API_BASE_URL, HfResponse, Transport, TransportError
from .hf.pace import BUCKET_API, RateGate, shared
from .hf.retry import StatusPolicy, delay_seconds, exponential_seconds, header_value
from .merge import MergeSpec

#: tree 接口每页硬上限；显式带上 limit，免得哪天上游默认值变了口径跟着漂。
TREE_LIMIT = 1000

#: 详情接口 `siblings` 的硬上限。实测两个文件数超过十万的库都正好停在 100,000，
#: 所以「条数到了这个数」既可能是真有十万个文件，也可能是被截断——一律按截断记，
#: 与「满页却没给 next 也标 truncated」同一条原则：宁可承认是下限。
SIBLINGS_CAP = 100_000

#: 抓取方式。`detail` 一次请求拿全（首选）；`tree` 按页翻（兜底，也是排障开关）。
FilesMode = Literal["detail", "tree"]

#: 文件清单的暂存目录；分片只写真数据、不碰正式表，故写它不必持锁。
#:
#: 与 `.staging/sizes`、`.staging/readme` 并列。`fetch-files` 原先整段持锁，
#: 每日 2 万条约 70 分钟——刷新链要它与 reconcile 并行，落盘只能走分片 + 短锁。
FILES_STAGING_DIR = ".staging/files"

#: 收尾合并的声明。
#:
#: **不做旧值保护**，这是既定取舍而非遗漏：`files` 的 `unavailable` 是真实结论
#: （这个库现在读不到树），覆盖旧的 `ok` 只说明事实变了；而「清单没取完」那一类
#: （`truncated`）由 `--refresh-truncated --max-pages>1` 显式点名，不靠合并阶段
#: 去猜「哪一份更全」。要改这条，先想清楚「旧清单 vs 新 404」哪个更接近真相。
FILES_MERGE = MergeSpec(
    table="files",
    staging_dir=FILES_STAGING_DIR,
    schema=FILES_SCHEMA,
    lock="fetch-files:merge",
)

#: 一页全是目录时额外允许翻的页数，防止「目录排在文件前」被误报成 0 个文件。
#: 只在**还没看到任何文件**时生效：一旦见到文件就严格按 `max_pages` 收工。
EMPTY_PAGE_GRACE = 3

#: 这张端点的状态分区。`/tree` 用 401/403/404 表示「这个库没有可看的树」，
#: 重试无意义；5xx 与 429 才是瞬时故障（与列表接口同一组，见 `hf.api.RETRYABLE_STATUS`）。
POLICY = StatusPolicy(
    retryable=frozenset({429, 500, 502, 503, 504}),
    permanent={401: "unavailable", 403: "unavailable", 404: "unavailable"},
)

#: 临时失败的成因。**处置各不相同**，所以不能只报个数：
#: - `rate`：撞 429，降并发或放慢，重试只会继续撞；
#: - `server`：5xx，上游抖动，下一轮自然好；
#: - `transport`：连接层失败（超时、代理断、握手失败），加超时或换链路；
#: - `parse`：200 但形态不对，上游改字段，要改解析而不是重试；
#: - `http`：其余非 200（400/416 之类），先查请求本身。
FAILURE_REASONS = ("rate", "server", "transport", "parse", "http")


def _reason_for(status: int) -> str:
    """HTTP 状态码 → 失败成因。只用于「重试用尽仍非 200」的那一支。"""
    if status == 429:
        return "rate"
    if status >= 500:
        return "server"
    return "http"


_NEXT_LINK = re.compile(r'<([^>]+)>\s*;\s*rel="?next"?', re.IGNORECASE)


@dataclass(frozen=True)
class FileResult:
    """单库文件清单查询结果。`payload_bytes` 用于量化真实流量，不落库。

    `reason` 只在临时失败时有值，取值是**有限的一组**（见 `FAILURE_REASONS`）：
    「失败了几个」不足以调参——429 要降并发、超时要加超时、5xx 要重试，三种处置
    完全不同，而它们此前全被压成同一个 `failed` 计数。
    """

    dataset_id: str
    status: str
    row: dict[str, Any]
    payload_bytes: int = 0
    reason: str = ""


@dataclass(frozen=True)
class FileSummary:
    snapshot_date: date
    requested: int
    fetched: int
    refreshed: int
    unavailable: int
    failed: int
    skipped: int
    table_rows: int
    downloaded_bytes: int
    elapsed_seconds: float
    #: 临时失败按原因分类的计数（键取自 `FAILURE_REASONS`）。只统计不入库的那些。
    failed_reasons: dict[str, int] = field(default_factory=dict)
    #: 提前收工的原因。目前只有 `"deadline"`（到了 `--until` 的钟点）。
    #: 它**不是失败**：已抓的全部有效，只是这一轮没把候选跑完。
    stopped: str | None = None


def next_cursor(headers: Mapping[str, str]) -> str | None:
    """从 `Link: <...&cursor=...>; rel="next"` 里取下一页游标。

    响应头大小写不敏感；取不到 next 就返回 None，让调用方按「已到末页」处理。
    """
    link = header_value(headers, "link")
    if not link:
        return None
    match = _NEXT_LINK.search(link)
    if match is None:
        return None
    return parse_qs(urlsplit(match.group(1)).query).get("cursor", [None])[0] or None


def parse_tree(body: bytes) -> list[dict[str, Any]] | None:
    """解析一页 tree。空列表是合法的空仓库，只有形态不对才返回 None。"""
    try:
        payload = orjson.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, list):
        return None
    return [
        item
        for item in payload
        if isinstance(item, dict)
        and isinstance(item.get("path"), str)
        and item.get("type", "file") == "file"
    ]


def parse_siblings(body: bytes) -> tuple[list[dict[str, Any]], bool] | None:
    """把详情接口的 `siblings` 归一成 tree 的形状：`[{"path", "size"}]`。

    返回 `(条目, 是否截断)`；形态不对返回 None，由调用方退到 tree 兜底。

    `siblings` 只列文件、不含目录条目（实测 0 个路径以 `/` 结尾），所以不必像
    tree 那样按 `type` 过滤——但仍跳过以 `/` 结尾的项，避免上游哪天改口径后
    把目录算进文件数。

    截断判定用**原始条目数**而不是过滤后的条目数：过滤掉了几个畸形项不代表
    清单完整，取原始数才是「上游给了多少」。
    """
    try:
        payload = orjson.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    siblings = payload.get("siblings")
    if not isinstance(siblings, list):
        return None

    entries: list[dict[str, Any]] = []
    for item in siblings:
        if not isinstance(item, dict):
            continue
        name = item.get("rfilename")
        if not isinstance(name, str) or not name or name.endswith("/"):
            continue
        entries.append({"path": name, "size": item.get("size")})
    return entries, len(siblings) >= SIBLINGS_CAP


class HfFilesApi:
    """单库文件清单：详情一次拿全，`/tree/main` 兜底。"""

    def __init__(
        self,
        transport: Transport,
        *,
        base_url: str = API_BASE_URL,
        max_retries: int = 3,
        max_pages: int = 1,
        mode: FilesMode = "detail",
        retry_base_delay: float = 2.0,
        sleeper: Callable[[float], None] | None = None,
        pacer: RateGate | None = None,
    ) -> None:
        self._transport = transport
        self._base_url = base_url
        self._max_retries = max_retries
        self._max_pages = max(1, max_pages)
        self._mode = mode
        self._retry_base_delay = retry_base_delay
        self._sleeper = sleeper or time.sleep
        #: 默认按上游配额节流：不加节流时并发只会把请求堆到 429 上，而退避会把
        #: worker 按「窗口剩余秒数」停住，吞吐反而低于配额（实测 2.76 / 3.33）。
        #: 取进程内共享的那个闸，而不是各建一个：配额按桶记账，多建就等于多领额度。
        self._pacer = pacer if pacer is not None else shared(BUCKET_API)

    def _get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        """所有请求的唯一出口：先按配额占位，再发。

        收成一个口子的理由与 `_open_transport` 相同——「哪条路径忘了节流」不会有
        任何症状，只会在跑久了以后表现为吞吐莫名偏低。
        """
        self._pacer.wait()
        return self._transport.get(url, params, headers)

    def fetch(self, dataset_id: str, snapshot_date: date) -> FileResult:
        if self._mode == "detail":
            detail = self._fetch_detail(dataset_id, snapshot_date)
            if detail is not None:
                return detail
        return self._fetch_tree(dataset_id, snapshot_date)

    def _fetch_tree(self, dataset_id: str, snapshot_date: date) -> FileResult:
        """按页翻 `/tree/main`：详情形态异常时的兜底，也是排障开关。"""
        entries: list[dict[str, Any]] = []
        cursor: str | None = None
        payload_bytes = 0
        truncated = False
        requests = 0
        # 一页可能全是目录：recursive 按路径排序，目录排在文件前，嵌套深的仓库
        # 头几页可能一个文件都没有。此时 file_count=0 只说明「这几页没看到文件」，
        # 不代表空仓库，故在尚无文件时允许多翻几页，兜底上限防病态仓库无限翻。
        hard_cap = self._max_pages + EMPTY_PAGE_GRACE

        while requests < hard_cap:
            response = self._request(dataset_id, cursor)
            requests += 1
            if response is None:
                return _failure(
                    dataset_id, "error", payload_bytes=payload_bytes, reason="transport"
                )
            payload_bytes += len(response.body)
            terminal = POLICY.terminal(response.status)
            if terminal is not None:
                return _failure(dataset_id, terminal, snapshot_date, payload_bytes=payload_bytes)
            if response.status != 200:
                return _failure(
                    dataset_id,
                    "error",
                    payload_bytes=payload_bytes,
                    reason=_reason_for(response.status),
                )

            parsed = parse_tree(response.body)
            if parsed is None:
                # 200 却不是数组属于上游形态变化，记临时失败等下轮重试，
                # 不把「空仓库」和「没拿到数据」混成同一件事。
                return _failure(dataset_id, "error", payload_bytes=payload_bytes, reason="parse")
            entries.extend(parsed)

            cursor = next_cursor(response.headers)
            has_next = cursor is not None or len(parsed) >= TREE_LIMIT
            if not has_next:
                truncated = False
                break
            truncated = True
            if cursor is None:
                break
            # 用满页数配额、且已经看到文件，才收工；一个文件都没看到就再翻一页。
            if requests >= self._max_pages and entries:
                break

        return _summarize(dataset_id, snapshot_date, entries, truncated, payload_bytes)

    def _request(self, dataset_id: str, cursor: str | None) -> HfResponse | None:
        params = {"recursive": "true", "expand": "false", "limit": str(TREE_LIMIT)}
        if cursor:
            params["cursor"] = cursor
        for attempt in range(self._max_retries + 1):
            try:
                response = self._get(
                    f"{self._base_url}/{dataset_id}/tree/main",
                    params,
                    {"Accept": "application/json"},
                )
            except TransportError:
                if attempt >= self._max_retries:
                    return None
                self._sleeper(exponential_seconds(attempt, base=self._retry_base_delay))
                continue
            if response.status == 200 or POLICY.terminal(response.status) is not None:
                return response
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
            return response
        return None

    def _fetch_detail(self, dataset_id: str, snapshot_date: date) -> FileResult | None:
        """`?blobs=true` 一次拿全；返回 None 表示形态不对、交给 tree 兜底。

        与分页路径的差别不只是请求数：`siblings` 带 `size`，所以 `total_bytes` 是
        真值而不是「这一页的和」；上限也从一页 1,000 条变成 10 万条。

        **形态不对时不落失败结论**，而是让调用方继续走 tree。上游给个空对象、把
        字段改名，或代理返回半截 JSON，这里都会拿到 None——把它记成 `error` 会让
        整库白等一轮重试，而 tree 往往还拿得到。两条路都走不通才算真失败。
        """
        for attempt in range(self._max_retries + 1):
            try:
                response = self._get(
                    f"{self._base_url}/{dataset_id}",
                    {"blobs": "true"},
                    {"Accept": "application/json"},
                )
            except TransportError:
                if attempt >= self._max_retries:
                    return _failure(dataset_id, "error", reason="transport")
                self._sleeper(exponential_seconds(attempt, base=self._retry_base_delay))
                continue

            terminal = POLICY.terminal(response.status)
            if terminal is not None:
                return _failure(dataset_id, terminal, snapshot_date)
            if response.status != 200:
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
                return _failure(dataset_id, "error", reason=_reason_for(response.status))

            parsed = parse_siblings(response.body)
            if parsed is None:
                return None
            entries, truncated = parsed
            return _summarize(dataset_id, snapshot_date, entries, truncated, len(response.body))
        return _failure(dataset_id, "error", reason="transport")


def _summarize(
    dataset_id: str,
    snapshot_date: date,
    entries: list[dict[str, Any]],
    truncated: bool,
    payload_bytes: int,
) -> FileResult:
    names = [item["path"] for item in entries]
    present = [
        value for value in (as_int(item.get("size")) for item in entries) if value is not None
    ]
    extensions = Counter(
        (PurePosixPath(name).suffix.lstrip(".").lower() or "(none)") for name in names
    )
    # 一个文件都没见到、又明确还有后续页：这是「没看到」，不是「没有」。
    # 此时 `0` 会被读成空仓库、`False` 会被读成没有 README，都是没验证过的事，
    # 故四个指标一律留空，宁可承认未知也不拿 0 冒充（与 total_bytes 同一原则）。
    blind = truncated and not entries
    return FileResult(
        dataset_id=dataset_id,
        status="ok",
        row={
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "status": "ok",
            "file_count": None if blind else len(entries),
            "total_bytes": None if blind else (sum(present) if present else None),
            "formats": (
                None if blind else json.dumps(dict(sorted(extensions.items())), ensure_ascii=False)
            ),
            "has_readme": (
                None
                if blind
                else any(PurePosixPath(name).name.upper().startswith("README") for name in names)
            ),
            "truncated": truncated,
        },
        payload_bytes=payload_bytes,
    )


def _failure(
    dataset_id: str,
    status: str,
    snapshot_date: date | None = None,
    *,
    payload_bytes: int = 0,
    reason: str = "",
) -> FileResult:
    """不可用的落库（下轮跳过），临时失败留空（下轮自然重试）。

    与 `sizes._failure` 同一套语义：把可救的记录钉成失败，或每轮重烧
    不可用的请求预算，都是静悄悄的成本泄漏。

    `reason` 只对临时失败有意义，且只进摘要不进表——表里没有这一列，也不该有：
    它是「这一轮为什么没拿到」的运行时诊断，不是关于这个库的事实。
    """
    if status != "unavailable" or snapshot_date is None:
        return FileResult(
            dataset_id=dataset_id,
            status=status,
            row={},
            payload_bytes=payload_bytes,
            reason=reason,
        )
    row: dict[str, Any] = {name: None for name in FILES_SCHEMA}
    row.update(
        {
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "status": "unavailable",
        }
    )
    return FileResult(dataset_id=dataset_id, status=status, row=row, payload_bytes=payload_bytes)


def partial_count(frame: pl.DataFrame | None) -> int:
    """清单只是下限的行数（`truncated=true`）。

    它不影响抓取（一页重抓只是重新确认截断），但影响读数：这些行的
    `file_count` / `total_bytes` / `formats` / `has_readme` 都只是下限，
    体检要能报出「多少个库的清单不完整」，否则这类读数会被当成真值。
    """
    if frame is None or frame.height == 0 or "truncated" not in frame.columns:
        return 0
    return frame.filter(pl.col("truncated").fill_null(False)).height


class FileFetcher:
    """按下载量取头部，并发补齐文件清单汇总。"""

    def __init__(
        self,
        api: HfFilesApi,
        *,
        workers: int = 8,
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
        dataset_ids: Iterable[str] | None = None,
        top: int = 1_000,
        refresh: Iterable[str] = (),
        existing: pl.DataFrame | None = None,
        flush: Callable[[pl.DataFrame], object] | None = None,
        flush_every: int = 100,
        deadline: float | None = None,
        clock: Callable[[], float] = time.time,
    ) -> tuple[FileSummary, pl.DataFrame]:
        """返回 (摘要, 结果表)。已处理的 id 默认跳过，实现断点续跑。

        `refresh` 是这一轮点名重抓的 id（清单没取完的、以及显式 `--retry-unavailable`
        的）：它们已有结论，按「已处理」跳过就永远是旧结论，故从跳过集合里摘出来。
        截断的重抓须配合更大的 `max_pages`，否则只是把截断重新确认一遍——那条守卫
        在命令层（`--refresh-truncated` 与 `--max-pages`）。
        """
        started = time.monotonic()
        candidates = list(dataset_ids or [])[:top]
        again = set(refresh)
        if existing is not None and existing.height:
            done = set(existing["id"].to_list())
        else:
            done = set()
        done -= again
        pending = [dataset_id for dataset_id in candidates if dataset_id not in done]
        skipped = len(candidates) - len(pending)
        refreshed = sum(1 for dataset_id in pending if dataset_id in again)

        traffic: list[int] = []

        def fetch(dataset_id: str) -> FileResult:
            result = self._api.fetch(dataset_id, snapshot_date)
            traffic.append(result.payload_bytes)
            return result

        results, frame, stopped = collect_rows(
            pending=pending,
            fetch=fetch,
            schema=FILES_SCHEMA,
            workers=self._workers,
            flush=flush,
            flush_every=flush_every,
            progress=self._progress,
            heartbeat=self._heartbeat,
            deadline=deadline,
            clock=clock,
        )
        ok = sum(1 for result in results if result.status == "ok")
        unavailable = sum(1 for result in results if result.status == "unavailable")
        # 只统计「不入库」的那一类：ok 与 unavailable 都是有结论的，不占这一列。
        # 顺序按 FAILURE_REASONS 固定，免得同一批数据两次跑出两种键序。
        counts = Counter(
            result.reason or "unknown"
            for result in results
            if result.status not in {"ok", "unavailable"}
        )
        failed_reasons = {name: counts[name] for name in FAILURE_REASONS if counts[name]}
        # 兜住没归类的（例：采集器在工作线程里抛异常，压根没走到 `_failure`）。
        # 只取 `FAILURE_REASONS` 的话，这类失败会在分类里凭空消失，而 `failed`
        # 总数又对不上——两个数互相矛盾比数字粗糙更难查。
        failed_reasons.update(
            {name: counts[name] for name in sorted(counts) if name not in FAILURE_REASONS}
        )

        return (
            FileSummary(
                snapshot_date=snapshot_date,
                requested=len(candidates),
                fetched=ok,
                refreshed=refreshed,
                unavailable=unavailable,
                failed=len(results) - ok - unavailable,
                skipped=skipped,
                table_rows=frame.height,
                downloaded_bytes=sum(traffic),
                elapsed_seconds=time.monotonic() - started,
                failed_reasons=failed_reasons,
                stopped=stopped,
            ),
            frame,
        )
