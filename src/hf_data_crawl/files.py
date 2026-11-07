"""按下载量取头部，补数据集的文件清单汇总（B 案轻量试点）。

为什么走 /tree/main 而不是单库详情
----------------------------------
2026-11-07 两轮实测：列表接口即便带 `expand[]=siblings` 或 `full=true` 也不返回
文件清单，`siblings` 只出现在 `/api/datasets/{id}` 详情里。但详情响应呈极端
长尾——下载量头部的两个库分别 13 MB / 44s 与 25 MB / 76s，按头部补 1000 个会
反复超时（首次 `--top 20` 跑满 240s 被杀）。改用
`/tree/main?recursive=true&expand=false`：每页固定 1000 条、约 110–230 KB /
0.4–0.7s，快两个数量级，且大库不再挂死。

口径边界（务必看清）
--------------------
tree 按页返回，`Link: rel="next"` 表示还有后续页。默认只取第一页，命中的库
以 `truncated=True` 标记「清单没取完」——此时 `file_count` / `total_bytes` /
`formats` / `has_readme` 全是**这一页的下限**，不是全库真值。要拿完整清单须显式
加大 `--max-pages`。只统计 `type=file` 的条目，目录不进文件数。满页却没给出
next 也一律标成截断：宁可承认是下限，也不假装完整。

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
import math
import re
import time
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import parse_qs, urlsplit

import polars as pl

from .api import DEFAULT_BASE_URL, HfResponse, Transport, TransportError
from .batch import collect_rows
from .coerce import as_int
from .tables import FILES_SCHEMA, url_for

#: tree 接口每页硬上限；显式带上 limit，免得哪天上游默认值变了口径跟着漂。
TREE_LIMIT = 1000

#: 明确表示「这个库没有可看的树」的状态码，重试无意义。
UNAVAILABLE_STATUS: frozenset[int] = frozenset({401, 403, 404})

#: 限流与网关抖动，值得退避重试。
RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})

_NEXT_LINK = re.compile(r'<([^>]+)>\s*;\s*rel="?next"?', re.IGNORECASE)


@dataclass(frozen=True)
class FileResult:
    """单库文件清单查询结果。`payload_bytes` 用于量化真实流量，不落库。"""

    dataset_id: str
    status: str
    row: dict[str, Any]
    payload_bytes: int = 0


@dataclass(frozen=True)
class FileSummary:
    snapshot_date: date
    requested: int
    fetched: int
    unavailable: int
    failed: int
    skipped: int
    table_rows: int
    downloaded_bytes: int
    elapsed_seconds: float


def next_cursor(headers: Mapping[str, str]) -> str | None:
    """从 `Link: <...&cursor=...>; rel="next"` 里取下一页游标。

    响应头大小写不敏感；取不到 next 就返回 None，让调用方按「已到末页」处理。
    """
    link = next((value for key, value in headers.items() if key.lower() == "link"), None)
    if not link:
        return None
    match = _NEXT_LINK.search(link)
    if match is None:
        return None
    return parse_qs(urlsplit(match.group(1)).query).get("cursor", [None])[0] or None


def parse_tree(body: bytes) -> list[dict[str, Any]] | None:
    """解析一页 tree。空列表是合法的空仓库，只有形态不对才返回 None。"""
    try:
        payload = json.loads(body.decode("utf-8"))
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


class HfFilesApi:
    """单库文件树查询：列表接口拿不到、详情接口又太重的那部分。"""

    def __init__(
        self,
        transport: Transport,
        *,
        base_url: str = DEFAULT_BASE_URL,
        max_retries: int = 3,
        max_pages: int = 1,
        retry_base_delay: float = 2.0,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._transport = transport
        self._base_url = base_url
        self._max_retries = max_retries
        self._max_pages = max(1, max_pages)
        self._retry_base_delay = retry_base_delay
        self._sleeper = sleeper or time.sleep

    def fetch(self, dataset_id: str, snapshot_date: date) -> FileResult:
        entries: list[dict[str, Any]] = []
        cursor: str | None = None
        payload_bytes = 0
        truncated = False

        for page in range(self._max_pages):
            response = self._request(dataset_id, cursor)
            if response is None:
                return _failure(dataset_id, "error", payload_bytes=payload_bytes)
            payload_bytes += len(response.body)
            if response.status in UNAVAILABLE_STATUS:
                return _failure(
                    dataset_id, "unavailable", snapshot_date, payload_bytes=payload_bytes
                )
            if response.status != 200:
                return _failure(dataset_id, "error", payload_bytes=payload_bytes)

            parsed = parse_tree(response.body)
            if parsed is None:
                # 200 却不是数组属于上游形态变化，记临时失败等下轮重试，
                # 不把「空仓库」和「没拿到数据」混成同一件事。
                return _failure(dataset_id, "error", payload_bytes=payload_bytes)
            entries.extend(parsed)

            cursor = next_cursor(response.headers)
            has_next = cursor is not None or len(parsed) >= TREE_LIMIT
            if not has_next:
                truncated = False
                break
            if cursor is None or page + 1 >= self._max_pages:
                truncated = True
                break
            truncated = True

        return _summarize(dataset_id, snapshot_date, entries, truncated, payload_bytes)

    def _request(self, dataset_id: str, cursor: str | None) -> HfResponse | None:
        params = {"recursive": "true", "expand": "false", "limit": str(TREE_LIMIT)}
        if cursor:
            params["cursor"] = cursor
        for attempt in range(self._max_retries + 1):
            try:
                response = self._transport.get(
                    f"{self._base_url}/{dataset_id}/tree/main",
                    params,
                    {"Accept": "application/json"},
                )
            except TransportError:
                if attempt >= self._max_retries:
                    return None
                self._sleeper(self._backoff(attempt))
                continue
            if response.status in UNAVAILABLE_STATUS or response.status == 200:
                return response
            if response.status in RETRYABLE_STATUS and attempt < self._max_retries:
                self._sleeper(self._delay_for(response, attempt))
                continue
            return response
        return None

    def _delay_for(self, response: HfResponse, attempt: int) -> float:
        """429 优先听上游的 Retry-After；没给或给脏值才用指数退避。

        实测匿名配额是 500 请求 / 300 秒固定窗口，429 的 `retry-after` 是窗口
        剩余秒数。固定退避 2 秒会在窗口重置前就重试，白白烧掉重试次数——首轮
        top 1000 有 321 个库因此被记成临时失败。非有限值与负数一律回退，
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
    return FileResult(
        dataset_id=dataset_id,
        status="ok",
        row={
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "status": "ok",
            "file_count": len(entries),
            "total_bytes": sum(present) if present else None,
            "formats": json.dumps(dict(sorted(extensions.items())), ensure_ascii=False),
            "has_readme": any(
                PurePosixPath(name).name.upper().startswith("README") for name in names
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
) -> FileResult:
    """不可用的落库（下轮跳过），临时失败留空（下轮自然重试）。

    与 `sizes._failure` 同一套语义：把可救的记录钉成失败，或每轮重烧
    不可用的请求预算，都是静悄悄的成本泄漏。
    """
    if status != "unavailable" or snapshot_date is None:
        return FileResult(dataset_id=dataset_id, status=status, row={}, payload_bytes=payload_bytes)
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


class FileFetcher:
    """按下载量取头部，并发补齐文件清单汇总。"""

    def __init__(
        self,
        api: HfFilesApi,
        *,
        workers: int = 8,
        progress: Callable[[int, int], None] | None = None,
    ) -> None:
        self._api = api
        self._workers = max(1, workers)
        self._progress = progress

    def run(
        self,
        snapshot_date: date,
        *,
        dataset_ids: Iterable[str] | None = None,
        top: int = 1_000,
        retry_unavailable: bool = False,
        existing: pl.DataFrame | None = None,
        flush: Callable[[pl.DataFrame], object] | None = None,
        flush_every: int = 100,
    ) -> tuple[FileSummary, pl.DataFrame]:
        """返回 (摘要, 结果表)。已处理的 id 默认跳过，实现断点续跑。"""
        started = time.monotonic()
        candidates = list(dataset_ids or [])[:top]
        if existing is not None and existing.height:
            done = (
                set(existing.filter(pl.col("status") == "ok")["id"].to_list())
                if retry_unavailable
                else set(existing["id"].to_list())
            )
        else:
            done = set()
        pending = [dataset_id for dataset_id in candidates if dataset_id not in done]
        skipped = len(candidates) - len(pending)

        traffic: list[int] = []

        def fetch(dataset_id: str) -> FileResult:
            result = self._api.fetch(dataset_id, snapshot_date)
            traffic.append(result.payload_bytes)
            return result

        results, frame = collect_rows(
            pending=pending,
            fetch=fetch,
            schema=FILES_SCHEMA,
            workers=self._workers,
            flush=flush,
            flush_every=flush_every,
            progress=self._progress,
        )
        ok = sum(1 for result in results if result.status == "ok")
        unavailable = sum(1 for result in results if result.status == "unavailable")

        return (
            FileSummary(
                snapshot_date=snapshot_date,
                requested=len(candidates),
                fetched=ok,
                unavailable=unavailable,
                failed=len(results) - ok - unavailable,
                skipped=skipped,
                table_rows=frame.height,
                downloaded_bytes=sum(traffic),
                elapsed_seconds=time.monotonic() - started,
            ),
            frame,
        )
