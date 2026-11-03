"""HF 数据集列表接口客户端。

本模块与外部世界之间只有一条缝：注入的 transport。
所有分页、字段构造、退避与配额处理都在此完成，上层不感知 HTTP。
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol
from urllib.parse import parse_qs, urlparse

from .checkpoint import Checkpoint

DEFAULT_BASE_URL = "https://huggingface.co/api/datasets"

#: HF 列表接口 expand[] 的合法字段白名单（实测由 400 响应回显取得）。
EXPAND_WHITELIST: frozenset[str] = frozenset(
    {
        "author",
        "cardData",
        "citation",
        "createdAt",
        "description",
        "disabled",
        "downloads",
        "downloadsAllTime",
        "gated",
        "lastModified",
        "likes",
        "mainSize",
        "paperswithcode_id",
        "private",
        "siblings",
        "sha",
        "tags",
        "trendingScore",
        "resourceGroup",
        "xetEnabled",
    }
)

MAX_LIMIT = 1000
RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True)
class HfResponse:
    """transport 的返回形态：状态码、响应头、原始字节。"""

    status: int
    headers: Mapping[str, str]
    body: bytes


class Transport(Protocol):
    """唯一外部边界。生产实现走 httpx，测试注入假实现。"""

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse: ...


@dataclass(frozen=True)
class Page:
    """一页扫描结果。"""

    records: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: str | None = None
    rate_remaining: int | None = None
    skipped: int = 0


def _header(headers: Mapping[str, str], name: str) -> str | None:
    """大小写不敏感取头。

    真实接口经 HTTP/2 会把头名规范成小写；按大写字面量查找会静默取空，
    进而导致分页在首页就「正常终止」——这类失败不报错，只会丢数据。
    """
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return None


def _parse_next_cursor(link_header: str | None) -> str | None:
    """从 `Link: <...cursor=xxx>; rel="next"` 中取出并解码游标。"""
    if not link_header:
        return None
    for part in link_header.split(","):
        if 'rel="next"' not in part:
            continue
        start = part.find("<")
        end = part.find(">")
        if start == -1 or end == -1:
            continue
        cursor = parse_qs(urlparse(part[start + 1 : end]).query).get("cursor")
        if cursor:
            return cursor[0]
    return None


def _parse_rate_limit(headers: Mapping[str, str]) -> tuple[int | None, int | None]:
    """解析 `RateLimit: "api";r=<剩余>;t=<重置秒>`，返回 (剩余, 重置秒)。"""
    raw = _header(headers, "RateLimit")
    if not raw:
        return None, None
    remaining: int | None = None
    reset_in: int | None = None
    for segment in raw.split(";"):
        segment = segment.strip()
        if segment.startswith("r="):
            remaining = int(segment[2:])
        elif segment.startswith("t="):
            reset_in = int(segment[2:])
    return remaining, reset_in


class HfDatasetApi:
    """按显式排序稳定遍历数据集列表。"""

    def __init__(
        self,
        transport: Transport,
        *,
        base_url: str = DEFAULT_BASE_URL,
        max_limit: int = MAX_LIMIT,
        sleeper: Callable[[float], None] | None = None,
        max_retries: int = 5,
        low_quota_threshold: int = 10,
        retry_base_delay: float = 1.0,
    ) -> None:
        self._transport = transport
        self._base_url = base_url
        self._max_limit = max_limit
        self._sleeper = sleeper or time.sleep
        self._max_retries = max_retries
        self._low_quota_threshold = low_quota_threshold
        self._retry_base_delay = retry_base_delay

    def scan(
        self,
        *,
        sort: str,
        direction: str = "-1",
        limit: int = MAX_LIMIT,
        expand: Sequence[str] = (),
        start_cursor: str | None = None,
        max_pages: int | None = None,
        checkpoint: Checkpoint | None = None,
    ) -> Iterator[Page]:
        """逐页产出扫描结果。

        `sort` 必填：不传时 HF 默认按 trendingScore 排，游标会随时间漂移，
        翻页过程中必然漏抓或重抓，故视为调用方缺陷。
        """
        if not sort:
            raise ValueError("sort 必填：默认排序不稳定，会导致翻页漏抓")
        unknown = set(expand) - EXPAND_WHITELIST
        if unknown:
            raise ValueError(f"expand 含白名单外字段: {sorted(unknown)}")

        params: dict[str, Any] = {
            "limit": str(min(limit, self._max_limit)),
            "sort": sort,
            "direction": direction,
        }
        if expand:
            params["expand[]"] = list(expand)

        cursor = start_cursor if start_cursor is not None else (checkpoint.cursor if checkpoint else None)
        if cursor:
            params["cursor"] = cursor

        pages = 0
        while True:
            response = self._request(params)
            payload = json.loads(response.body.decode("utf-8"))
            records, skipped = self._valid_records(payload)
            next_cursor = _parse_next_cursor(_header(response.headers, "Link"))
            remaining, reset_in = _parse_rate_limit(response.headers)

            yield Page(
                records=records,
                next_cursor=next_cursor,
                rate_remaining=remaining,
                skipped=skipped,
            )

            if checkpoint is not None:
                checkpoint.save(next_cursor)

            pages += 1
            if next_cursor is None or (max_pages is not None and pages >= max_pages):
                return

            if remaining is not None and remaining < self._low_quota_threshold and reset_in:
                self._sleeper(float(reset_in))

            params["cursor"] = next_cursor

    def _request(self, params: dict[str, Any]) -> HfResponse:
        attempt = 0
        while True:
            response = self._transport.get(
                self._base_url,
                dict(params),
                {"Accept": "application/json"},
            )
            if response.status == 200:
                return response
            if response.status in RETRYABLE_STATUS and attempt < self._max_retries:
                self._sleeper(self._retry_delay(response, attempt))
                attempt += 1
                continue
            raise RuntimeError(f"HF 接口返回 {response.status}: {response.body[:200]!r}")

    def _retry_delay(self, response: HfResponse, attempt: int) -> float:
        _, reset_in = _parse_rate_limit(response.headers)
        if reset_in:
            return float(reset_in)
        return min(self._retry_base_delay * (2**attempt), 60.0)

    @staticmethod
    def _valid_records(payload: Any) -> tuple[list[dict[str, Any]], int]:
        if not isinstance(payload, list):
            raise RuntimeError(f"响应不是列表: {type(payload).__name__}")
        valid: list[dict[str, Any]] = []
        skipped = 0
        for item in payload:
            if not isinstance(item, dict) or not item.get("id") or not item.get("lastModified"):
                skipped += 1
                continue
            valid.append(item)
        return valid, skipped
