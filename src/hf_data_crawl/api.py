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

#: 所有写入 Raw 的扫描共同遵守的最低字段契约。HF 的 `expand[]` 是白名单裁剪，
#: 任何写入 Raw 的扫描都必须至少请求这些字段：不同来源共享同一去重池，缺字段的
#: 分块一旦后写，就会覆盖完整记录，让整列静默变空（2026-11-04 实测 private 曾
#: 100% 为空）。由 `ScanSpec` 在构造期校验，并由 `tests/test_schema_contract.py`
#: 与表结构互校。
#:
#: 末两项不对应 Bronze 列：它们稀疏（全库约 0.5% / 0.3%），但实测只多约 0.2%
#: 带宽，放进基座可让每一种来源的 Raw 都留下可回溯副本，顺带消除 lean/enrich
#: 两档字段分叉。它们不进 Bronze，也不改任何宽表列。
IDENTITY_EXPAND: tuple[str, ...] = (
    "author",
    "createdAt",
    "description",
    "disabled",
    "downloads",
    "downloadsAllTime",
    "gated",
    "lastModified",
    "likes",
    "mainSize",
    "private",
    "resourceGroup",
    "sha",
    "tags",
    "trendingScore",
    "xetEnabled",
    "citation",
    "paperswithcode_id",
)

#: 实测列表接口「参数合法、但收到请求也不返回」的字段（2026-11-07）。
#:
#: 依据：383 万条 Raw 全量普查里二者出现 0 次；逐项探接口，带 `expand[]=siblings`
#: 或 `full=true` 的列表响应都没有 `siblings`，`resourceGroup` 同样缺席。
#: - `siblings` 只在单库详情接口出现，该接口一次约 1.2 MB（含文件清单），
#:   `/tree/main?recursive=true` 也要约 200 KB 且上限 1000 条，逐库补 112 万不现实；
#: - `resourceGroup` 有历史占位列且整列为空，见 fielddocs 与 DATA.md 第五节。
#: 白名单只是「参数不会 400」，不等于「列表接口会返回」，别把两者混为一谈。
LIST_UNRETURNED_FIELDS: frozenset[str] = frozenset({"siblings", "resourceGroup"})

#: 其中仍保留在 IDENTITY_EXPAND 里的历史占位项：只为不破坏既有 schema 而随请求带上，
#: 其余不返回字段一律不得进入任何扫描请求（契约测试钉死）。
ALLOWED_PLACEHOLDERS: frozenset[str] = frozenset({"resourceGroup"})

MAX_LIMIT = 1000
RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})


class TransportError(RuntimeError):
    """传输层失败（连接中断、TLS 握手失败、代理断流）。

    与 HTTP 状态码错误分开：状态码是服务器明确答复，连接异常是没收到答复，
    两者都该重试，但原因与日志文本不同。
    """


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


@dataclass(frozen=True)
class Probe:
    """单库探测结果：究竟是不见了，还是藏起来了。"""

    status: int
    resolved_id: str | None = None

    @property
    def verdict(self) -> str:
        """把 HTTP 状态翻译成业务语义。

        - 200 且 `resolved_id` 非空：库还在，只是 id 变了（HF 支持改名，会 307
          到新地址，transport 已跟随重定向，故这里看到的是最终的 200）；
          `resolved_id` 只在与请求的 id 不同时才有值；
        - 200 且 `resolved_id` 为空：库还在，id 没变（present）；
        - 401/403：转为私有或需授权，数据仍在，不该算「消失」；
        - 404：真的没了。
        """
        if self.status == 200:
            return "renamed" if self.resolved_id else "present"
        if self.status in {401, 403}:
            return "restricted"
        if self.status == 404:
            return "deleted"
        return "unknown"


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
            # 档位偶尔会重复列同一字段（基座与专属项重叠时）。HTTP 重复键只会
            # 白增请求长度；去重且保序，让实际发出的清单与声明严格一致。
            params["expand[]"] = list(dict.fromkeys(expand))

        cursor = (
            start_cursor
            if start_cursor is not None
            else (checkpoint.cursor if checkpoint else None)
        )
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

    def probe(self, dataset_id: str) -> Probe:
        """单库探测，用于复核下架判定。

        列表扫描里「没扫到」有三种可能：删除、转私有、改名。三者对历史数据的
        含义完全不同——转私有不该从统计里消失，改名的则要能续上原有观测史。
        问一次详情接口是唯一可靠的分辨方式：改名会 307 到新地址（transport
        已跟随重定向，最终看到 200 与新 id），转私有返回 401，真删除返回 404。

        成本约 1.8 KB/次，只在每轮判定下架后对那几十上百条做，不用于批量。
        """
        try:
            response = self._transport.get(
                f"{self._base_url}/{dataset_id}",
                {"blobs": "false"},
                {"Accept": "application/json"},
            )
        except TransportError:
            # 探测失败不等于数据集消失，留待下轮再判。
            return Probe(status=0)

        if response.status != 200:
            return Probe(status=response.status)
        try:
            payload = json.loads(response.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return Probe(status=response.status)
        resolved = payload.get("id") if isinstance(payload, dict) else None
        # 只有返回的 id 与请求的不同才算改名：库还在、id 没变时详情返回的就是
        # 它自己的 id，若一律记下，存活的库会被判成 renamed。
        renamed_to = resolved if isinstance(resolved, str) and resolved != dataset_id else None
        return Probe(status=200, resolved_id=renamed_to or None)

    def _request(self, params: dict[str, Any]) -> HfResponse:
        attempt = 0
        while True:
            try:
                response = self._transport.get(
                    self._base_url,
                    dict(params),
                    {"Accept": "application/json"},
                )
            except TransportError:
                if attempt >= self._max_retries:
                    raise
                self._sleeper(self._backoff(attempt))
                attempt += 1
                continue

            if response.status == 200:
                return response
            if response.status in RETRYABLE_STATUS and attempt < self._max_retries:
                self._sleeper(self._retry_delay(response, attempt))
                attempt += 1
                continue
            raise RuntimeError(f"HF 接口返回 {response.status}: {response.body[:200]!r}")

    def _backoff(self, attempt: int) -> float:
        return min(self._retry_base_delay * (2**attempt), 60.0)

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
