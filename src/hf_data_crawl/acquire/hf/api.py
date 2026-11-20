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

import orjson

from .checkpoint import Checkpoint

#: 列表接口根（API 域）。同仓 `readme.SITE_BASE_URL` 是站点根，两者的路径段
#: 不可互换——曾同名 `DEFAULT_BASE_URL` 并存，读代码时极易拿错一个。
API_BASE_URL = "https://huggingface.co/api/datasets"

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
#: 100% 为空）。由 `ScanSpec` 在构造期校验，并由 `tests/pipeline/test_schema_contract.py`
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
#:   `/tree/main?recursive=true` 每页约 110-230 KB 且上限 1000 条，逐库补 112 万
#:   不现实（但按头部补可行，见 files.py）；
#: - `resourceGroup` 有历史占位列且整列为空，见 fielddocs 与 DATA.md 第五节。
#: 白名单只是「参数不会 400」，不等于「列表接口会返回」，别把两者混为一谈。
LIST_UNRETURNED_FIELDS: frozenset[str] = frozenset({"siblings", "resourceGroup"})

#: 其中仍保留在 IDENTITY_EXPAND 里的历史占位项：只为不破坏既有 schema 而随请求带上，
#: 其余不返回字段一律不得进入任何扫描请求（契约测试钉死）。
ALLOWED_PLACEHOLDERS: frozenset[str] = frozenset({"resourceGroup"})

MAX_LIMIT = 1000
RETRYABLE_STATUS: frozenset[int] = frozenset({429, 500, 502, 503, 504})

#: 单次退避的上限（秒）。服务器给的 `retry-after` / `t=` 可能等于整个限流窗口，
#: 照单全睡会让一次调用的退避加起来长达半小时以上；封顶后由调用方的预算兜底。
#:
#: 仅用于**逐条探测**（`probe` / `record`）：它们各自持着写锁，一次退避拖住整轮，
#: 2026-11-13 实测堵死日常链 2 小时 50 分。扫描路径见 `MAX_RATE_LIMIT_SLEEP`。
MAX_RETRY_SLEEP = 30.0

#: 扫描路径（`scan`）单次退避的上限：睡满一个配额窗口。
#:
#: 这里的 30 秒封顶是 2026-11-20 刷新链首跑失败的**直接原因**：全站扫 1140 页，
#: 配额约 1000 请求 / 5 分钟，第 1000 页前后必然撞 429；`t=300` 被截成 30 秒，
#: 睡 5 轮共 150 秒，等不满一个窗口，于是「重试已耗尽（HTTP 429）」直接判负，
#: 下游 build / export / deliver 全被跳过。扫描不持写锁、又是链条里最长的活，
#: 它等得起——把窗口睡满，比假装重试更快地拿到数据。
MAX_RATE_LIMIT_SLEEP = 600.0

#: 扫描路径遇**连接中断**（非 HTTP 状态）时额外给的尝试次数。
#:
#: 与 429 分开：429 有 `t=` 可以照睡，是确定性的；SSL / 连接中断没有答复，只能
#: 靠退避熬过去，而代理抖动的恢复时间不由我们决定。默认 5 次共 31 秒，2026-11-20
#: 00:31 那次 SSL EOF 熬不过去，collect 直接判负、下游全跳过。加到 8 次后
#: 1+2+4+8+16+32+60+60 ≈ 183 秒，够盖住一次短时的握手失败。
TRANSPORT_RETRY_EXTRA = 3

#: 单页两次请求之间的最小间隔（秒）低于这个数就不做匀速。
#: 换算过来是 2 请求/秒——配额比这宽松时，网络的往返本身就是瓶颈，
#: 再睡只是白白拖慢（实测正常一轮全站扫描 12–20 分钟，瓶颈在延迟不在配额）。
PACE_FLOOR = 0.5

#: 匀速时单页最长睡多久。再长就不如直接等窗口重开：`r=3;t=42`（每请求 14 秒）
#: 与「睡 42 秒重开」总时长相同，但后者少 2 次白跑的往返。
MAX_PACE_SLEEP = 60.0


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
        # `int()` 直接抛：`t=41.5`（实测出现过的小数秒）与空值 `r=` 都会让整轮
        # 扫描作废——一个响应头的格式抖动不该比配额本身更有杀伤力。解析失败就
        # 让该字段留 None，退避回落到指数，不中断扫描。
        try:
            if segment.startswith("r="):
                remaining = int(float(segment[2:]))
            elif segment.startswith("t="):
                reset_in = int(float(segment[2:]))
        except ValueError:
            continue
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
        base_url: str = API_BASE_URL,
        max_limit: int = MAX_LIMIT,
        sleeper: Callable[[float], None] | None = None,
        max_retries: int = 5,
        retry_base_delay: float = 1.0,
    ) -> None:
        self._transport = transport
        self._base_url = base_url
        self._max_limit = max_limit
        self._sleeper = sleeper or time.sleep
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        #: 重试耗尽时最后一次见到的失败，供错误消息带上。没有它，
        #: `持续限流或不可用` 分不清 429 与 5xx，而两者的处置完全不同
        #: （等配额 vs 等上游恢复）——2026-11-16 排障时只能靠手动重放请求才确认。
        self._last_failure: str | None = None

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
            payload = orjson.loads(response.body)
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
            # 0 与 None 同义：不设上限。照字面比会让 `--card-pages 0` 第一页就返回，
            # 调用方以为「全刷」而实际只刷了一页，且停止原因看起来完全正常。
            if next_cursor is None:
                return
            if max_pages is not None and max_pages > 0 and pages >= max_pages:
                return

            # 取下一页之前按剩余配额匀速。原先是「剩余 < 10 才等」，等于每次都要
            # 先把配额打光、撞一次 429、再等窗口——长跑（全站 1140 页）必然如此。
            pace = self._pace(remaining, reset_in)
            if pace > 0:
                self._sleeper(pace)

            params["cursor"] = next_cursor

    def record(self, dataset_id: str) -> dict[str, Any] | None:
        """取单个数据集的完整记录（含 `cardData` 与 `sha`）。

        存在的理由是「按需补卡片」：全量扫一遍要 1100 多页 / 十几个小时，而实际
        被交付、被查询命中的常常只有几十个库。对这批库逐条问一次，代价几十 KB，
        换到的却是默认面里那两列（`license_name` / `pretty_name`）不再空着。

        取不到就返回 None——补卡片是锦上添花，不能因为它让交付失败。
        """
        try:
            response = self._transport.get(
                f"{self._base_url}/{dataset_id}",
                {"blobs": "false"},
                {"Accept": "application/json"},
            )
        except TransportError:
            return None
        if response.status != 200:
            return None
        try:
            payload = orjson.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        return payload if isinstance(payload, dict) else None

    def probe(self, dataset_id: str) -> Probe:
        """单库探测，用于复核下架判定。

        列表扫描里「没扫到」有三种可能：删除、转私有、改名。三者对历史数据的
        含义完全不同——转私有不该从统计里消失，改名的则要能续上原有观测史。
        问一次详情接口是唯一可靠的分辨方式：改名会 307 到新地址（transport
        已跟随重定向，最终看到 200 与新 id），转私有返回 401，真删除返回 404。

        成本约 1.8 KB/次，只在每轮判定下架后对那几十上百条做，不用于批量。
        """
        try:
            response = self._fetch(
                f"{self._base_url}/{dataset_id}",
                {"blobs": "false"},
                {"Accept": "application/json"},
            )
        except TransportError:
            # 连不上不等于库没了。落成 unknown，调用方留待下轮，绝不写库。
            return Probe(status=0)
        if response is None:
            # 重试耗尽是「不知道」，不是「没了」。
            return Probe(status=0)
        if response.status != 200:
            return Probe(status=response.status)
        try:
            payload = orjson.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return Probe(status=response.status)
        resolved = payload.get("id") if isinstance(payload, dict) else None
        # 只有返回的 id 与请求的不同才算改名：库还在、id 没变时详情返回的就是
        # 它自己的 id，若一律记下，存活的库会被判成 renamed。
        renamed_to = resolved if isinstance(resolved, str) and resolved != dataset_id else None
        return Probe(status=200, resolved_id=renamed_to or None)

    def _request(self, params: dict[str, Any]) -> HfResponse:
        # 扫描路径传 `honor_window=True`：见 MAX_RATE_LIMIT_SLEEP 的注释——
        # 30 秒封顶让它在 429 上必然重试耗尽，而它并不持锁，等得起一个整窗。
        response = self._fetch(
            self._base_url, dict(params), {"Accept": "application/json"}, honor_window=True
        )
        if response is None:
            detail = self._last_failure or "未知"
            raise RuntimeError(f"HF 接口持续限流或不可用（最后一次：{detail}），重试已耗尽")
        if response.status == 200:
            return response
        raise RuntimeError(f"HF 接口返回 {response.status}: {response.body[:200]!r}")

    def _fetch(
        self,
        url: str,
        params: dict[str, Any],
        headers: dict[str, str],
        *,
        honor_window: bool = False,
    ) -> HfResponse | None:
        """单次 GET，带退避重试；可重试状态耗尽仍不成功时返回 None。

        `honor_window` 决定 429 退避要不要睡满服务器给的窗口：扫描（长跑、不持锁）
        要，逐条探测（持写锁、单次调用）不要。两者共用一条实现是刻意的，但**退避
        策略必须分开**——同一条上限不可能同时对「1140 页」和「2000 次单条」成立。

        扫描与探测共用这一条，是刻意的：`probe` 原先直接调 transport，429 会原样
        落成 `unknown`，而 `unknown` 在判定表里是「先别动」——配额紧的时候，一个
        已经在 HF 上删掉的库会永远停在 `active`，既不报错也不再被复核。探测比扫描
        更经不起这种沉默：扫描漏一页下轮还能补，状态判错了却没有任何机制回头查。
        """
        # 连接中断与 429 用两套预算：前者靠熬（没有答复可依据），后者靠睡 `t=`。
        transport_limit = self._max_retries + (TRANSPORT_RETRY_EXTRA if honor_window else 0)
        attempt = 0
        while True:
            try:
                response = self._transport.get(url, params, headers)
            except TransportError:
                # 连接中断是「没收到答复」，重试耗尽后原样抛出，让调用方知道不是限流。
                if attempt >= transport_limit:
                    raise
                self._sleeper(self._backoff(attempt))
                attempt += 1
                continue

            if response.status not in RETRYABLE_STATUS:
                return response
            if attempt >= self._max_retries:
                self._last_failure = f"HTTP {response.status}"
                return None
            self._sleeper(self._retry_delay(response, attempt, honor_window=honor_window))
            attempt += 1

    def _backoff(self, attempt: int) -> float:
        return min(self._retry_base_delay * (2**attempt), 60.0)

    def _retry_delay(
        self, response: HfResponse, attempt: int, *, honor_window: bool = False
    ) -> float:
        _, reset_in = _parse_rate_limit(response.headers)
        if reset_in:
            # `t=` 是「窗口还剩多少秒」，最长等于整个窗口（实测 300 秒）。
            # 探测路径封顶 30 秒：一次探测退避 5 次就是 25 分钟，2000 个 × 8 线程
            # 足以拖成几小时，还攥着写锁不放（2026-11-13 实测堵死日常链 2 小时 50 分）。
            # 扫描路径睡满窗口：它的瓶颈就是配额，截断只会让重试一次次白跑。
            # 多睡 1 秒是给服务器时钟留余量——按整秒边界醒来常常还是同一窗口。
            if honor_window:
                return min(float(reset_in) + 1.0, MAX_RATE_LIMIT_SLEEP)
            return min(float(reset_in), MAX_RETRY_SLEEP)
        return min(self._retry_base_delay * (2**attempt), 60.0)

    def _pace(self, remaining: int | None, reset_in: int | None) -> float:
        """按剩余配额算出「取下一页前该睡多久」，不睡则返回 0。

        原先只在「剩余 < 10」时睡满整窗，等于每次都先把配额打光、撞一次 429。
        一页 1000 条的扫描要 1140 页，配额约 1000 请求 / 5 分钟——打光之后必然
        卡在窗口上，而重试上限又等不满窗口，于是整轮判负。

        这里改成匀速：`reset_in / remaining` 就是「这一个请求还能占多少秒」。
        配额宽裕时它小于 PACE_FLOOR（网络延迟本来就更慢），不睡；配额紧时它
        自然变长，把一轮扫描摊进多个窗口，中途不撞 429。比「撞了再退」稳，
        也比固定延时诚实——速率来自服务器自己给的头，不是猜的。
        """
        if not remaining or not reset_in:
            return 0.0
        if remaining <= 0:
            return min(float(reset_in) + 1.0, MAX_RATE_LIMIT_SLEEP)
        rate = reset_in / remaining
        if rate < PACE_FLOOR:
            return 0.0
        if rate > MAX_PACE_SLEEP:
            # 配额将尽：与其按 60 秒一个请求慢慢磨，不如等窗口重开一次拿满。
            return min(float(reset_in) + 1.0, MAX_RATE_LIMIT_SLEEP)
        return rate

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
