"""演练数据：从 datasets-server 的 viewer 取前几行，看「这个数据集长什么样」。

为什么单开一张表、不进默认导出
------------------------------
`AGENTS.md` 写着「不下载数据集文件」。viewer 的几行不是文件，但它是**内容**而非
元数据，性质上跨过了那条边界。故沿用 `readme` 表的先例（DECISIONS #94）：单开一张
`preview` 表、不进默认导出、只在点名或取头部时抓。

抓到的只是某一刻的切片
----------------------
viewer 背后是自动转出来的 parquet，上游重写数据集，行序与内容都会变。故这份数据
不参与可复现交付，`fetched_at` 记到时刻，让人知道它代表哪一刻。

一个请求的天花板
----------------
`/rows` 要 `config` 与 `split` 两个参数，而底库里只存了 config 名、没存 split。
故这两个参数由调用方给默认值（`default` / `train`），对不上的库会落成
`unavailable`。能在 `/splits` 里问到真名再补是下一步；头部一万个库里绝大多数
就是这两个值。

响应要分四类
------------
200 取到；404 说「这个库没有 viewer」（`ylecun/mnist` 就是）；401 说「受限或不
存在」——两种情况的报文一字不差，只能都记成永久缺失；429/5xx 是瞬时，重试。

单元格截断
----------
3 行也可能 161 KB（长文本 + list 列）。每格超过 `MAX_CELL_CHARS` 就截断并记
`truncated=True`：样例要的是「长什么样」，不是全文。
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

import orjson
import polars as pl

from ..platform.coerce import as_int
from ..platform.tables import PREVIEW_SCHEMA, url_for
from .batch import collect_rows
from .hf.api import Transport, TransportError
from .hf.retry import StatusPolicy, exponential_seconds
from .merge import MergeSpec
from .readme import RateLimiter

#: 默认取几行。样例的用途是「看一眼」，三条足够看出列与值的形状。
DEFAULT_ROWS = 3

#: 默认每秒放行几个请求。`/rows` 比 `/size` 敏感得多：实测 8 并发无节制跑一阵之后，
#: 200 个库全部 429、吞吐掉到 0.5 req/s。主动匀速是唯一稳的做法（教训同 readme）。
DEFAULT_RATE = 4.0

#: 暂存目录（相对数据根）：与 `.staging/sizes`、`.staging/files`、`.staging/readme`
#: 并列。写的是完整相对路径而不是裸目录名——`batch.staging_root()` 只做
#: `store.root / subdir`，传 "preview" 会把分片落到 `data/preview/`，doctor 的
#: 残留检查与清理都看不见它（2026-11-20 由测试审计发现）。
PREVIEW_STAGING_DIR = ".staging/preview"

#: 收尾合并的声明。
#:
#: **不做旧值保护**：样例是「某一刻的切片」，旧的 `ok` 与新 `unavailable` 之间
#: 没有「谁更全」的次序——库里换了数据，旧样例就不再代表现在。这与 `size` /
#: `readme` 不同：那两张表的旧值是**观测结果**（行数、正文），不该被一次失败抹掉。
PREVIEW_MERGE = MergeSpec(
    table="preview",
    staging_dir=PREVIEW_STAGING_DIR,
    schema=PREVIEW_SCHEMA,
    lock="fetch-preview:merge",
)

#: 每攒多少条落一次分片。样例一行只有几 KB，200 条一次足够细。
PREVIEW_FLUSH_EVERY = 200

#: 默认抓多少个库。样例要的是「有代表性的一批」，不是全站；一万个体量下
#: 截断后约 10 MB。
PREVIEW_TOP = 10_000

#: `/rows` 的 `length` 上限；再大要自己翻页，那样就不是样例了。
MAX_ROWS = 100

#: 每格字符上限。取 500 是因为实测 3 行可以到 161 KB，而一屏能读的也就这么多。
MAX_CELL_CHARS = 500

#: 截断过的值后面缀这一句，让人一眼看出「这不是全文」。
TRUNCATION_MARK = "…（截断）"

DEFAULT_PREVIEW_URL = "https://datasets-server.huggingface.co/rows"

#: `config` 传 `auto` 时先问这个端点要真实的 config 名。
DEFAULT_SPLITS_URL = "https://datasets-server.huggingface.co/splits"

#: `--config auto`：让每个库自己去问「你的 config 叫什么」。
#:
#: 原先一律传 `default`，但**多数库的 config 不叫 default**：`openai/gsm8k` 是
#: `main`、`allenai/c4` 是 `en`、`Salesforce/wikitext` 是 `wikitext-2-raw-v1`。
#: 更糟的是传错名字不会得到 422「config 不存在」，而是 429/500——于是它看起来
#: 像限流，实际是参数错（2026-11-18 实测：top 20 里 9 个「临时失败」全是这一类）。
AUTO_CONFIG = "auto"

#: 这张端点的状态分区。`/rows` 用 404 表示「这个库没有 viewer」，401 表示受限或
#: 不存在——都是永久缺失。5xx 是瞬时的，退避重试。
#:
#: **429 不在分区里**：CloudFront 按 IP 限流，撞上之后每个线程各自退避只会把封禁
#: 拖长（实测 8 并发跑了一阵后，200 个库全部 429、吞吐掉到 0.5 req/s）。故它由
#: 请求循环单独处理：设了 `--throttle-wait` 就睡过再试，没设就整轮中止。
POLICY = StatusPolicy(
    retryable=frozenset({500, 502, 503, 504}),
    permanent={401: "unavailable", 404: "unavailable"},
)

#: 撞到限流。整轮立即停：继续请求只会延长封禁。
THROTTLED_STATUS = 429


class Throttled(RuntimeError):
    """被上游限流（429）。整轮中止，已取到的行仍然可以并入。"""


class ThrottleGate:
    """撞 429 后全体停一停：限流按 IP，一个人撞了，别人也不该继续打。

    为什么要有它：这个接口的额度小到「一个窗口只够几十条」，而整轮中止意味着
    每撞一次就白跑一次。给它 `--throttle-wait` 把「中止」换成「睡过再试」，
    一次进程就能跨过多个窗口把量补上来（实测每天最多推进几十行是靠这个）。

    `wait` 只睡一次剩余时长：多个线程会先后醒来，但 `trip` 取 `max`，后到的会
    把前一次剩下的时间一起等掉，不会出现「刚醒就再撞」的锯齿。
    """

    def __init__(
        self,
        wait_seconds: float,
        *,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._wait = max(0.0, wait_seconds)
        self._sleep = sleep
        self._clock = clock
        self._lock = threading.Lock()
        self._until = 0.0

    def trip(self) -> None:
        with self._lock:
            self._until = max(self._until, self._clock() + self._wait)

    def wait(self) -> None:
        with self._lock:
            remaining = self._until - self._clock()
        if remaining > 0:
            self._sleep(remaining)


@dataclass(frozen=True)
class PreviewResult:
    """一个库取样例的结果。`ok` 才带行，其余状态不落库。"""

    dataset_id: str
    status: Literal["ok", "unavailable", "error"]
    row: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PreviewSummary:
    """一轮抓取的计数。`unavailable` 会落库（下轮跳过），`failed` 不会。

    `aborted` 表示中途撞上限流：已取到的行有效，但这一轮不算跑完。
    """

    fetched: int = 0
    unavailable: int = 0
    failed: int = 0
    truncated: int = 0
    elapsed_seconds: float = 0.0
    aborted: bool = False
    #: 本轮撞了几次 429（设了 --throttle-wait 时它们只是等待，不算失败）。
    throttles: int = 0


class HfPreviewApi:
    """单库取前几行。`/rows` 不支持批量，一条请求一个数据集。"""

    def __init__(
        self,
        transport: Transport,
        *,
        rows: int = DEFAULT_ROWS,
        config: str = AUTO_CONFIG,
        split: str = "train",
        rate: float = DEFAULT_RATE,
        base_url: str = DEFAULT_PREVIEW_URL,
        max_retries: int = 3,
        retry_base_delay: float = 2.0,
        throttle_wait: float = 0.0,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._transport = transport
        self._rows = max(1, min(rows, MAX_ROWS))
        self._config = config
        self._split = split
        self._base_url = base_url
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._sleeper = sleeper or time.sleep
        #: 撞 429 之后睡多久再继续；0 表示整轮中止（默认）。
        self._gate = ThrottleGate(throttle_wait, sleep=self._sleeper) if throttle_wait > 0 else None
        #: 一个库最多被限流几次就放弃它，免得一条坏数据把整轮拖住。
        self._max_throttles = 3
        #: 本轮撞了几次 429，只用于报数。
        self.throttles = 0
        # 主动匀速，而不是「撞 429 再退避」：与 readme 同一个教训（见 `RateLimiter`）。
        self._limiter = RateLimiter(rate)

    def _resolve(self, dataset_id: str, snapshot_date: date) -> tuple[str, str] | PreviewResult:
        """确定这个库的 (config, split)；确定不了就返回一个终态结果。

        `config != auto` 时原样返回，不发请求。

        `auto` 要问一次 `/splits`：`/rows` 的 `config` 是必填的，而各库的 config
        名五花八门（`openai/gsm8k` 是 `main`、`allenai/c4` 是 `en`）。传错不会收到
        「config 不存在」，而是 429/500，所以只能先问。

        **三种失败要分开**（2026-11-18 实测各自都真出现过）：
        - 401/404：这个库没有 viewer → `unavailable`，下轮不再问
        - 429/5xx/网络错误：上游一时不可用 → `error`，下轮重试。原先这里一律记
          `unavailable`，于是 `openai/gsm8k` 明明有 `main/train` 也被钉成「无 viewer」
        - `splits: []` 且 `pending` 非空：正在生成 → `error`，下次再来
        """
        if self._config != AUTO_CONFIG:
            return (self._config, self._split)
        snoozed = 0
        while True:
            if self._gate is not None:
                self._gate.wait()
            self._limiter.acquire()
            try:
                response = self._transport.get(
                    DEFAULT_SPLITS_URL, {"dataset": dataset_id}, {"Accept": "application/json"}
                )
            except TransportError:
                return PreviewResult(dataset_id, "error")
            if response.status != THROTTLED_STATUS:
                break
            # `/splits` 撞限流也不能静默记成 error——否则这一头在吞 429、另一头继续打，
            # 实测连吞 26 个才由 `/rows` 发现（2026-11-18）。
            if not self._snooze(snoozed):
                raise Throttled(f"{dataset_id} 被限流（429）")
            snoozed += 1
        if POLICY.terminal(response.status) is not None:
            return _unavailable(dataset_id, snapshot_date, self._config, self._split)
        if response.status != 200:
            return PreviewResult(dataset_id, "error")
        try:
            payload = orjson.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return PreviewResult(dataset_id, "error")
        if not isinstance(payload, dict):
            return PreviewResult(dataset_id, "error")

        splits = payload.get("splits")
        pending = payload.get("pending")
        if isinstance(splits, list) and splits:
            # 优先 `train`，没有就取第一条：样例要的是「这个库长什么样」，不是某个
            # 特定 split 的完整性。
            for item in splits:
                if isinstance(item, dict) and item.get("split") == "train":
                    return (str(item.get("config")), "train")
            first = splits[0]
            if isinstance(first, dict) and first.get("config"):
                return (str(first["config"]), str(first.get("split") or "train"))
            return PreviewResult(dataset_id, "error")
        if isinstance(pending, list) and pending:
            # viewer 正在生成，过一阵就有了。
            return PreviewResult(dataset_id, "error")
        return _unavailable(dataset_id, snapshot_date, self._config, self._split)

    def fetch(self, dataset_id: str, snapshot_date: date) -> PreviewResult:
        resolved = self._resolve(dataset_id, snapshot_date)
        if isinstance(resolved, PreviewResult):
            return resolved
        config, split = resolved
        attempt = 0
        snoozed = 0
        while True:
            if self._gate is not None:
                self._gate.wait()
            self._limiter.acquire()
            try:
                response = self._transport.get(
                    self._base_url,
                    {
                        "dataset": dataset_id,
                        "config": config,
                        "split": split,
                        "offset": 0,
                        "length": self._rows,
                    },
                    {"Accept": "application/json"},
                )
            except TransportError:
                if attempt >= self._max_retries:
                    return PreviewResult(dataset_id, "error")
                self._sleeper(exponential_seconds(attempt, base=self._retry_base_delay))
                attempt += 1
                continue

            if response.status == 200:
                return _success(dataset_id, snapshot_date, config, split, response.body)
            if response.status == THROTTLED_STATUS:
                # 按 IP 限流：设了闸门就睡过再试，没设就整轮中止（继续打只会更久）。
                if self._snooze(snoozed):
                    snoozed += 1
                    continue
                raise Throttled(f"{dataset_id} 被限流（429）")
            if POLICY.retry(response.status) and attempt < self._max_retries:
                self._sleeper(exponential_seconds(attempt, base=self._retry_base_delay))
                attempt += 1
                continue
            if POLICY.terminal(response.status) is not None:
                # 永久缺失：记下来，下轮不再为它烧一次请求。
                return _unavailable(dataset_id, snapshot_date, config, split)
            return PreviewResult(dataset_id, "error")

    def _snooze(self, already: int) -> bool:
        """撞 429 之后怎么处理：返回 True 表示「睡过再试」，False 表示放弃这个库。

        没设 `--throttle-wait` 就中止——那是默认的保守行为；设了就睡一觉继续，
        一个进程能跨过多个限流窗口，把「每天只推进几十行」变成「一次跑够」。

        **上限是按库算的，不是按轮算的**：`already` 是这个库本轮已经睡过几次。
        原先拿全局计数当上限，于是整轮睡满三次之后任何一次 429 都会中止整轮——
        2026-11-20 实测：209 个库里跑到第 120 个就整轮停了，前功尽弃。
        """
        if self._gate is None or already >= self._max_throttles:
            return False
        self.throttles += 1
        self._gate.trip()
        return True


def _success(
    dataset_id: str, snapshot_date: date, config: str, split: str, body: bytes
) -> PreviewResult:
    try:
        payload = orjson.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return PreviewResult(dataset_id, "error")
    raw_rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(raw_rows, list):
        return PreviewResult(dataset_id, "error")

    features = payload.get("features")
    clipped, truncated = _clip_rows(raw_rows)
    return PreviewResult(
        dataset_id,
        "ok",
        {
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "fetched_at": datetime.now(UTC),
            "status": "ok",
            "config": config,
            "split": split,
            "rows_total": as_int(payload.get("num_rows_total")),
            "row_count": len(clipped),
            "features": json.dumps(features, ensure_ascii=False)
            if isinstance(features, list)
            else None,
            "rows": json.dumps(clipped, ensure_ascii=False),
            "truncated": truncated,
        },
    )


def _clip_rows(raw_rows: list[Any]) -> tuple[list[dict[str, Any]], bool]:
    """把行裁到可存的形状；返回（行, 有没有截断过）。

    结构化的值（列表列、图像/音频对象）先压成字符串再截：留着结构，每行大小就
    不可控，而这张表的价值在于「一眼看出形状」，不在保真。
    """
    rows: list[dict[str, Any]] = []
    truncated = False
    for item in raw_rows:
        if not isinstance(item, dict):
            continue
        values = item.get("row")
        if not isinstance(values, dict):
            continue
        clipped: dict[str, Any] = {}
        for name, value in values.items():
            if isinstance(value, (list, dict)):
                text = json.dumps(value, ensure_ascii=False)
                if len(text) > MAX_CELL_CHARS:
                    text = text[:MAX_CELL_CHARS] + TRUNCATION_MARK
                    truncated = True
                clipped[name] = text
            elif isinstance(value, str) and len(value) > MAX_CELL_CHARS:
                clipped[name] = value[:MAX_CELL_CHARS] + TRUNCATION_MARK
                truncated = True
            else:
                clipped[name] = value
        rows.append({"row_idx": item.get("row_idx"), "row": clipped})
    return rows, truncated


def _unavailable(dataset_id: str, snapshot_date: date, config: str, split: str) -> PreviewResult:
    """永久缺失也落库：一行 `status=unavailable`，下轮据此跳过。

    与 `size` 表同一条规矩——`error` 不落库（可救的别钉成失败），`unavailable`
    落库（不可救的别每轮重烧）。
    """
    row: dict[str, Any] = {name: None for name in PREVIEW_SCHEMA}
    row.update(
        {
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "fetched_at": datetime.now(UTC),
            "status": "unavailable",
            "config": config,
            "split": split,
        }
    )
    return PreviewResult(dataset_id, "unavailable", row)


class PreviewFetcher:
    """按名单并发取样例，结果按 id 归并成一张表。

    并发与落盘全部走 `batch.collect_rows`——和 `fetch-sizes` / `fetch-files` /
    `fetch-readme` 同一个引擎：滑动窗口、卡住心跳、边跑边落暂存分片。这里不再
    自己开线程池，否则「暂存再合并」这件事就得在四条链路上各写一遍。
    """

    def __init__(
        self,
        api: HfPreviewApi,
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
        dataset_ids: list[str],
        *,
        flush: Callable[[pl.DataFrame], object] | None = None,
        flush_every: int = PREVIEW_FLUSH_EVERY,
    ) -> tuple[PreviewSummary, pl.DataFrame]:
        started = time.monotonic()
        # `collect_rows` 会把异常直接抛出来，那时它拼好的表也一起没了；用
        # `on_result` 自己留一份，撞限流中止时还能把已取到的行交出去。
        results: list[PreviewResult] = []
        aborted = False
        try:
            _, frame = collect_rows(
                pending=dataset_ids,
                fetch=lambda item: self._api.fetch(item, snapshot_date),
                schema=PREVIEW_SCHEMA,
                workers=self._workers,
                flush=flush,
                flush_every=flush_every,
                progress=self._progress,
                heartbeat=self._heartbeat,
                on_result=results.append,
            )
        except Throttled:
            # 按 IP 限流：继续打只会延长封禁。已取到的行仍然有效，交给调用方并入。
            aborted = True
            frame = _frame([item.row for item in results if item.row])

        rows = [item.row for item in results if item.row]
        summary = PreviewSummary(
            fetched=sum(1 for item in results if item.status == "ok"),
            unavailable=sum(1 for item in results if item.status == "unavailable"),
            failed=sum(1 for item in results if item.status == "error"),
            truncated=sum(1 for item in rows if item.get("truncated")),
            elapsed_seconds=time.monotonic() - started,
            aborted=aborted,
            throttles=self._api.throttles,
        )
        return summary, frame


def _frame(rows: list[dict[str, Any]]) -> pl.DataFrame:
    """按登记表的列序与类型成帧；没有行时也给出带 schema 的空帧。"""
    if not rows:
        return pl.DataFrame(schema=PREVIEW_SCHEMA)
    frame = pl.DataFrame(rows)
    return frame.select(
        [
            pl.col(name).cast(dtype)
            for name, dtype in PREVIEW_SCHEMA.items()
            if name in frame.columns
        ]
    )
