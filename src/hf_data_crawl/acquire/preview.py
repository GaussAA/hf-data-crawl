"""演练数据：从 datasets-server 的 viewer 取前几行，看「这个数据集长什么样」。

为什么单开一张表、不进默认导出
------------------------------
`AGENTS.md` 写着「不下载数据集文件」。viewer 的几行不是文件，但它是**内容**而非
元数据，性质上跨过了那条边界。故沿用 `readme` 表的先例（GLOSSARY #94）：单开一张
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
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from typing import Any, Literal

import orjson
import polars as pl

from ..platform.coerce import as_int
from ..platform.tables import PREVIEW_SCHEMA, url_for
from .hf.api import Transport, TransportError

#: 默认取几行。样例的用途是「看一眼」，三条足够看出列与值的形状。
DEFAULT_ROWS = 3

#: 默认抓多少个库。样例要的是「有代表性的一批」，不是全站；一万个体量下
#: 截断后约 10 MB，跑一轮十来分钟。
PREVIEW_TOP = 10_000

#: `/rows` 的 `length` 上限；再大要自己翻页，那样就不是样例了。
MAX_ROWS = 100

#: 每格字符上限。取 500 是因为实测 3 行可以到 161 KB，而一屏能读的也就这么多。
MAX_CELL_CHARS = 500

#: 截断过的值后面缀这一句，让人一眼看出「这不是全文」。
TRUNCATION_MARK = "…（截断）"

DEFAULT_PREVIEW_URL = "https://datasets-server.huggingface.co/rows"

#: `/rows` 用 404 表示「这个库没有 viewer」，401 表示受限或不存在——都是永久缺失。
UNAVAILABLE_STATUS = frozenset({401, 404})

#: 429/5xx 是瞬时的，退避重试。
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


@dataclass(frozen=True)
class PreviewResult:
    """一个库取样例的结果。`ok` 才带行，其余状态不落库。"""

    dataset_id: str
    status: Literal["ok", "unavailable", "error"]
    row: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PreviewSummary:
    """一轮抓取的计数。`unavailable` 会落库（下轮跳过），`failed` 不会。"""

    fetched: int = 0
    unavailable: int = 0
    failed: int = 0
    truncated: int = 0
    elapsed_seconds: float = 0.0


class HfPreviewApi:
    """单库取前几行。`/rows` 不支持批量，一条请求一个数据集。"""

    def __init__(
        self,
        transport: Transport,
        *,
        rows: int = DEFAULT_ROWS,
        config: str = "default",
        split: str = "train",
        base_url: str = DEFAULT_PREVIEW_URL,
        max_retries: int = 3,
        retry_base_delay: float = 2.0,
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

    def fetch(self, dataset_id: str, snapshot_date: date) -> PreviewResult:
        attempt = 0
        while True:
            try:
                response = self._transport.get(
                    self._base_url,
                    {
                        "dataset": dataset_id,
                        "config": self._config,
                        "split": self._split,
                        "offset": 0,
                        "length": self._rows,
                    },
                    {"Accept": "application/json"},
                )
            except TransportError:
                if attempt >= self._max_retries:
                    return PreviewResult(dataset_id, "error")
                self._sleeper(self._backoff(attempt))
                attempt += 1
                continue

            if response.status == 200:
                return _success(dataset_id, snapshot_date, self._config, self._split, response.body)
            if response.status in RETRYABLE_STATUS and attempt < self._max_retries:
                self._sleeper(self._backoff(attempt))
                attempt += 1
                continue
            if response.status in UNAVAILABLE_STATUS:
                # 永久缺失：记下来，下轮不再为它烧一次请求。
                return _unavailable(dataset_id, snapshot_date, self._config, self._split)
            return PreviewResult(dataset_id, "error")

    def _backoff(self, attempt: int) -> float:
        return min(self._retry_base_delay * (2**attempt), 30.0)


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
    """按名单并发取样例，结果按 id 归并成一张表。"""

    def __init__(
        self,
        api: HfPreviewApi,
        *,
        workers: int = 8,
        progress: Callable[[int, int], None] | None = None,
    ) -> None:
        self._api = api
        self._workers = max(1, workers)
        self._progress = progress

    def run(
        self, snapshot_date: date, dataset_ids: list[str]
    ) -> tuple[PreviewSummary, pl.DataFrame]:
        started = time.monotonic()
        results: list[PreviewResult] = []
        total = len(dataset_ids)
        done = 0
        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = [pool.submit(self._api.fetch, item, snapshot_date) for item in dataset_ids]
            for future in as_completed(futures):
                results.append(future.result())
                done += 1
                if self._progress is not None:
                    self._progress(done, total)

        rows = [item.row for item in results if item.row]
        summary = PreviewSummary(
            fetched=sum(1 for item in results if item.status == "ok"),
            unavailable=sum(1 for item in results if item.status == "unavailable"),
            failed=sum(1 for item in results if item.status == "error"),
            truncated=sum(1 for item in rows if item.get("truncated")),
            elapsed_seconds=time.monotonic() - started,
        )
        return summary, _frame(rows)


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
