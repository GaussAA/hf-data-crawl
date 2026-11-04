"""用 datasets-server 补数据集的规模与形状。

列表接口给的是仓库字节数（`mainSize`），卡片给的行数只覆盖 7.3%。
真正回答「这个数据集多少行、多少列」的是 datasets-server 的 `/size`：

    {"size": {"dataset": {"num_rows": ..., "num_bytes_memory": ...},
              "configs": [{"config": ..., "num_rows": ..., "num_columns": ...}]}}

同类接口还有 `/splits`、`/statistics`、`/is-valid`，本模块只用 `/size`：
它是唯一一次请求就能拿到全库规模与列数的入口。

接口脾性（2026-11-04 实测 110 条样本）：

- 成功率约 70%：未做 parquet 转换的数据集返回 500，私有或受保护的返回 401，
  不支持的类型返回 501。**这些是永久性缺失，不该重试**——重试只会白烧时间。
- 并发 16 稳定在 22 req/s，未见 429。再高未验证，保守取 16。

因此本模块把「不可用」与「临时失败」分开记账：前者落 `status=unavailable`，
下次默认跳过；后者不落库，下次自然重试。
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from typing import Any

import polars as pl

from .api import Transport, TransportError

DEFAULT_SIZE_URL = "https://datasets-server.huggingface.co/size"

#: 明确表示「这个数据集没有规模数据」的状态码。重试无意义。
UNAVAILABLE_STATUS: frozenset[int] = frozenset({401, 403, 404, 500, 501})

#: 值得重试的状态码：限流与网关抖动。
RETRYABLE_STATUS: frozenset[int] = frozenset({429, 502, 503, 504})

SIZE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "snapshot_date": pl.Date,
    "status": pl.Utf8,
    "num_rows": pl.Int64,
    "estimated_num_rows": pl.Int64,
    "num_bytes_original": pl.Int64,
    "num_bytes_parquet": pl.Int64,
    "num_bytes_memory": pl.Int64,
    "config_count": pl.Int64,
    "num_columns_max": pl.Int64,
    "configs": pl.Utf8,
}


@dataclass(frozen=True)
class SizeResult:
    """单库规模查询结果。"""

    dataset_id: str
    status: str
    row: dict[str, Any]


@dataclass(frozen=True)
class SizeSummary:
    snapshot_date: date
    requested: int
    fetched: int
    unavailable: int
    failed: int
    skipped: int
    table_rows: int
    elapsed_seconds: float


class HfSizeApi:
    """单库规模查询。与列表接口不同，这里一条请求一个数据集。"""

    def __init__(
        self,
        transport: Transport,
        *,
        base_url: str = DEFAULT_SIZE_URL,
        max_retries: int = 3,
        retry_base_delay: float = 2.0,
        sleeper: Callable[[float], None] | None = None,
    ) -> None:
        self._transport = transport
        self._base_url = base_url
        self._max_retries = max_retries
        self._retry_base_delay = retry_base_delay
        self._sleeper = sleeper or time.sleep

    def fetch(self, dataset_id: str, snapshot_date: date) -> SizeResult:
        attempt = 0
        while True:
            try:
                response = self._transport.get(
                    self._base_url, {"dataset": dataset_id}, {"Accept": "application/json"}
                )
            except TransportError:
                if attempt >= self._max_retries:
                    return _failure(dataset_id, "error")
                self._sleeper(self._backoff(attempt))
                attempt += 1
                continue

            if response.status == 200:
                return _success(dataset_id, snapshot_date, response.body)
            if response.status in UNAVAILABLE_STATUS:
                # 永久缺失：记下来，下次不再浪费一次请求。
                return _failure(dataset_id, "unavailable", snapshot_date)
            if response.status in RETRYABLE_STATUS and attempt < self._max_retries:
                self._sleeper(self._backoff(attempt))
                attempt += 1
                continue
            return _failure(dataset_id, "error")

    def _backoff(self, attempt: int) -> float:
        return min(self._retry_base_delay * (2**attempt), 30.0)


def _success(dataset_id: str, snapshot_date: date, body: bytes) -> SizeResult:
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _failure(dataset_id, "error")
    size = payload.get("size") if isinstance(payload, dict) else None
    if not isinstance(size, dict):
        return _failure(dataset_id, "error")
    dataset = size.get("dataset") if isinstance(size.get("dataset"), dict) else {}
    configs = size.get("configs") if isinstance(size.get("configs"), list) else []

    return SizeResult(
        dataset_id=dataset_id,
        status="ok",
        row={
            "id": dataset_id,
            "snapshot_date": snapshot_date,
            "status": "ok",
            "num_rows": _as_int(dataset.get("num_rows")),
            "estimated_num_rows": _as_int(dataset.get("estimated_num_rows")),
            "num_bytes_original": _as_int(dataset.get("num_bytes_original_files")),
            "num_bytes_parquet": _as_int(dataset.get("num_bytes_parquet_files")),
            "num_bytes_memory": _as_int(dataset.get("num_bytes_memory")),
            "config_count": len(configs) or None,
            "num_columns_max": _max_columns(configs),
            "configs": json.dumps(configs, ensure_ascii=False) if configs else None,
        },
    )


def _failure(dataset_id: str, status: str, snapshot_date: date | None = None) -> SizeResult:
    """不可用的记进表里（下轮跳过）；临时失败留空（下轮自然重试）。

    两者都必须返回 SizeResult，但只有前者带行：`error` 若也落库，等于把可救的
    记录永久钉成失败；`unavailable` 若不落库，每轮都要为它重烧一次请求。
    """
    if status != "unavailable" or snapshot_date is None:
        return SizeResult(dataset_id=dataset_id, status=status, row={})
    row: dict[str, Any] = {name: None for name in SIZE_SCHEMA}
    row.update({"id": dataset_id, "snapshot_date": snapshot_date, "status": "unavailable"})
    return SizeResult(dataset_id=dataset_id, status=status, row=row)


def _max_columns(configs: Sequence[Any]) -> int | None:
    values = [
        _as_int(config.get("num_columns"))
        for config in configs
        if isinstance(config, dict)
    ]
    present = [value for value in values if value is not None]
    return max(present) if present else None


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


class SizeFetcher:
    """按下载量取头部，并发补齐规模数据。"""

    def __init__(
        self,
        api: HfSizeApi,
        *,
        workers: int = 16,
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
        top: int = 20_000,
        retry_unavailable: bool = False,
        existing: pl.DataFrame | None = None,
        flush: Callable[[pl.DataFrame], None] | None = None,
        flush_every: int = 5_000,
    ) -> tuple[SizeSummary, pl.DataFrame]:
        """返回 (摘要, 结果表)。

        `flush` 用于边跑边落库：十万条要跑一小时，只在末尾写一次等于把一小时
        的成果押在最后一个字节上——中断、断电、进程被杀都会全丢。给了 flush
        就每攒满 `flush_every` 条先交出去一次，最坏损失一个批次。
        返回值仍是完整结果，调用方照常整体并入（按 id 覆盖，重复合并无副作用）。
        """
        started = time.monotonic()
        candidates = list(dataset_ids or [])[:top]
        done = _resolved_ids(existing, retry_unavailable=retry_unavailable)
        pending = [dataset_id for dataset_id in candidates if dataset_id not in done]
        skipped = len(candidates) - len(pending)

        results: list[SizeResult] = []
        rows: list[dict[str, Any]] = []
        buffer: list[dict[str, Any]] = []
        completed = 0
        with ThreadPoolExecutor(max_workers=self._workers) as pool:
            futures = {
                pool.submit(self._api.fetch, dataset_id, snapshot_date): dataset_id
                for dataset_id in pending
            }
            for future in as_completed(futures):
                result = future.result()
                results.append(result)
                if result.row:
                    rows.append(result.row)
                    buffer.append(result.row)
                completed += 1

                if flush is not None and len(buffer) >= max(1, flush_every):
                    flush(pl.DataFrame(buffer, schema=SIZE_SCHEMA))
                    buffer.clear()
                if self._progress is not None and (
                    completed % 100 == 0 or completed == len(pending)
                ):
                    self._progress(completed, len(pending))

        frame = (
            pl.DataFrame(rows, schema=SIZE_SCHEMA)
            if rows
            else pl.DataFrame(schema=SIZE_SCHEMA)
        )
        ok = sum(1 for result in results if result.status == "ok")
        unavailable = sum(1 for result in results if result.status == "unavailable")

        return (
            SizeSummary(
                snapshot_date=snapshot_date,
                requested=len(candidates),
                fetched=ok,
                unavailable=unavailable,
                failed=len(results) - ok - unavailable,
                skipped=skipped,
                table_rows=frame.height,
                elapsed_seconds=time.monotonic() - started,
            ),
            frame,
        )


def _resolved_ids(existing: pl.DataFrame | None, *, retry_unavailable: bool) -> set[str]:
    """已处理过的 id。默认连不可用的一并跳过，避免每轮重烧 30% 的预算。"""
    if existing is None or existing.height == 0:
        return set()
    if retry_unavailable:
        # 只重试 ok 以外的，ok 的没必要再查。
        return set(existing.filter(pl.col("status") == "ok")["id"].to_list())
    return set(existing["id"].to_list())


def tail(count: int, total: int) -> int:
    """进度百分比。抽出来便于单测边界（total=0 时不许除零）。"""
    if total <= 0:
        return 0
    return min(100, math.floor(count / total * 100))
