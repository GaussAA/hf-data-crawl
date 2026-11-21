"""用 datasets-server 补数据集的规模与形状。

列表接口给的是仓库字节数（`mainSize`），卡片里声明的行数只覆盖少数数据集
（当前覆盖率见 `data/DATA.md` 的「HF 页面上的数字」一节，那里按实际数据计算）。
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
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

import orjson
import polars as pl

from ..platform.coerce import as_int
from ..platform.tables import SIZE_SCHEMA, url_for
from .batch import (
    StagingWriter as _StagingWriter,
)
from .batch import (
    clear_staged as _clear_staged,
)
from .batch import (
    collect_rows,
)
from .batch import (
    read_staged as _read_staged,
)
from .enrich import DEFAULT_TOP
from .hf.api import Transport, TransportError

if TYPE_CHECKING:
    from ..platform.store import DatasetStore

DEFAULT_SIZE_URL = "https://datasets-server.huggingface.co/size"

#: 明确表示「这个数据集没有规模数据」的状态码。重试无意义。
#:
#: 401/403 是权限，404 是「上游确实没有」。**500/501 也在这里，尽管 HTTP 语义上
#: 它们是服务器错误**——因为 HF 的 `/size` 接口用这两个码表示「不支持该数据集」，
#: 而不是瞬时故障。
#:
#: 实测（2026-11-17）：抽样 15 个此前标记为 unavailable 的库，全部在 0.3–0.9 秒
#: 返回 500/501/404，无一挂起、无一恢复；同批 `ok` 的库抽样 5 个全部 200。
#: 曾按 HTTP 语义把它们挪进 `RETRYABLE_STATUS`，结果每个库空转 5 次重试 + 退避
#: 约 31 秒，1,269 个这样的库折算 3.6 小时——那次「卡住」的真相就在这里。
#: 要再动这一行，先重跑上面那个抽样。
UNAVAILABLE_STATUS: frozenset[int] = frozenset({401, 403, 404, 500, 501})

#: 可重试：429 限流与网关抖动（502/503/504）。这些才是真正的瞬时故障。
RETRYABLE_STATUS: frozenset[int] = frozenset({429, 502, 503, 504})


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
        payload = orjson.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return _failure(dataset_id, "error")
    size = payload.get("size") if isinstance(payload, dict) else None
    if not isinstance(size, dict):
        return _failure(dataset_id, "error")
    raw_dataset = size.get("dataset")
    raw_configs = size.get("configs")
    dataset: dict[str, Any] = raw_dataset if isinstance(raw_dataset, dict) else {}
    configs: list[Any] = raw_configs if isinstance(raw_configs, list) else []

    return SizeResult(
        dataset_id=dataset_id,
        status="ok",
        row={
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "fetched_at": datetime.now(UTC),
            "status": "ok",
            "num_rows": as_int(dataset.get("num_rows")),
            "estimated_num_rows": as_int(dataset.get("estimated_num_rows")),
            "num_bytes_original": as_int(dataset.get("num_bytes_original_files")),
            "num_bytes_parquet": as_int(dataset.get("num_bytes_parquet_files")),
            "num_bytes_memory": as_int(dataset.get("num_bytes_memory")),
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
    row.update(
        {
            "id": dataset_id,
            "url": url_for(dataset_id),
            "snapshot_date": snapshot_date,
            "status": "unavailable",
        }
    )
    return SizeResult(dataset_id=dataset_id, status=status, row=row)


def _max_columns(configs: Sequence[Any]) -> int | None:
    values = [as_int(config.get("num_columns")) for config in configs if isinstance(config, dict)]
    present = [value for value in values if value is not None]
    return max(present) if present else None


class SizeFetcher:
    """按下载量取头部，并发补齐规模数据。"""

    def __init__(
        self,
        api: HfSizeApi,
        *,
        workers: int = 16,
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
        top: int = DEFAULT_TOP,
        retry_unavailable: bool = False,
        existing: pl.DataFrame | None = None,
        refresh: Iterable[str] = (),
        flush: Callable[[pl.DataFrame], object] | None = None,
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
        done = _resolved_ids(existing, retry_unavailable=retry_unavailable, refresh=refresh)
        pending = [dataset_id for dataset_id in candidates if dataset_id not in done]
        skipped = len(candidates) - len(pending)

        results, frame = collect_rows(
            pending=pending,
            fetch=lambda dataset_id: self._api.fetch(dataset_id, snapshot_date),
            schema=SIZE_SCHEMA,
            workers=self._workers,
            flush=flush,
            flush_every=flush_every,
            progress=self._progress,
            heartbeat=self._heartbeat,
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


def _resolved_ids(
    existing: pl.DataFrame | None,
    *,
    retry_unavailable: bool,
    refresh: Iterable[str] = (),
) -> set[str]:
    """已处理过的 id。默认连不可用的一并跳过，避免每轮重烧 30% 的预算。

    `refresh` 里的 id 一律从「已处理」中剔除：规模是**内容的函数**，库一旦改动
    （`lastModified` 越过抓取日），旧的行数就不再是事实。跳过判据只能表达
    「问过没」，表达不了「问过之后它变了没有」——这一层必须由调用方显式提出。
    """
    if existing is None or existing.height == 0:
        return set()
    if retry_unavailable:
        # 只重试 ok 以外的，ok 的没必要再查。
        done = set(existing.filter(pl.col("status") == "ok")["id"].to_list())
    else:
        done = set(existing["id"].to_list())
    return done - set(refresh)


#: 规模表的暂存目录；分片只写真数据、不碰正式表，故写它不必持锁。
STAGING_DIR = ".staging/sizes"


def protect_ok_rows(previous: pl.DataFrame, fresh: pl.DataFrame) -> pl.DataFrame:
    """不让「不可用」覆盖「已拿到」。

    为什么需要：`store.upsert` 的替换键是整行，而 `unavailable` 行除 `status` 与
    `snapshot_date` 外全是 null。一次瞬时 500（它在 `UNAVAILABLE_STATUS` 里，与
    404 同等对待）就会把旧行的 `num_rows` / `configs` 一起抹掉；此后
    `_resolved_ids` 把该 id 长期跳过——它只认「有没有行」，不看状态——只有周任务
    的 `--retry-unavailable` 可能把它救回来。而那次降级在表里只表现为「值变空」。

    判据只看「旧行是不是 ok」：从未成功抓过的 id 照常写 `unavailable`（那是真实
    结论，也是永久结论），已经拿到数字的 id 保持原样——`snapshot_date` 因此诚实地
    表示「这个值是哪天取得的」，而不是「哪天问过」（data/DATA.md 第五节第 14 条）。
    """
    if previous.height == 0 or fresh.height == 0:
        return fresh
    protected = previous.filter(pl.col("status") == "ok").select("id")
    if protected.height == 0:
        return fresh
    degraded = fresh.filter(pl.col("status") != "ok").join(protected, on="id", how="semi")
    if degraded.height == 0:
        return fresh
    return fresh.join(degraded.select("id"), on="id", how="anti")


def read_staged(store: DatasetStore) -> tuple[pl.DataFrame, list[Path]]:
    """读出上次中断留下的规模分片，返回 (结果表, 分片路径)。"""
    return _read_staged(store, STAGING_DIR, SIZE_SCHEMA)


class StagingWriter(_StagingWriter):
    """规模表的暂存写入：目录固定为自己的那一个。"""

    def __init__(self, store: DatasetStore, snapshot_date: date) -> None:
        super().__init__(store, snapshot_date, STAGING_DIR)


def clear_staged(store: DatasetStore, paths: Sequence[Path] | None = None) -> None:
    """删除已并入正式表的规模分片；不给路径就清空暂存目录。"""
    _clear_staged(store, paths, STAGING_DIR)


def tail(count: int, total: int) -> int:
    """进度百分比。抽出来便于单测边界（total=0 时不许除零）。"""
    if total <= 0:
        return 0
    return min(100, math.floor(count / total * 100))
