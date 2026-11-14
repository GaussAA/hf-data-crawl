"""用 datasets-server 补数据集的规模与形状。

列表接口给的是仓库字节数（`mainSize`），卡片里声明的行数只覆盖少数数据集
（当前覆盖率见 `docs/DATA.md` 的「HF 页面上的数字」一节，那里按实际数据计算）。
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
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import polars as pl

from ..platform.atomic import write_parquet
from ..platform.coerce import as_int
from ..platform.tables import SIZE_SCHEMA, url_for
from .batch import collect_rows
from .hf.api import Transport, TransportError

if TYPE_CHECKING:
    from ..platform.store import DatasetStore

DEFAULT_SIZE_URL = "https://datasets-server.huggingface.co/size"

#: 明确表示「这个数据集没有规模数据」的状态码。重试无意义。
UNAVAILABLE_STATUS: frozenset[int] = frozenset({401, 403, 404, 500, 501})

#: 值得重试的状态码：限流与网关抖动。
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
        payload = json.loads(body.decode("utf-8"))
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


def stale_ids(current: pl.DataFrame, existing: pl.DataFrame) -> pl.DataFrame:
    """规模已落后于内容的行：`lastModified` 晚于规模抓取日，且上次是成功的。

    判据取「抓取当天」而非更细的时刻，是因为 `size.snapshot_date` 只有日期：
    同日之内的改动可能漏判一天，下一轮仍会被发现（日期越过即成立），不会永久
    漏掉。宁可晚一天，也不拿「看起来相等」冒充新鲜。

    新库不在此列：它压根没有规模行，`join` 时自然缺席，走常规缺口补抓。
    `unavailable` 的也不在此列：那是「上游没做 parquet 转换」，改 README 不会
    让它变得有行数，逐日重烧只是白费预算。要重试那批，用周任务的全量趟加
    `--retry-unavailable`，那是另一件事。
    """
    empty = current.head(0)
    if current.height == 0 or existing.height == 0:
        return empty
    # 两张表都有 `status`：不先改名，`pl.col("status")` 取到的是底库的在架状态
    # （active/disappeared），过滤恒不成立——判据会静默变成「永远没有陈旧」。
    joined = current.join(
        existing.select("id", "snapshot_date", pl.col("status").alias("size_status")),
        on="id",
        how="inner",
    )
    return joined.filter(
        (pl.col("size_status") == "ok")
        & (pl.col("last_modified").dt.date() > pl.col("snapshot_date"))
    )


#: 抓取期间的暂存目录（相对数据根）。分片只写真数据、不碰正式表，故写它不必持锁。
STAGING_DIR = ".staging/sizes"


def staging_root(store: DatasetStore) -> Path:
    """暂存分片所在目录。"""
    return store.root / STAGING_DIR


def staged_shards(store: DatasetStore) -> list[Path]:
    """现有暂存分片，按文件名排序。

    同一轮内序号递增，文件名序即写入序；跨轮顺序无关紧要——各轮的 id 集合
    已经互斥（本轮开跑时就把已有分片记作「已处理」）。
    """
    root = staging_root(store)
    if not root.exists():
        return []
    return sorted(root.glob("*.parquet"))


def read_staged(store: DatasetStore) -> tuple[pl.DataFrame, list[Path]]:
    """读出上次中断留下的暂存分片，返回 (结果表, 分片路径)。

    分片写盘是原子的，读到的要么是完整的一批、要么不存在；不必做半截容错。
    但并发跑的另一个进程可能在列目录之后、读文件之前把某个分片删掉（它合并完
    自己的那份），丢掉一个分片不算错误——那一批已经由它并入正式表了。
    """
    paths = staged_shards(store)
    frames: list[pl.DataFrame] = []
    for path in paths:
        try:
            frames.append(pl.read_parquet(path))
        except FileNotFoundError:
            continue
    if not frames:
        return pl.DataFrame(schema=SIZE_SCHEMA), paths
    return pl.concat(frames, how="vertical_relaxed"), paths


class StagingWriter:
    """抓取期间的追加式暂存：一批一个分片，原子写入，不碰正式表。

    为什么要它：`fetch-sizes` 全量跑三小时，若整段持锁，当天的 `collect` 会直接
    因拿不到锁而失败（2026-11-07 实际事故）。把写入改成「锁外写分片、收尾短锁
    合并」之后，长耗时部分不再占锁；中断时已抓到的批次也还在分片里，下一轮
    据此续抓，而不是从头再烧一遍请求。
    """

    def __init__(self, store: DatasetStore, snapshot_date: date) -> None:
        self._root = staging_root(store)
        self._prefix = snapshot_date.isoformat()
        # 用随机串而不是 pid 做分片标识：pid 会被复用，复用之后这一轮的分片会
        # 直接覆盖上一轮遗留的同名分片（原子替换不会报错，只是数据悄悄没了）。
        self._run = uuid4().hex[:12]
        self._seq = 0
        #: 已写进分片的行数。尾部不足 `flush_every` 的那批由调用方补写。
        self.flushed = 0

    def write(self, frame: pl.DataFrame) -> Path | None:
        """原子写一个分片；空批返回 None。"""
        if frame.height == 0:
            return None
        self._root.mkdir(parents=True, exist_ok=True)
        target = self._root / f"{self._prefix}-{self._run}-{self._seq:04d}.parquet"
        write_parquet(frame, target)
        self._seq += 1
        self.flushed += frame.height
        return target


def clear_staged(store: DatasetStore, paths: Sequence[Path] | None = None) -> None:
    """删除已并入正式表的暂存分片；不给路径就清空整个暂存目录。

    只删「这一次读到的路径」而不是整目录：并发跑的另一轮可能刚写进新分片，
    那是它的成果，不能替它丢掉。
    """
    targets = staged_shards(store) if paths is None else list(paths)
    for path in targets:
        path.unlink(missing_ok=True)
    root = staging_root(store)
    if root.exists() and not any(root.iterdir()):
        root.rmdir()


def tail(count: int, total: int) -> int:
    """进度百分比。抽出来便于单测边界（total=0 时不许除零）。"""
    if total <= 0:
        return 0
    return min(100, math.floor(count / total * 100))
