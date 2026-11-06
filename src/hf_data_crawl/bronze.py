"""Bronze 层：由 Raw 整体重建某一天的标量表。

以「分区整体重建」而非追加实现幂等：给定同一天的 Raw，输出永远一致。

解压、解析、逐条编码是纯 CPU 活，且各分块互不依赖，所以文件多时交给进程池并行：
111 万行（2452 个分块）的实测里，串行 271 秒，12 个进程 22 秒，输出逐行相等。
合并前按排序键整体排序，因此并行与否不影响「同 id 以最后写入者为准」。
"""

from __future__ import annotations

import json
import os
import shutil
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from . import raw
from .coerce import as_bool, as_float, as_int, as_json, as_text
from .tables import BRONZE_SCHEMA, table

if TYPE_CHECKING:
    from .store import DatasetStore

#: 分块数达到此值才起进程池；Windows 上起进程要秒级，小批量并行反而更慢。
PARALLEL_MIN_FILES = 64
MAX_WORKERS = 12


def to_row(record: Any, snapshot_date: date, page_file: str) -> dict[str, Any] | None:
    """一条原始记录 → Bronze 行；缺 id 或形态不对则丢弃。"""
    if not isinstance(record, dict):
        return None
    dataset_id = record.get("id")
    if not isinstance(dataset_id, str) or not dataset_id:
        return None
    tags = record.get("tags")
    return {
        "id": dataset_id,
        "author": as_text(record.get("author")),
        "sha": as_text(record.get("sha")),
        "created_at": as_text(record.get("createdAt")),
        "last_modified": as_text(record.get("lastModified")),
        "downloads": as_int(record.get("downloads")),
        "downloads_all_time": as_int(record.get("downloadsAllTime")),
        "likes": as_int(record.get("likes")),
        "trending_score": as_float(record.get("trendingScore")),
        "gated": as_text(record.get("gated")),
        "private": as_bool(record.get("private")),
        "disabled": as_bool(record.get("disabled")),
        "main_size": as_int(record.get("mainSize")),
        "xet_enabled": as_bool(record.get("xetEnabled")),
        "resource_group": as_json(record.get("resourceGroup")),
        "tags": [str(tag) for tag in tags] if isinstance(tags, list) else [],
        "description": as_text(record.get("description")),
        "raw": json.dumps(record, ensure_ascii=False, sort_keys=True),
        "page_file": page_file,
        "snapshot_date": snapshot_date,
    }


def _load_rows(job: tuple[str, str]) -> tuple[tuple[Any, ...], pl.DataFrame]:
    """一个分块 → (排序键, 该分块的 Bronze 行)。顶层函数，进程池可直接调用。"""
    path, snapshot = job
    snapshot_date = date.fromisoformat(snapshot)
    batch = raw.load_batch(Path(path))
    rows = [
        row
        for record in batch.records
        if (row := to_row(record, snapshot_date, batch.file)) is not None
    ]
    return batch.key, pl.DataFrame(rows, schema=BRONZE_SCHEMA)


def _load_all(files: list[Path], snapshot_date: date) -> list[tuple[tuple[Any, ...], pl.DataFrame]]:
    jobs = [(str(path), snapshot_date.isoformat()) for path in files]
    if len(jobs) < PARALLEL_MIN_FILES:
        return [_load_rows(job) for job in jobs]
    workers = max(1, min(MAX_WORKERS, os.cpu_count() or 1))
    try:
        with ProcessPoolExecutor(max_workers=workers) as pool:
            return list(pool.map(_load_rows, jobs, chunksize=16))
    except BrokenProcessPool:
        # 子进程异常终止（实测在副本上出现过一次，原因未查明）时，进程池整体作废。
        # 分块互不依赖、解析是纯函数，从头串行重算结果相同，只是慢；
        # 好过让一次可重算的 build 因为并行加速这个可选项而整体失败。
        return [_load_rows(job) for job in jobs]


def prune(store: DatasetStore) -> list[date]:
    """只保留最新一天的 Bronze 分区，返回被删的日期。

    Bronze 是 Raw 的直接展开，随时可在秒级到半分钟内重建；历史上每天一份近乎
    全量的拷贝只是占地方。属性历史由 Silver 变更流水承担，指标历史由观测流水
    承担，当天的运行态更新只需要当天这一份。
    """
    files = sorted(store.root.glob(table("bronze").glob()))
    removed: list[date] = []
    for path in files[:-1]:
        partition = path.parent
        removed.append(date.fromisoformat(partition.name.removeprefix("dt=")))
        shutil.rmtree(partition, ignore_errors=True)
    return removed


def build(store: DatasetStore, snapshot_date: date) -> int:
    """从该日期的 Raw 整体重建 Bronze 分区，返回写入标量行数。"""
    files = raw.batch_files(store, snapshot_date)
    if not files:
        return 0

    parts = _load_all(files, snapshot_date)
    parts.sort(key=lambda part: part[0])
    frames = [frame for _key, frame in parts if frame.height]
    if not frames:
        return 0

    frame = pl.concat(frames, how="vertical").unique(
        subset=["id"], keep="last", maintain_order=True
    )

    store.write("bronze", frame, snapshot_date)
    return frame.height
