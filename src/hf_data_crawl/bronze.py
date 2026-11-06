"""Bronze 层：由 Raw 整体重建某一天的标量表。

以「分区整体重建」而非追加实现幂等：给定同一天的 Raw，输出永远一致。
"""

from __future__ import annotations

import json
from datetime import date
from typing import TYPE_CHECKING, Any

import polars as pl

from . import raw
from .coerce import as_bool, as_float, as_int, as_json, as_text
from .tables import BRONZE_SCHEMA

if TYPE_CHECKING:
    from .store import DatasetStore


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


def build(store: DatasetStore, snapshot_date: date) -> int:
    """从该日期的 Raw 整体重建 Bronze 分区，返回写入标量行数。"""
    batches = raw.read_batches(store, snapshot_date)
    if not batches:
        return 0

    rows: list[dict[str, Any]] = []
    for batch in batches:
        for record in batch.records:
            row = to_row(record, snapshot_date, batch.file)
            if row is not None:
                rows.append(row)

    if not rows:
        return 0

    frame = pl.DataFrame(rows, schema=BRONZE_SCHEMA)
    frame = frame.unique(subset=["id"], keep="last", maintain_order=True)

    store.write("bronze", frame, snapshot_date)
    return frame.height
