"""分层存储：Raw 原样留存，Bronze 由 Raw 幂等重建。

设计要点：
- Raw 文件名带内容摘要，同页同内容重写为同一文件，同页不同内容则另存——
  覆盖式命名会静默丢掉前一批数据，这是不可接受的。
- Bronze 以「分区整体重建」而非追加实现幂等：给定同一天的 Raw，输出永远一致。
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

BRONZE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "author": pl.Utf8,
    "sha": pl.Utf8,
    "created_at": pl.Utf8,
    "last_modified": pl.Utf8,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
    "gated": pl.Utf8,
    "private": pl.Boolean,
    "tags": pl.List(pl.Utf8),
    "description": pl.Utf8,
    "raw": pl.Utf8,
    "page_file": pl.Utf8,
    "snapshot_date": pl.Date,
}


class DatasetStore:
    """按 dt 分区的本地数据仓。"""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    @property
    def raw_dir(self) -> Path:
        return self._root / "raw"

    @property
    def bronze_dir(self) -> Path:
        return self._root / "bronze"

    def write_raw(
        self,
        *,
        snapshot_date: date,
        page_index: int,
        records: list[dict[str, Any]],
        meta: dict[str, Any] | None = None,
    ) -> Path:
        """落一页原始响应，返回文件路径。同内容重复写不新增文件。"""
        payload = {
            "meta": {
                **(meta or {}),
                "snapshot_date": snapshot_date.isoformat(),
                "page_index": page_index,
                "record_count": len(records),
            },
            "records": records,
        }
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        digest = hashlib.sha256(body).hexdigest()[:8]

        target_dir = self.raw_dir / f"dt={snapshot_date.isoformat()}"
        target_dir.mkdir(parents=True, exist_ok=True)
        path = target_dir / f"part-{page_index:06d}-{digest}.json.gz"
        if path.exists():
            return path

        temp = path.with_suffix(".gz.tmp")
        with gzip.open(temp, "wb") as handle:
            handle.write(body)
        os.replace(temp, path)
        return path

    def build_bronze(self, snapshot_date: date) -> int:
        """从该日期的 Raw 整体重建 Bronze 分区，返回写入标量行数。"""
        partition = self.raw_dir / f"dt={snapshot_date.isoformat()}"
        files = sorted(partition.glob("*.json.gz")) if partition.exists() else []
        if not files:
            return 0

        rows: list[dict[str, Any]] = []
        for path in files:
            payload = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
            for record in payload.get("records", []):
                row = self._to_row(record, snapshot_date, path.name)
                if row is not None:
                    rows.append(row)

        if not rows:
            return 0

        frame = pl.DataFrame(rows, schema=BRONZE_SCHEMA)
        frame = frame.unique(subset=["id"], keep="last", maintain_order=True)

        target_dir = self.bronze_dir / f"dt={snapshot_date.isoformat()}"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / "bronze.parquet"
        temp = target.with_suffix(".parquet.tmp")
        frame.write_parquet(temp)
        os.replace(temp, target)
        return frame.height

    def read_bronze(self, snapshot_date: date) -> pl.DataFrame:
        target = self.bronze_dir / f"dt={snapshot_date.isoformat()}" / "bronze.parquet"
        if not target.exists():
            return pl.DataFrame(schema=BRONZE_SCHEMA)
        return pl.read_parquet(target)

    @staticmethod
    def _to_row(record: Any, snapshot_date: date, page_file: str) -> dict[str, Any] | None:
        if not isinstance(record, dict):
            return None
        dataset_id = record.get("id")
        if not isinstance(dataset_id, str) or not dataset_id:
            return None
        tags = record.get("tags")
        return {
            "id": dataset_id,
            "author": record.get("author"),
            "sha": record.get("sha"),
            "created_at": record.get("createdAt"),
            "last_modified": record.get("lastModified"),
            "downloads": _as_int(record.get("downloads")),
            "downloads_all_time": _as_int(record.get("downloadsAllTime")),
            "likes": _as_int(record.get("likes")),
            "gated": _as_text(record.get("gated")),
            "private": record.get("private") if isinstance(record.get("private"), bool) else None,
            "tags": [str(tag) for tag in tags] if isinstance(tags, list) else [],
            "description": record.get("description"),
            "raw": json.dumps(record, ensure_ascii=False, sort_keys=True),
            "page_file": page_file,
            "snapshot_date": snapshot_date,
        }


def _as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
