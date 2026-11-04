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
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from .card import ENRICH_SCHEMA, extract_card_row
from .silver import SILVER_SCHEMA, derive_row

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

OBSERVATIONS_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "snapshot_date": pl.Date,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
}

CURRENT_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "author": pl.Utf8,
    "sha": pl.Utf8,
    "last_modified": pl.Utf8,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
    "gated": pl.Utf8,
    "private": pl.Boolean,
    "first_seen_at": pl.Date,
    "last_seen_at": pl.Date,
    "status": pl.Utf8,
}

METRIC_COLUMNS: tuple[str, ...] = ("downloads", "downloads_all_time", "likes")


@dataclass(frozen=True)
class StateUpdate:
    """一次状态更新的结果，供运行摘要使用。"""

    appended: int
    current_rows: int


class DatasetStore:
    """按 dt 分区的本地数据仓。"""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)

    @property
    def root(self) -> Path:
        return self._root

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
        label: str = "part",
    ) -> Path:
        """落一页原始响应，返回文件路径。同内容重复写不新增文件。

        `label` 参与文件名，用于让不同扫描批次的顺序可辨（"top" 排在 "part" 之后，
        因此同一 id 在 Bronze 去重时以刷新批次的新值胜出）。
        """
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
        path = target_dir / f"{label}-{page_index:06d}-{digest}.json.gz"
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

    @property
    def silver_dir(self) -> Path:
        return self._root / "silver"

    @property
    def state_dir(self) -> Path:
        return self._root / "state"

    @property
    def enrich_dir(self) -> Path:
        return self._root / "enrich"

    def build_enrich(self, snapshot_date: date) -> int:
        """把该日期的 card 批次抽成卡片表，按 id 覆盖旧值。"""
        partition = self.raw_dir / f"dt={snapshot_date.isoformat()}"
        found = list(partition.glob("card-*.json.gz")) if partition.exists() else []
        # 按修改时间排序：同一日内多次补齐时，后跑的那批必须胜出，
        # 文件名里的页码与摘要都不带时序，靠字典序会随机取到旧卡片。
        files = sorted(found, key=lambda path: path.stat().st_mtime)
        if not files:
            return self.read_enrich().height

        rows: list[dict[str, Any]] = []
        for path in files:
            payload = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
            for record in payload.get("records", []):
                row = extract_card_row(record, snapshot_date)
                if row is not None:
                    rows.append(row)

        if not rows:
            return self.read_enrich().height

        fresh = pl.DataFrame(rows, schema=ENRICH_SCHEMA).unique(
            subset=["id"], keep="last", maintain_order=True
        )
        target = self.enrich_dir / "carddata.parquet"
        if target.exists():
            previous = pl.read_parquet(target)
            merged = pl.concat(
                [previous.filter(~pl.col("id").is_in(fresh["id"].to_list())), fresh],
                how="vertical_relaxed",
            )
        else:
            merged = fresh

        merged = merged.select(list(ENRICH_SCHEMA)).sort("id")
        _write_parquet(merged, target)
        return merged.height

    def read_enrich(self) -> pl.DataFrame:
        target = self.enrich_dir / "carddata.parquet"
        if not target.exists():
            return pl.DataFrame(schema=ENRICH_SCHEMA)
        return pl.read_parquet(target)

    def build_silver(self, snapshot_date: date) -> int:
        """由 Bronze 派生 Silver 分区，返回行数。"""
        bronze = self.read_bronze(snapshot_date)
        if bronze.height == 0:
            return 0
        rows = [derive_row(row, snapshot_date) for row in bronze.iter_rows(named=True)]
        frame = pl.DataFrame(rows, schema=SILVER_SCHEMA)
        target = self.silver_dir / f"dt={snapshot_date.isoformat()}" / "silver.parquet"
        _write_parquet(frame, target)
        return frame.height

    def read_silver(self, snapshot_date: date) -> pl.DataFrame:
        target = self.silver_dir / f"dt={snapshot_date.isoformat()}" / "silver.parquet"
        if not target.exists():
            return pl.DataFrame(schema=SILVER_SCHEMA)
        return pl.read_parquet(target)

    def read_observations(self) -> pl.DataFrame:
        files = sorted((self.state_dir / "observations").glob("dt=*/obs.parquet"))
        if not files:
            return pl.DataFrame(schema=OBSERVATIONS_SCHEMA)
        return pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")

    def read_current(self) -> pl.DataFrame:
        target = self.state_dir / "current.parquet"
        if not target.exists():
            return pl.DataFrame(schema=CURRENT_SCHEMA)
        frame = pl.read_parquet(target)
        if "status" not in frame.columns:
            # 兼容旧数据：状态列缺失时视为全部在架。
            frame = frame.with_columns(pl.lit("active").alias("status"))
        return frame

    def mark_disappeared(self, dataset_ids: list[str]) -> int:
        """把指定数据集标为下架。只改状态，不动观测史与 last_seen_at。"""
        if not dataset_ids:
            return 0
        current = self.read_current()
        if current.height == 0:
            return 0
        updated = current.with_columns(
            pl.when(pl.col("id").is_in(dataset_ids))
            .then(pl.lit("disappeared"))
            .otherwise(pl.col("status"))
            .alias("status")
        )
        changed = int((updated["status"] != current["status"]).sum())
        if changed:
            _write_parquet(updated.select(list(CURRENT_SCHEMA)), self.state_dir / "current.parquet")
        return changed

    def update_state(self, snapshot_date: date) -> StateUpdate:
        """按当日 Silver 更新观测流水与当前态。

        观测只在该日指标与「该日之前的最后一条观测」不同时追加；
        同日重跑先重建该日分区，故结果与一次跑完一致。
        """
        silver = self.read_silver(snapshot_date)
        if silver.height == 0:
            return StateUpdate(appended=0, current_rows=self.read_current().height)

        existing = self.read_observations()
        before = existing.filter(pl.col("snapshot_date") == snapshot_date).height

        previous = (
            existing.filter(pl.col("snapshot_date") < snapshot_date)
            .sort("snapshot_date")
            .group_by("id", maintain_order=True)
            .agg(
                [
                    pl.col(column).last().alias(f"{column}_prev")
                    for column in METRIC_COLUMNS
                ]
            )
        )
        joined = silver.select(["id", *METRIC_COLUMNS]).join(previous, on="id", how="left")

        changed = pl.col(f"{METRIC_COLUMNS[0]}_prev").is_null()
        for column in METRIC_COLUMNS:
            changed = changed | (
                pl.col(column).fill_null(-1) != pl.col(f"{column}_prev").fill_null(-1)
            )

        new_observations = joined.filter(changed).select(
            [
                pl.col("id"),
                pl.lit(snapshot_date, dtype=pl.Date).alias("snapshot_date"),
                *[pl.col(column) for column in METRIC_COLUMNS],
            ]
        )

        target = (
            self.state_dir / "observations" / f"dt={snapshot_date.isoformat()}" / "obs.parquet"
        )
        if new_observations.height == 0:
            if target.exists():
                target.unlink()
        else:
            _write_parquet(new_observations.select(list(OBSERVATIONS_SCHEMA)), target)

        current = self._rebuild_current(snapshot_date, silver)
        _write_parquet(current, self.state_dir / "current.parquet")

        return StateUpdate(
            appended=max(0, new_observations.height - before),
            current_rows=current.height,
        )

    def _rebuild_current(self, snapshot_date: date, silver: pl.DataFrame) -> pl.DataFrame:
        """勾稽当前态：本轮出现的取 Silver，未出现的沿用原行，不擅自改状态。"""
        carried_columns = [
            "id",
            "author",
            "sha",
            "last_modified",
            *METRIC_COLUMNS,
            "gated",
            "private",
            "last_seen_at",
            "status",
        ]
        previous = self.read_current()
        today_ids = silver["id"].to_list()
        carried = previous.filter(~pl.col("id").is_in(today_ids)).select(carried_columns)

        today = silver.select(
            [
                "id",
                "author",
                "sha",
                "last_modified",
                *METRIC_COLUMNS,
                "gated",
                "private",
            ]
        ).with_columns(
            pl.lit(snapshot_date, dtype=pl.Date).alias("last_seen_at"),
            pl.lit("active").alias("status"),
        )

        first_seen = self.read_observations().group_by("id").agg(
            pl.col("snapshot_date").min().alias("first_seen_at")
        )

        combined = pl.concat(
            [carried.select(carried_columns), today.select(carried_columns)],
            how="vertical_relaxed",
        )
        return (
            combined.join(first_seen, on="id", how="left")
            .with_columns(
                pl.col("first_seen_at").fill_null(pl.lit(snapshot_date, dtype=pl.Date))
            )
            .select(list(CURRENT_SCHEMA))
            .sort("id")
        )

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


def _write_parquet(frame: pl.DataFrame, target: Path) -> None:
    """先写临时文件再替换，避免半截 parquet 被后续读取。"""
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(".parquet.tmp")
    frame.write_parquet(temp)
    os.replace(temp, target)
