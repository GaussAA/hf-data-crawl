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
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from .card import CARD_CONFIGS_SCHEMA, ENRICH_SCHEMA, extract_card_row, extract_config_rows
from .silver import SILVER_SCHEMA, derive_row
from .sizes import SIZE_SCHEMA

#: 规模表文件名。与卡片表同放 enrich/，因为它同样是「按需补齐」的产物。
SIZE_FILENAME = "size.parquet"

BRONZE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "author": pl.Utf8,
    "sha": pl.Utf8,
    "created_at": pl.Utf8,
    "last_modified": pl.Utf8,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
    "trending_score": pl.Float64,
    "gated": pl.Utf8,
    "private": pl.Boolean,
    "disabled": pl.Boolean,
    "main_size": pl.Int64,
    "xet_enabled": pl.Boolean,
    "resource_group": pl.Utf8,
    "tags": pl.List(pl.Utf8),
    "description": pl.Utf8,
    "raw": pl.Utf8,
    "page_file": pl.Utf8,
    "snapshot_date": pl.Date,
}

#: 由本地派生、不对应 HF 响应字段的列。契约测试据此反推源字段清单。
BRONZE_DERIVED_COLUMNS: frozenset[str] = frozenset({"raw", "page_file", "snapshot_date"})

#: Bronze 列名 → HF 响应字段名。**契约的唯一真相源**。
#:
#: 存在的理由：HF 的 `expand[]` 是白名单裁剪，列与请求清单是两份会各自漂移的
#: 声明。2026-11-04 实测事故即由此而来——`private` 列已声明、请求里却没带，
#: 于是 111.5 万行整列为空且无任何报错。此表把两者钉在一起，
#: 由 `tests/test_schema_contract.py` 双向校验。
BRONZE_SOURCE_FIELDS: dict[str, str] = {
    "id": "id",
    "author": "author",
    "sha": "sha",
    "created_at": "createdAt",
    "last_modified": "lastModified",
    "downloads": "downloads",
    "downloads_all_time": "downloadsAllTime",
    "likes": "likes",
    "trending_score": "trendingScore",
    "gated": "gated",
    "private": "private",
    "disabled": "disabled",
    "main_size": "mainSize",
    "xet_enabled": "xetEnabled",
    "resource_group": "resourceGroup",
    "tags": "tags",
    "description": "description",
}

#: 列表接口无条件返回、无需写进 `expand[]` 的字段。
UNCONDITIONAL_FIELDS: frozenset[str] = frozenset({"id"})

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
    "created_at": pl.Utf8,
    "last_modified": pl.Utf8,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
    "trending_score": pl.Float64,
    "gated": pl.Utf8,
    "private": pl.Boolean,
    "disabled": pl.Boolean,
    "main_size": pl.Int64,
    "xet_enabled": pl.Boolean,
    "resource_group": pl.Utf8,
    "first_seen_at": pl.Date,
    "last_seen_at": pl.Date,
    "status": pl.Utf8,
}

#: current 中由 Silver 直接带过来、不需要额外计算的列。
_CURRENT_CARRIED: tuple[str, ...] = (
    "id",
    "author",
    "sha",
    "created_at",
    "last_modified",
    "downloads",
    "downloads_all_time",
    "likes",
    "trending_score",
    "gated",
    "private",
    "disabled",
    "main_size",
    "xet_enabled",
    "resource_group",
)

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
        written_at: str | None = None,
    ) -> Path:
        """落一页原始响应，返回文件路径。同内容重复写不新增文件。

        **排序的唯一依据是 `written_at`，它必须落在 payload 里。** 不能交给
        文件系统：mtime 在复制目录、解压归档、换机迁移后都会变，届时同一批
        Raw 会推出不同的 Bronze——而 `rebuild` 声称「Raw 是唯一事实来源」，
        两者不能同时为真。（2026-11-04 实测：字典序让旧批次压住新批次，
        头部两万名的 main_size 只剩 11,552 条。）

        `label` 只用于让人一眼看出是哪一批（part / top / card），**不参与排序**。
        同内容重写会复用已有文件，其 written_at 保持首次写入时刻——内容相同，
        谁胜出都不影响结果。
        """
        payload = {
            "meta": {
                **(meta or {}),
                "snapshot_date": snapshot_date.isoformat(),
                "page_index": page_index,
                "record_count": len(records),
                "written_at": written_at or _utc_now(),
            },
            "records": records,
        }
        body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
        # 摘要**不含**写入时刻：否则同一页每跑一次就多一个文件，幂等性失效。
        # 排除后，「同内容重写复用已有文件」照旧成立，其 written_at 保持首次写入
        # 时刻——内容相同，谁胜出都不影响结果。历史文件本就没有该字段，故同一规则
        # 算出的摘要与从前一致，老文件仍能被复用。
        digest_source = json.dumps(
            {
                **payload,
                "meta": {
                    key: value
                    for key, value in payload["meta"].items()
                    if key != "written_at"
                },
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        digest = hashlib.sha256(digest_source).hexdigest()[:8]

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
        batches = self._read_raw_batches(partition)
        if not batches:
            return 0

        rows: list[dict[str, Any]] = []
        for _key, records, page_file in batches:
            for record in records:
                row = self._to_row(record, snapshot_date, page_file)
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

    @staticmethod
    def _read_raw_batches(
        partition: Path, pattern: str = "*.json.gz"
    ) -> list[tuple[tuple[Any, ...], list[dict[str, Any]], str]]:
        """读出分块并按写入时刻升序排列，同 id 以最后写入者为准。

        排序发生在**解压之后**：序存在 payload 的 `written_at` 里，文件名与文件
        属性都不作数。build 本来就要解压全部文件，因此这不是额外成本。

        旧分块（此字段引入之前）回退到 mtime——它们已经稳定、不会再变，
        影响锁在历史里；键首位的 0/1 保证新分块永远排在其后，迁移期不会出现
        「新写的数据输给历史数据」。
        """
        if not partition.exists():
            return []

        batches: list[tuple[tuple[Any, ...], list[dict[str, Any]], str]] = []
        for path in partition.glob(pattern):
            payload = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
            meta = payload.get("meta") or {}
            stamp = meta.get("written_at")
            if isinstance(stamp, str) and stamp:
                key: tuple[Any, ...] = (1, stamp, path.name)
            else:
                # 定宽浮点串，保证字典序等于数值序。
                key = (0, "", f"{path.stat().st_mtime:020.6f}", path.name)
            batches.append((key, payload.get("records", []), path.name))

        batches.sort(key=lambda item: item[0])
        return batches

    def read_bronze(self, snapshot_date: date) -> pl.DataFrame:
        target = self.bronze_dir / f"dt={snapshot_date.isoformat()}" / "bronze.parquet"
        if not target.exists():
            return pl.DataFrame(schema=BRONZE_SCHEMA)
        return _align_frame(pl.read_parquet(target), BRONZE_SCHEMA)

    @property
    def silver_dir(self) -> Path:
        return self._root / "silver"

    @property
    def state_dir(self) -> Path:
        return self._root / "state"

    @property
    def enrich_dir(self) -> Path:
        return self._root / "enrich"

    def build_enrich(self, snapshot_date: date) -> tuple[int, int]:
        """把该日期的 card 批次抽成两张表，按 id 覆盖旧值。

        返回 (宽表行数, 明细行数)。一次扫描产出两张表：卡片体量小但字段多，
        重扫两次的代价（重复解压 25 个分块）没有必要。
        """
        partition = self.raw_dir / f"dt={snapshot_date.isoformat()}"
        batches = self._read_raw_batches(partition, pattern="card-*.json.gz")
        if not batches:
            return self.read_enrich().height, self.read_card_configs().height

        card_rows: list[dict[str, Any]] = []
        config_rows: list[dict[str, Any]] = []
        for _key, records, _name in batches:
            for record in records:
                row = extract_card_row(record, snapshot_date)
                if row is not None:
                    card_rows.append(row)
                config_rows.extend(extract_config_rows(record, snapshot_date))

        table_rows = self._merge_cards(card_rows, ENRICH_SCHEMA, "carddata.parquet")
        config_count = self._merge_cards(
            config_rows,
            CARD_CONFIGS_SCHEMA,
            "card_configs.parquet",
            dedupe=["id", "config_name", "split"],
        )
        return table_rows, config_count

    def _merge_cards(
        self,
        rows: list[dict[str, Any]],
        schema: dict[str, pl.DataType],
        filename: str,
        *,
        dedupe: list[str] | None = None,
    ) -> int:
        """把本批卡片并入既有表：同 id 以新值为准，其余保留。"""
        target = self.enrich_dir / filename
        if not rows:
            return self.read_card_table(filename, schema).height

        fresh = pl.DataFrame(rows, schema=schema).unique(
            subset=dedupe or ["id"], keep="last", maintain_order=True
        )
        if target.exists():
            # 旧表是上一次规则下的产物，可能缺新列——先对齐再拼，否则直接抛错。
            previous = _align_frame(pl.read_parquet(target), schema)
            merged = pl.concat(
                [
                    previous.filter(~pl.col("id").is_in(fresh["id"].unique().to_list())),
                    fresh,
                ],
                how="vertical_relaxed",
            )
        else:
            merged = fresh

        _write_parquet(merged.select(list(schema)).sort("id"), target)
        return merged.height

    def read_enrich(self) -> pl.DataFrame:
        return self.read_card_table("carddata.parquet", ENRICH_SCHEMA)

    def read_card_configs(self) -> pl.DataFrame:
        return self.read_card_table("card_configs.parquet", CARD_CONFIGS_SCHEMA)

    def read_card_table(self, filename: str, schema: dict[str, pl.DataType]) -> pl.DataFrame:
        target = self.enrich_dir / filename
        if not target.exists():
            return pl.DataFrame(schema=schema)
        return _align_frame(pl.read_parquet(target), schema)

    def read_sizes(self) -> pl.DataFrame:
        return self.read_card_table(SIZE_FILENAME, SIZE_SCHEMA)

    def merge_sizes(self, frame: pl.DataFrame) -> int:
        """把新一批规模数据并入总表，同 id 以新值为准。

        不可用的记录也一并落库：不记下来，下一轮还得为它们重烧 30% 的请求预算。
        """
        target = self.enrich_dir / SIZE_FILENAME
        fresh = frame.unique(subset=["id"], keep="last")
        if target.exists() and fresh.height:
            previous = _align_frame(pl.read_parquet(target), SIZE_SCHEMA)
            merged = pl.concat(
                [
                    previous.filter(~pl.col("id").is_in(fresh["id"].to_list())),
                    fresh,
                ],
                how="vertical_relaxed",
            )
        elif target.exists():
            merged = _align_frame(pl.read_parquet(target), SIZE_SCHEMA)
        else:
            merged = fresh

        _write_parquet(merged.select(list(SIZE_SCHEMA)).sort("id"), target)
        return merged.height

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
        return _align_frame(pl.read_parquet(target), SILVER_SCHEMA)

    def read_observations(self) -> pl.DataFrame:
        files = sorted((self.state_dir / "observations").glob("dt=*/obs.parquet"))
        if not files:
            return pl.DataFrame(schema=OBSERVATIONS_SCHEMA)
        return pl.concat([pl.read_parquet(path) for path in files], how="vertical_relaxed")

    def read_current(self) -> pl.DataFrame:
        target = self.state_dir / "current.parquet"
        if not target.exists():
            return pl.DataFrame(schema=CURRENT_SCHEMA)
        # 旧表的列不全会让下游 select 直接抛错，故一律按当前 schema 对齐；
        # status 缺失时视为全部在架，沿用历史语义。
        frame = _align_frame(pl.read_parquet(target), CURRENT_SCHEMA)
        if frame["status"].null_count():
            frame = frame.with_columns(pl.col("status").fill_null("active"))
        return frame

    def mark_disappeared(self, dataset_ids: list[str]) -> int:
        """把指定数据集标为下架。只改状态，不动观测史与 last_seen_at。"""
        return self.mark_status({dataset_id: "disappeared" for dataset_id in dataset_ids})

    def mark_status(self, updates: dict[str, str]) -> int:
        """按 id 批量改状态，返回实际发生变化的行数。

        状态不再只有「在架/下架」两极：`restricted` 表示转为私有、`renamed` 表示
        改名（原 id 失效但库还在）。把它们一律记成 disappeared，会让下架数虚高，
        也让改名后的库在下轮变成「新增」而丢掉原有观测史。
        """
        if not updates:
            return 0
        current = self.read_current()
        if current.height == 0:
            return 0

        patch = pl.DataFrame(
            {"id": list(updates), "next_status": list(updates.values())},
            schema={"id": pl.Utf8, "next_status": pl.Utf8},
        )
        updated = (
            current.join(patch, on="id", how="left")
            .with_columns(pl.coalesce(["next_status", "status"]).alias("status"))
            .drop("next_status")
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
        carried_columns = [*_CURRENT_CARRIED, "last_seen_at", "status"]
        previous = self.read_current()
        today_ids = silver["id"].to_list()
        carried = previous.filter(~pl.col("id").is_in(today_ids)).select(carried_columns)

        today = silver.select(list(_CURRENT_CARRIED)).with_columns(
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
            "author": _as_text(record.get("author")),
            "sha": _as_text(record.get("sha")),
            "created_at": _as_text(record.get("createdAt")),
            "last_modified": _as_text(record.get("lastModified")),
            "downloads": _as_int(record.get("downloads")),
            "downloads_all_time": _as_int(record.get("downloadsAllTime")),
            "likes": _as_int(record.get("likes")),
            "trending_score": _as_float(record.get("trendingScore")),
            "gated": _as_text(record.get("gated")),
            "private": _as_bool(record.get("private")),
            "disabled": _as_bool(record.get("disabled")),
            "main_size": _as_int(record.get("mainSize")),
            "xet_enabled": _as_bool(record.get("xetEnabled")),
            "resource_group": _as_structured_text(record.get("resourceGroup")),
            "tags": [str(tag) for tag in tags] if isinstance(tags, list) else [],
            "description": _as_text(record.get("description")),
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


def _as_float(value: Any) -> float | None:
    """趋势分取浮点：接口当前给整数，但不把「今天恰好是整数」当成契约。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _as_bool(value: Any) -> bool | None:
    """只认同真正的布尔。

    这里刻意不接受 "true"/"false" 字符串：`gated` 的 "manual"/"auto" 与
    `private` 的 true/false 是两种语义，混用会让「是否受限」无法区分。
    """
    return value if isinstance(value, bool) else None


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _as_structured_text(value: Any) -> str | None:
    """把可能是标量、对象或数组的字段压成可读文本。

    `resourceGroup` 实测多为 null，少数场合是对象；直接 str() 会得到 Python
    字典字面量（单引号，非 JSON），后续无法再解析，故对复合类型走 JSON。
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value or None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return _as_text(value)


def _utc_now() -> str:
    """带微秒的 UTC 时刻，作为 Raw 的写入序。

    微秒足够：每页之间至少隔着一次网络往返，不可能同微秒落两次盘。
    用 UTC 而非本地时间，避免跨时区迁移后序颠倒。
    """
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _align_frame(frame: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    """把读出的表对齐到目标 schema：缺列补空、多列丢弃。

    分区是按 dt 落盘的，而 schema 会随字段扩充而变。若不做对齐，旧分区读出来
    少一列就会在后续 `select` 处抛 KeyError，表现为「新增字段后整个库打不开」。
    补空而非重算，是因为旧分区里那些数据本就不存在（当初没请求该字段）。
    """
    missing = [name for name in schema if name not in frame.columns]
    if missing:
        frame = frame.with_columns(
            [pl.lit(None, dtype=schema[name]).alias(name) for name in missing]
        )
    extra = [name for name in frame.columns if name not in schema]
    return frame.drop(extra).select(list(schema))


def _write_parquet(frame: pl.DataFrame, target: Path) -> None:
    """先写临时文件再替换，避免半截 parquet 被后续读取。"""
    target.parent.mkdir(parents=True, exist_ok=True)
    temp = target.with_suffix(".parquet.tmp")
    frame.write_parquet(temp)
    os.replace(temp, target)
