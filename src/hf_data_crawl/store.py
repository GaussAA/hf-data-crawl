"""数据仓入口：持有根目录，并提供按登记表读写的表访问。

流程各有各的模块，这里不再放逻辑：

- `raw`     原始响应落盘与按写入序读回
- `bronze`  由 Raw 整体重建某天的标量表
- `card`    由卡片批次抽出宽表与明细表
- `state`   观测流水、当前态、状态标记

下面的 `write_raw / build_* / update_state / mark_status` 只是一行转发，保留是为了
让现有调用方不必同时改。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from . import bronze, card, raw, state
from .atomic import write_parquet
from .silver import derive_row
from .state import StateUpdate
from .status import Status
from .tables import SILVER_SCHEMA, table

__all__ = ["DatasetStore", "StateUpdate"]


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

    @property
    def silver_dir(self) -> Path:
        return self._root / "silver"

    @property
    def state_dir(self) -> Path:
        return self._root / "state"

    @property
    def enrich_dir(self) -> Path:
        return self._root / "enrich"

    # ------------------------------------------------------------ 表读写
    #
    # 路径与 schema 只从登记表取。这里是整个仓库里唯一把「逻辑名」翻成磁盘路径、
    # 并按 schema 读写的地方：改名或加列只动 `tables.py`，读与写一起跟着变。

    def path(self, name: str, dt: date | None = None) -> Path:
        """某张表的落盘路径。分区表必须给 dt。"""
        return self._root / table(name).relative(dt.isoformat() if dt else None)

    def has(self, name: str, dt: date | None = None) -> bool:
        """表是否已有数据。分区表不给 dt 时，任一分区存在即算有。"""
        spec = table(name)
        if spec.partitioned and dt is None:
            return any(self._root.glob(spec.glob()))
        return self.path(name, dt).exists()

    def read(self, name: str, dt: date | None = None) -> pl.DataFrame:
        """读一张表（或一个分区），缺列补空、多列丢弃；不存在时返回空帧。"""
        spec = table(name)
        target = self.path(name, dt)
        if not target.exists():
            return pl.DataFrame(schema=spec.schema)
        return _align_frame(pl.read_parquet(target), spec.schema)

    def read_all(self, name: str) -> pl.DataFrame:
        """读一张表的全部分区并合并。每个分区先各自对齐，再拼接。

        旧分区缺新列时，若先拼再对齐，`concat` 会在拼接处抛错或列不齐；逐个对齐
        之后再拼，「新增字段后整个库打不开」就不会发生。
        """
        spec = table(name)
        if not spec.partitioned:
            return self.read(name)
        files = sorted(self._root.glob(spec.glob()))
        if not files:
            return pl.DataFrame(schema=spec.schema)
        return pl.concat(
            [_align_frame(pl.read_parquet(path), spec.schema) for path in files],
            how="vertical_relaxed",
        )

    def write(self, name: str, frame: pl.DataFrame, dt: date | None = None) -> Path:
        """原子写一张表（或一个分区），列序按 schema 固定。"""
        target = self.path(name, dt)
        write_parquet(frame.select(list(table(name).schema)), target)
        return target

    def upsert(self, name: str, frame: pl.DataFrame, dedupe: list[str] | None = None) -> int:
        """把本批并入既有表：同 id 以新值为准，其余保留。返回合并后的行数。

        - 替换键固定为 `id`：某个 id 在新批里出现，它在旧表里的行**整体**被换掉，
          因此卡片里删掉的 config 不会残留。
        - `dedupe` 只管新批内部的去重键（默认 `id`），与替换键无关。
        - 空新批不写盘，直接返回现有行数：没有变化就不碰文件。
        """
        if frame.height == 0:
            return self.read(name).height

        fresh = frame.unique(subset=dedupe or ["id"], keep="last", maintain_order=True).select(
            list(table(name).schema)
        )
        previous = self.read(name)
        merged = pl.concat(
            [previous.filter(~pl.col("id").is_in(fresh["id"].unique().to_list())), fresh],
            how="vertical_relaxed",
        )
        self.write(name, merged.sort("id"))
        return merged.height

    # ------------------------------------------------------------ 具名读法

    def read_bronze(self, snapshot_date: date) -> pl.DataFrame:
        return self.read("bronze", snapshot_date)

    def read_silver(self, snapshot_date: date) -> pl.DataFrame:
        return self.read("silver", snapshot_date)

    def read_observations(self) -> pl.DataFrame:
        return self.read_all("observations")

    def read_enrich(self) -> pl.DataFrame:
        return self.read("carddata")

    def read_card_configs(self) -> pl.DataFrame:
        return self.read("card_configs")

    def read_sizes(self) -> pl.DataFrame:
        return self.read("size")

    def read_current(self) -> pl.DataFrame:
        # 旧表的列不全会让下游 select 直接抛错，`read` 已按当前 schema 对齐；
        # status 缺失时视为全部在架，沿用历史语义。
        frame = self.read("current")
        if frame["status"].null_count():
            frame = frame.with_columns(pl.col("status").fill_null(Status.ACTIVE.value))
        return frame

    def merge_sizes(self, frame: pl.DataFrame) -> int:
        """把新一批规模数据并入总表，同 id 以新值为准。

        不可用的记录也一并落库：不记下来，下一轮还得为它们重烧 30% 的请求预算。
        """
        return self.upsert("size", frame)

    # ------------------------------------------------------------ 流程转发

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
        return raw.write(
            self,
            snapshot_date=snapshot_date,
            page_index=page_index,
            records=records,
            meta=meta,
            label=label,
            written_at=written_at,
        )

    def build_bronze(self, snapshot_date: date) -> int:
        return bronze.build(self, snapshot_date)

    def build_silver(self, snapshot_date: date) -> int:
        """由 Bronze 派生 Silver 分区，返回行数。"""
        frame_in = self.read_bronze(snapshot_date)
        if frame_in.height == 0:
            return 0
        rows = [derive_row(row, snapshot_date) for row in frame_in.iter_rows(named=True)]
        frame = pl.DataFrame(rows, schema=SILVER_SCHEMA)
        self.write("silver", frame, snapshot_date)
        return frame.height

    def build_enrich(self, snapshot_date: date) -> tuple[int, int]:
        return card.build(self, snapshot_date)

    def update_state(self, snapshot_date: date) -> StateUpdate:
        return state.update(self, snapshot_date)

    def mark_status(self, updates: dict[str, str]) -> int:
        return state.mark_status(self, updates)


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
