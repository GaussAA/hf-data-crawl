"""数据仓入口：持有根目录，并提供按登记表读写的通用表访问。

这里只做通用读写：按表名与日期取路径、读、写。领域规则各归其所：
Raw 落盘在 `pipeline.raw`，状态标记在 `pipeline.state`，规模与文件清单的合并在
`acquire.sizes` / `acquire.files`。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .atomic import _temp_for, replace, write_parquet
from .tables import PolarsType, table

__all__ = ["DatasetStore"]


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
    def state_dir(self) -> Path:
        """游标与会话文件所在目录；表文件一律走 `path(name)`，不要在此拼。"""
        return self._root / "state"

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

    def read(
        self, name: str, dt: date | None = None, *, columns: Sequence[str] | None = None
    ) -> pl.DataFrame:
        """读一张表（或一个分区），缺列补空、多列丢弃；不存在时返回空帧。

        给 `columns` 时只读这几列，**不做 schema 对齐**：它存在的意义就是「只要
        少量列时别把几 GB 的正文读进来」。2026-11-18 实测：`fetch-readme` 收尾
        只想判断「旧表里哪些 id 已经是 ok」，却因整表读入把峰值推到十几 GB。
        """
        spec = table(name)
        target = self.path(name, dt)
        if not target.exists():
            if columns is not None:
                return pl.DataFrame(schema={col: spec.schema[col] for col in columns})
            return pl.DataFrame(schema=spec.schema)
        if columns is not None:
            return pl.read_parquet(target, columns=list(columns))
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
        """原子写一张表（或一个分区），列序按 schema 固定、压缩级别按登记表给。"""
        spec = table(name)
        target = self.path(name, dt)
        write_parquet(
            frame.select(list(spec.schema)),
            target,
            compression_level=spec.compression_level,
        )
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

    def merge_by_id(self, name: str, frame: pl.DataFrame) -> int:
        """把一批行并入单文件大表：同 id 整体替换，其余原样保留，新行接在末尾。

        与 `upsert` 只差两点，但这两点正是它存在的理由：

        - **不把旧表读进内存**：逐批 copy 旧文件（跳过被替换的 id），再写新行。
        - **不排序**：`upsert` 的 `merged.sort("id")` 要整表复制一遍。

        `upsert` 是「read 全表 → filter → concat → sort → write」，峰值约为表体积的
        三倍。对 `readme` 这种一百万行、正文 2.8 GB 的表，等于十几 GB——2026-11-18
        本机 16 GB 内存实测直接 OOM，只能靠一次性脚本绕路。这里是同一件事的流式写法。

        代价是行序变成「旧行按原序 + 新行追加」，不再按 id 排序。消费者是 SQL 与
        parquet 扫描，都不依赖物理顺序；要守的只有「同 id 只留一行」。

        只支持单文件表：分区表按 dt 分片，替换范围与分区无关，流式合并没有意义
        （也容易写错），故直接拒绝。
        """
        spec = table(name)
        if spec.partitioned:
            raise ValueError(f"{name} 是分区表，merge_by_id 只用于单文件表")
        target = self.path(name)
        if frame.height == 0:
            return pq.ParquetFile(target).metadata.num_rows if target.exists() else 0

        fresh = frame.unique(subset=["id"], keep="last", maintain_order=True).select(
            list(spec.schema)
        )
        if not target.exists():
            write_parquet(fresh, target, compression_level=spec.compression_level)
            return fresh.height

        old = pq.ParquetFile(target)
        temp = _temp_for(target)
        try:
            schema = old.schema_arrow
            # 列类型以**磁盘上的**为准：polars 的 Utf8 转 arrow 可能得到 large_string，
            # 与新旧两半拼进同一个文件时类型必须逐列相同。
            arrow = fresh.to_arrow().cast(schema)
            replaced = pa.array(fresh["id"].to_list(), type=schema.field("id").type)
            options: dict[str, object] = {"compression": "zstd"}
            if spec.compression_level is not None:
                options["compression_level"] = spec.compression_level
            sink = pq.ParquetWriter(temp, schema, **options)
            kept = 0
            try:
                for batch in old.iter_batches(batch_size=50_000):
                    mask = pc.invert(pc.is_in(batch.column("id"), value_set=replaced))
                    kept += int(pc.sum(mask).as_py() or 0)
                    sink.write_batch(batch.filter(mask))
                for piece in arrow.to_batches(max_chunksize=50_000):
                    sink.write_batch(piece)
            finally:
                # Windows 下句柄不关，`replace` 会报 WinError 5（2026-11-18 实测）。
                sink.close()
        finally:
            old.close()
        replace(temp, target)
        return kept + fresh.height


def _align_frame(frame: pl.DataFrame, schema: dict[str, PolarsType]) -> pl.DataFrame:
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
    frame = frame.drop(extra).select(list(schema))
    # 时间列的单位/时区可能因写入方版本不同而不一致（ms vs us、naive vs UTC）。
    # `concat(how="vertical_relaxed")` 对 datetime **不做**超类型推断，会直接抛
    # `SchemaError: failed to determine supertype of datetime[us, UTC] and datetime[ms]`
    # ——表现为「加了列之后整个库打不开」，而 `_align_frame` 原本只补列不碰类型。
    #
    # 只对时间列按 schema 显式 cast：其余类型不碰，静默把浮点截成整数或把字符串
    # 猜成数字，比读不出来更坏。
    for name, kind in schema.items():
        if isinstance(kind, pl.Datetime) and frame.schema[name] != kind:
            frame = frame.with_columns(pl.col(name).cast(kind))
    return frame
