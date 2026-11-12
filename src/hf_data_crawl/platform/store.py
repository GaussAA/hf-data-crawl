"""数据仓入口：持有根目录，并提供按登记表读写的通用表访问。

这里只做通用读写：按表名与日期取路径、读、写。领域规则各归其所：
Raw 落盘在 `pipeline.raw`，状态标记在 `pipeline.state`，规模与文件清单的合并在
`acquire.sizes` / `acquire.files`。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from .atomic import write_parquet
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
    return frame.drop(extra).select(list(schema))
