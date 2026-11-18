"""表读写层的行为测试：`path / has / read / read_all / write / upsert`。

关注的是**漂移**：路径与 schema 只从登记表取，所以改登记表，读与写必须一起变；
旧分区缺列时，读取要补空而不是整库打不开。
"""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.platform import tables
from hf_data_crawl.platform.atomic import write_parquet
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import (
    CARD_CONFIGS_SCHEMA,
    OBSERVATIONS_SCHEMA,
    SIZE_SCHEMA,
    TIMESTAMP,
)

DAY1 = date(2026, 11, 1)
DAY2 = date(2026, 11, 2)


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def size_row(dataset_id: str, num_rows: int | None) -> dict[str, object]:
    row: dict[str, object] = {name: None for name in SIZE_SCHEMA}
    row.update({"id": dataset_id, "snapshot_date": DAY1, "status": "ok", "num_rows": num_rows})
    return row


def sizes(*rows: dict[str, object]) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=SIZE_SCHEMA)


def config_row(dataset_id: str, config: str, split: str, examples: int) -> dict[str, object]:
    row: dict[str, object] = {name: None for name in CARD_CONFIGS_SCHEMA}
    row.update(
        {
            "id": dataset_id,
            "snapshot_date": DAY1,
            "config_name": config,
            "split": split,
            "num_examples": examples,
        }
    )
    return row


# ---------------------------------------------------------------- 路径与存在性


def test_分区表必须给日期(store: DatasetStore) -> None:
    with pytest.raises(ValueError):
        store.path("bronze")


def test_has按分区或任一分区判断(store: DatasetStore) -> None:
    assert not store.has("observations")
    write_parquet(
        pl.DataFrame({"id": ["a"]}, schema={"id": pl.Utf8}),
        store.path("observations", DAY1),
    )

    assert store.has("observations")
    assert store.has("observations", DAY1)
    assert not store.has("observations", DAY2)


def test_不存在的表读出空帧且列齐全(store: DatasetStore) -> None:
    frame = store.read("current")

    assert frame.height == 0
    assert list(frame.columns) == list(tables.table("current").schema)


# ---------------------------------------------------------------- 统一对齐


def test_旧分区缺列时观测表仍可读且补空(store: DatasetStore) -> None:
    """新增字段后，旧分区没有该列。读取不能因此整体失败。"""
    full = pl.DataFrame(
        {
            "id": ["a"],
            "url": ["https://huggingface.co/datasets/a"],
            "snapshot_date": [DAY2],
            "downloads": [5],
            "downloads_all_time": [9],
            "likes": [1],
        },
        schema=OBSERVATIONS_SCHEMA,
    )
    legacy = pl.DataFrame(
        {"id": ["b"], "snapshot_date": [DAY1], "downloads": [3]},
        schema={"id": pl.Utf8, "snapshot_date": pl.Date, "downloads": pl.Int64},
    )
    write_parquet(legacy, store.path("observations", DAY1))
    write_parquet(full, store.path("observations", DAY2))

    merged = store.read_all("observations")

    assert list(merged.columns) == list(OBSERVATIONS_SCHEMA)
    assert merged.height == 2
    old = merged.filter(pl.col("id") == "b").row(0, named=True)
    assert old["downloads"] == 3
    assert old["likes"] is None
    assert old["downloads_all_time"] is None


def test_read_all对非分区表等同于read(store: DatasetStore) -> None:
    store.upsert("size", sizes(size_row("acme/a", 1)))

    assert store.read_all("size").equals(store.read("size"))


# ---------------------------------------------------------------- 登记表驱动


def test_改登记表路径后读写一起跟着变(store: DatasetStore, monkeypatch: pytest.MonkeyPatch) -> None:
    """读与写各自拼路径时，改名会让一边写到新处、另一边仍读旧处，读到空表且不报错。"""
    moved = replace(tables.table("size"), path="elsewhere/size.parquet")
    monkeypatch.setitem(tables.TABLES, "size", moved)

    store.upsert("size", sizes(size_row("acme/a", 7)))

    assert (store.root / "elsewhere" / "size.parquet").exists()
    assert store.read("size").row(0, named=True)["num_rows"] == 7


# ---------------------------------------------------------------- upsert


def test_upsert同id以新值为准其余保留(store: DatasetStore) -> None:
    store.upsert("size", sizes(size_row("acme/a", 1), size_row("acme/b", 2)))

    total = store.upsert("size", sizes(size_row("acme/b", 20), size_row("acme/c", 3)))

    assert total == 3
    by_id = {row["id"]: row["num_rows"] for row in store.read("size").iter_rows(named=True)}
    assert by_id == {"acme/a": 1, "acme/b": 20, "acme/c": 3}


def test_upsert新批内同id取最后一条(store: DatasetStore) -> None:
    store.upsert("size", sizes(size_row("acme/a", 1), size_row("acme/a", 2)))

    assert store.read("size").row(0, named=True)["num_rows"] == 2


def test_upsert结果按id排序(store: DatasetStore) -> None:
    store.upsert("size", sizes(size_row("acme/c", 3), size_row("acme/a", 1)))
    store.upsert("size", sizes(size_row("acme/b", 2)))

    assert store.read("size")["id"].to_list() == ["acme/a", "acme/b", "acme/c"]


def test_upsert空批不写盘并返回现有行数(store: DatasetStore) -> None:
    empty = pl.DataFrame(schema=SIZE_SCHEMA)

    assert store.upsert("size", empty) == 0
    assert not store.has("size")

    store.upsert("size", sizes(size_row("acme/a", 1)))
    before = store.path("size").stat().st_mtime_ns

    assert store.upsert("size", empty) == 1
    assert store.path("size").stat().st_mtime_ns == before


def test_upsert读到旧表缺列时先对齐再合并(store: DatasetStore) -> None:
    legacy = pl.DataFrame(
        {"id": ["acme/a"], "num_rows": [1]},
        schema={"id": pl.Utf8, "num_rows": pl.Int64},
    )
    write_parquet(legacy, store.path("size"))

    total = store.upsert("size", sizes(size_row("acme/b", 2)))

    assert total == 2
    assert list(store.read("size").columns) == list(SIZE_SCHEMA)


def test_明细表复合去重且卡片删掉的config不残留(store: DatasetStore) -> None:
    """新批内按 (id, config, split) 去重；替换旧行时按 id 整体换，旧 config 不留。"""
    keys = ["id", "config_name", "split"]
    first = pl.DataFrame(
        [
            config_row("acme/a", "default", "train", 10),
            config_row("acme/a", "extra", "train", 5),
            config_row("acme/b", "default", "train", 7),
        ],
        schema=CARD_CONFIGS_SCHEMA,
    )
    store.upsert("card_configs", first, dedupe=keys)

    second = pl.DataFrame(
        [
            config_row("acme/a", "default", "train", 11),
            config_row("acme/a", "default", "train", 12),
        ],
        schema=CARD_CONFIGS_SCHEMA,
    )
    total = store.upsert("card_configs", second, dedupe=keys)

    rows = {
        (row["id"], row["config_name"], row["split"]): row["num_examples"]
        for row in store.read("card_configs").iter_rows(named=True)
    }
    assert total == 2
    assert rows == {("acme/a", "default", "train"): 12, ("acme/b", "default", "train"): 7}


def test_时间单位不一致的分区也能读(tmp_path: Path) -> None:
    """ms 与 us 混在一起时 `vertical_relaxed` 会抛 SchemaError，整表打不开。

    `_align_frame` 原本只补缺列、不碰类型：一个毫秒精度或 tz-naive 的分区就能让
    `read_all` 抛错，表现为「加了列之后整个库打不开」。只对时间列按 schema cast，
    其余类型不碰——静默把浮点截成整数比读不出来更坏。
    """
    from hf_data_crawl.platform.store import _align_frame

    mixed = pl.DataFrame(
        {
            "id": ["acme/a"],
            "created_at": pl.Series([datetime(2026, 11, 1)], dtype=pl.Datetime("ms")),
        }
    )
    schema: dict[str, Any] = {"id": pl.Utf8, "created_at": TIMESTAMP}

    aligned = _align_frame(mixed, schema)

    assert aligned.schema["created_at"] == TIMESTAMP
    assert aligned["created_at"].item() == datetime(2026, 11, 1, tzinfo=UTC)


def test_压缩级别按登记表传给写盘(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """readme 是唯一调高级别的表；这条防「改了 TableSpec 却没接上 store.write」。

    parquet metadata 只记 codec、不记级别，所以没法从落盘文件反查——只能拦写盘
    那一刻的参数。哪天有人把 `store.write` 改回只传 frame，这里会立刻变红。
    """
    from hf_data_crawl.platform import store as store_module
    from hf_data_crawl.platform.tables import CURRENT_SCHEMA, README_SCHEMA

    captured: dict[str, int | None] = {}

    def spy(frame: pl.DataFrame, path: Path, **options: Any) -> None:
        captured["level"] = options.get("compression_level")
        path.parent.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path)

    monkeypatch.setattr(store_module, "write_parquet", spy)
    target = DatasetStore(tmp_path / "data")

    target.write("readme", pl.DataFrame(schema=README_SCHEMA))
    assert captured["level"] == 12

    target.write("current", pl.DataFrame(schema=CURRENT_SCHEMA))
    assert captured["level"] is None
