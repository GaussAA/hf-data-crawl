"""表读写层的行为测试：`path / has / read / read_all / write / upsert`。

关注的是**漂移**：路径与 schema 只从登记表取，所以改登记表，读与写必须一起变；
旧分区缺列时，读取要补空而不是整库打不开。
"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from hf_data_crawl import tables
from hf_data_crawl.atomic import write_parquet
from hf_data_crawl.store import DatasetStore
from hf_data_crawl.tables import CARD_CONFIGS_SCHEMA, OBSERVATIONS_SCHEMA, SIZE_SCHEMA

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
