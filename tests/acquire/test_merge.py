"""收尾合并的行为测试：分片并入、旧值保护、清分片。

四张富化表的收尾原先各写一遍，形状相同、差别只在四处。这里测的是**形状**本身：
读残留分片 → 保护旧值 → 并入正式表 → 清分片。各表的具体声明（谁保护哪个状态）
由 `test_sizes` / `test_readme` 从各自的 `MergeSpec` 取，不在这里另抄一份。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from hf_data_crawl.acquire.merge import keep_ok_rows, merge_staged
from hf_data_crawl.acquire.sizes import SIZE_MERGE
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import SIZE_SCHEMA

DAY = date(2026, 11, 21)


def _size_row(dataset_id: str, status: str, rows: int | None) -> dict[str, object]:
    return {
        "id": dataset_id,
        "url": f"https://huggingface.co/datasets/{dataset_id}",
        "snapshot_date": DAY,
        "status": status,
        "num_rows": rows,
    }


def _write_shard(store: DatasetStore, subdir: str, frame: pl.DataFrame) -> Path:
    root = store.root / subdir
    root.mkdir(parents=True, exist_ok=True)
    path = root / "2026-11-21-deadbeef-0000.parquet"
    frame.write_parquet(path)
    return path


def test_分片并入正式表后即清空(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    shard = _write_shard(
        store,
        SIZE_MERGE.staging_dir,
        pl.DataFrame([_size_row("acme/a", "ok", 10)], schema=SIZE_SCHEMA),
    )

    merged = merge_staged(store, SIZE_MERGE, 5.0)

    assert merged == 1
    assert store.read("size").height == 1
    assert not shard.exists(), "并入之后分片必须删掉，否则下一轮会重复并一次"


def test_没有分片时不碰正式表(tmp_path: Path) -> None:
    """空跑不该写盘：没有变化就保持安静。"""
    store = DatasetStore(tmp_path / "data")
    store.upsert("size", pl.DataFrame([_size_row("acme/a", "ok", 10)], schema=SIZE_SCHEMA))
    before = (store.root / "enrich" / "size.parquet").stat().st_mtime_ns

    merged = merge_staged(store, SIZE_MERGE, 5.0)

    assert merged == 0
    assert (store.root / "enrich" / "size.parquet").stat().st_mtime_ns == before


def test_合并时按声明保护已拿到的数字(tmp_path: Path) -> None:
    """一次瞬时 500 不该把 `num_rows` 抹空——保护由 `MergeSpec.degraded` 声明。"""
    store = DatasetStore(tmp_path / "data")
    store.upsert(
        "size",
        pl.DataFrame([_size_row("acme/a", "ok", 1200)], schema=SIZE_SCHEMA),
    )
    _write_shard(
        store,
        SIZE_MERGE.staging_dir,
        pl.DataFrame([_size_row("acme/a", "unavailable", None)], schema=SIZE_SCHEMA),
    )

    merge_staged(store, SIZE_MERGE, 5.0)

    row = store.read("size").row(0, named=True)
    assert row["status"] == "ok" and row["num_rows"] == 1200


def test_没有旧值的行照常落盘(tmp_path: Path) -> None:
    """从未成功过的库，`unavailable` 是真实结论，也是永久结论——没有理由挡。"""
    store = DatasetStore(tmp_path / "data")
    _write_shard(
        store,
        SIZE_MERGE.staging_dir,
        pl.DataFrame([_size_row("acme/new", "unavailable", None)], schema=SIZE_SCHEMA),
    )

    merge_staged(store, SIZE_MERGE, 5.0)

    assert store.read("size")["status"].to_list() == ["unavailable"]


# ---------------------------------------------------------------- 保护策略本身


def test_降级集合为空时不保护() -> None:
    previous = pl.DataFrame([_size_row("acme/a", "ok", 10)], schema=SIZE_SCHEMA)
    fresh = pl.DataFrame([_size_row("acme/a", "unavailable", None)], schema=SIZE_SCHEMA)

    kept = keep_ok_rows(previous, fresh, degraded=frozenset())

    assert kept["status"].to_list() == ["unavailable"], "没声明降级就照新值写"


def test_只挡声明里列出的状态() -> None:
    """`readme` 只挡 `error`：`absent` 是明确结论，可以覆盖旧正文。

    这条盯的是「哪些状态算降级」必须由声明决定，而不是各表各写一个表达式——
    两份逐字相同的函数就是这么分叉的。
    """
    previous = pl.DataFrame([_size_row("acme/a", "ok", 10)], schema=SIZE_SCHEMA)
    fresh = pl.DataFrame(
        [
            _size_row("acme/a", "error", None),
            _size_row("acme/b", "absent", None),
        ],
        schema=SIZE_SCHEMA,
    )
    previous = pl.concat(
        [previous, pl.DataFrame([_size_row("acme/b", "ok", 20)], schema=SIZE_SCHEMA)]
    )

    kept = keep_ok_rows(previous, fresh, degraded=frozenset({"error"}))

    assert sorted(kept["id"].to_list()) == ["acme/b"], "acme/a 是 error 被挡下，acme/b 照覆盖"
