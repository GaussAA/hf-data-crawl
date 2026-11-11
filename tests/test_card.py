"""卡片抽取的行为测试。

卡片是 HF 上最不规范的数据：`configs` 与 `dataset_info` 可能只出现一个，
`features` 可能嵌套，`license` 可能是字符串或列表。抽取必须容错，
且「取不到」与「真的是 0」必须区分——否则会凭空造出一批空数据集。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.pipeline import card, raw
from hf_data_crawl.pipeline.card import extract_card_row, extract_config_rows
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import CARD_CONFIGS_SCHEMA, ENRICH_SCHEMA

DAY = date(2026, 11, 4)


def wikitext_style() -> dict[str, Any]:
    """真实结构样本：两个 config，各自带 splits 与 data_files。"""
    return {
        "id": "acme/wikitext",
        "author": "acme",
        "downloads": 1000,
        "paperswithcode_id": "wikitext-103",
        "cardData": {
            "pretty_name": "WikiText",
            "license": ["cc-by-sa-4.0", "mit"],
            "license_link": "https://example.org/license",
            "language": ["en"],
            "tags": ["language-modeling", "benchmark"],
            "task_categories": ["text-generation"],
            "size_categories": ["100M<n<1B"],
            "multilinguality": "monolingual",
            "annotations_creators": ["crowdsourced"],
            "language_creators": ["found"],
            "source_datasets": ["original"],
            "homepage": "https://example.org",
            "description": "卡片描述文本",
            "arxiv": "2401.00001",
            "configs": [
                {
                    "config_name": "wikitext-103-raw-v1",
                    "data_files": [
                        {"path": "wikitext-103-raw-v1/test-*", "split": "test"},
                        {"path": "wikitext-103-raw-v1/train-*", "split": "train"},
                    ],
                },
                {
                    "config_name": "metadata",
                    "data_files": [{"path": "metadata/train-*", "split": "train"}],
                },
            ],
            "dataset_info": [
                {
                    "config_name": "wikitext-103-raw-v1",
                    "dataset_size": 548_965_325,
                    "download_size": 315_466_397,
                    "features": [{"name": "text", "dtype": "string"}],
                    "splits": [
                        {"name": "test", "num_bytes": 1_305_088, "num_examples": 4358},
                        {"name": "train", "num_bytes": 546_500_949, "num_examples": 1_801_350},
                    ],
                }
            ],
        },
    }


# ---------------------------------------------------------------- 宽表


def test_卡片标量与列表字段全部抽出() -> None:
    row = extract_card_row(wikitext_style(), DAY)

    assert row is not None
    assert row["pretty_name"] == "WikiText"
    assert row["license_name"] == "cc-by-sa-4.0,mit"
    assert row["license_link"] == "https://example.org/license"
    assert row["card_languages"] == ["en"]
    assert row["card_task_categories"] == ["text-generation"]
    assert row["card_size_categories"] == ["100M<n<1B"]
    assert row["card_multilinguality"] == "monolingual"
    assert row["card_annotations_creators"] == ["crowdsourced"]
    assert row["card_source_datasets"] == ["original"]
    assert row["card_homepage"] == "https://example.org"
    assert row["card_tags"] == ["language-modeling", "benchmark"]


def test_宽表汇总行数与字节() -> None:
    row = extract_card_row(wikitext_style(), DAY)

    assert row is not None
    assert row["card_num_rows"] == 4358 + 1_801_350
    assert row["card_dataset_bytes"] == 548_965_325
    assert row["card_config_count"] == 2


def test_没有结构信息时汇总为空而不是零() -> None:
    """0 会被读成「空数据集」，而事实是「未知」——两者必须分开。"""
    record = wikitext_style()
    record["cardData"] = {"pretty_name": "仅名字"}

    row = extract_card_row(record, DAY)

    assert row is not None
    assert row["card_num_rows"] is None
    assert row["card_dataset_bytes"] is None
    assert row["card_config_count"] is None


def test_宽表产出的键与schema一致() -> None:
    row = extract_card_row(wikitext_style(), DAY)

    assert row is not None
    assert set(row) == set(ENRICH_SCHEMA)


def test_无卡片返回空() -> None:
    assert extract_card_row({"id": "acme/x"}, DAY) is None
    assert extract_card_row({"id": "acme/x", "cardData": {}}, DAY) is None


# ---------------------------------------------------------------- 明细表


def test_明细表按config与split展开() -> None:
    rows = extract_config_rows(wikitext_style(), DAY)

    pairs = [(row["config_name"], row["split"]) for row in rows]
    assert ("wikitext-103-raw-v1", "test") in pairs
    assert ("wikitext-103-raw-v1", "train") in pairs


def test_明细表带上行数与字节() -> None:
    rows = extract_config_rows(wikitext_style(), DAY)
    train = next(row for row in rows if row["split"] == "train")

    assert train["num_examples"] == 1_801_350
    assert train["num_bytes"] == 546_500_949
    assert train["dataset_size"] == 548_965_325


def test_明细表的data_files按split归属() -> None:
    rows = extract_config_rows(wikitext_style(), DAY)
    test_row = next(row for row in rows if row["split"] == "test")

    assert json.loads(test_row["data_files"]) == ["wikitext-103-raw-v1/test-*"]


def test_only_configs的config补骨架行() -> None:
    """dataset_info 未覆盖的 config（如 metadata 子集）不能整个消失。"""
    rows = extract_config_rows(wikitext_style(), DAY)
    metadata = [row for row in rows if row["config_name"] == "metadata"]

    assert len(metadata) == 1
    assert metadata[0]["split"] is None
    assert json.loads(metadata[0]["data_files"]) == ["metadata/train-*"]


def test_嵌套features存成可解析的json() -> None:
    record = wikitext_style()
    record["cardData"]["dataset_info"][0]["features"] = [
        {"name": "conversations", "list": [{"name": "from", "dtype": "string"}]}
    ]

    rows = extract_config_rows(record, DAY)

    parsed = json.loads(rows[0]["features"])
    assert parsed[0]["list"][0]["name"] == "from"


def test_畸形卡片不中断抽取() -> None:
    """一张坏卡不该让整批 2 万条一起失败。"""
    record = {
        "id": "acme/broken",
        "cardData": {
            "configs": "这不是列表",
            "dataset_info": [None, {"config_name": "d", "splits": "也不是列表"}],
            "license": {"奇怪": "结构"},
        },
    }

    row = extract_card_row(record, DAY)
    rows = extract_config_rows(record, DAY)

    assert row is not None and rows


def test_明细表产出的键与schema一致() -> None:
    rows = extract_config_rows(wikitext_style(), DAY)

    assert rows
    for row in rows:
        assert set(row) == set(CARD_CONFIGS_SCHEMA)


# ---------------------------------------------------------------- 落表


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def test_两张表都落盘且行数正确(store: DatasetStore) -> None:
    raw.write(
        store, snapshot_date=DAY, page_index=1, records=[wikitext_style()], meta={}, label="card"
    )

    card_rows, config_rows = card.build(store, DAY)

    assert card_rows == 1
    # 两个 split + metadata 骨架行
    assert config_rows == 3
    assert store.read("carddata").height == 1
    assert store.read("card_configs").height == 3


def test_重复补齐不产生重复明细行(store: DatasetStore) -> None:
    raw.write(
        store, snapshot_date=DAY, page_index=1, records=[wikitext_style()], meta={}, label="card"
    )
    card.build(store, DAY)
    raw.write(
        store, snapshot_date=DAY, page_index=2, records=[wikitext_style()], meta={}, label="card"
    )

    card.build(store, DAY)

    table = store.read("card_configs")
    assert table.height == 3
    assert table.select(["id", "config_name", "split"]).is_duplicated().sum() == 0


def test_旧版卡片表缺列时能继续并入(store: DatasetStore) -> None:
    """升级后的第一次 enrich 会读到上一版只有 7 列的卡片表，不能直接抛错。"""
    target = store.path("carddata")
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {"id": ["acme/old"], "pretty_name": ["旧集"], "license_name": ["mit"]}
    ).write_parquet(target)

    raw.write(
        store, snapshot_date=DAY, page_index=1, records=[wikitext_style()], meta={}, label="card"
    )
    card.build(store, DAY)

    table = store.read("carddata")
    assert table.height == 2
    assert set(table.columns) == set(ENRICH_SCHEMA)
    assert table.filter(pl.col("id") == "acme/old").row(0, named=True)["card_num_rows"] is None


# ---------------------------------------------------------------- 并行与串行等价


def _seed_card_pages(store: DatasetStore) -> None:
    """多页、跨页同 id，且每页的卡片内容不同：后写入的页必须胜出。"""
    for page in range(1, 6):
        record = wikitext_style()
        record["cardData"]["pretty_name"] = f"WikiText-{page}"
        raw.write(
            store,
            snapshot_date=DAY,
            page_index=page,
            records=[record, {**wikitext_style(), "id": "acme/other", "downloads": page}],
            meta={},
            label="card",
            written_at=f"2026-11-04T00:00:0{page}.000000+00:00",
        )


def test_串行与并行产出逐字节等价(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """并行只是加速：两路产出的行序、内容与落盘字节都必须相同。"""
    serial_store = DatasetStore(tmp_path / "serial")
    parallel_store = DatasetStore(tmp_path / "parallel")
    for store in (serial_store, parallel_store):
        _seed_card_pages(store)

    # 只有 5 个文件，靠降阈值与分块强制走出不同的执行路径。
    monkeypatch.setattr(card, "PARALLEL_MIN_FILES", 10_000)
    card.build(serial_store, DAY)

    monkeypatch.setattr(card, "PARALLEL_MIN_FILES", 1)
    monkeypatch.setattr(card, "CHUNK_FILES", 2)
    card.build(parallel_store, DAY)

    for name in ("carddata", "card_configs"):
        assert serial_store.path(name).read_bytes() == parallel_store.path(name).read_bytes()
    # 顺带确认「同 id 以最后写入者为准」没有在归并时被改掉。
    assert (
        parallel_store.read("carddata")
        .filter(pl.col("id") == "acme/wikitext")
        .row(0, named=True)["pretty_name"]
        == "WikiText-5"
    )


def test_进程池崩溃时退回串行且结果不变(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """抽行是纯函数：池坏了重算就行，不能让可重算的 build 整体失败。"""
    from concurrent.futures.process import BrokenProcessPool

    serial_store = DatasetStore(tmp_path / "serial")
    broken_store = DatasetStore(tmp_path / "broken")
    for store in (serial_store, broken_store):
        _seed_card_pages(store)

    monkeypatch.setattr(card, "PARALLEL_MIN_FILES", 10_000)
    card.build(serial_store, DAY)

    class BrokenPool:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> BrokenPool:
            return self

        def __exit__(self, *exc: Any) -> None:
            return None

        def map(self, *args: Any, **kwargs: Any) -> list[Any]:
            raise BrokenProcessPool("子进程异常终止")

    monkeypatch.setattr(card, "PARALLEL_MIN_FILES", 1)
    monkeypatch.setattr(card, "CHUNK_FILES", 2)
    monkeypatch.setattr(card, "ProcessPoolExecutor", BrokenPool)
    card.build(broken_store, DAY)

    for name in ("carddata", "card_configs"):
        assert serial_store.path(name).read_bytes() == broken_store.path(name).read_bytes()
