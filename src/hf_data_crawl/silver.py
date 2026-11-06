"""Silver 派生：把扁平标签拆成可 join 的维度。

整张表用 polars 表达式一次算完，不再逐行转 Python 字典：111 万行的实测里，
逐行版本 82 秒，向量化版本 1.5 秒，输出逐行相等。规则本身（标签怎么归维度）
不变，写在 `derive` 的注释里；`tests/test_silver_derive.py` 用一份逐行参考实现
对照，防止向量化把边界情形悄悄改掉。
"""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

import polars as pl

from .tables import SILVER_SCHEMA

if TYPE_CHECKING:
    from .store import DatasetStore

#: 标签前缀 → Silver 维度列名。
#:
#: 取值依据是全站 950 万条标签的实测频次：`library` 31.1%、`modality` 12.7%、
#: `region` 11.7%、`format` 8.6%、`size_categories` 8.4%、`license` 4.2%、
#: `language` 4.1%、`task_categories` 3.7%。此前只拆了 5 类，覆盖 51.5%，
#: `modality` / `region` / `format` 这三个高频维度全被丢在原 tags 列里，
#: 导致「多模态数据集有多少」这类问题无法用结构化字段回答。
TAG_DIMENSIONS: dict[str, str] = {
    "task_categories": "task_categories",
    "task_ids": "task_ids",
    "language": "languages",
    "size_categories": "size_categories",
    "license": "licenses",
    "library": "libraries",
    "modality": "modalities",
    "format": "formats",
    "region": "regions",
    "multilinguality": "multilinguality",
    "source_datasets": "source_datasets",
    "annotations_creators": "annotations_creators",
    "language_creators": "language_creators",
    "arxiv": "arxiv_ids",
    "doi": "dois",
}

#: 无前缀标签。占全部标签的 13.7%（130 万条），旧式模态标记（`text` / `image` /
#: `audio` / `tabular`）都在这里——不单独成列，等于把模态信息砍掉一半。
PLAIN_TAG_COLUMN = "plain_tags"

_ROW = "__row"
_EMPTY_LIST = pl.lit(pl.Series([[]], dtype=pl.List(pl.Utf8)))


def _classify(indexed: pl.DataFrame) -> pl.DataFrame:
    """把每行的标签炸开，逐个判定归入哪个维度列、取什么值。

    规则（与逐行版本一致）：
    - 以第一个 `:` 切成 前缀 / 取值；没有冒号，或冒号后为空，算无前缀标签，
      整串去首尾空白后进 `plain_tags`，去空后为空则丢弃；
    - 有取值但前缀不在 `TAG_DIMENSIONS` 里，直接丢弃（仍保留在原 `tags` 列）；
    - 取值不去空白：`"language: en"` 的取值是 `" en"`，保持原样。
    """
    mapping = pl.DataFrame(
        {"prefix": list(TAG_DIMENSIONS), "dimension": list(TAG_DIMENSIONS.values())},
        schema={"prefix": pl.Utf8, "dimension": pl.Utf8},
    )
    # 用正则一次取出前缀与取值，而不是 `str.find` + `str.slice`：前者返回字节偏移、
    # 后者按字符计，前缀含中文时两者错位，会把「某前缀:值」误判成无前缀标签。
    split = r"(?s)^([^:]*):(.*)$"
    entries = (
        indexed.select(_ROW, pl.col("tags").fill_null(_EMPTY_LIST).alias("tag"))
        .explode("tag")
        .filter(pl.col("tag").is_not_null())
        .with_columns(
            pl.col("tag").str.extract(split, 1).alias("prefix"),
            pl.col("tag").str.extract(split, 2).alias("value"),
        )
        .join(mapping, on="prefix", how="left")
    )
    plain = pl.col("value").is_null() | (pl.col("value") == "")
    return entries.select(
        _ROW,
        pl.when(plain)
        .then(pl.lit(PLAIN_TAG_COLUMN))
        .otherwise(pl.col("dimension"))
        .alias("column"),
        pl.when(plain)
        .then(pl.col("tag").str.strip_chars())
        .otherwise(pl.col("value"))
        .alias("item"),
    ).filter(pl.col("column").is_not_null() & (pl.col("item") != ""))


def derive(bronze: pl.DataFrame, snapshot_date: date) -> pl.DataFrame:
    """由 Bronze 整表派生 Silver：保持行序，维度列保持标签原序并去重。"""
    columns = [*TAG_DIMENSIONS.values(), PLAIN_TAG_COLUMN]
    indexed = bronze.with_row_index(_ROW)
    grouped = (
        _classify(indexed)
        .group_by(_ROW)
        .agg(
            [
                pl.col("item")
                .filter(pl.col("column") == name)
                .unique(maintain_order=True)
                .alias(name)
                for name in columns
            ]
        )
    )
    return (
        indexed.join(grouped, on=_ROW, how="left")
        .sort(_ROW)
        .with_columns(
            pl.col("tags").fill_null(_EMPTY_LIST),
            *[pl.col(name).fill_null(_EMPTY_LIST) for name in columns],
            pl.lit(snapshot_date, dtype=pl.Date).alias("snapshot_date"),
        )
        .select(list(SILVER_SCHEMA))
    )


def build(store: DatasetStore, snapshot_date: date) -> int:
    """由 Bronze 派生该日期的 Silver 分区，返回行数；无 Bronze 则不写。"""
    bronze = store.read_bronze(snapshot_date)
    if bronze.height == 0:
        return 0
    frame = derive(bronze, snapshot_date)
    store.write("silver", frame, snapshot_date)
    return frame.height
