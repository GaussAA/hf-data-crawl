"""Silver 派生：把扁平标签拆成可 join 的维度，并只记录属性的变更。

整张表用 polars 表达式一次算完，不再逐行转 Python 字典：111 万行的实测里，
逐行版本 82 秒，向量化版本 1.5 秒，输出逐行相等。规则本身（标签怎么归维度）
不变；`tests/test_silver_derive.py` 用一份逐行参考实现对照，防止向量化把边界
情形悄悄改掉。

Silver 是属性变更流水，不是每日快照
-----------------------------------
两天之间 111 万个共同数据集里，只有约 0.3% 的属性（sha、描述、标签……）变了；
但约 53% 的指标（下载、点赞）变了。把两者放在一张按日全量的表里，等于每天重复存
一份 99.7% 相同的快照。所以：

- 指标只进 `observations`（变了才追加）；
- Silver 只存属性，且一个 id 只在**首次出现或属性变了**才追加一行；
- 某个 id 的当前属性 = 它最近的一行（`latest`），历史 = 它的全部行。

变更判定对空值敏感：`null → 有值` 也算变化（例如接口新增了一个字段），否则最新
属性里会永远缺这一列。比较的参照是「早于该日的最近一行」，因此同日重跑结果不变。
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

#: 参与变更判定的列：除键与日期之外的全部列。由 schema 求得而非手抄，
#: 新增属性列自动进入判定，不会出现「列加了却不触发变更」的静默缺陷。
ATTRIBUTE_COLUMNS: tuple[str, ...] = tuple(
    column for column in SILVER_SCHEMA if column not in {"id", "snapshot_date"}
)

_ROW = "__row"
_KNOWN = "__known"
_PREV = "__prev"
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
    """由 Bronze 整表派生该日的属性：保持行序，维度列保持标签原序并去重。

    只做派生，不做变更判定；指标列随 `select` 被丢弃。
    """
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


def latest(store: DatasetStore, *, before: date | None = None) -> pl.DataFrame:
    """每个 id 最近的一行属性；给了 `before` 则只看严格早于该日的行。"""
    history = store.read_all("silver")
    if before is not None:
        history = history.filter(pl.col("snapshot_date") < before)
    return history.sort("snapshot_date").unique(subset=["id"], keep="last", maintain_order=True)


def changed_rows(today: pl.DataFrame, previous: pl.DataFrame) -> pl.DataFrame:
    """今日属性里「没见过」或「与上一行不同」的行。比较对空值敏感。"""
    known = previous.select(
        "id",
        pl.lit(True).alias(_KNOWN),
        *[pl.col(column).alias(f"{column}{_PREV}") for column in ATTRIBUTE_COLUMNS],
    )
    joined = today.join(known, on="id", how="left")
    differs = pl.col(_KNOWN).is_null()
    for column in ATTRIBUTE_COLUMNS:
        differs = differs | pl.col(column).ne_missing(pl.col(f"{column}{_PREV}"))
    return joined.filter(differs).select(list(SILVER_SCHEMA))


def build(store: DatasetStore, snapshot_date: date) -> int:
    """由 Bronze 追加该日的属性变更，返回写入的行数；无 Bronze 则不动。

    没有任何变化时不留空分区，并清掉同日重跑前遗留的旧分区。
    """
    bronze = store.read_bronze(snapshot_date)
    if bronze.height == 0:
        return 0
    delta = changed_rows(derive(bronze, snapshot_date), latest(store, before=snapshot_date))
    if delta.height == 0:
        store.path("silver", snapshot_date).unlink(missing_ok=True)
        return 0
    store.write("silver", delta, snapshot_date)
    return delta.height
