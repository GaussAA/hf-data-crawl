"""向量化 Silver 派生与逐行参考实现的对照。

`silver.derive` 用 polars 表达式整表计算，规则散在正则和连接里，边界情形很容易被
悄悄改掉（实测就出过一次：前缀含中文时，`str.find` 返回字节偏移而 `str.slice` 按
字符计，「某前缀:值」被误判成无前缀标签）。这里保留旧的逐行实现作参考，
手写的边界用例加一批随机用例，一律要求两者逐行相等。
"""

from __future__ import annotations

import random
from datetime import date

import polars as pl
import pytest

from hf_data_crawl.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS, derive
from hf_data_crawl.tables import BRONZE_SCHEMA, SILVER_SCHEMA

DAY = date(2026, 11, 4)
DIMENSION_COLUMNS = [*TAG_DIMENSIONS.values(), PLAIN_TAG_COLUMN]


def reference(tags: list[str]) -> dict[str, list[str]]:
    """旧的逐行实现，原样保留作为规则的权威表述。"""
    buckets: dict[str, list[str]] = {column: [] for column in TAG_DIMENSIONS.values()}
    buckets[PLAIN_TAG_COLUMN] = []
    for tag in tags:
        prefix, separator, value = tag.partition(":")
        if not separator or not value:
            plain = tag.strip()
            if plain and plain not in buckets[PLAIN_TAG_COLUMN]:
                buckets[PLAIN_TAG_COLUMN].append(plain)
            continue
        column = TAG_DIMENSIONS.get(prefix)
        if column is None:
            continue
        if value not in buckets[column]:
            buckets[column].append(value)
    return buckets


def bronze_of(tag_lists: list[list[str]]) -> pl.DataFrame:
    rows = []
    for index, tags in enumerate(tag_lists):
        row: dict[str, object] = {name: None for name in BRONZE_SCHEMA}
        row.update({"id": f"acme/ds-{index}", "tags": tags, "snapshot_date": DAY})
        rows.append(row)
    return pl.DataFrame(rows, schema=BRONZE_SCHEMA)


def assert_matches_reference(tag_lists: list[list[str]]) -> None:
    result = derive(bronze_of(tag_lists), DAY)

    assert list(result.columns) == list(SILVER_SCHEMA)
    assert result["id"].to_list() == [f"acme/ds-{i}" for i in range(len(tag_lists))]
    for position, tags in enumerate(tag_lists):
        expected = reference(tags)
        row = result.row(position, named=True)
        assert row["tags"] == tags
        for column in DIMENSION_COLUMNS:
            assert row[column] == expected[column], (tags, column)


EDGE_CASES: list[list[str]] = [
    [],
    ["language:en"],
    ["language:en", "language:en", "language:zh"],  # 去重且保持原序
    ["language:zh", "language:en"],  # 原序而非字典序
    ["text", "image", "text"],  # 无前缀，去重
    ["  text  ", "   ", ""],  # 去空白；全空白与空串丢弃
    ["language:"],  # 冒号后为空：按整串进无前缀
    ["language: en"],  # 取值不去空白
    [":en"],  # 空前缀不在映射里：丢弃
    ["language:en:US"],  # 只按第一个冒号切
    ["unknown_prefix:value"],  # 未知前缀：丢弃，仍留在 tags
    ["某前缀:值", "language:en"],  # 前缀含中文（字节偏移与字符偏移不同）
    ["语言:中文", "language:中文"],
    ["license:mit", "license:apache-2.0", "task_categories:text-classification"],
    ["language:a\nb"],  # 取值含换行
    ["modality:text", "text", "format:parquet", "region:us", "library:datasets"],
]


@pytest.mark.parametrize("tags", EDGE_CASES, ids=[repr(case)[:40] for case in EDGE_CASES])
def test_边界情形与逐行参考实现一致(tags: list[str]) -> None:
    assert_matches_reference([tags])


def test_整批边界情形一次派生也一致() -> None:
    """多行同批：行序保持，行与行之间互不串。"""
    assert_matches_reference(EDGE_CASES)


def test_随机标签组合与逐行参考实现一致() -> None:
    rng = random.Random(20261106)
    prefixes = [*TAG_DIMENSIONS, "unknown", "", "某前缀", "Language"]
    values = ["en", "zh", " en", "", "a:b", "x y", "中文", "text", "a\nb"]
    bare = ["text", "image", " audio ", "", "   ", "tabular"]

    def random_tag() -> str:
        if rng.random() < 0.25:
            return rng.choice(bare)
        return f"{rng.choice(prefixes)}:{rng.choice(values)}"

    tag_lists = [[random_tag() for _ in range(rng.randint(0, 12))] for _ in range(3000)]

    assert_matches_reference(tag_lists)


def test_空表派生出空表且列齐全() -> None:
    result = derive(pl.DataFrame(schema=BRONZE_SCHEMA), DAY)

    assert result.height == 0
    assert list(result.columns) == list(SILVER_SCHEMA)


def test_标签为空值时当作空列表() -> None:
    frame = bronze_of([["language:en"], []]).with_columns(
        pl.when(pl.col("id") == "acme/ds-1").then(None).otherwise(pl.col("tags")).alias("tags")
    )

    result = derive(frame, DAY)

    assert result["tags"].to_list() == [["language:en"], []]
    assert result["languages"].to_list() == [["en"], []]
