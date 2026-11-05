"""Silver 派生：把扁平标签拆成可 join 的维度。

纯函数，不碰 IO——便于单独验证解析规则，也便于日后换规则回填历史。
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .tables import SILVER_SCHEMA

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


def split_tag_dimensions(tags: list[str]) -> dict[str, list[str]]:
    """按前缀把标签归入维度列，保持原序并去重。"""
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


def derive_row(bronze_row: dict[str, Any], snapshot_date: Any) -> dict[str, Any]:
    """由一条 Bronze 行派生一条 Silver 行。"""
    tags = bronze_row.get("tags") or []
    return {
        "id": bronze_row["id"],
        "author": bronze_row.get("author"),
        "sha": bronze_row.get("sha"),
        "created_at": bronze_row.get("created_at"),
        "last_modified": bronze_row.get("last_modified"),
        "downloads": bronze_row.get("downloads"),
        "downloads_all_time": bronze_row.get("downloads_all_time"),
        "likes": bronze_row.get("likes"),
        "trending_score": bronze_row.get("trending_score"),
        "gated": bronze_row.get("gated"),
        "private": bronze_row.get("private"),
        "disabled": bronze_row.get("disabled"),
        "main_size": bronze_row.get("main_size"),
        "xet_enabled": bronze_row.get("xet_enabled"),
        "resource_group": bronze_row.get("resource_group"),
        "description": bronze_row.get("description"),
        "tags": tags,
        **split_tag_dimensions(tags),
        "raw": bronze_row.get("raw"),
        "snapshot_date": snapshot_date,
    }
