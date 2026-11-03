"""Silver 派生：把扁平标签拆成可 join 的维度。

纯函数，不碰 IO——便于单独验证解析规则，也便于日后换规则回填历史。
"""

from __future__ import annotations

from typing import Any

import polars as pl

#: 标签前缀 → Silver 维度列名。未列出的前缀（format/modality/arxiv 等）只留在 tags 原列。
TAG_DIMENSIONS: dict[str, str] = {
    "task_categories": "task_categories",
    "language": "languages",
    "size_categories": "size_categories",
    "license": "licenses",
    "library": "libraries",
}

SILVER_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "author": pl.Utf8,
    "sha": pl.Utf8,
    "created_at": pl.Utf8,
    "last_modified": pl.Utf8,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
    "gated": pl.Utf8,
    "private": pl.Boolean,
    "description": pl.Utf8,
    "tags": pl.List(pl.Utf8),
    "task_categories": pl.List(pl.Utf8),
    "languages": pl.List(pl.Utf8),
    "size_categories": pl.List(pl.Utf8),
    "licenses": pl.List(pl.Utf8),
    "libraries": pl.List(pl.Utf8),
    "raw": pl.Utf8,
    "snapshot_date": pl.Date,
}


def split_tag_dimensions(tags: list[str]) -> dict[str, list[str]]:
    """按前缀把标签归入维度列，保持原序并去重。"""
    buckets: dict[str, list[str]] = {column: [] for column in TAG_DIMENSIONS.values()}
    for tag in tags:
        prefix, separator, value = tag.partition(":")
        if not separator or not value:
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
        "gated": bronze_row.get("gated"),
        "private": bronze_row.get("private"),
        "description": bronze_row.get("description"),
        "tags": tags,
        **split_tag_dimensions(tags),
        "raw": bronze_row.get("raw"),
        "snapshot_date": snapshot_date,
    }
