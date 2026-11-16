"""Bronze 行编码：一条原始记录怎么落成一列。

为什么单独成文件：`to_row` 是本项目唯一把外部 JSON 摊成列的地方，写错不会报错，
只会让整列静默变空（2026-11-04 的 `private` 那次就是）。而 `tags` 尤其要紧——
它是 16 个标签维度的唯一来源，也是 `platform.coerce.as_list` 专门存在的理由：
上游可能把列表序列化成字符串。

历史行为：`tags` 用 `[...] if isinstance(tags, list) else []` 手写，绕开了
`as_list`。于是 `tags: "language:en"` 会落成空列表，silver 再把它当「属性变了」
写进变更流水——上游改回来时还要再写一轮才能恢复，全程零报错。
"""

from __future__ import annotations

from datetime import date
from typing import Any

from hf_data_crawl.pipeline.bronze import to_row

DAY = date(2026, 11, 16)


def test_标签是字符串时按单元素列表保留() -> None:
    """上游把 tags 序列化成字符串时，不能静默当成「没有标签」。"""
    row = to_row({"id": "a/b", "tags": "language:en"}, DAY, "f.json.gz")

    assert row is not None
    assert row["tags"] == ["language:en"]


def test_标签是列表时原样保留() -> None:
    row = to_row({"id": "a/b", "tags": ["language:en", "task:text"]}, DAY, "f.json.gz")

    assert row is not None
    assert row["tags"] == ["language:en", "task:text"]


def test_标签缺失或不可解释时留空列表() -> None:
    """空列表与缺失同义，都落成 `[]` 而不是 None——与历史行为一致。"""
    uninterpretable: tuple[Any, ...] = (None, [], {}, 0)
    for tags in uninterpretable:
        row = to_row({"id": "a/b", "tags": tags}, DAY, "f.json.gz")
        assert row is not None
        assert row["tags"] == [], f"tags={tags!r} 应落成空列表"


def test_缺_id的记录被丢弃() -> None:
    """没有 id 的行无法定位，丢弃。"""
    for record in ({"downloads": 1}, {"id": 123}, {"id": ""}, "not a dict"):
        assert to_row(record, DAY, "f.json.gz") is None


def test_取不到的值一律留空而不是猜() -> None:
    """`private: "true"` 是字符串不是布尔；`downloads: "很多"` 也不该变成 0。"""
    row = to_row(
        {"id": "a/b", "private": "true", "downloads": "很多", "mainSize": 3.0, "likes": 3.5},
        DAY,
        "f.json.gz",
    )

    assert row is not None
    assert row["private"] is None
    assert row["downloads"] is None
    assert row["main_size"] == 3  # 整值浮点收下
    assert row["likes"] is None  # 非整值不收——猜成 4 或 3 都是猜


def test_原始行完整保留以便回溯() -> None:
    """`raw` 列是唯一能回溯上游的形状；任何字段都不能在摊平过程中丢掉。"""
    record: dict[str, Any] = {"id": "a/b", "unknown_field": {"nested": True}}
    row = to_row(record, DAY, "f.json.gz")

    assert row is not None
    assert "unknown_field" in row["raw"]
