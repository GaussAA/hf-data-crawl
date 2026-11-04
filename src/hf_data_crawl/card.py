"""cardData 的抽取规则。

HF 卡片结构不统一：license 可能是字符串或列表，arxiv 同理，字段时有时无。
抽取一律容错，缺字段留空，不因一张卡片畸形而中断整批。
"""

from __future__ import annotations

from typing import Any

import polars as pl

ENRICH_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "snapshot_date": pl.Date,
    "pretty_name": pl.Utf8,
    "paperswithcode_id": pl.Utf8,
    "arxiv": pl.Utf8,
    "license_name": pl.Utf8,
    "citation": pl.Utf8,
    "card_description": pl.Utf8,
}


def has_card(record: dict[str, Any]) -> bool:
    card = record.get("cardData")
    return isinstance(card, dict) and bool(card)


def extract_card_row(record: dict[str, Any], snapshot_date: Any) -> dict[str, Any] | None:
    """从一条完整记录里抽出卡片字段；无卡片则返回 None。"""
    card = record.get("cardData")
    if not isinstance(card, dict) or not card:
        return None

    return {
        "id": record["id"],
        "snapshot_date": snapshot_date,
        "pretty_name": _as_text(card.get("pretty_name")),
        "paperswithcode_id": _as_text(
            record.get("paperswithcode_id") or card.get("paperswithcode_id")
        ),
        "arxiv": _as_text(card.get("arxiv") or record.get("arxiv")),
        "license_name": _as_text(card.get("license")),
        "citation": _as_text(card.get("citation") or record.get("citation")),
        "card_description": _as_text(card.get("description")),
    }


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        parts = [str(item) for item in value if item is not None]
        return ",".join(parts) if parts else None
    text = str(value).strip()
    return text or None
