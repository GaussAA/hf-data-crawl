"""榜单页的行为测试：单文件可看、空态可读、注入被转义。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl.gold import GoldCatalog
from hf_data_crawl.report import Reporter
from hf_data_crawl.store import DatasetStore

DAY1 = date(2026, 11, 3)
DAY2 = date(2026, 11, 4)


def record(dataset_id: str, downloads: int, pretty_name: str | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": dataset_id,
        "author": "acme",
        "sha": "sha-1",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "downloads": downloads,
        "downloadsAllTime": downloads * 2,
        "likes": 3,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en", "task_categories:text-classification"],
    }
    if pretty_name:
        payload["cardData"] = {"pretty_name": pretty_name, "license": "mit"}
    return payload


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    instance.write_raw(
        snapshot_date=DAY1,
        page_index=1,
        records=[record("acme/alpha", 500, "Alpha 数据集"), record("acme/beta", 100)],
        meta={},
    )
    instance.build_bronze(DAY1)
    instance.build_silver(DAY1)
    instance.update_state(DAY1)
    instance.write_raw(
        snapshot_date=DAY1,
        page_index=1,
        records=[record("acme/alpha", 500, "Alpha 数据集")],
        meta={"mode": "enrich"},
        label="card",
    )
    instance.build_enrich(DAY1)
    return instance


def test_生成单文件页面且含关键数据(store: DatasetStore, tmp_path: Path) -> None:
    reporter = Reporter(GoldCatalog(store))

    summary = reporter.write(tmp_path / "out" / "dashboard.html", top=10)

    content = summary.path.read_text(encoding="utf-8")
    assert summary.path.exists()
    assert summary.bytes > 1000
    assert "<!DOCTYPE html>" in content
    assert "acme/alpha" in content
    assert "Alpha 数据集" in content
    assert "text-classification" in content


def test_页面包含原生控件基线样式(store: DatasetStore, tmp_path: Path) -> None:
    """滚动条、选区、焦点环等原生控件必须一起造境，否则在深色页面里格外突兀。"""
    content = Reporter(GoldCatalog(store)).write(tmp_path / "dashboard.html").path.read_text(
        encoding="utf-8"
    )

    for token in (
        "color-scheme:dark",
        "scrollbar-color",
        "::-webkit-scrollbar-thumb",
        "caret-color",
        "::-webkit-search-cancel-button",
    ):
        assert token in content


def test_增速为空时给出可读说明(store: DatasetStore, tmp_path: Path) -> None:
    reporter = Reporter(GoldCatalog(store))

    summary = reporter.write(tmp_path / "dashboard.html")

    assert "增速榜暂为空" in summary.path.read_text(encoding="utf-8")


def test_数据里的尖括号被转义(store: DatasetStore, tmp_path: Path) -> None:
    store.write_raw(
        snapshot_date=DAY2,
        page_index=2,
        records=[record("acme/<script>", 900)],
        meta={},
    )
    store.build_bronze(DAY2)
    store.build_silver(DAY2)
    store.update_state(DAY2)
    reporter = Reporter(GoldCatalog(store))

    summary = reporter.write(tmp_path / "dashboard.html")

    content = summary.path.read_text(encoding="utf-8")
    assert "acme/&lt;script&gt;" in content
    assert "acme/<script>" not in content
