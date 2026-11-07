"""榜单页的行为测试：单文件可看、空态可读、注入被转义。"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import card
from hf_data_crawl.derive import derive_day
from hf_data_crawl.gold import GoldCatalog
from hf_data_crawl.report import Reporter, _num
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
    derive_day(instance, DAY1)
    instance.write_raw(
        snapshot_date=DAY1,
        page_index=1,
        records=[record("acme/alpha", 500, "Alpha 数据集")],
        meta={"mode": "enrich"},
        label="card",
    )
    card.build(instance, DAY1)
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
    content = (
        Reporter(GoldCatalog(store))
        .write(tmp_path / "dashboard.html")
        .path.read_text(encoding="utf-8")
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

    assert "暂无正增长可比" in summary.path.read_text(encoding="utf-8")


def _table_html(content: str, table_id: str) -> str:
    start = content.index(f'<table id="{table_id}"')
    return content[start : content.index("</table>", start)]


def test_增速榜有数据时列出起点当前与增量(store: DatasetStore, tmp_path: Path) -> None:
    """页面的增速榜与 `query --board growth` 是同一口径，数值必须对得上。"""
    store.write_raw(
        snapshot_date=DAY2,
        page_index=2,
        records=[record("acme/alpha", 700)],
        meta={},
    )
    derive_day(store, DAY2)
    catalog = GoldCatalog(store)
    board = catalog.board("growth", limit=10).row(0, named=True)

    content = Reporter(catalog).write(tmp_path / "dashboard.html").path.read_text(encoding="utf-8")

    assert "暂无正增长可比" not in content
    growth_html = _table_html(content, "growth")
    assert "acme/alpha" in growth_html
    for value in (board["downloads_start"], board["downloads_now"], board["growth"]):
        assert f"{value:,}" in growth_html or str(value) in growth_html
    assert (board["downloads_start"], board["downloads_now"], board["growth"]) == (500, 700, 200)


def test_具备可访问性语义(store: DatasetStore, tmp_path: Path) -> None:
    content = (
        Reporter(GoldCatalog(store))
        .write(tmp_path / "dashboard.html")
        .path.read_text(encoding="utf-8")
    )

    assert 'scope="col"' in content
    assert "<caption>" in content
    assert 'aria-pressed="true"' in content
    assert 'aria-live="polite"' in content


def test_不含已知反模式(store: DatasetStore, tmp_path: Path) -> None:
    """这份清单来自 2026-11-04 的复盘，防止改版时又把雷踩回去。"""
    content = (
        Reporter(GoldCatalog(store))
        .write(tmp_path / "dashboard.html")
        .path.read_text(encoding="utf-8")
    )

    assert "background-clip:text" not in content  # 渐变文字
    assert "◆" not in content  # 装饰符号
    assert ".pill" not in content  # 死代码
    assert 'colspan="99"' not in content  # 写死列数
    assert ".table-wrap" not in content  # 嵌套滚动容器


def test_数据里的尖括号被转义(store: DatasetStore, tmp_path: Path) -> None:
    store.write_raw(
        snapshot_date=DAY2,
        page_index=2,
        records=[record("acme/<script>", 900)],
        meta={},
    )
    derive_day(store, DAY2)
    reporter = Reporter(GoldCatalog(store))

    summary = reporter.write(tmp_path / "dashboard.html")

    content = summary.path.read_text(encoding="utf-8")
    assert "acme/&lt;script&gt;" in content
    assert "acme/<script>" not in content


def test_数据集名渲染成可点开的链接(store: DatasetStore, tmp_path: Path) -> None:
    """榜单是拿来「找到东西」的，找到之后第一件事就是去页面看。"""
    content = (
        Reporter(GoldCatalog(store)).write(tmp_path / "link.html").path.read_text(encoding="utf-8")
    )

    assert 'href="https://huggingface.co/datasets/acme/alpha"' in content


def test_外链带安全属性(store: DatasetStore, tmp_path: Path) -> None:
    """新窗口打开且断开 opener，避免被打开方通过 window.opener 反向操作本页。"""
    content = (
        Reporter(GoldCatalog(store)).write(tmp_path / "safe.html").path.read_text(encoding="utf-8")
    )

    assert 'target="_blank"' in content
    assert 'rel="noopener noreferrer"' in content


# ---------------------------------------------------------------- 2026-11-07 重构后的展示契约


def _page(store: DatasetStore, tmp_path: Path, *, top: int = 10) -> str:
    """渲染一份页面并返回文本，供下面几条契约断言共用。"""
    return (
        Reporter(GoldCatalog(store))
        .write(tmp_path / "dashboard.html", top=top)
        .path.read_text(encoding="utf-8")
    )


def test_累计榜是独立集合而不是同一批行重排(store: DatasetStore, tmp_path: Path) -> None:
    """早先「累计」只是把 30 天榜那几行重排；成员不同时应拿到完全不同的表。"""
    payload = record("acme/gamma", 50)
    payload["downloadsAllTime"] = 999_999
    store.write_raw(snapshot_date=DAY2, page_index=3, records=[payload], meta={})
    derive_day(store, DAY2)
    catalog = GoldCatalog(store)

    content = _page(store, tmp_path, top=2)

    assert "acme/gamma" not in _table_html(content, "ranking")
    assert "acme/gamma" in _table_html(content, "ranking-alltime")
    assert "按累计下载排序" in _table_html(content, "ranking-alltime")
    assert catalog.board("ranking_alltime", 2)["id"][0] == "acme/gamma"
    assert catalog.board("ranking", 2)["id"].to_list() == ["acme/alpha", "acme/beta"]


def test_分布表的大数值带千分位(store: DatasetStore, tmp_path: Path) -> None:
    """sum(downloads) 经 DuckDB 常是 Decimal，早先会裸输出 119773807。"""
    payload = record("acme/big", 119_773_807)
    store.write_raw(snapshot_date=DAY2, page_index=4, records=[payload], meta={})
    derive_day(store, DAY2)
    expected = GoldCatalog(store).board("categories", 10)["downloads"][0]

    content = _page(store, tmp_path)

    assert f"{expected:,.0f}" in _table_html(content, "categories")
    assert _num(Decimal("119773807")) == "119,773,807"


def test_增速榜给出具体观测区间(store: DatasetStore, tmp_path: Path) -> None:
    """只给 span_days=1 时，读者无从判断「最近两个观测点」是哪两天。"""
    store.write_raw(
        snapshot_date=DAY2,
        page_index=2,
        records=[record("acme/alpha", 700)],
        meta={},
    )
    derive_day(store, DAY2)

    content = _page(store, tmp_path)

    assert "11-03 → 11-04" in _table_html(content, "growth")
    assert "观测区间" in _table_html(content, "growth")


def test_状态按四值分开列(store: DatasetStore, tmp_path: Path) -> None:
    """「status <> active」把转私有与改名都算成失效，与词表定义冲突。"""
    content = _page(store, tmp_path)

    for term in ("在架", "已删除", "转私有", "已改名"):
        assert term in content
    assert "其中已失效" not in content
    assert 'class="status-split"' in content


def test_覆盖率与占比都是可比较的形式(store: DatasetStore, tmp_path: Path) -> None:
    """孤立绝对数不能说明「这张榜能代表全站多少」。"""
    content = _page(store, tmp_path)

    for token in (
        'class="cover-row"',
        'class="cover-track"',
        "data-p0=",
        "data-p1=",
        'data-measurable="1"',
        "data-cap1=",
    ):
        assert token in content, token


def test_口径说明集中且默认折叠(store: DatasetStore, tmp_path: Path) -> None:
    """六段 caption 各说一句，不如一处可展开的对照表。"""
    content = _page(store, tmp_path)

    assert '<details class="glossary">' in content
    assert "downloads_all_time" in content
    assert "card_num_rows" in content
    assert "main_size" in content


def test_页面暴露完整交互契约(store: DatasetStore, tmp_path: Path) -> None:
    """切换、筛选、排序、度量与明暗彩蛋都在单文件里，不依赖外部脚本。"""
    content = _page(store, tmp_path)

    for token in (
        'id="q"',
        'data-searchable="1"',
        'aria-sort="none"',
        'data-target="ranking-alltime"',
        'data-measure="1"',
        "dblclick",
        'html[data-theme="light"]',
        "color-scheme:light",
    ):
        assert token in content, token
