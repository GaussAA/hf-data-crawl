"""榜单页的行为测试：单文件可看、空态可读、注入被转义。"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl.pipeline import card, raw, state
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.runs import Outcome, write_run
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.query.gold import GoldCatalog
from hf_data_crawl.query.report import Reporter, _num

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
    raw.write(
        instance,
        snapshot_date=DAY1,
        page_index=1,
        records=[record("acme/alpha", 500, "Alpha 数据集"), record("acme/beta", 100)],
        meta={},
    )
    derive_day(instance, DAY1)
    raw.write(
        instance,
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
    raw.write(
        store,
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
    raw.write(
        store,
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
    raw.write(store, snapshot_date=DAY2, page_index=3, records=[payload], meta={})
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
    raw.write(store, snapshot_date=DAY2, page_index=4, records=[payload], meta={})
    derive_day(store, DAY2)
    expected = GoldCatalog(store).board("categories", 10)["downloads"][0]

    content = _page(store, tmp_path)

    assert f"{expected:,.0f}" in _table_html(content, "categories")
    assert _num(Decimal("119773807")) == "119,773,807"


def test_增速榜给出具体观测区间(store: DatasetStore, tmp_path: Path) -> None:
    """只给 span_days=1 时，读者无从判断「最近两个观测点」是哪两天。"""
    raw.write(
        store,
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


def test_占比用统一的data属性标注(store: DatasetStore, tmp_path: Path) -> None:
    """孤立绝对数不能说明「这张榜能代表全站多少」；占比的可比性靠属性而非文案。"""
    content = _page(store, tmp_path)

    for token in (
        "data-p0=",
        "data-p1=",
        'data-measurable="1"',
        "data-cap1=",
    ):
        assert token in content, token


def test_补齐矩阵把四种结局分开画(store: DatasetStore, tmp_path: Path) -> None:
    """「没有」与「读不到」是两件事，压成一个百分比就分不出来了。

    2026-11-20 之前首屏是四条单值覆盖率：卡片 68.6%、规模 74.4%、增速 98.1%、
    在架 99.9% 同形并排，单位其实有三种（我们有 / 有两次读数 / 还公开）。这一版把
    增补层拆成「有 / 没有 / 读不到 / 还没抓」四段，段由数据驱动，图例在行上方。
    """
    content = _page(store, tmp_path)

    for token in (
        'class="fill-legend"',
        'class="sw have"',
        'class="sw absent"',
        'class="sw unknown"',
        'class="sw pending"',
        'class="fill-track"',
    ):
        assert token in content, token
    assert content.count('class="fill-row"') == 5


def test_补齐矩阵的余量按资产各自的语义算(store: DatasetStore, tmp_path: Path) -> None:
    """卡片缺行是「没有卡片」，文件清单缺行是「还没抓」——同一个余量，两种事实。

    夹具里两个数据集、只有 alpha 有卡片；文件清单与预览一行都没有。若把余量一律
    记成「没有」，读者会把「还没抓」当成「作者没写」。
    """
    content = _page(store, tmp_path)
    card_row = content[content.index(">卡片</span>") : content.index(">规模</span>")]
    files_row = content[content.index(">文件清单</span>") : content.index(">预览</span>")]

    assert "没有 1" in card_row
    assert "未抓" not in card_row
    assert "未抓 2" in files_row


def test_报头交代这是哪一版(store: DatasetStore, tmp_path: Path) -> None:
    """每日产物必须说明自己诞生于何时、上一轮链跑完没有。

    `report` 在链尾执行，只能报**上一轮**；没有记录时直说没有，不能编一个「全部成功」。
    """
    write_run(
        store,
        "refresh",
        datetime(2026, 11, 20, 0, 30, 6),
        Outcome(
            payload={
                "elapsed_seconds": 1241.6,
                "steps": [
                    {"name": "collect", "code": 0, "seconds": 35.5, "note": ""},
                    {"name": "preview", "code": 1, "seconds": 1124.3, "note": ""},
                ],
                "failed": ["preview"],
            }
        ),
    )

    content = _page(store, tmp_path)

    assert "本版生成" in content
    assert "上一轮链 1/2 步成功" in content
    assert "preview 返回非 0" in content


def test_手工补跑的局部链不算一轮(store: DatasetStore, tmp_path: Path) -> None:
    """`--only` 的每一步都标着「不在 --only 里」，报成 5/15 会让读者以为链退化了。"""
    write_run(
        store,
        "refresh",
        datetime(2026, 11, 20, 12, 37),
        Outcome(
            payload={
                "steps": [
                    {"name": "collect", "code": None, "seconds": 0.0, "note": "不在 --only 里"},
                    {"name": "sizes", "code": 0, "seconds": 75.0, "note": ""},
                ],
                "failed": [],
            }
        ),
    )

    content = _page(store, tmp_path)

    assert "刷新链：无完整记录" in content


def test_页脚视图清单来自当前视图而不是手抄(store: DatasetStore, tmp_path: Path) -> None:
    """页脚曾写着 7 个视图，实际挂着 15 个——手抄的清单不会报错，只会变成假话。"""
    catalog = GoldCatalog(store)
    try:
        expected = catalog.views()
    finally:
        catalog.close()

    content = _page(store, tmp_path)

    assert f"视图 {len(expected)} 个" in content
    assert "preview" in content


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


def test_下载合计标注含多少非在架行(store: DatasetStore) -> None:
    """合计是全表求和，含非在架行的冻结值。

    不提示的话，那个数会被当成「今天全站的读数」；而它其中一部分停在几天前。
    """
    state.mark_status(store, {"acme/beta": "disappeared"})
    reporter = Reporter(GoldCatalog(store))

    content = reporter.render(top=10)

    assert "含 1 行非在架" in content
    assert "值停在上次见到的日期" in content


def test_非在架行在榜里被标记且样式随页下发(store: DatasetStore) -> None:
    """榜单 SQL 不带 status，标记得由渲染层按 id 补；只写标记不给样式等于没标。"""
    state.mark_status(store, {"acme/beta": "disappeared"})

    content = Reporter(GoldCatalog(store)).render(top=10)

    assert '<tr class="frozen"' in content
    assert 'title="非在架' in content
    assert "tbody tr.frozen .id::after" in content


def test_全部在架时不多话(store: DatasetStore) -> None:
    """没有非在架行时不印提示、不打标记——天天喊等于没喊。"""
    content = Reporter(GoldCatalog(store)).render(top=10)

    assert 'class="frozen"' not in content
    assert "行非在架" not in content
