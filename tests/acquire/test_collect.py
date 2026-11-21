"""S4 · collect 子命令（日增量）的行为测试。

验的是外部行为：跑完落了几行、什么时候停、停了之后能不能续上。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from hf_data_crawl.acquire.collect import CHECKPOINT_FILENAME, WATERMARK_FILENAME, Collector
from hf_data_crawl.acquire.hf.api import HfDatasetApi, HfResponse, Transport
from hf_data_crawl.acquire.hf.checkpoint import FileCheckpoint
from hf_data_crawl.acquire.hf.watermark import WatermarkCheckpoint
from hf_data_crawl.pipeline import raw, state
from hf_data_crawl.platform.store import DatasetStore

DAY1 = date(2026, 11, 1)
DAY2 = date(2026, 11, 2)


class FakeCursorTransport:
    """按游标回放的假 transport，行为对齐真接口的 cursor 分页。"""

    def __init__(self, pages: list[list[dict[str, Any]]], rate_remaining: int = 400) -> None:
        self.pages = pages
        self.rate_remaining = rate_remaining
        self.calls: list[dict[str, Any]] = []
        base = "https://huggingface.co/api/datasets"
        self._links = [f"{base}?cursor=c{index + 2}" for index in range(len(pages) - 1)] + [None]
        self._map = {
            parse_qs(urlparse(link).query)["cursor"][0]: index + 1
            for index, link in enumerate(self._links)
            if link is not None
        }

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls.append(dict(params))
        cursor = params.get("cursor")
        index = 0 if cursor is None else self._map[str(cursor)]
        link = self._links[index]
        response_headers = {"RateLimit": f'"api";r={self.rate_remaining};t=180'}
        if link is not None:
            response_headers["Link"] = f'<{link}>; rel="next"'
        import json

        return HfResponse(
            status=200,
            headers=response_headers,
            body=json.dumps(self.pages[index]).encode(),
        )


def page(
    prefix: str,
    count: int,
    downloads: int = 100,
    last_modified: str = "2026-11-01T00:00:00.000Z",
) -> list[dict[str, Any]]:
    return [
        {
            "id": f"{prefix}/ds-{index}",
            "author": prefix,
            "lastModified": last_modified,
            "createdAt": "2025-01-01T00:00:00.000Z",
            "sha": f"sha-{prefix}-{index}",
            "downloads": downloads,
            "downloadsAllTime": downloads * 2,
            "likes": 1,
            "gated": False,
            "private": False,
            "description": "d",
            "tags": ["language:en", "license:mit"],
        }
        for index in range(count)
    ]


def card_page(
    prefix: str, count: int, last_modified: str = "2026-11-02T00:00:00.000Z"
) -> list[dict[str, Any]]:
    """同一批记录，但带上 cardData——变化集卡片扫描请求的就是这一档。"""
    records = page(prefix, count, last_modified=last_modified)
    for item in records:
        item["cardData"] = {"pretty_name": f"卡片 {item['id']}", "license": "mit"}
    return records


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def collector(transport: Transport, store: DatasetStore, **kwargs: Any) -> Collector:
    return Collector(api=HfDatasetApi(transport=transport), store=store, **kwargs)


def test_空库首轮跑通并产出当前态(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 3), page("b", 2)])

    summary = collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    assert summary.stopped_reason == "end"
    assert summary.records == 5
    assert state.read_current(store).height == 5
    assert store.read_all("observations").height == 5


def test_次轮无变更在连续N页后提前停止(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 3), page("b", 2)])
    collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    again = FakeCursorTransport([page("a", 3), page("b", 2), page("c", 2)])
    summary = collector(again, store).run(DAY2, limit=100, pages_without_new=2)

    assert summary.stopped_reason in {"watermark_no_new", "watermark_timestamp"}
    assert summary.new_ids == 0
    assert len(again.calls) == 2
    assert state.read_current(store).height == 5


def test_已有底库但无水位线时首轮走到末页(store: DatasetStore) -> None:
    """首轮 `known` 来自现有 current（全站 id 都认识），后备判据会几页就停。

    那会把水位线钉在「最新几页的最老时间」上，之后每轮增量只覆盖最新几条，中间
    那段永远扫不到（列表按 lastModified 倒序，越老的越在后面）。首轮必须走到末页
    ——那是首次建库的正常成本。次轮有基线，后备判据照旧生效。
    """
    collector(FakeCursorTransport([page("a", 3), page("b", 2)]), store).run(
        DAY1, limit=100, pages_without_new=5
    )
    # 抹掉水位线：模拟「reconcile 已建好底库、但从未 collect 过」的真实起点
    (store.state_dir / WATERMARK_FILENAME).unlink(missing_ok=True)

    again = FakeCursorTransport([page("a", 3), page("b", 2), page("c", 2)])
    summary = collector(again, store).run(DAY2, limit=100, pages_without_new=1)

    assert summary.stopped_reason == "end", "首轮不能靠「无新 id」提前停"
    assert len(again.calls) == 3
    assert summary.new_ids == 2


def test_有界运行保留游标且续跑不重复(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 2), page("b", 2), page("c", 2)])
    runner = collector(transport, store)

    first = runner.run(DAY1, limit=100, max_pages=1, pages_without_new=9)
    assert first.stopped_reason == "max_pages"
    assert first.records == 2

    second = runner.run(DAY1, limit=100, pages_without_new=9)

    assert second.records == 4  # 只抓剩余两页，不重抓已处理的第一页
    assert state.read_current(store).height == 6
    assert state.read_current(store)["id"].n_unique() == 6


def test_dry_run只探一页且不落盘(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 3), page("b", 3)])

    summary = collector(transport, store).run(DAY1, limit=100, dry_run=True)

    assert summary.stopped_reason == "dry_run"
    assert summary.preview is not None and len(summary.preview) == 3
    assert len(transport.calls) == 1
    assert not store.raw_dir.exists()
    assert not store.state_dir.exists()


def test_摘要配额余量与响应头一致(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 1)], rate_remaining=123)

    summary = collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    assert summary.rate_remaining == 123


def test_默认lean档不请求cardData(store: DatasetStore) -> None:
    transport = FakeCursorTransport([page("a", 1)])

    collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    expand = transport.calls[0]["expand[]"]
    assert "cardData" not in expand
    assert "downloadsAllTime" in expand


class SortAwareTransport:
    """按 sort 参数分流回放的假 transport，用于验证顶部池刷新。"""

    def __init__(self, pages_by_sort: dict[str, list[list[dict[str, Any]]]]) -> None:
        self._pages = pages_by_sort
        self._index = {key: 0 for key in pages_by_sort}
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        import json

        self.calls.append(dict(params))
        sort = str(params["sort"])
        pages = self._pages[sort]
        index = self._index[sort]
        self._index[sort] = index + 1
        response_headers = {"RateLimit": '"api";r=450;t=180'}
        if index + 1 < len(pages):
            response_headers["Link"] = (
                f'<https://huggingface.co/api/datasets?sort={sort}&cursor={sort}-{index + 2}>; rel="next"'
            )
        return HfResponse(
            status=200,
            headers=response_headers,
            body=json.dumps(pages[index]).encode(),
        )


def test_顶部刷新按条数换算页数并单独落盘(store: DatasetStore) -> None:
    transport = SortAwareTransport(
        {
            "lastModified": [page("a", 2)],
            "downloads": [page("t", 1), page("u", 1), page("v", 1)],
        }
    )

    summary = collector(transport, store).run(DAY1, limit=2, refresh_top=5, pages_without_new=5)

    assert summary.top_pages == 3  # ceil(5 / 2)
    assert summary.refreshed == 3
    assert len(list(store.raw_dir.rglob(f"top-*{raw.SUFFIX}"))) == 3
    assert state.read_current(store).height == 5


def test_未指定顶部刷新时只扫一趟(store: DatasetStore) -> None:
    transport = SortAwareTransport({"lastModified": [page("a", 2)]})

    summary = collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    assert summary.top_pages == 0
    assert summary.refreshed == 0
    assert {call["sort"] for call in transport.calls} == {"lastModified"}


def test_顶部刷新使用下载量排序(store: DatasetStore) -> None:
    transport = SortAwareTransport({"lastModified": [page("a", 1)], "downloads": [page("t", 1)]})

    collector(transport, store).run(DAY1, limit=100, refresh_top=100, pages_without_new=5)

    downloads_calls = [call for call in transport.calls if call["sort"] == "downloads"]
    assert downloads_calls
    assert downloads_calls[0]["direction"] == "-1"


def test_全站扫描时不再单独刷顶部池(store: DatasetStore) -> None:
    """主扫描已扫完全站，头部 2 万条必然含在里面，再按下载量取一遍是重复请求。

    2026-11-21 实测：top 的 20,000 条里 19,792 条与主扫描逐字段相同（99.0%）。
    同一接口同一天为同一批库发两次请求、只为换个排序，没有信息增量。
    """
    transport = SortAwareTransport(
        {"lastModified": [page("a", 3)], "downloads": [page("t", 1), page("u", 1)]}
    )

    summary = collector(transport, store).run(
        DAY1, limit=3, refresh_top=6, pages_without_new=5, full_scan=True
    )

    assert summary.top_pages == 0
    assert summary.refreshed == 0
    assert summary.top_reason == "covered_by_full_scan"
    assert {call["sort"] for call in transport.calls} == {"lastModified"}, (
        "不该再发下载量排序的请求"
    )


def test_增量扫描时顶部池照旧刷(store: DatasetStore) -> None:
    """跳过只成立在「已扫全站」这一前提上；增量扫描没覆盖头部，这一趟仍必要。"""
    transport = SortAwareTransport({"lastModified": [page("a", 1)], "downloads": [page("t", 1)]})

    summary = collector(transport, store).run(DAY1, limit=100, refresh_top=100, pages_without_new=5)

    assert summary.top_pages == 1
    assert summary.top_reason != "covered_by_full_scan"


# ---------------------------------------------------------------- 水位线跨轮


def _dated(prefix: str, count: int, last_modified: str) -> list[dict[str, Any]]:
    return [{**row, "lastModified": last_modified} for row in page(prefix, count)]


def test_正常完成后水位线留给下一轮(store: DatasetStore) -> None:
    """完成态清游标、留水线：下轮必须能加载到非空水位线。"""
    transport = FakeCursorTransport([page("a", 2)])
    summary = collector(transport, store).run(DAY1, limit=100, pages_without_new=5)

    assert summary.stopped_reason == "end"
    watermark_path = store.state_dir / WATERMARK_FILENAME
    assert watermark_path.exists()
    reloaded = WatermarkCheckpoint(
        FileCheckpoint(store.state_dir / CHECKPOINT_FILENAME), watermark_path
    )
    assert reloaded.watermark.latest_modified == "2026-11-01T00:00:00.000Z"
    assert reloaded.cursor is None, "完成态游标应作废"


def test_次轮凭水位线在越过边界时停止(store: DatasetStore) -> None:
    """端到端：上轮水线 11-01，下轮先扫新记录，遇到早于基线的记录才停止。

    首页全部晚于基线时必须继续翻页；直到某页最老记录严格早于基线，才触发
    时间戳停止。这条用例把「基线固定 + 比较方向正确」一起钉住。
    """
    first = FakeCursorTransport([page("a", 2)])
    first_summary = collector(first, store).run(DAY1, limit=100, pages_without_new=5)
    assert first_summary.stopped_reason == "end"

    again = FakeCursorTransport(
        [
            _dated("b", 2, "2026-11-02T00:00:00.000Z"),  # 全晚于基线，继续
            _dated("c", 2, "2026-10-31T23:00:00.000Z"),  # 最老早于基线，停止
        ]
    )
    summary = collector(again, store).run(DAY2, limit=100, pages_without_new=5)

    assert summary.stopped_reason == "watermark_timestamp"
    assert len(again.calls) == 2


def test_首轮没有水位线时不跑卡片趟(store: DatasetStore) -> None:
    """没有「变化」可言时抓全站等于那 14 小时的一次性补齐，不该混进日常增量。"""
    transport = FakeCursorTransport([card_page("a", 2)])

    summary = collector(transport, store).run(DAY1, limit=100, cards=True)

    assert summary.card_reason == "no_watermark"
    assert summary.card_pages == 0


def test_有水位线时卡片随增量抓且不进bronze(store: DatasetStore) -> None:
    collector(FakeCursorTransport([page("a", 2)]), store).run(DAY1, limit=100, pages_without_new=5)

    transport = FakeCursorTransport([card_page("b", 2)])
    summary = collector(transport, store).run(DAY2, limit=100, cards=True)

    assert summary.card_reason == "end"
    assert summary.card_records == 2
    # 卡片批次不进 Bronze 去重池：两页同为 b/ds-0、b/ds-1，bronze 仍是 2 行。
    assert summary.bronze_rows == 2
    assert store.read("carddata").height == 2
    names = [p.name for p in (store.raw_dir / f"dt={DAY2.isoformat()}").glob(f"*{raw.SUFFIX}")]
    assert any(name.startswith("card-") for name in names)


def test_卡片趟受页数上限保护(store: DatasetStore) -> None:
    """水位线异常或过长时，不能一次把全站 1117 页拉完。"""
    collector(FakeCursorTransport([page("a", 2)]), store).run(DAY1, limit=100, pages_without_new=5)

    transport = FakeCursorTransport([card_page("b", 2), card_page("c", 2), card_page("d", 2)])
    summary = collector(transport, store).run(DAY2, limit=100, cards=True, card_max_pages=2)

    assert summary.card_pages == 2
    assert summary.card_reason == "max_pages"


def test_full_scan_无视时间水位线走到末页(store: DatasetStore) -> None:
    """`--full-scan` 必须把**两条**停止条件都关掉。

    只关「连续 N 页无新记录」还不够：越过旧边界的那一页会因**时间**水位线停下。
    2026-11-20 首次刷新链实测就是这个形状——6 页、31 秒、`stopped_reason=
    watermark_timestamp`，而那一轮正是「采集定稿」，列表字段只覆盖了 48.6%。
    """
    collector(FakeCursorTransport([page("a", 3)]), store).run(DAY1, limit=100, pages_without_new=5)

    # 次轮：同样的 id、更老的 lastModified——正常增量会在第一页就因 stale 停下。
    stale_pages = [
        page("a", 3, last_modified="2026-10-01T00:00:00.000Z"),
        page("b", 2, last_modified="2026-10-01T00:00:00.000Z"),
    ]
    again = FakeCursorTransport(stale_pages)
    summary = collector(again, store).run(DAY2, limit=100, pages_without_new=2, full_scan=True)

    assert summary.stopped_reason == "end", "full_scan 不该被时间水位线截断"
    assert len(again.calls) == 2
