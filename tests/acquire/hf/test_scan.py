"""ScanRun 的行为测试。

这条时序的关键不在「翻页」，而在**游标必须在每页写完盘之后立刻保存**：
调用方随时可能在下一页之前停下，而 `api.scan` 的生成器此刻已经暂停，
它内部那句 `checkpoint.save` 不会再执行。少存一页，续跑时就会重抓上一页。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl.acquire.hf.api import IDENTITY_EXPAND, Page
from hf_data_crawl.acquire.hf.scan import ScanRun, ScanSpec
from hf_data_crawl.pipeline import raw
from hf_data_crawl.platform.store import DatasetStore

DAY = date(2026, 11, 4)


class FakeScanApi:
    """按预设页序列产出，并记下调用参数。"""

    def __init__(self, pages: list[Page]) -> None:
        self.pages = pages
        self.kwargs: dict[str, Any] = {}

    def scan(self, **kwargs: Any) -> Any:
        self.kwargs = kwargs
        yield from self.pages


class RecordingCheckpoint:
    """只记录保存了什么游标。"""

    def __init__(self) -> None:
        self.saved: list[str | None] = []

    @property
    def cursor(self) -> str | None:
        return None

    def save(self, cursor: str | None) -> None:
        self.saved.append(cursor)


def page(*ids: str, cursor: str | None = "next", skipped: int = 0) -> Page:
    return Page(
        records=[{"id": dataset_id} for dataset_id in ids],
        next_cursor=cursor,
        rate_remaining=480,
        skipped=skipped,
    )


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def spec(**overrides: Any) -> ScanSpec:
    base: dict[str, Any] = {
        "sort": "lastModified",
        "expand": IDENTITY_EXPAND,
        "mode": "lean",
    }
    base.update(overrides)
    return ScanSpec(**base)


# ---------------------------------------------------------------- 计数与落盘


def test_逐页落盘并累计计数(store: DatasetStore) -> None:
    api = FakeScanApi([page("a", "b"), page("c", cursor=None)])

    summary = ScanRun(api, store).run(DAY, spec())

    assert summary.pages == 2
    assert summary.records == 3
    assert summary.rate_remaining == 480
    assert summary.stopped_reason == "end"
    assert len(list((store.raw_dir / f"dt={DAY.isoformat()}").glob(f"*{raw.SUFFIX}"))) == 2


def test_跳过计数被累计(store: DatasetStore) -> None:
    api = FakeScanApi([page("a", skipped=3), page("b", cursor=None, skipped=1)])

    summary = ScanRun(api, store).run(DAY, spec())

    assert summary.skipped == 4


def test_dry_run不写盘(store: DatasetStore) -> None:
    api = FakeScanApi([page("a", cursor=None)])

    ScanRun(api, store).run(DAY, spec(write=False))

    partition = store.raw_dir / f"dt={DAY.isoformat()}"
    assert not partition.exists() or not list(partition.glob(f"*{raw.SUFFIX}"))


def test_label与mode落进raw的meta(store: DatasetStore) -> None:
    import json

    api = FakeScanApi([page("a", cursor=None)])

    ScanRun(api, store).run(DAY, spec(label="top", mode="top-refresh"))

    path = next((store.raw_dir / f"dt={DAY.isoformat()}").glob(f"top-*{raw.SUFFIX}"))
    meta = json.loads(raw.unpack(path.read_bytes(), name=path.name).decode("utf-8"))["meta"]
    assert meta["mode"] == "top-refresh"
    assert meta["sort"] == "lastModified"


# ---------------------------------------------------------------- 游标


def test_每页写完盘就保存游标(store: DatasetStore) -> None:
    checkpoint = RecordingCheckpoint()
    api = FakeScanApi([page("a", cursor="c2"), page("b", cursor="c3"), page("c", cursor=None)])

    ScanRun(api, store).run(DAY, spec(checkpoint=checkpoint))

    assert checkpoint.saved == ["c2", "c3", None]


def test_中途停止时最后一页的游标也已保存(store: DatasetStore) -> None:
    """这是 collect 那段注释所守护的语义：先存游标，再判停。"""
    checkpoint = RecordingCheckpoint()
    api = FakeScanApi([page("a", cursor="c2"), page("b", cursor="c3"), page("c", cursor="c4")])

    def stop_at_two(_page: Page, index: int) -> str | None:
        return "watermark" if index >= 2 else None

    summary = ScanRun(api, store).run(DAY, spec(checkpoint=checkpoint, on_page=stop_at_two))

    assert summary.pages == 2
    assert summary.stopped_reason == "watermark"
    assert checkpoint.saved == ["c2", "c3"], "第二页的游标必须在 break 之前落盘"


def test_钩子先于游标保存(store: DatasetStore) -> None:
    """钩子带副作用时，先存游标会在两者之间留下「少做一页」的窗口。"""
    events: list[str] = []

    class OrderedCheckpoint:
        @property
        def cursor(self) -> str | None:
            return None

        def save(self, cursor: str | None) -> None:
            events.append(f"save:{cursor}")

    def hook(_page: Page, index: int) -> None:
        events.append(f"hook:{index}")
        return None

    api = FakeScanApi([page("a", cursor="c2"), page("b", cursor=None)])

    ScanRun(api, store).run(DAY, spec(checkpoint=OrderedCheckpoint(), on_page=hook))

    assert events == ["hook:1", "save:c2", "hook:2", "save:None"]


def test_钩子之后被打断时已见集合不缺页(store: DatasetStore) -> None:
    """模拟被杀在「钩子」与「存游标」之间：续跑会重抓这一页，最坏是重复追加。"""
    from hf_data_crawl.acquire.session import ReconcileSession

    checkpoint_name = "reconcile-checkpoint.json"
    session = ReconcileSession.open(store, checkpoint_filename=checkpoint_name)

    def remember_then_die(page_obj: Page, index: int) -> None:
        session.remember(page_obj.records)
        if index == 2:
            raise KeyboardInterrupt
        return None

    api = FakeScanApi([page("a", cursor="c2"), page("b", cursor="c3"), page("c", cursor=None)])
    with pytest.raises(KeyboardInterrupt):
        ScanRun(api, store).run(DAY, spec(checkpoint=session.checkpoint, on_page=remember_then_die))

    resumed = ReconcileSession.open(store, checkpoint_filename=checkpoint_name)

    assert resumed.resumed is True
    assert resumed.checkpoint.cursor == "c2", "第 2 页的游标没存，续跑会重抓它"
    assert set(resumed.seen()) == {"a", "b"}, "第 2 页的 id 已在钩子里记下，不缺页"


def test_不传游标时不保存任何东西(store: DatasetStore) -> None:
    api = FakeScanApi([page("a", cursor=None)])

    ScanRun(api, store).run(DAY, spec())

    assert not list(store.state_dir.glob("*.json"))


# ---------------------------------------------------------------- 停止规则


def test_页数上限停在准确的原因上(store: DatasetStore) -> None:
    """生成器到限时是静默 return，只有本层判才能说出为什么停。"""
    api = FakeScanApi([page("a"), page("b"), page("c"), page("d")])

    summary = ScanRun(api, store).run(DAY, spec(max_pages=2))

    assert summary.pages == 2
    assert summary.stopped_reason == "max_pages"


def test_钩子返回原因即停止且原因被记录(store: DatasetStore) -> None:
    api = FakeScanApi([page("a"), page("b"), page("c")])

    def halt_on_second(_page: Page, index: int) -> str | None:
        return "custom" if index == 2 else None

    summary = ScanRun(api, store).run(DAY, spec(on_page=halt_on_second))

    assert summary.pages == 2
    assert summary.stopped_reason == "custom"


def test_钩子收到的是本页与页码(store: DatasetStore) -> None:
    seen: list[tuple[int, list[str]]] = []
    api = FakeScanApi([page("a", "b"), page("c", cursor=None)])

    def record(page_obj: Page, index: int) -> None:
        seen.append((index, [item["id"] for item in page_obj.records]))
        return None

    ScanRun(api, store).run(DAY, spec(on_page=record))

    assert seen == [(1, ["a", "b"]), (2, ["c"])]


def test_页数上限为零表示不设上限(store: DatasetStore) -> None:
    """`--card-pages 0` 在刷新链里写作「变化 ∪ 缺口全刷」。

    照字面比 `pages >= 0` 会在第一页就停：站上以为刷完了，停止原因还写着
    `max_pages`——一个完全正常的编码，只是含义反了。
    """
    api = FakeScanApi([page("a"), page("b"), page("c", cursor=None)])

    summary = ScanRun(api, store).run(DAY, spec(max_pages=0))

    assert summary.pages == 3
    assert summary.stopped_reason == "end"


def test_页数上限由ScanRun自己数(store: DatasetStore) -> None:
    """上限若交给 API 生成器，停止原因与已落盘页数都会错。"""
    api = FakeScanApi([page("a", cursor="next"), page("b", cursor="next"), page("c", cursor=None)])

    summary = ScanRun(api, store).run(DAY, spec(max_pages=2))

    assert summary.pages == 2
    assert summary.stopped_reason == "max_pages"


# ---------------------------------------------------------------- 当天去重


def stored(store: DatasetStore) -> list[Path]:
    partition = store.raw_dir / f"dt={DAY.isoformat()}"
    return sorted(partition.glob(f"*{raw.SUFFIX}")) if partition.exists() else []


def test_整页都已存在则一个文件都不写(store: DatasetStore) -> None:
    """日常链里 `collect --full-scan` 与 `reconcile` 都全站扫、都用同一个 expand。

    11-21 实测两份交集 1,140,886 条，其中 1,140,360 条逐字段相同（99.95%）——
    第二份原样落盘就是每天多存 165 MB 的同一批数据。重复页整页不写，
    不是「少写一点」，是「同一份内容当天只存一次」。
    """
    existing = raw.DayRecords()
    existing.remember([{"id": "a"}, {"id": "b"}])
    api = FakeScanApi([page("a", "b", cursor=None)])

    summary = ScanRun(api, store).run(DAY, spec(dedup=existing))

    assert summary.records == 2, "扫到的条数照实记"
    assert summary.deduped == 2, "落盘量与抓取量分开记"
    assert stored(store) == []


def test_部分重复只落差额并记下跳过数(store: DatasetStore) -> None:
    import json

    existing = raw.DayRecords()
    existing.remember([{"id": "a"}, {"id": "b"}])
    api = FakeScanApi([page("a", "b", "c", cursor=None)])

    summary = ScanRun(api, store).run(DAY, spec(dedup=existing))

    assert summary.deduped == 2
    (path,) = stored(store)
    payload = json.loads(raw.unpack(path.read_bytes(), name=path.name))
    assert [item["id"] for item in payload["records"]] == ["c"]
    assert payload["meta"]["deduped"] == 2


def test_内容变了即使id相同也照落(store: DatasetStore) -> None:
    """只按 id 判重会把「内容变了」当成「已有」，那才是真的丢数据。

    11-21 两份之间有 526 条 id 相同、内容不同（相隔 19 分钟里下载量等变了）。
    指纹取「id 与全部字段」，所以它们一定会落盘、且以更晚的 written_at 胜出。
    """
    existing = raw.DayRecords()
    existing.remember([{"id": "a", "value": 1}])
    api = FakeScanApi(
        [Page(records=[{"id": "a", "value": 2}], next_cursor=None, rate_remaining=1, skipped=0)]
    )

    summary = ScanRun(api, store).run(DAY, spec(dedup=existing))

    assert summary.deduped == 0
    assert len(stored(store)) == 1


def test_同一轮里两页出现同一条只落一次(store: DatasetStore) -> None:
    api = FakeScanApi([page("a"), page("a", cursor=None)])

    summary = ScanRun(api, store).run(DAY, spec(dedup=raw.DayRecords()))

    assert summary.deduped == 1
    assert len(stored(store)) == 1


def test_不传指纹集合时照旧整页落盘(store: DatasetStore) -> None:
    """`collect` / 顶部刷新 / 卡片补齐都不去重：第一份必须原样落。"""
    api = FakeScanApi([page("a", "b", cursor=None)])

    summary = ScanRun(api, store).run(DAY, spec())

    assert summary.deduped == 0
    assert len(stored(store)) == 1
