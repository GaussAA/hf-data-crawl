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

from hf_data_crawl.hf.api import IDENTITY_EXPAND, Page
from hf_data_crawl.hf.scan import ScanRun, ScanSpec
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
    assert len(list((store.raw_dir / f"dt={DAY.isoformat()}").glob("*.json.gz"))) == 2


def test_跳过计数被累计(store: DatasetStore) -> None:
    api = FakeScanApi([page("a", skipped=3), page("b", cursor=None, skipped=1)])

    summary = ScanRun(api, store).run(DAY, spec())

    assert summary.skipped == 4


def test_停止原因走到末页时为end(store: DatasetStore) -> None:
    api = FakeScanApi([page("a", cursor=None)])

    assert ScanRun(api, store).run(DAY, spec()).stopped_reason == "end"


def test_dry_run不写盘(store: DatasetStore) -> None:
    api = FakeScanApi([page("a", cursor=None)])

    ScanRun(api, store).run(DAY, spec(write=False))

    partition = store.raw_dir / f"dt={DAY.isoformat()}"
    assert not partition.exists() or not list(partition.glob("*.json.gz"))


def test_label与mode落进raw的meta(store: DatasetStore) -> None:
    import gzip
    import json

    api = FakeScanApi([page("a", cursor=None)])

    ScanRun(api, store).run(DAY, spec(label="top", mode="top-refresh"))

    path = next((store.raw_dir / f"dt={DAY.isoformat()}").glob("top-*.json.gz"))
    meta = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))["meta"]
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


def test_不把页数上限交给生成器(store: DatasetStore) -> None:
    """交给生成器会得到错误的停止原因，且钩子看不到最后一页之后的状态。"""
    api = FakeScanApi([page("a", cursor=None)])

    ScanRun(api, store).run(DAY, spec(max_pages=5))

    assert "max_pages" not in api.kwargs
