"""`raw.DayRecords` 的契约：当天已有的内容不再落第二份，但一行都不能丢。

为什么值得单独测：这是全项目唯一「少写数据」的地方。判宽了白占磁盘（现状：
每天多存 165 MB 的重复全站），判窄了会把「内容变了」当成「已有」而真的丢数据
——后者不会报错，只会在某天重放时发现某列悄悄回到了旧值。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

from hf_data_crawl.pipeline import raw
from hf_data_crawl.platform.store import DatasetStore

DAY = date(2026, 11, 21)


def store_at(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def write_page(store: DatasetStore, records: list[dict[str, Any]], *, page: int = 1) -> Path:
    return raw.write(
        store,
        snapshot_date=DAY,
        page_index=page,
        records=records,
        meta={"mode": "lean", "sort": "lastModified"},
        source="lean_lastModified",
    )


# ---------------------------------------------------------------- 指纹


def test_指纹与键序无关() -> None:
    """上游同一份记录的键序不保证稳定；键序变了不该被当成新内容。"""
    assert raw.fingerprint({"id": "a", "x": 1, "y": 2}) == raw.fingerprint(
        {"y": 2, "id": "a", "x": 1}
    )


def test_内容不同则指纹不同() -> None:
    """只按 id 判重会把「内容变了」当「已有」——这是最危险的判宽。"""
    assert raw.fingerprint({"id": "a", "downloads": 1}) != raw.fingerprint(
        {"id": "a", "downloads": 2}
    )


def test_指纹长度固定() -> None:
    assert len(raw.fingerprint({"id": "a"})) == raw.FP_BYTES


# ---------------------------------------------------------------- 载入


def test_载入当天已落分块(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    records = [{"id": "a"}, {"id": "b"}]
    write_page(store, records)

    keys = raw.DayRecords.load(store, DAY)

    assert len(keys) == 2
    assert keys.unseen(records) == []


def test_当天没有分块时是空集合(tmp_path: Path) -> None:
    """单独跑 reconcile、collect 未跑：去重必须退化成「全部照落」。"""
    store = store_at(tmp_path)

    keys = raw.DayRecords.load(store, DAY)

    assert len(keys) == 0
    assert keys.unseen([{"id": "a"}]) == [{"id": "a"}]


def test_只载入指定标签的分块(tmp_path: Path) -> None:
    """`part` 与 `card` 的字段集不同，混在一起会把「多带字段的同一批」也算成已有。"""
    store = store_at(tmp_path)
    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[{"id": "a"}],
        label="card",
        source="card_refresh_lastModified",
    )

    assert len(raw.DayRecords.load(store, DAY, label="part")) == 0
    assert len(raw.DayRecords.load(store, DAY, label="card")) == 1


# ---------------------------------------------------------------- 判定


def test_未见过的记录保持原序返回(tmp_path: Path) -> None:
    store = store_at(tmp_path)
    write_page(store, [{"id": "b"}])

    fresh = raw.DayRecords.load(store, DAY).unseen([{"id": "a"}, {"id": "b"}, {"id": "c"}])

    assert [item["id"] for item in fresh] == ["a", "c"], "顺序参与 written_at 之外的派生稳定性"


def test_记下刚写盘的记录后不再重复返回(tmp_path: Path) -> None:
    keys = raw.DayRecords()
    keys.remember([{"id": "a"}])

    assert keys.unseen([{"id": "a"}]) == []


def test_载入后再写盘不影响已建索引(tmp_path: Path) -> None:
    """索引是快照：扫描中途由 ScanRun 自己 `remember` 新落的记录，不靠重读磁盘。"""
    store = store_at(tmp_path)
    keys = raw.DayRecords.load(store, DAY)
    write_page(store, [{"id": "a"}])

    assert keys.unseen([{"id": "a"}]) == [{"id": "a"}]
