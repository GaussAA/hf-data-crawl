"""补抓批次补进运行态的契约测试（2026-11-08）。

背景：`bronze.build` 只读 `part` / `top`（当日扫描），卡片批次（`card-*`，enrich
补抓）按设计不进标量快照。2026-11-08 首次全量重放时发现：有库**只**出现在卡片
批次里，于是 `current` 有、raw 重放不出来（实测 27 行）。

第一版修法把补抓批次并进 Bronze，实测行数从 233 万涨到 338 万（+45%），而真正
只缺 203 个库——正是 `bronze.build` docstring 警告的膨胀。现在的规则：**Bronze 保持
纯净，补位置移到状态层**，只在 id 未出现时补，不抢扫描批次的胜负。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import bronze, raw, state
from hf_data_crawl.raw import BRONZE_LABELS, CARD_LABEL
from hf_data_crawl.store import DatasetStore

DAY = date(2026, 11, 7)


def record(dataset_id: str, downloads: int = 1, description: str = "d") -> dict[str, Any]:
    return {
        "id": dataset_id,
        "author": dataset_id.split("/")[0],
        "sha": f"sha-{dataset_id}",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "downloads": downloads,
        "downloadsAllTime": downloads * 10,
        "likes": 0,
        "gated": False,
        "private": False,
        "description": description,
        "tags": [],
    }


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def write(store: DatasetStore, *records: dict[str, Any], label: str = "part") -> None:
    store.write_raw(snapshot_date=DAY, page_index=1, records=list(records), meta={}, label=label)


def test_只在补抓批次出现的库补进current(store: DatasetStore) -> None:
    """这类库确实被观察过；不管它，全量重放就会比增量少行。"""
    write(store, record("acme/in-part"))
    write(store, record("acme/only-card"), label=CARD_LABEL)
    bronze.build(store, DAY)

    state.update(store, DAY)

    assert set(store.read_current()["id"].to_list()) == {"acme/in-part", "acme/only-card"}
    assert store.read_current().height == 2


def test_bronze仍只收扫描批次(store: DatasetStore) -> None:
    """补位置在状态层，Bronze 若被顺带撑大就违背了它的口径。"""
    write(store, record("acme/in-part"))
    write(store, record("acme/only-card"), label=CARD_LABEL)

    assert bronze.build(store, DAY) == 1
    assert store.read_bronze(DAY)["id"].to_list() == ["acme/in-part"]


def test_重复id仍以扫描批次为准(store: DatasetStore) -> None:
    """补抓批次内容可能更新，但它不是当日扫描，不许覆盖扫描得到的值。"""
    write(store, record("acme/ds", downloads=100, description="from-part"))
    write(store, record("acme/ds", downloads=999, description="from-card"), label=CARD_LABEL)
    bronze.build(store, DAY)

    state.update(store, DAY)

    row = store.read_current().row(0, named=True)
    assert row["downloads"] == 100
    assert row["downloads_all_time"] == 1000, "补抓批次的 9990 不许盖掉扫描值"


def test_只有补抓批次时也能建出current(store: DatasetStore) -> None:
    """极端情形：某天只跑了 enrich 没跑扫描，仍不该得到空底库。"""
    write(store, record("acme/only-card"), label=CARD_LABEL)

    state.update(store, DAY)

    assert store.read_current()["id"].to_list() == ["acme/only-card"]


def test_补进来的库也落一条观测(store: DatasetStore) -> None:
    """不落观测，这个库就永远没有指标历史，榜单与增速都看不到它。"""
    write(store, record("acme/only-card", downloads=7), label=CARD_LABEL)

    state.update(store, DAY)

    observations = store.read_observations()
    assert observations["id"].to_list() == ["acme/only-card"]
    assert observations["downloads"].to_list() == [7]


def test_没有补抓批次时行为不变(store: DatasetStore) -> None:
    write(store, record("acme/a"), record("acme/b"))
    bronze.build(store, DAY)

    state.update(store, DAY)

    assert store.read_current().height == 2


def test_补充分块与主批次分开取(store: DatasetStore) -> None:
    write(store, record("acme/a"))
    write(store, record("acme/b"), label=CARD_LABEL)

    assert [p.name.split("-")[0] for p in raw.bronze_files(store, DAY)] == ["part"]
    assert [p.name.split("-")[0] for p in raw.supplement_files(store, DAY)] == ["card"]


def test_卡片批次仍不进bronze去重池() -> None:
    """补位置是「按 id 补空位」，不是把 card 升格为同级来源——别顺手改这个常量。"""
    assert CARD_LABEL not in BRONZE_LABELS
