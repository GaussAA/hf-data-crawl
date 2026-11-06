"""推导一天的入口：固定顺序、结果齐全、空日不提前返回。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import bronze, card, derive, silver, state
from hf_data_crawl.derive import derive_day
from hf_data_crawl.raw import CARD_LABEL
from hf_data_crawl.store import DatasetStore

DAY = date(2026, 11, 3)


def record(index: int, *, with_card: bool = False) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": 10 * index,
        "downloadsAllTime": 20 * index,
        "likes": index,
        "private": False,
        "description": "d",
        "tags": ["language:en"],
    }
    if with_card:
        payload["cardData"] = {"pretty_name": f"名{index}", "license": "mit"}
    return payload


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def test_有raw时各层都有数据且结果对得上(store: DatasetStore) -> None:
    store.write_raw(snapshot_date=DAY, page_index=1, records=[record(1), record(2)], meta={})

    result = derive_day(store, DAY)

    assert result.bronze_rows == 2
    assert result.silver_rows == 2
    assert result.state.current_rows == 2
    assert result.state.appended == 2
    assert store.read_bronze(DAY).height == 2
    assert store.read_silver(DAY).height == 2
    assert store.read_observations().height == 2
    assert store.read_current().height == 2


def test_不跑卡片时卡片行数是None而不是0(store: DatasetStore) -> None:
    store.write_raw(snapshot_date=DAY, page_index=1, records=[record(1)], meta={})

    result = derive_day(store, DAY)

    assert result.card_rows is None
    assert result.config_rows is None
    assert not store.has("carddata")


def test_enrich时产出卡片表(store: DatasetStore) -> None:
    store.write_raw(
        snapshot_date=DAY,
        page_index=1,
        records=[record(1, with_card=True)],
        meta={},
        label=CARD_LABEL,
    )

    result = derive_day(store, DAY, enrich=True)

    assert result.card_rows == 1
    assert result.config_rows == 0
    assert store.read_enrich().height == 1


def test_调用次序固定为bronze_silver_card_state(
    store: DatasetStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[str] = []

    def spy(module: Any, name: str, label: str) -> None:
        original = getattr(module, name)

        def wrapper(*args: Any, **kwargs: Any) -> Any:
            calls.append(label)
            return original(*args, **kwargs)

        monkeypatch.setattr(module, name, wrapper)

    spy(bronze, "build", "bronze")
    spy(silver, "build", "silver")
    spy(card, "build", "card")
    spy(state, "update", "state")

    derive_day(store, DAY, enrich=True)
    assert calls == ["bronze", "silver", "card", "state"]

    calls.clear()
    derive_day(store, DAY)
    assert calls == ["bronze", "silver", "state"]


def test_空日不提前返回且保留现有当前态行数(store: DatasetStore) -> None:
    store.write_raw(snapshot_date=DAY, page_index=1, records=[record(1), record(2)], meta={})
    derive_day(store, DAY)

    empty_day = date(2026, 11, 4)
    result = derive_day(store, empty_day)

    assert result.bronze_rows == 0
    assert result.silver_rows == 0
    assert result.state.appended == 0
    assert result.state.current_rows == 2


def test_入口模块直接引用各步而非store转发() -> None:
    """删除 store 上的转发方法时，推导入口不应受影响。"""
    assert derive.bronze is bronze
    assert derive.silver is silver
    assert derive.card is card
    assert derive.state is state


def test_只保留最新一天的bronze且旧分区可重建(store: DatasetStore) -> None:
    day1, day2 = DAY, date(2026, 11, 4)
    store.write_raw(snapshot_date=day1, page_index=1, records=[record(1), record(2)], meta={})
    store.write_raw(snapshot_date=day2, page_index=1, records=[record(1)], meta={})

    derive_day(store, day1)
    derive_day(store, day2)

    assert not store.has("bronze", day1)
    assert store.has("bronze", day2)
    # 旧分区从 Raw 秒级可得，内容与当初一致
    assert bronze.build(store, day1) == 2


def test_日增量只覆盖部分行时current不丢其余行(store: DatasetStore) -> None:
    """Bronze 只留最新一天后，当天只含部分行；其余数据集必须靠 current 保住。"""
    day2 = date(2026, 11, 4)
    store.write_raw(snapshot_date=DAY, page_index=1, records=[record(1), record(2)], meta={})
    store.write_raw(snapshot_date=day2, page_index=1, records=[record(1)], meta={})

    derive_day(store, DAY)
    derive_day(store, day2)

    assert sorted(store.read_current()["id"].to_list()) == ["acme/ds-1", "acme/ds-2"]
