"""Schema 与请求清单的契约测试。

存在的理由（2026-11-04 实测事故）：
HF 的 `expand[]` 是**白名单裁剪**——传了它就只返回列出的字段，连默认响应里
本就有的 `private` / `disabled` 都会被裁掉。而本项目里「表声明的列」和「请求
的字段」是两份独立维护的清单，两者一旦漂移，整列会静默变空：

    private 列已写入 BRONZE_SCHEMA，LEAN_EXPAND 却没请求它
    → 1,115,078 行 100% 为 null，管线照常报 success。

这类缺陷不会抛错、不会让测试变红（除非专门断言），只能靠契约测试钉死。
因此本文件把 BRONZE_SOURCE_FIELDS 作为唯一真相源，向两侧同时校验。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import bronze
from hf_data_crawl.api import EXPAND_WHITELIST
from hf_data_crawl.collect import LEAN_EXPAND
from hf_data_crawl.enrich import ENRICH_EXPAND
from hf_data_crawl.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS
from hf_data_crawl.store import DatasetStore
from hf_data_crawl.tables import (
    BRONZE_DERIVED_COLUMNS,
    BRONZE_SCHEMA,
    BRONZE_SOURCE_FIELDS,
    CURRENT_SCHEMA,
    SILVER_SCHEMA,
    UNCONDITIONAL_FIELDS,
)

DAY = date(2026, 11, 4)


# ---------------------------------------------------------------- 清单互洽


def test_bronze每一列都有明确的来源() -> None:
    """列集合必须恰好等于「源字段映射 + 本地派生列」，不许有来历不明的列。"""
    assert set(BRONZE_SCHEMA) == set(BRONZE_SOURCE_FIELDS) | set(BRONZE_DERIVED_COLUMNS)


def test_bronze的源字段必须全部写进lean请求() -> None:
    """核心断言：声明要存的列，请求里必须真的带上。

    这是 private 整列为空那次事故的回归护栏。
    """
    required = set(BRONZE_SOURCE_FIELDS.values()) - UNCONDITIONAL_FIELDS
    missing = required - set(LEAN_EXPAND)

    assert not missing, (
        f"这些字段在 BRONZE_SCHEMA 中有列，却没写进 LEAN_EXPAND，整列将恒为空：{sorted(missing)}"
    )


def test_lean请求不越出接口白名单() -> None:
    """白名单外的字段会让接口直接 400，必须在提交前拦住。"""
    assert set(LEAN_EXPAND) <= set(EXPAND_WHITELIST)


def test_enrich请求不越出接口白名单() -> None:
    assert set(ENRICH_EXPAND) <= set(EXPAND_WHITELIST)


def test_enrich请求是lean的超集() -> None:
    """两档一旦分叉，同日既有 part 又有 card 分区时，Bronze 去重可能
    让缺字段的那份胜出，导致整列为空。故强制 enrich ⊇ lean。"""
    assert set(LEAN_EXPAND) <= set(ENRICH_EXPAND)


def test_silver每一列都能由bronze或标签维度得到() -> None:
    """Silver 是 Bronze 的纯派生层，不许引入无来源的列。"""
    derivable = set(BRONZE_SCHEMA) | set(TAG_DIMENSIONS.values()) | {PLAIN_TAG_COLUMN}
    missing = set(SILVER_SCHEMA) - derivable

    assert not missing, f"Silver 有列在 Bronze 与标签维度里都找不到来源：{sorted(missing)}"


def test_每个标签维度都在silver里有对应列() -> None:
    """TAG_DIMENSIONS 加了映射却不加列，拆分结果会静默丢失。"""
    missing = set(TAG_DIMENSIONS.values()) - set(SILVER_SCHEMA)

    assert not missing, f"这些标签维度没有对应的 Silver 列：{sorted(missing)}"


def test_current每一列都能由bronze或本地计算得到() -> None:
    """current 取当日 Bronze 的标量状态（含指标），Silver 只记属性变更、不含指标。"""
    computed = {"first_seen_at", "last_seen_at", "status"}
    missing = set(CURRENT_SCHEMA) - (set(BRONZE_SCHEMA) | computed)

    assert not missing, f"Current 有列既不在 Bronze 也非本地计算：{sorted(missing)}"


def test_silver不含指标列() -> None:
    """指标每天约一半的行在变，历史归 observations；放进 Silver 会重复存近乎全量快照。"""
    from hf_data_crawl.state import METRIC_COLUMNS

    assert set(METRIC_COLUMNS).isdisjoint(SILVER_SCHEMA)


# ---------------------------------------------------------------- 映射落地


def test_to_row产出的键与bronze_schema完全一致() -> None:
    """映射表改了但 _to_row 没跟上（或反之），会在这里立刻暴露。"""
    record = {
        "id": "acme/ds-1",
        "author": "acme",
        "sha": "sha-1",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "downloads": 1,
        "downloadsAllTime": 2,
        "likes": 3,
        "trendingScore": 4,
        "gated": False,
        "private": False,
        "disabled": False,
        "mainSize": 5,
        "xetEnabled": True,
        "resourceGroup": None,
        "tags": [],
        "description": "x",
    }

    row = bronze.to_row(record, DAY, "part-1.json.gz")

    assert row is not None
    assert set(row) == set(BRONZE_SCHEMA)


# ---------------------------------------------------------------- 端到端


def _full_record() -> dict[str, Any]:
    """一条字段全满的记录，覆盖 HF 列表接口 20 个白名单字段里的可批量部分。"""
    return {
        "id": "acme/full",
        "author": "acme",
        "sha": "dce01c9b08f87459cf36a430d809084718273017",
        "createdAt": "2025-03-13T17:19:43.000Z",
        "lastModified": "2026-10-22T20:33:36.000Z",
        "downloads": 81780,
        "downloadsAllTime": 1_234_567,
        "likes": 1389,
        "trendingScore": 483,
        "gated": "manual",
        "private": False,
        "disabled": False,
        "mainSize": 24_256_428,
        "xetEnabled": True,
        "resourceGroup": {"id": "rg-1", "name": "默认"},
        "tags": ["language:en", "format:parquet"],
        "description": "演示数据集",
    }


def test_全字段记录落库后没有一列是空的(tmp_path: Path) -> None:
    """最强的一条：不只是「声明对得上」，而是「抓得到、存得下」。

    任何一列变空，用户拿到的就是一张看起来正常、实际缺失的宽表。
    """
    store = DatasetStore(tmp_path / "data")
    store.write_raw(snapshot_date=DAY, page_index=1, records=[_full_record()], meta={})

    assert bronze.build(store, DAY) == 1
    row = store.read_bronze(DAY).row(0, named=True)

    empty = [name for name, value in row.items() if value is None and name != "raw"]
    assert not empty, f"下列列虽然声明了却没有值：{empty}"

    assert row["main_size"] == 24_256_428
    assert row["trending_score"] == 483.0
    assert row["private"] is False
    assert row["disabled"] is False
    assert row["xet_enabled"] is True


def test_结构化的resource_group存成可再解析的json(tmp_path: Path) -> None:
    """直接 str() 会得到单引号的 Python 字面量，之后再 json.loads 必然失败。"""
    store = DatasetStore(tmp_path / "data")
    store.write_raw(snapshot_date=DAY, page_index=1, records=[_full_record()], meta={})
    bronze.build(store, DAY)

    raw_value = store.read_bronze(DAY).row(0, named=True)["resource_group"]

    assert json.loads(raw_value) == {"id": "rg-1", "name": "默认"}


def test_类型不符时留空而不是猜(tmp_path: Path) -> None:
    """字段类型异常（如 private 给成字符串）宁可留空，也不许静默转成 True。"""
    store = DatasetStore(tmp_path / "data")
    record = _full_record() | {"private": "true", "mainSize": "很大", "trendingScore": None}
    store.write_raw(snapshot_date=DAY, page_index=1, records=[record], meta={})
    bronze.build(store, DAY)

    row = store.read_bronze(DAY).row(0, named=True)

    assert row["private"] is None
    assert row["main_size"] is None
    assert row["trending_score"] is None


def test_旧分区缺列时读出来补空而不是报错(tmp_path: Path) -> None:
    """扩列后旧分区只有老列，读取必须补空；否则整个库一升级就打不开。"""
    import polars as pl

    store = DatasetStore(tmp_path / "data")
    target = store.path("bronze", DAY)
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": ["acme/old"], "downloads": [1], "snapshot_date": [DAY]}).write_parquet(
        target
    )

    frame = store.read_bronze(DAY)

    assert set(frame.columns) == set(BRONZE_SCHEMA)
    assert frame.row(0, named=True)["main_size"] is None


@pytest.mark.parametrize("column", ["private", "disabled", "main_size", "trending_score"])
def test_事故列不再恒空(column: str, tmp_path: Path) -> None:
    """逐列点名：这四列是本次补齐的核心，任何一列回到全空都算回归。"""
    store = DatasetStore(tmp_path / "data")
    store.write_raw(snapshot_date=DAY, page_index=1, records=[_full_record()], meta={})
    bronze.build(store, DAY)

    assert store.read_bronze(DAY)[column].null_count() == 0


# ---------------------------------------------------------------- 最新一行的两种实现


def test_current的带入列由schema求交而来() -> None:
    """手抄的子集一定会漏，而漏掉的表现是「视图有、物化表是 null」，不报错。"""
    from hf_data_crawl.state import CARRIED_COLUMNS, COMPUTED_COLUMNS

    expected = set(CURRENT_SCHEMA) - COMPUTED_COLUMNS

    assert set(CARRIED_COLUMNS) == expected
    assert set(CURRENT_SCHEMA) == set(CARRIED_COLUMNS) | COMPUTED_COLUMNS


def test_current的计算列不出现在silver里() -> None:
    """status / first_seen_at / last_seen_at 是本地判断的结果，不属于源数据。

    若把它们混进 Silver，就会出现两个 status 各说各话的局面。
    """
    from hf_data_crawl.state import COMPUTED_COLUMNS

    assert COMPUTED_COLUMNS.isdisjoint(SILVER_SCHEMA)
