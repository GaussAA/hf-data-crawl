"""S6 · 从 Raw 重建派生层的行为测试。

核心不变式：重建结果必须与「逐日正常跑」逐字节等价，否则重建就等于造了一份新数据。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.derive import derive_day
from hf_data_crawl.rebuild import discover_raw_dates, rebuild_from_raw
from hf_data_crawl.store import DatasetStore

DAY1 = date(2026, 11, 1)
DAY2 = date(2026, 11, 2)


def record(index: int, downloads: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 2,
        "likes": 1,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en", "license:mit", "task_categories:text-classification"],
    }


def seed(store: DatasetStore, snapshot: date, records: list[dict[str, Any]]) -> None:
    store.write_raw(snapshot_date=snapshot, page_index=1, records=records, meta={})
    derive_day(store, snapshot)


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    seed(instance, DAY1, [record(1, 100), record(2, 50)])
    seed(instance, DAY2, [record(1, 180), record(2, 50)])
    return instance


def test_发现的日期按升序排列(store: DatasetStore) -> None:
    assert discover_raw_dates(store) == [DAY1, DAY2]


def test_重建后的current与原结果完全一致(store: DatasetStore) -> None:
    expected_current = store.read_current()
    expected_obs = store.read_observations()

    summary = rebuild_from_raw(store)

    assert summary.dates == [DAY1, DAY2]
    assert store.read_current().equals(expected_current)
    assert store.read_observations().equals(expected_obs)


def test_派生层被删光也能重建(store: DatasetStore) -> None:
    import shutil

    # 分区表的路径是 <表目录>/dt=<日期>/<文件>，往上两级就是整张表的目录。
    for name in ("bronze", "silver"):
        shutil.rmtree(store.path(name, DAY1).parents[1])
    shutil.rmtree(store.state_dir)

    summary = rebuild_from_raw(store)

    assert summary.current_rows == 2
    # Silver 是属性变更流水：两天属性没变，只有首日的两行。
    assert store.read_all("silver").height == 2
    # 观测只有 ds-1 在 DAY2 变了，因此两条
    assert store.read_observations().height == 3


def test_无raw时返回空结果且不抛错(tmp_path: Path) -> None:
    summary = rebuild_from_raw(DatasetStore(tmp_path / "empty"))

    assert summary.dates == []
    assert summary.current_rows == 0


def test_不规范的raw目录名被忽略(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    (store.raw_dir / "dt=garbage").mkdir(parents=True)

    assert discover_raw_dates(store) == []


def test_重建不清除规模表(store: DatasetStore) -> None:
    """规模表来自 datasets-server，raw 里没有对应数据，删掉就白丢一轮抓取。

    2026-11-04 实测踩过：build 把 enrich 整个删掉，两万条规模数据（约 13 分钟）
    无声消失，页面上的「规模覆盖」直接归零。
    """
    import polars as pl

    from hf_data_crawl.sizes import SIZE_SCHEMA

    store.merge_sizes(
        pl.DataFrame(
            [
                {
                    "id": "acme/ds-1",
                    "snapshot_date": DAY2,
                    "status": "ok",
                    "num_rows": 42,
                    "estimated_num_rows": None,
                    "num_bytes_original": 10,
                    "num_bytes_parquet": 9,
                    "num_bytes_memory": 8,
                    "config_count": 1,
                    "num_columns_max": 3,
                    "configs": None,
                }
            ],
            schema=SIZE_SCHEMA,
        )
    )

    rebuild_from_raw(store)

    kept = store.read_sizes()
    assert kept.height == 1
    assert kept.row(0, named=True)["num_rows"] == 42


def test_重建会重算卡片表(store: DatasetStore) -> None:
    """卡片表可由 raw 重算，规则一改就该整体重来，旧的必须被清掉。"""
    store.write_raw(
        snapshot_date=DAY2,
        page_index=1,
        records=[{"id": "acme/ds-1", "cardData": {"pretty_name": "正式名"}}],
        meta={},
        label="card",
    )

    rebuild_from_raw(store)

    assert store.read_enrich().height == 1
    assert store.read_enrich().row(0, named=True)["pretty_name"] == "正式名"


def test_重建保留对账探测出的失效状态(store: DatasetStore) -> None:
    """失效状态来自单库探测，不在 raw 里，重建不得抹掉。

    2026-11-05 实测踩到：463 条 restricted / renamed 在一次 build 后全部变回
    active，页面上「已失效」直接归零，且不报任何错。
    """
    store.mark_status({"acme/ds-2": "restricted"})

    rebuild_from_raw(store)

    assert status_of(store, "acme/ds-2") == "restricted"
    assert status_of(store, "acme/ds-1") == "active"


def test_重建后新增数据仍为在架(store: DatasetStore) -> None:
    """留存失效状态不等于把所有库钉死在旧状态。"""
    store.mark_status({"acme/ds-1": "renamed"})

    rebuild_from_raw(store)

    assert status_of(store, "acme/ds-1") == "renamed"
    assert store.read_current().filter(pl.col("status") == "active").height == 1


def status_of(store: DatasetStore, dataset_id: str) -> str:
    frame = store.read_current().filter(pl.col("id") == dataset_id)
    return frame.row(0, named=True)["status"]


def test_上次重建中断时仍能从快照恢复状态(store: DatasetStore) -> None:
    """rebuild 不是事务性的：清除 state 与写回状态之间被打断，判断就丢了。

    2026-11-05 实测：一次 build 被 SIGTERM 杀掉，恰好死在两者之间，
    463 条 restricted / renamed 全部归零。快照让下一次 build 还能接上。
    """
    import shutil

    from hf_data_crawl.status import retain

    store.mark_status({"acme/ds-2": "restricted"})
    retain(store)  # 重建的第一步：快照落盘
    # 模拟中断现场：快照已落盘，state 已被清空，状态还没写回
    shutil.rmtree(store.state_dir)

    rebuild_from_raw(store)

    assert status_of(store, "acme/ds-2") == "restricted"


def test_重建成功后快照被删除(store: DatasetStore) -> None:
    """快照只表示「重建进行中」；留着会被下一次重建当成「上次被打断」。"""
    store.mark_status({"acme/ds-1": "renamed"})

    rebuild_from_raw(store)

    assert not (store.root / "meta" / "failed-status.json").exists()
    assert status_of(store, "acme/ds-1") == "renamed"


def test_重建后已恢复的库不会被旧快照改回去(store: DatasetStore) -> None:
    store.mark_status({"acme/ds-2": "restricted"})
    rebuild_from_raw(store)
    store.mark_status({"acme/ds-2": "active"})  # 之后对账把它恢复了

    rebuild_from_raw(store)

    assert status_of(store, "acme/ds-2") == "active"
