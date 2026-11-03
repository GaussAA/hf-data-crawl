"""S7 · Gold 视图与 query 的行为测试。

金标值一律写死字面量，不用被测代码的算法反推，否则测试只是自证。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl.gold import GoldCatalog
from hf_data_crawl.store import DatasetStore

DAY1 = date(2026, 11, 1)
DAY4 = date(2026, 11, 4)
DAY5 = date(2026, 11, 5)


def record(
    index: int,
    *,
    downloads: int,
    licenses: list[str] | None = None,
    languages: list[str] | None = None,
    gated: bool = False,
) -> dict[str, Any]:
    tags = [f"license:{item}" for item in (licenses or ["mit"])]
    tags += [f"language:{item}" for item in (languages or ["en"])]
    tags.append("task_categories:text-classification")
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 10,
        "likes": 1,
        "gated": gated,
        "private": False,
        "description": "d",
        "tags": tags,
    }


def seed(store: DatasetStore, snapshot: date, records: list[dict[str, Any]]) -> None:
    store.write_raw(snapshot_date=snapshot, page_index=1, records=records, meta={})
    store.build_bronze(snapshot)
    store.build_silver(snapshot)
    store.update_state(snapshot)


@pytest.fixture()
def catalog(tmp_path: Path) -> GoldCatalog:
    store = DatasetStore(tmp_path / "data")
    seed(
        store,
        DAY1,
        [
            record(1, downloads=100, licenses=["mit"]),
            record(2, downloads=50, licenses=["apache-2.0"], languages=["zh", "en"]),
            record(3, downloads=1000, licenses=["gpl-3.0"], gated=True),
        ],
    )
    seed(
        store,
        DAY4,
        [
            record(1, downloads=260, licenses=["mit"]),
            record(2, downloads=60, licenses=["apache-2.0"], languages=["zh", "en"]),
        ],
    )
    return GoldCatalog(store)


def test_增速榜只计两个及以上观测点(catalog: GoldCatalog) -> None:
    rows = catalog.board("growth", limit=50)

    assert rows["id"].to_list() == ["acme/ds-1", "acme/ds-2"]


def test_增速数值与金标一致(catalog: GoldCatalog) -> None:
    rows = catalog.board("growth", limit=50)

    assert rows["growth"].to_list() == [160, 10]
    assert rows["growth_pct"].to_list() == [160.0, 20.0]


def test_当前态榜按三十天下载降序(catalog: GoldCatalog) -> None:
    rows = catalog.board("downloads", limit=2)

    assert rows["id"].to_list() == ["acme/ds-3", "acme/ds-1"]


def test_累计榜按累计下载降序(catalog: GoldCatalog) -> None:
    rows = catalog.board("alltime", limit=1)

    assert rows["id"].to_list() == ["acme/ds-3"]


def test_可按许可证与gated过滤(catalog: GoldCatalog) -> None:
    mit_rows = catalog.run("SELECT id FROM datasets WHERE list_contains(licenses, 'mit')")
    gated_rows = catalog.run("SELECT id FROM datasets WHERE gated = 'true'")

    assert mit_rows["id"].to_list() == ["acme/ds-1"]
    assert gated_rows["id"].to_list() == ["acme/ds-3"]


def test_分类榜按任务类别聚合(catalog: GoldCatalog) -> None:
    rows = catalog.run(
        "SELECT unnest(task_categories) AS category, count(*) AS n FROM datasets GROUP BY 1"
    )

    assert rows.height >= 1


def test_新增数据在同一连接内即时可见(catalog: GoldCatalog) -> None:
    """视图读的是 parquet 文件，非物化副本：补数后无需重建视图即可查到。"""
    before = catalog.board("growth", limit=50)
    assert before.filter(before["id"] == "acme/ds-1")["growth"].to_list() == [160]

    seed(catalog.store, DAY5, [record(1, downloads=300, licenses=["mit"])])

    after = catalog.board("growth", limit=50)
    assert after.filter(after["id"] == "acme/ds-1")["growth"].to_list() == [200]


def test_无数据时给出可读错误(tmp_path: Path) -> None:
    from hf_data_crawl.gold import GoldNotReady

    with pytest.raises(GoldNotReady, match="collect"):
        GoldCatalog(DatasetStore(tmp_path / "empty"))
