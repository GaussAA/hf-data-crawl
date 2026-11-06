"""Bronze 的并行路径：与串行结果逐行相同，且不改变「同 id 以最后写入者为准」。

测试里的分块数远不到进程池阈值，默认只会走串行；这里把阈值降到 1 强制走并行。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import bronze
from hf_data_crawl.store import DatasetStore

DAY = date(2026, 11, 3)


def record(index: int, downloads: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "sha": f"sha-{index}",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "downloads": downloads,
        "downloadsAllTime": downloads * 2,
        "likes": 1,
        "private": False,
        "description": "d",
        "tags": ["language:en", "任意前缀:值"],
    }


def seed(store: DatasetStore) -> None:
    # 多页、跨页同 id：后写入的页必须胜出。
    for page in range(1, 6):
        store.write_raw(
            snapshot_date=DAY,
            page_index=page,
            records=[record(index, downloads=page * 10) for index in range(1, 4)],
            meta={},
            written_at=f"2026-11-03T00:00:0{page}.000000+00:00",
        )


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def test_并行与串行结果逐行相同(store: DatasetStore, monkeypatch: pytest.MonkeyPatch) -> None:
    seed(store)

    monkeypatch.setattr(bronze, "PARALLEL_MIN_FILES", 10_000)
    bronze.build(store, DAY)
    serial = store.read_bronze(DAY).sort("id")

    monkeypatch.setattr(bronze, "PARALLEL_MIN_FILES", 1)
    bronze.build(store, DAY)
    parallel = store.read_bronze(DAY).sort("id")

    assert parallel.height == 3
    assert parallel.equals(serial)


def test_并行时同id仍以最后写入者为准(store: DatasetStore, monkeypatch: pytest.MonkeyPatch) -> None:
    seed(store)
    monkeypatch.setattr(bronze, "PARALLEL_MIN_FILES", 1)

    bronze.build(store, DAY)

    assert set(store.read_bronze(DAY)["downloads"].to_list()) == {50}
