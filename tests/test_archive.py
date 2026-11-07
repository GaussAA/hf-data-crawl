"""Raw 归档：合并小分块并去重，但不改变任何一条记录的胜负。"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import archive, bronze, raw
from hf_data_crawl.archive import ArchiveMismatch, archive_day
from hf_data_crawl.cli import main
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
        "tags": ["language:en"],
    }


def seed(store: DatasetStore) -> None:
    """五页，同一批 id 反复出现：后写入的页必须胜出；另有 top 与 card 标签。"""
    for page in range(1, 6):
        store.write_raw(
            snapshot_date=DAY,
            page_index=page,
            records=[record(index, downloads=page * 10) for index in range(1, 4)]
            + [record(100 + page, downloads=1)],
            meta={},
            written_at=f"2026-11-03T00:00:0{page}.000000+00:00",
        )
    store.write_raw(
        snapshot_date=DAY,
        page_index=1,
        records=[record(1, downloads=999)],
        meta={},
        label="top",
        written_at="2026-11-03T01:00:00.000000+00:00",
    )
    store.write_raw(
        snapshot_date=DAY,
        page_index=1,
        records=[{**record(1, downloads=5), "cardData": {"pretty_name": "A"}}],
        meta={},
        label="card",
        written_at="2026-11-03T02:00:00.000000+00:00",
    )


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    seed(instance)
    return instance


def bronze_of(store: DatasetStore):
    bronze.build(store, DAY)
    return store.read_bronze(DAY).sort("id").drop("page_file")


def test_归档前后bronze逐行相等(store: DatasetStore) -> None:
    before = bronze_of(store)

    result = archive_day(store, DAY)

    assert result is not None
    assert bronze_of(store).equals(before)


def test_同id的胜负不变且重复记录被去掉(store: DatasetStore) -> None:
    def downloads(frame: Any, dataset_id: str) -> list[int]:
        return frame.filter(frame["id"] == dataset_id)["downloads"].to_list()

    before = bronze_of(store)
    # bronze.build 只读 part / top：ds-1 最后写入的是 01:00 的 top（999）；
    # 02:00 的 card 批次属于卡片表，不参与 Bronze；ds-2 最后写入的是第 5 页的 part（50）。
    assert downloads(before, "acme/ds-1") == [999]
    assert downloads(before, "acme/ds-2") == [50]

    result = archive_day(store, DAY)

    assert result is not None
    assert result.records_after < result.records_before
    after = bronze_of(store)
    assert downloads(after, "acme/ds-1") == [999]
    assert downloads(after, "acme/ds-2") == [50]


def test_原文件移入备份目录而不是删除(store: DatasetStore) -> None:
    sources = raw.batch_files(store, DAY)

    result = archive_day(store, DAY)

    assert result is not None and result.backup_dir is not None
    backed_up = sorted(path.name for path in result.backup_dir.iterdir())
    assert backed_up == sorted(path.name for path in sources)
    assert all(raw.is_archive(path) for path in raw.batch_files(store, DAY))


def test_重复归档是空操作(store: DatasetStore) -> None:
    assert archive_day(store, DAY) is not None

    assert archive_day(store, DAY) is None


def test_没有raw的日期返回None(store: DatasetStore) -> None:
    assert archive_day(store, date(2026, 1, 1)) is None


def test_card标签只合并不去重(store: DatasetStore) -> None:
    from hf_data_crawl import card

    before = [batch.records for batch in raw.read_batches(store, DAY, label=raw.CARD_LABEL)]

    archive_day(store, DAY)

    after = [batch.records for batch in raw.read_batches(store, DAY, label=raw.CARD_LABEL)]
    assert after == before
    assert card.build(store, DAY)[0] == 1


def test_验证失败时撤销新分片且原文件不动(
    store: DatasetStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    sources = raw.batch_files(store, DAY)
    original = archive._archive_label

    def corrupt(directory: Path, label: str, paths: list[Path]) -> list[Path]:
        created = original(directory, label, paths)
        if label == "part" and created:
            # 故意丢掉分片里的记录，让归档前后的 Bronze 对不上
            import gzip
            import json

            payload = json.loads(gzip.decompress(created[0].read_bytes()).decode("utf-8"))
            payload["records"] = payload["records"][:1]
            payload["groups"] = payload["groups"][:1]
            payload["groups"][0]["count"] = 1
            created[0].write_bytes(gzip.compress(json.dumps(payload).encode("utf-8")))
        return created

    monkeypatch.setattr(archive, "_archive_label", corrupt)

    with pytest.raises(ArchiveMismatch):
        archive_day(store, DAY)

    assert raw.batch_files(store, DAY) == sources
    assert not (store.root / "backup").exists()


def test_cli按天数门槛只归档老分区(store: DatasetStore) -> None:
    code = main(["--data-dir", str(store.root), "archive", "--older-than-days", "30"])

    assert code == 0
    assert not any(raw.is_archive(path) for path in raw.batch_files(store, DAY))


def test_cli指定日期会无视天数门槛(store: DatasetStore) -> None:
    code = main(["--data-dir", str(store.root), "archive", "--date", DAY.isoformat()])

    assert code == 0
    assert all(raw.is_archive(path) for path in raw.batch_files(store, DAY))
