"""冷区 Raw 压缩的契约测试。

核心不变式：压缩只保留「每个 id 最后一次写入的原始响应」，且**不改变默认
build 的结果**；原分块只移入 backup、不删除；校验失败必须撤销新分片。
"""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

import hf_data_crawl.compact as compact_module
from hf_data_crawl import raw, state
from hf_data_crawl.compact import CompactMismatch, compact
from hf_data_crawl.derive import derive_day
from hf_data_crawl.rebuild import discover_raw_dates, rebuild_from_raw
from hf_data_crawl.retention import manifest_path, read_compressed_dates
from hf_data_crawl.store import DatasetStore

COLD = [date(2026, 10, day) for day in (1, 2, 3)]
HOT = date(2026, 11, 7)


def record(index: int, downloads: int, sha: str) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "sha": sha,
        "lastModified": "2026-10-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "downloads": downloads,
        "downloadsAllTime": downloads * 2,
        "likes": 1,
        "private": False,
        "tags": ["language:en"],
        "description": "d",
    }


def seed(store: DatasetStore) -> None:
    """三天冷区 + 一天热区，同一 id 的值逐日变化。"""
    plan: dict[date, list[dict[str, Any]]] = {
        COLD[0]: [record(1, 100, "s1"), record(2, 50, "s2")],
        COLD[1]: [record(1, 180, "s1b"), record(2, 50, "s2")],
        COLD[2]: [record(1, 180, "s1b"), record(3, 10, "s3")],
        HOT: [record(1, 200, "s1c"), record(3, 10, "s3")],
    }
    for number, day in enumerate([*COLD, HOT], 1):
        raw.write(
            store,
            snapshot_date=day,
            page_index=1,
            records=plan[day],
            meta={},
            written_at=f"2026-11-{number:02d}T00:00:00.000000+00:00",
        )
        derive_day(store, day)


def state_fingerprint(store: DatasetStore) -> dict[str, str]:
    out: dict[str, str] = {}
    for name, frame in (
        ("observations", store.read_all("observations")),
        ("silver", store.read_all("silver")),
        ("current", state.read_current(store)),
    ):
        columns = sorted(frame.columns)
        keys = [column for column in ("id", "snapshot_date") if column in frame.columns]
        payload = repr(frame.select(columns).sort(keys).to_dict(as_series=False))
        out[name] = hashlib.sha256(payload.encode()).hexdigest()[:16]
    return out


def baseline_records(store: DatasetStore) -> list[dict[str, Any]]:
    files = sorted((store.root / "cold" / "raw").glob("baseline-*.json.gz"))
    rows: list[dict[str, Any]] = []
    for path in files:
        for batch in raw.load_batches(path):
            rows.extend(batch.records)
    return rows


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    seed(instance)
    return instance


# ---------------------------------------------------------------- 正确性


def test_基线只留每个id的最后写入记录(store: DatasetStore) -> None:
    result = compact(store, COLD[:2])

    assert result is not None
    rows = baseline_records(store)
    by_id = {row["id"]: row for row in rows}
    assert set(by_id) == {"acme/ds-1", "acme/ds-2"}
    assert by_id["acme/ds-1"]["downloads"] == 180  # COLD[1] 胜出
    assert by_id["acme/ds-1"]["sha"] == "s1b"


def test_基线groups保留来源命名空间与写入序(store: DatasetStore) -> None:
    compact(store, COLD[:2])

    path = next((store.root / "cold" / "raw").glob("baseline-*.json.gz"))
    payload = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))

    assert payload["meta"]["baseline"] is True
    assert all("key" in group and "source" in group for group in payload["groups"])


def test_原分块完整移入backup不删除(store: DatasetStore) -> None:
    before = {
        day: {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in raw.batch_files(store, day)
        }
        for day in COLD[:2]
    }

    result = compact(store, COLD[:2])

    assert result is not None and result.backup_files == sum(
        len(group) for group in before.values()
    )
    for day in COLD[:2]:
        assert raw.batch_files(store, day) == []
        target = store.root / "backup" / "raw" / f"dt={day.isoformat()}"
        assert {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest() for path in target.iterdir()
        } == before[day]


def test_热区与未指定日期不受影响(store: DatasetStore) -> None:
    hot_before = [path.name for path in raw.batch_files(store, HOT)]
    cold3_before = [path.name for path in raw.batch_files(store, COLD[2])]

    compact(store, COLD[:2])

    assert [path.name for path in raw.batch_files(store, HOT)] == hot_before
    assert [path.name for path in raw.batch_files(store, COLD[2])] == cold3_before


def test_manifest登记压缩日期(store: DatasetStore) -> None:
    compact(store, COLD[:2])

    assert read_compressed_dates(store) == set(COLD[:2])
    payload = json.loads(manifest_path(store).read_text(encoding="utf-8"))
    assert payload["baselines"]


def test_压缩前后默认build结果逐行一致(store: DatasetStore) -> None:
    baseline = state_fingerprint(store)
    rebuild_from_raw(store)

    assert state_fingerprint(store) == baseline

    compact(store, COLD)

    assert discover_raw_dates(store) == [HOT]
    rebuild_from_raw(store)
    assert state_fingerprint(store) == baseline


def test_多轮压缩把已有基线并入(store: DatasetStore) -> None:
    compact(store, COLD[:2])
    compact(store, COLD[2:])

    rows = baseline_records(store)
    by_id = {row["id"]: row for row in rows}
    assert by_id["acme/ds-1"]["downloads"] == 180
    assert by_id["acme/ds-3"]["downloads"] == 10
    assert read_compressed_dates(store) == set(COLD)


# ---------------------------------------------------------------- 防御


def test_没有源文件时返回None(store: DatasetStore) -> None:
    assert compact(store, [date(2026, 1, 1)]) is None


def test_重复压缩遇到已存在的备份目标直接拒绝(store: DatasetStore) -> None:
    compact(store, COLD[:2])
    # 人为把同名文件放回 data/raw，制造「备份目标已存在」的重复压缩现场。
    target = store.root / "backup" / "raw" / f"dt={COLD[0].isoformat()}"
    source = next(target.iterdir())
    partition = store.raw_dir / f"dt={COLD[0].isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    (partition / source.name).write_bytes(source.read_bytes())

    with pytest.raises(CompactMismatch, match="备份目标已存在"):
        compact(store, [COLD[0]])


def test_校验失败时撤销新分片且原文件不动(
    store: DatasetStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    before = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for day in COLD[:2]
        for path in raw.batch_files(store, day)
    }
    monkeypatch.setattr(compact_module, "_written", lambda _created: ({"acme/ds-1": "伪造"}, 1))

    with pytest.raises(CompactMismatch, match="冷区基线校验失败"):
        compact(store, COLD[:2])

    assert list((store.root / "cold" / "raw").glob("baseline-*.json.gz")) == []
    assert {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for day in COLD[:2]
        for path in raw.batch_files(store, day)
    } == before
    assert not manifest_path(store).exists()


# ---------------------------------------------------------------- CLI


def test_compact命令端到端可用(tmp_path: Path) -> None:
    from hf_data_crawl.cli import main

    store = DatasetStore(tmp_path / "cli-data")
    seed(store)
    day = COLD[0]

    code = main(["--data-dir", str(store.root), "compact", "--date", day.isoformat()])

    assert code == 0
    assert raw.batch_files(store, day) == []
    assert list((store.root / "cold" / "raw").glob("baseline-*.json.gz"))
    assert read_compressed_dates(store) == {day}
