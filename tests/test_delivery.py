"""交付的行为测试。

守四条判据：三件齐全、同一底库两次交付逐字节相同、0 行与超限不落盘、
行数突变要标疑但**不阻断**（交货比不交好，可疑必须留在纸面上）。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl
import pytest
import yaml

from hf_data_crawl.cli import main
from hf_data_crawl.delivery import DeliveryBlocked, deliver
from hf_data_crawl.derive import derive_day
from hf_data_crawl.requirement import DELIVERIES_DIR, MANIFEST_NAME, SPEC_NAME
from hf_data_crawl.store import DatasetStore

DAY = date(2026, 11, 1)
NOW = datetime(2026, 11, 8, 15, 20, 0, tzinfo=UTC)


def record(index: int, *, downloads: int, likes: int = 2) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 10,
        "likes": likes,
        "private": False,
        "description": "d",
        "tags": ["license:mit"],
    }


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    """三个库：2000 下载 / 1500 下载 / 500 下载，点赞都是 2。"""
    made = DatasetStore(tmp_path / "data")
    made.write_raw(
        snapshot_date=DAY,
        page_index=1,
        records=[
            record(1, downloads=2000),
            record(2, downloads=1500),
            record(3, downloads=500),
        ],
        meta={},
    )
    derive_day(made, DAY)
    return made


def make_request(root: Path, **overrides: Any) -> Path:
    body: dict[str, Any] = {
        "id": "REQ-0001",
        "title": "高下载数据集",
        "requester": "张三",
        "version": 1,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": 1000}],
    }
    body.update(overrides)
    path = root / "REQ-0001" / SPEC_NAME
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    return path


def delivery_dirs(root: Path) -> list[Path]:
    base = root / "REQ-0001" / DELIVERIES_DIR
    if not base.is_dir():
        return []
    return sorted(
        path for path in base.iterdir() if path.is_dir() and not path.name.startswith(".")
    )


def test_交付写出三件套(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)

    result = deliver(store, root, "REQ-0001", now=NOW)

    names = {path.name for path in result.directory.iterdir()}
    assert names == {"data.parquet", MANIFEST_NAME, "README.md"}
    assert result.rows == 2


def test_manifest记下底库指纹与校验和(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)

    result = deliver(store, root, "REQ-0001", now=NOW)

    body = yaml.safe_load((result.directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert len(body["source"]["current_md5"]) == 32
    assert body["source"]["current_rows"] == 3
    assert body["result"]["rows"] == 2
    assert len(body["result"]["files"][0]["sha256"]) == 64
    assert body["criteria"][0]["field"] == "downloads"
    assert body["health"]["status"] == "first"


def test_同一底库两次交付逐字节相同(store: DatasetStore, tmp_path: Path) -> None:
    """可复现的判据。靠的是编译时那条 "id" ASC 兜底。"""
    root = tmp_path / "req"
    make_request(root)

    first = deliver(store, root, "REQ-0001", now=NOW)
    second = deliver(store, root, "REQ-0001", now=NOW.replace(second=30))

    assert first.files[0]["sha256"] == second.files[0]["sha256"]
    assert first.directory != second.directory


def test_零行不交付且不留目录(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, criteria=[{"metric": "下载量", "op": ">", "value": 999999}])

    with pytest.raises(DeliveryBlocked, match="min_rows"):
        deliver(store, root, "REQ-0001", now=NOW)

    assert delivery_dirs(root) == []


def test_超上限不交付且不留目录(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, guard={"min_rows": 1, "max_rows": 1})

    with pytest.raises(DeliveryBlocked, match="max_rows"):
        deliver(store, root, "REQ-0001", now=NOW)

    assert delivery_dirs(root) == []


def test_失败时不留半成品目录(store: DatasetStore, tmp_path: Path) -> None:
    """中途失败只能留下 .tmp-*，绝不能出现「有数据没说明」。"""
    root = tmp_path / "req"
    make_request(root, output={"format": "csv", "columns": ["id", "不存在的列"]})

    with pytest.raises(DeliveryBlocked):
        deliver(store, root, "REQ-0001", now=NOW)

    leftovers = [path for path in (root / "REQ-0001" / DELIVERIES_DIR).glob(".tmp-*")]
    assert leftovers == []


def test_行数突变标疑但不阻断(store: DatasetStore, tmp_path: Path) -> None:
    """交货比不交货好——但「这次不对劲」必须留在纸面上。"""
    root = tmp_path / "req"
    # 把绝对阈值调到 10：这份夹具只有 2 行，默认的 100 行阈值永远不会触发。
    make_request(root, alert={"row_delta_pct": 20, "row_delta_abs": 10})
    previous = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    previous.mkdir(parents=True)
    (previous / MANIFEST_NAME).write_text(
        json.dumps({"spec_version": 1, "result": {"rows": 100}}), encoding="utf-8"
    )

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.health.status == "suspect"
    assert result.health.delta_abs == -98
    readme = (result.directory / "README.md").read_text(encoding="utf-8")
    assert "已标疑" in readme.splitlines()[2]


def test_小波动不标疑(store: DatasetStore, tmp_path: Path) -> None:
    """12 行变 15 行是 +25%，但绝对差只有 3 行——只判百分比会天天误报。"""
    root = tmp_path / "req"
    make_request(root)
    previous = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    previous.mkdir(parents=True)
    (previous / MANIFEST_NAME).write_text(
        json.dumps({"spec_version": 1, "result": {"rows": 1}}), encoding="utf-8"
    )

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.health.status == "ok"
    assert result.health.delta_abs == 1


def test_口径升版后不比较行数(store: DatasetStore, tmp_path: Path) -> None:
    """改了口径必然行数大变；此时比较只会天天发假警报，而假警报会训练人忽略告警。"""
    root = tmp_path / "req"
    make_request(root, version=2)
    previous = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    previous.mkdir(parents=True)
    (previous / MANIFEST_NAME).write_text(
        json.dumps({"spec_version": 1, "result": {"rows": 100}}), encoding="utf-8"
    )

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.health.status == "ok"
    assert result.health.supersedes == 1
    assert "口径已从 v1 改到 v2" in result.health.note


def test_上次档案读不出来时如实说明而非假装正常(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)
    previous = root / "REQ-0001" / DELIVERIES_DIR / "20261101T000000"
    previous.mkdir(parents=True)
    (previous / MANIFEST_NAME).write_text("{ 坏掉的 json", encoding="utf-8")

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.health.status == "unknown"
    assert "读不出来" in result.health.note


def test_csv格式(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, output={"format": "csv"})

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert (result.directory / "data.csv").exists()
    assert not (result.directory / "data.parquet").exists()


def test_both格式写两份(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, output={"format": "both"})

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert (result.directory / "data.parquet").exists()
    assert (result.directory / "data.csv").exists()
    assert len(result.files) == 2


def test_首次交付留存口径快照(store: DatasetStore, tmp_path: Path) -> None:
    """只留「真的用过」的口径——没用过的版本没有保留价值。"""
    root = tmp_path / "req"
    make_request(root)

    deliver(store, root, "REQ-0001", now=NOW)

    assert (root / "REQ-0001" / "history" / "v1.yaml").exists()


def test_输出列可写词条(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root, output={"format": "parquet", "columns": ["id", "下载量"]})

    result = deliver(store, root, "REQ-0001", now=NOW)

    assert result.columns == ("id", "downloads")
    assert pl.read_parquet(result.directory / "data.parquet").height == 2


def test_一次性需求重复交付需force(store: DatasetStore, tmp_path: Path) -> None:
    root = tmp_path / "req"
    make_request(root)
    argv = [
        "--data-dir",
        str(store.root),
        "--requests-dir",
        str(root),
        "request",
        "run",
        "REQ-0001",
    ]

    assert main(argv) == 0
    assert main(argv) == 1
    assert main([*argv, "--force"]) == 0
