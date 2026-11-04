"""S8 · 运行摘要与失败留痕的行为测试。

验收点：成功与失败都留痕、失败必须带可读原因、连续两轮可对比。
"""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

from hf_data_crawl.cli import main
from hf_data_crawl.runs import read_runs
from hf_data_crawl.store import DatasetStore

DAY1 = date(2026, 11, 1)


def record(index: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": 100,
        "downloadsAllTime": 200,
        "likes": 1,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en"],
    }


def seed(store: DatasetStore) -> None:
    store.write_raw(snapshot_date=DAY1, page_index=1, records=[record(1)], meta={})
    store.build_bronze(DAY1)
    store.build_silver(DAY1)
    store.update_state(DAY1)


def test_成功运行写入可读摘要(tmp_path: Path) -> None:
    root = tmp_path / "data"
    seed(DatasetStore(root))

    code = main(["--data-dir", str(root), "build"])

    runs = read_runs(DatasetStore(root))
    assert code == 0
    assert len(runs) == 1
    assert runs[0]["command"] == "build"
    assert runs[0]["status"] == "success"
    assert runs[0]["payload"]["current_rows"] == 1


def test_失败运行留下可读原因(tmp_path: Path) -> None:
    root = tmp_path / "data"
    broken = root / "raw" / f"dt={DAY1.isoformat()}" / "part-000001-deadbeef.json.gz"
    broken.parent.mkdir(parents=True)
    broken.write_bytes(b"not a gzip payload")

    code = main(["--data-dir", str(root), "build"])

    runs = read_runs(DatasetStore(root))
    assert code == 1
    assert runs[0]["status"] == "failed"
    assert "error" in runs[0]["payload"]
    assert runs[0]["payload"]["error"]


def test_连续两轮摘要可对比(tmp_path: Path) -> None:
    root = tmp_path / "data"
    seed(DatasetStore(root))

    main(["--data-dir", str(root), "build"])
    main(["--data-dir", str(root), "build"])

    runs = read_runs(DatasetStore(root))
    assert len(runs) == 2
    assert runs[0]["finished_at"] <= runs[1]["started_at"]
    assert runs[0]["payload"]["current_rows"] == runs[1]["payload"]["current_rows"]


def test_损坏的摘要文件不阻断其余读取(tmp_path: Path) -> None:
    root = tmp_path / "data"
    store = DatasetStore(root)
    seed(store)
    main(["--data-dir", str(root), "build"])
    (root / "runs" / "broken.json").write_text("{半截", encoding="utf-8")

    runs = read_runs(store)

    assert len(runs) == 1


def test_同一秒内两轮的顺序按内容而非文件名(tmp_path: Path) -> None:
    """撞名文件 `-build-2.json` 的字典序排在 `-build.json` 之前。

    若按文件名排序，后一轮会被读到前面，「连续两轮对比」于是读到颠倒的一对，
    而且不报错——只是 finished_at <= started_at 这条检查突然不成立。
    """
    store = DatasetStore(tmp_path / "data")
    runs_dir = store.root / "runs"
    runs_dir.mkdir(parents=True)
    (runs_dir / "20261105T001500-build.json").write_text(
        json.dumps(
            {
                "command": "build",
                "status": "success",
                "started_at": "2026-11-05T00:15:00.000000",
                "finished_at": "2026-11-05T00:15:09.000000",
                "payload": {},
            }
        ),
        encoding="utf-8",
    )
    (runs_dir / "20261105T001500-build-2.json").write_text(
        json.dumps(
            {
                "command": "build",
                "status": "success",
                "started_at": "2026-11-05T00:15:20.000000",
                "finished_at": "2026-11-05T00:15:29.000000",
                "payload": {},
            }
        ),
        encoding="utf-8",
    )

    runs = read_runs(store)

    assert [item["started_at"] for item in runs] == [
        "2026-11-05T00:15:00.000000",
        "2026-11-05T00:15:20.000000",
    ]


def test_摘要中日期与路径被序列化为字符串(tmp_path: Path) -> None:
    root = tmp_path / "data"
    seed(DatasetStore(root))
    main(["--data-dir", str(root), "build"])

    payload = read_runs(DatasetStore(root))[0]["payload"]

    body = json.dumps(payload, ensure_ascii=False)
    assert isinstance(payload["dates"], list)
    assert isinstance(payload["dates"][0], str)
