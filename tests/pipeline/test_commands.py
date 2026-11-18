"""`pipeline` 命令层的行为测试。

命令层唯一真正承载策略的地方，是 `build --full` 的放行闸门：冷区日期一旦被
重放清空，silver / observations 的历史就再也补不回来。此前这道闸门只有注释
说明、没有测试——安全边界不能只写在文字里。
"""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.commands import run_bronze_view, run_build
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform import retention
from hf_data_crawl.platform.store import DatasetStore

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
        "tags": ["language:en", "license:mit"],
    }


def seeded(tmp_path: Path) -> DatasetStore:
    store = DatasetStore(tmp_path / "data")
    for moment, downloads in ((DAY1, 100), (DAY2, 180)):
        raw.write(
            store,
            snapshot_date=moment,
            page_index=1,
            records=[record(1, downloads)],
            meta={},
        )
        derive_day(store, moment)
    return store


def compress(store: DatasetStore, snapshot: date) -> None:
    """把某天的 Raw 搬进冷区并登记进 manifest——`compact` 的结果，手工复现。"""
    shutil.move(
        str(store.raw_dir / f"dt={snapshot.isoformat()}"),
        retention.backup_raw_dir(store, snapshot),
    )
    manifest = retention.manifest_path(store)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        json.dumps({"compressed_dates": [snapshot.isoformat()]}),
        encoding="utf-8",
    )


def test_full_遇冷区缺口时拒绝并给出退路(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = seeded(tmp_path)
    compress(store, DAY1)
    args = argparse.Namespace(full=True, stale=False, allow_cold_loss=False)

    outcome = run_build(args, store)

    assert outcome.code == 1
    assert outcome.payload["cold_gaps"] == 1
    printed = capsys.readouterr().out
    assert "不可逆" in printed
    assert "--allow-cold-loss" in printed


def test_full_拒绝发生在动手之前(tmp_path: Path) -> None:
    """闸门若先清后拦，历史已经没了——所以拒绝时派生层必须原封不动。"""
    store = seeded(tmp_path)
    compress(store, DAY1)
    before = store.read_all("observations").height

    run_build(argparse.Namespace(full=True, stale=False, allow_cold_loss=False), store)

    assert store.read_all("observations").height == before


def test_full_显式放行后才继续(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = seeded(tmp_path)
    compress(store, DAY1)
    args = argparse.Namespace(full=True, stale=False, allow_cold_loss=True)

    outcome = run_build(args, store)

    assert outcome.code == 0
    assert "已按 --allow-cold-loss 放行" in capsys.readouterr().out


def test_full_与_stale_互斥(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = seeded(tmp_path)

    outcome = run_build(
        argparse.Namespace(full=True, stale=True, allow_cold_loss=False),
        store,
    )

    assert outcome.code == 1
    assert "互斥" in capsys.readouterr().out


def test_stale_已对齐时不再重建(tmp_path: Path) -> None:
    """对齐就是对齐：没有滞后还重放一遍，等于凭空改写一遍历史。"""
    store = seeded(tmp_path)

    outcome = run_build(
        argparse.Namespace(full=False, stale=True, allow_cold_loss=False),
        store,
    )

    assert outcome.code == 0
    assert outcome.payload["mode"] == "already-aligned"


def test_bronze_view_导出_csv(tmp_path: Path) -> None:
    store = seeded(tmp_path)
    target = tmp_path / "out.csv"
    args = argparse.Namespace(date=DAY1.isoformat(), out=str(target), id=None, limit=10)

    outcome = run_bronze_view(args, store)

    assert outcome.code == 0
    assert outcome.payload["rows"] == 1
    text = target.read_text(encoding="utf-8")
    assert text.count("\n") >= 2
    # 嵌套列（tags / raw）按 JSON 文本写出：既能落地，也不丢信息。
    assert '"language:en"' in text


def test_bronze_view_非_csv后缀按_parquet_写(tmp_path: Path) -> None:
    store = seeded(tmp_path)
    target = tmp_path / "out.parquet"
    args = argparse.Namespace(date=DAY1.isoformat(), out=str(target), id=None, limit=10)

    outcome = run_bronze_view(args, store)

    assert outcome.code == 0
    assert pl.read_parquet(target).height == 1


def test_bronze_view_空日不报错且零行(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = seeded(tmp_path)
    args = argparse.Namespace(date="2020-01-01", out=None, id=None, limit=10)

    outcome = run_bronze_view(args, store)

    assert outcome.code == 0
    assert outcome.payload["rows"] == 0
    assert "没有可派生的记录" in capsys.readouterr().out
