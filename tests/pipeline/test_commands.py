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
from hf_data_crawl.pipeline.commands import (
    _describe_gaps,
    _print_build,
    _write_csv,
    run_bronze_view,
    run_build,
)
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.pipeline.rebuild import RebuildSummary
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


def test_full_无冷区缺口时直接全量重放(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """有缺口要拦，没缺口就得真跑——否则「全量」只剩拒绝一条路。"""
    store = seeded(tmp_path)

    outcome = run_build(
        argparse.Namespace(full=True, stale=False, allow_cold_loss=False),
        store,
    )

    assert outcome.code == 0
    assert outcome.payload.mode == "full"
    assert "全量重放（显式 --full）" in capsys.readouterr().out


def test_多天冷区缺口报最早最晚而不是区间() -> None:
    text = _describe_gaps([DAY1, DAY2])

    assert "2 天已压入冷区" in text
    assert DAY1.isoformat() in text and DAY2.isoformat() in text


def test_bronze_view_按id只打印该行(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = seeded(tmp_path)
    args = argparse.Namespace(date=DAY1.isoformat(), out=None, id="acme/ds-1", limit=10)

    outcome = run_bronze_view(args, store)

    out = capsys.readouterr().out
    assert outcome.code == 0
    assert outcome.payload["id"] == "acme/ds-1"
    assert "last_modified" in out and "acme/ds-1" in out


def test_bronze_view_按id查不到时明说(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = seeded(tmp_path)
    args = argparse.Namespace(date=DAY1.isoformat(), out=None, id="acme/missing", limit=10)

    outcome = run_bronze_view(args, store)

    assert outcome.code == 0
    assert "这一天没有 id = acme/missing" in capsys.readouterr().out


def test_bronze_view_limit外还有行时提示总数(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    store = seeded(tmp_path)
    args = argparse.Namespace(date=DAY1.isoformat(), out=None, id=None, limit=0)

    run_bronze_view(args, store)

    assert "共 1 行" in capsys.readouterr().out


def test_扁平表导出csv不走嵌套转换(tmp_path: Path) -> None:
    """嵌套列才需要逐行 JSON；没有嵌套列时仍应走 polars 的原生 writer。"""
    target = tmp_path / "flat.csv"

    _write_csv(pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}), target)

    assert target.read_text(encoding="utf-8").splitlines() == ["a,b", "1,x", "2,y"]


def test_空目录重建直接说明无可重建(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = DatasetStore(tmp_path / "empty")

    outcome = run_build(
        argparse.Namespace(full=False, stale=False, allow_cold_loss=False),
        store,
    )

    assert outcome.code == 0
    assert "raw 下没有任何分区" in capsys.readouterr().out


def summary(**overrides: Any) -> RebuildSummary:
    base: dict[str, Any] = {
        "dates": [DAY1],
        "bronze_rows": 1,
        "silver_rows": 1,
        "observations": 1,
        "current_rows": 1,
    }
    return RebuildSummary(**(base | overrides))


def test_重建摘要会喊观测骤降(capsys: pytest.CaptureFixture[str]) -> None:
    """重放先清分区再写回；不喊出来，删掉的历史没有任何痕迹。"""
    _print_build(summary(shrinks=[(DAY1, 10, 4)]))

    out = capsys.readouterr().out
    assert "只写回不到一半" in out
    assert "2026-11-01（10→4）" in out


def test_重建摘要会报卡片重算行数(capsys: pytest.CaptureFixture[str]) -> None:
    _print_build(summary(card_rows=5, config_rows=2))

    out = capsys.readouterr().out
    assert "卡片 5 行" in out
    assert "config 明细 2 行" in out
