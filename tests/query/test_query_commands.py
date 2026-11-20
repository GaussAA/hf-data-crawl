"""`query` / `report` 命令处理函数的成功路径与空结果。

`_warn_frozen_rows` 已有单测，但没人验过它真的被 `run_query` 调用；`run_report`
的落盘位置与默认路径也一直没测——这两条都是用户直接看到的出口。
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import CURRENT_SCHEMA
from hf_data_crawl.query.commands import run_query, run_report

DAY = date(2026, 11, 4)


def store_with_current(tmp_path: Path) -> DatasetStore:
    """最小可查询底库：两行 current，一在架、一改名。"""
    store = DatasetStore(tmp_path / "data")
    data: dict[str, list[object]] = {name: [None, None] for name in CURRENT_SCHEMA}
    data["id"] = ["acme/active", "acme/renamed"]
    data["status"] = ["active", "renamed"]
    data["last_seen_at"] = [DAY, DAY]
    store.write("current", pl.DataFrame(data, schema=CURRENT_SCHEMA))
    return store


def test_查询有结果时打印并提示冻结行(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = store_with_current(tmp_path)

    code = run_query(
        argparse.Namespace(sql="SELECT id, status FROM datasets ORDER BY id", board=None, limit=10),
        store,
    )

    out = capsys.readouterr().out
    assert code == 0
    assert "acme/active" in out and "acme/renamed" in out
    assert "1 行状态不是 active" in out


def test_查询无结果时给人话而不是空表(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    store = store_with_current(tmp_path)

    code = run_query(
        argparse.Namespace(
            sql="SELECT id FROM datasets WHERE id = 'acme/missing'", board=None, limit=10
        ),
        store,
    )

    assert code == 0
    assert "查询无结果" in capsys.readouterr().out


@pytest.mark.parametrize("explicit", [False, True])
def test_报告落在默认位置或指定路径(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], *, explicit: bool
) -> None:
    store = store_with_current(tmp_path)
    target = tmp_path / "custom.html"

    outcome = run_report(
        argparse.Namespace(out=str(target) if explicit else None, top=10),
        store,
    )

    expected = target if explicit else store.root / "reports" / "dashboard.html"
    assert outcome.code == 0
    assert expected.is_file()
    assert "榜单页已生成" in capsys.readouterr().out
    assert "acme/active" in expected.read_text(encoding="utf-8")
