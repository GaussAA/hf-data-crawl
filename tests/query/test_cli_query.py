"""`query` 命令对冻结行（非在架）的提示：只提示，不改数据、不过滤。"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

DAY = date(2026, 11, 4)
DAY_ARG = DAY.isoformat()


def test_查询结果含冻结行要出提示(capsys: pytest.CaptureFixture[str]) -> None:
    """改名后的旧 id 永远不再更新；不提示，它的旧值会被当成今天的读数。"""
    from hf_data_crawl.query.commands import _warn_frozen_rows

    _warn_frozen_rows(
        pl.DataFrame(
            {
                "id": ["a/1", "a/2", "a/3"],
                "downloads": [10, 20, 30],
                "status": ["active", "renamed", "disappeared"],
            }
        )
    )

    out = capsys.readouterr().out
    assert "2 行状态不是 active" in out


def test_查询结果全在架时不打扰(capsys: pytest.CaptureFixture[str]) -> None:
    from hf_data_crawl.query.commands import _warn_frozen_rows

    _warn_frozen_rows(pl.DataFrame({"id": ["a/1"], "status": ["active"]}))

    assert capsys.readouterr().out == ""


def test_结果里没有状态列就不作声(capsys: pytest.CaptureFixture[str]) -> None:
    """多数查询不投影 status；没有这一列就无从判断，别凭 id 猜。"""
    from hf_data_crawl.query.commands import _warn_frozen_rows

    _warn_frozen_rows(pl.DataFrame({"id": ["a/1"], "downloads": [10]}))

    assert capsys.readouterr().out == ""
