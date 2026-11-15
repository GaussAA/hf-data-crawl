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


def test_结果里没有状态列但含id要说明口径(capsys: pytest.CaptureFixture[str]) -> None:
    """自定义投影/手写 SQL 不投影 status；不说一声，旧值会被当成今天读数。

    仍然不凭 id 猜具体哪几行失效（那要额外查底库），但「无法判断」这件事
    必须说出口，否则 --sql 与自定义 output.columns 正好绕开唯一的护栏。
    """
    from hf_data_crawl.query.commands import _warn_frozen_rows

    _warn_frozen_rows(pl.DataFrame({"id": ["a/1"], "downloads": [10]}))

    out = capsys.readouterr().out
    assert "未包含 status" in out
    assert "最后一次" in out


def test_纯聚合结果既无id也无状态列不打扰(capsys: pytest.CaptureFixture[str]) -> None:
    """COUNT(*) 这类结果没有「行」的概念，提示反而是噪音。"""
    from hf_data_crawl.query.commands import _warn_frozen_rows

    _warn_frozen_rows(pl.DataFrame({"count": [1]}))

    assert capsys.readouterr().out == ""
