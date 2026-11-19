"""文件 adapter：开文件、选 sheet、取表头、按行取标识，以及「拒公式」。

这一层才碰 openpyxl。它存在的理由是依赖方向：档案层 `delivery.py` import 的是
`header.py` 那个纯核，不该顺着一条 import 就传递依赖 Excel。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import openpyxl
import pytest

from hf_data_crawl.delivery.header import HeaderError
from hf_data_crawl.delivery.sheet import first_sheet_name, header_of, key_rows, open_sheet


def build(path: Path, rows: list[list[Any]], *, title: str = "数据") -> Path:
    """造一张最小的书：按行写进去，第一行通常是表头。"""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    assert sheet is not None  # openpyxl 新建工作簿必有活动表，类型上却可能是 None
    sheet.title = title
    for row in rows:
        sheet.append(row)
    workbook.save(path)
    return path


def test_按列名顺序读表头并跳过空列(tmp_path: Path) -> None:
    book = build(
        tmp_path / "in.xlsx", [["URL", "", "下载量", None, "备注"], ["a/b", "x", 1, "y", "z"]]
    )

    parsed = header_of(book)

    assert parsed.names == ("URL", "下载量", "备注")
    assert parsed.columns["下载量"] == 3


def test_文件里没有那个_sheet_就报错(tmp_path: Path) -> None:
    book = build(tmp_path / "in.xlsx", [["URL"]])

    with pytest.raises(HeaderError, match="没有 sheet"):
        header_of(book, sheet="不存在的表")


def test_表头可以不在第一行(tmp_path: Path) -> None:
    book = build(tmp_path / "in.xlsx", [["基础信息"], ["URL", "下载量"]])

    assert header_of(book, header_row=2).names == ("URL", "下载量")


def test_表头是公式就报错而不是把公式当列名(tmp_path: Path) -> None:
    book = build(tmp_path / "in.xlsx", [['=CONCAT("a","b")', "下载量"]])

    with pytest.raises(HeaderError, match="公式"):
        header_of(book)


def test_标识列里的公式不被当成_id(tmp_path: Path) -> None:
    """公式没缓存值时读到的是公式文本，整行会静默落进 missing——看起来像「认不出这个 id」。"""
    book = build(
        tmp_path / "in.xlsx",
        [["URL", "下载量"], ['=CONCAT("acme","/","ds-1")', 1]],
    )
    _workbook, worksheet = open_sheet(book)

    with pytest.raises(HeaderError, match="公式"):
        key_rows(worksheet, header_row=1, key_column=1)


def test_标识列空的行不算数据(tmp_path: Path) -> None:
    book = build(
        tmp_path / "in.xlsx",
        [["URL", "下载量"], ["acme/ds-1", 1], [None, 2], ["acme/ds-2", 3]],
    )
    _workbook, worksheet = open_sheet(book)

    assert key_rows(worksheet, header_row=1, key_column=1) == [(2, "acme/ds-1"), (4, "acme/ds-2")]


# ---------------------------------------------------------------- 取第一个 sheet 的名字


def two_sheets(path: Path, first: str, second: str) -> Path:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    assert sheet is not None
    sheet.title = first
    workbook.create_sheet(second)
    workbook.save(path)
    return path


def test_取的是工作簿里的第一页而不是字典序(tmp_path: Path) -> None:
    """受理时填进骨架的名字要跟人打开文件看到的一致。

    按字典序排会让「乙表」排在「甲表」后面，于是骨架写的不是第一页——人对着
    文件核对时怎么都对不上。
    """
    book = two_sheets(tmp_path / "两页.xlsx", "乙表", "甲表")

    assert first_sheet_name(book) == "乙表"


def test_不是工作簿时翻成HeaderError并带上路径(tmp_path: Path) -> None:
    """受理要能当场失败：留一个读不出来的源表在需求目录里，代价是几天后才发现。"""
    fake = tmp_path / "其实是文本.xlsx"
    fake.write_text("这不是工作簿", encoding="utf-8")

    with pytest.raises(HeaderError, match="打不开"):
        first_sheet_name(fake)


def test_文件不存在时同样翻成HeaderError(tmp_path: Path) -> None:
    with pytest.raises(HeaderError, match="打不开"):
        first_sheet_name(tmp_path / "没有这个文件.xlsx")
