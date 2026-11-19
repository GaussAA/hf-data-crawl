"""表格文件的唯一 openpyxl 住户：开文件、选 sheet、取表头、按行取标识。

读表头被拆成两半，理由是依赖方向：

- `header.py` 是纯核，吃「表头那一行的值」。档案层 `delivery.py` import 它，
  只为比「这次的输入表和上次一样吗」。
- 本模块开文件、选 sheet、拒公式。只有交付的执行体与比对用得到。

若把「开文件」塞进 `header.py`，档案层就会顺着那条 import 传递依赖 openpyxl——
DECISIONS #86 刚把档案层从「认形态」里摘出来，不能从这条缝里塞回去。

拒公式是第一版就有的规则，来历见 `read_value`。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import openpyxl

from .header import Header, HeaderError, parse


def open_sheet(path: Path, *, sheet: str | None = None) -> tuple[Any, Any]:
    """打开工作簿并选定 sheet；文件里没有这个 sheet 就报错。"""
    workbook = openpyxl.load_workbook(path)
    name = sheet or workbook.sheetnames[0]
    if name not in workbook.sheetnames:
        raise HeaderError(f"{path}: 没有 sheet {name!r}（现有 {workbook.sheetnames}）")
    return workbook, workbook[name]


def header_of(path: Path, *, sheet: str | None = None, header_row: int = 1) -> Header:
    """只读一份文件里的表头。

    受理新输入时用它比对字段：那一刻还没有 spec，`from_mapping` 那套校验用不上，
    手上只有一份文件。
    """
    _workbook, worksheet = open_sheet(path, sheet=sheet)
    return read_header(worksheet, header_row=header_row)


def read_header(worksheet: Any, *, header_row: int = 1) -> Header:
    """表头行 → `Header`。空列名跳过、重名报错，规则全在 `header.parse` 里。"""
    return parse(_row_values(worksheet, header_row))


def key_rows(worksheet: Any, *, header_row: int, key_column: int) -> list[tuple[int, str]]:
    """标识列非空的行：[(Excel 行号, 标识原文)]。

    只扫一遍：原先 `_data_rows` 先找出行号、再从同一批单元格取一遍值，5 万行的表
    因此被走两遍。顺带在这里拒公式——标识列是公式时读到的是 `=CONCAT(...)`，
    整行会静默落进 missing，看起来像「这批 id 都认不出来」。
    """
    found: list[tuple[int, str]] = []
    for row in range(header_row + 1, worksheet.max_row + 1):
        value = read_value(worksheet, row, key_column)
        if value in (None, ""):
            continue
        found.append((row, str(value).strip()))
    return found


def read_value(worksheet: Any, row: int, column: int) -> Any:
    """读一个单元格；是公式就报错。

    输入表是「对方给的表」，里面常有算出来的列。公式没有缓存值时读到的是公式文本
    （`=CONCAT(A2,"/",B2)`），有缓存值时读到的是上次打开时的旧值——两种都不能当数用，
    而前者更坏：它会一路传进 `resolve_ids`，整行静默落进 missing，报出来的却是
    「这些 id 认不出来」，与真正的原因隔了两层。
    """
    cell = worksheet.cell(row=row, column=column)
    if cell.data_type == "f":
        raise HeaderError(
            f"{worksheet.title} 的 {cell.coordinate} 是公式，读不到真实值；"
            "请另存一份「只有值」的副本再交付"
        )
    return cell.value


def _row_values(worksheet: Any, row: int) -> list[Any]:
    """某一行的原始值，按列序，直到 `max_column`。表头里同样不许有公式。"""
    return [read_value(worksheet, row, column) for column in range(1, worksheet.max_column + 1)]
