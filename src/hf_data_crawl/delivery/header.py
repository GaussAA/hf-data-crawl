"""交付档案里的表头：解析、指纹与差异。

三件事同住，因为它们谈的是同一个概念——**表头 = 有序列名 + 列位置**：
`parse` 把表头那一行的原始值变成 `Header`，`header_digest` / `header_change`
在这个值上算指纹与差异。

`header_digest` / `header_change` 原先住在 `backfill.py`，只因「表头」这个概念最早
出现在回填。可档案层也要用它比「这次的输入表和上次一样吗」——于是档案层不得不
import 回填模块，只为了一个纯函数。搬到独立模块后，谁都能用，谁都不必认识回填。

**这里不含 openpyxl**：`parse` 吃的是「单元格的值」，不是单元格本身。开文件、选
sheet、拒公式都在 `delivery/sheet.py`——否则 `delivery.py`（档案层）会顺着这条
import 传递依赖 Excel，而 GLOSSARY #86 刚把档案层从「认形态」里摘出来。
"""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any


class HeaderError(ValueError):
    """表头读不出来：重名列、空表头，或值根本不在单元格里（公式）。

    定义在纯核里而不是某个执行体里：读表头与「这次是 SQL 还是 Excel」无关。
    回填与比对各自接住、翻成自己的 `FillError` / `CompareError`。
    """


@dataclass(frozen=True)
class Header:
    """一行表头解析出来的东西：列名顺序，以及列名到 Excel 列号（1 起）。"""

    names: tuple[str, ...]
    columns: dict[str, int]


def parse(values: Sequence[Any]) -> Header:
    """表头行的原始值 → `Header`，按列序。

    空列名跳过（占位列不该参与比对）；重名直接报错——两个「下载量」谁是谁，没有任何
    下游能猜对，而猜错的后果是把数值写进错误的列。
    """
    names: list[str] = []
    columns: dict[str, int] = {}
    for position, value in enumerate(values, start=1):
        if value is None:
            continue
        name = str(value).strip()
        if not name:
            continue
        if name in columns:
            raise HeaderError(
                f"表头有重名列 {name!r}（第 {columns[name]} 列与第 {position} 列），无法定位"
            )
        columns[name] = position
        names.append(name)
    if not names:
        raise HeaderError("表头是空的：整行没有列名")
    return Header(names=tuple(names), columns=columns)


def header_digest(columns: tuple[str, ...]) -> str:
    """表头指纹：列名与顺序。加一列、删一列、换个次序，指纹都会变。"""
    return hashlib.sha256("\n".join(columns).encode("utf-8")).hexdigest()


def header_change(old: tuple[str, ...], new: tuple[str, ...]) -> dict[str, Any]:
    """两份表头的差异：加列、减列、共同列是否挪了位。

    不试图给出「最小移动序列」：人要的是「哪几列不一样」，不是「怎么挪最省」，
    后者算得再准也没人按它搬。
    """
    old_set, new_set = set(old), set(new)
    common_old = [name for name in old if name in new_set]
    common_new = [name for name in new if name in old_set]
    return {
        "added": [name for name in new if name not in old_set],
        "removed": [name for name in old if name not in new_set],
        "reordered": common_old != common_new,
    }
