"""口径版本比对：两版 spec 到底差在哪。

为什么要有
----------
`request diff` 回答变更治理里最先被问、也最难答的一句「这次改了什么」。没有它，
人只能把两份 YAML 摊在屏幕上逐行看——而 YAML 里一半是注释与键序，眼睛必漏。

比对范围与指纹对齐
------------------
`spec_digest` 只算决定产物的那部分，这里也只比那部分：标题、提出人、用途、节奏
不进差异表，免得「改个错别字」被当成一版变更。但两侧真有这类改动时会单独提一句
——它们确实变了，只是不影响产物，不该混在口径差异里吓人。

对齐方式
--------
条件按 `metric` 对齐、回填列按列名对齐：插一条条件不该让后面每条都显示「变了」。
其余按位置比——`output.columns` 的顺序本身就是口径的一部分。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .requirement import Spec, spec_body

#: 只在一侧存在的叶子值。用一个独有对象而非 None：null 本身是合法取值。
_MISSING = object()

#: 单元格里过长内容的截断长度。SQL 与模板串整段铺开会把表撑坏。
_CELL_LIMIT = 48


@dataclass(frozen=True)
class Change:
    """一处差异。`path` 是人读路径，`old`/`new` 为 `_MISSING` 表示该侧没有。"""

    path: str
    old: Any
    new: Any


def _walk(into: dict[str, Any], path: str, value: Any) -> None:
    """把嵌套结构摊成「路径 → 叶子值」。标量列表并成一行，省得逐项铺。"""
    if isinstance(value, dict):
        for key, item in value.items():
            _walk(into, f"{path}.{key}" if path else str(key), item)
        return
    if isinstance(value, list):
        if all(isinstance(item, str | int | float | bool) for item in value):
            into[path] = "、".join(str(item) for item in value) if value else "（空）"
            return
        for index, item in enumerate(value):
            _walk(into, f"{path}[{index}]", item)
        return
    into[path] = value


def leaves(spec: Spec) -> dict[str, Any]:
    """把一份口径摊平。条件与回填列按名对齐，其余按路径。"""
    body = spec_body(spec)
    out: dict[str, Any] = {}
    for key, value in body.items():
        if key == "criteria":
            for item in value:
                out[f"criteria[{item['metric']}].op"] = item["op"]
                out[f"criteria[{item['metric']}].value"] = item["value"]
            continue
        if key == "order_by":
            for item in value:
                out[f"order_by[{item['metric']}]"] = "降序" if item["descending"] else "升序"
            continue
        if key == "fill":
            for field, content in value.items():
                if field != "columns":
                    _walk(out, f"fill.{field}", content)
                    continue
                for column in content:
                    name = column["name"]
                    for attr, cell in column.items():
                        # 默认值（None/False/空串）不入表：它们只是「没写」，
                        # 写全了再比，差异表里会全是噪声。
                        if attr == "name" or cell in (None, False, ""):
                            continue
                        out[f"fill.columns[{name}].{attr}"] = cell
            continue
        _walk(out, key, value)
    return out


def compare(old: Spec, new: Spec) -> list[Change]:
    """两版口径的差异，按路径首次出现的顺序。"""
    before, after = leaves(old), leaves(new)
    order = list(before) + [path for path in after if path not in before]
    changes: list[Change] = []
    for path in order:
        left = before.get(path, _MISSING)
        right = after.get(path, _MISSING)
        if left != right:
            changes.append(Change(path=path, old=left, new=right))
    return changes


def side_notes(old: Spec, new: Spec) -> list[str]:
    """不影响产物、但确实变了的字段。与口径差异分开说。"""
    pairs = (
        ("标题", old.title, new.title),
        ("提出人", old.requester, new.requester),
        ("用途", old.purpose, new.purpose),
        ("交付节奏", old.cadence.value, new.cadence.value),
    )
    return [f"- {label}：{before} → {after}" for label, before, after in pairs if before != after]


def cell(value: Any) -> str:
    """单元格文本：缺值说「无」，长文本截断，换行压平成一行。"""
    if value is _MISSING:
        return "（无）"
    if value is True:
        return "是"
    if value is False:
        return "否"
    text = str(value).replace("\r", " ").replace("\n", " ").strip()
    if not text:
        return "（空）"
    return text if len(text) <= _CELL_LIMIT else text[: _CELL_LIMIT - 1] + "…"


def table(changes: list[Change]) -> list[str]:
    """Markdown 表：项目 / 旧版 / 新版。"""
    lines = ["| 项目 | 旧版 | 新版 |", "| --- | --- | --- |"]
    lines += [f"| `{item.path}` | {cell(item.old)} | {cell(item.new)} |" for item in changes]
    return lines
