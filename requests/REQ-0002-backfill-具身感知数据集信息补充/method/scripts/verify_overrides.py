"""交付前自检：判读表与输入清单、口径声明是否对得上。

**这不是当初判读过程的原件。** 那次判读是逐份读 README、看 datasets-server
splits 做的，在会话里一次性完成，没有留下脚本——这正是本需求当初缺的那一环，
写在这里是为了不假装它存在。

这份脚本解决另一个问题：结论表本身有没有错位。列名拼错、URL 不在清单里、
该人判的列被机械列顶替、声明了要人判却一格没填，这些都能在这里一次查出来。
它不联网、只读不写，所以每一版 `overrides.json` 都能随时复验。

用法（在仓库根目录）：

    .venv/Scripts/python.exe requests/<需求目录>/method/scripts/verify_overrides.py

有问题时退出码 1。
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import openpyxl
import yaml

from hf_data_crawl.backfill import FillError, parse_reference

#: method/scripts/ 往上数三层就是需求目录。不写死绝对路径：目录要能整体搬走。
REQUEST_DIR = Path(__file__).resolve().parents[2]


def load_spec() -> dict[str, Any]:
    path = REQUEST_DIR / "spec.yaml"
    body = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(body, dict):
        raise SystemExit(f"{path} 顶层不是映射")
    return body


def read_keys(fill: dict[str, Any]) -> list[str]:
    """按列名定位标识列（不按列号：对方插一列，列号就全错了）。"""
    source = fill["source"]
    path = REQUEST_DIR / source["path"]
    book = openpyxl.load_workbook(path, read_only=True, data_only=True)
    sheet = book[source["sheet"]] if source.get("sheet") else book.worksheets[0]
    header_row = int(source.get("header_row", 1))
    key_column = source["key_column"]
    rows = sheet.iter_rows(min_row=header_row, values_only=True)
    header = next(rows)
    names = [str(cell).strip() if cell is not None else "" for cell in header]
    if key_column not in names:
        raise SystemExit(f"{path} 第 {header_row} 行没有 {key_column!r} 列；实得 {names}")
    index = names.index(key_column)
    # 标识列的原文是完整链接（`https://huggingface.co/datasets/作者/名称`），
    # 判读表的键却是 `作者/名称`。这里直接复用引擎的解析函数，不另写一套正则：
    # 两套规则一旦分叉，结果是「每一行都匹配不到」，而报告只会说「未命中」。
    keys: list[str] = []
    for row in rows:
        raw = row[index]
        if raw in (None, ""):
            continue
        try:
            keys.append(parse_reference(str(raw)))
        except FillError as error:
            raise SystemExit(f"{path} 有一行标识解析不出来：{error}") from None
    book.close()
    return keys


def declared(fill: dict[str, Any]) -> dict[str, str]:
    """列名 → 取值方式。只认 spec 里声明过的那 21 列。"""
    out: dict[str, str] = {}
    for name, rule in fill["columns"].items():
        if not isinstance(rule, dict) or not rule:
            raise SystemExit(f"列 {name!r} 的规则不是映射")
        out[str(name)] = next(iter(rule))
    return out


def main() -> int:
    spec = load_spec()
    fill = spec["fill"]
    overrides: dict[str, dict[str, str]] = json.loads(
        (REQUEST_DIR / "overrides.json").read_text(encoding="utf-8")
    )

    keys = set(read_keys(fill))
    rules = declared(fill)
    problems: list[str] = []
    #: 机械列被人判结果覆盖了多少格。不是错误——判读本来就优先于机械值——
    #: 但要报出来：它意味着「引擎在这里不可信」，值得复核。
    overrode: dict[str, int] = {}

    for dataset_id, cells in overrides.items():
        if dataset_id not in keys:
            problems.append(f"{dataset_id} 不在输入清单里（URL 可能写错，或对方表已更新）")
        for column, value in cells.items():
            if column not in rules:
                problems.append(f"{dataset_id} 的 {column!r} 未在 spec 声明，写进去也不会被采用")
                continue
            if not str(value).strip():
                problems.append(f"{dataset_id} 的 {column!r} 是空值；空的就不该写进判读表")
            if rules[column] != "manual":
                overrode[column] = overrode.get(column, 0) + 1

    filled: dict[str, int] = {}
    for cells in overrides.values():
        for column in cells:
            filled[column] = filled.get(column, 0) + 1
    manual = [name for name, kind in rules.items() if kind == "manual"]
    untouched = [name for name in manual if name not in filled]

    print(f"需求目录：{REQUEST_DIR.name}")
    print(f"  输入清单 {len(keys)} 行 · 判读表 {len(overrides)} 库 / {sum(filled.values())} 格")
    print(
        f"  声明 {len(rules)} 列：机械 {sum(1 for value in rules.values() if value != 'manual')}"
        f" · 人判 {len(manual)}"
    )
    print(
        "  逐列判读格数：" + "、".join(f"{name} {count}" for name, count in sorted(filled.items()))
    )
    if untouched:
        print("  一格未判的人判列（README 未写明或底库缺该库，属正常）：" + "、".join(untouched))
    if overrode:
        print(
            "  机械列被人判结果覆盖："
            + "、".join(f"{name} {count}" for name, count in sorted(overrode.items()))
            + "（判读优先，但意味着这些列引擎不可信）"
        )
    if problems:
        print(f"\n发现 {len(problems)} 处问题：")
        for text in problems:
            print(f"  [!] {text}")
        return 1
    print("\n判读表与口径一致。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
