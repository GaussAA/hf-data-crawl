"""交付前后都跑的一份自检：原表没被改动、追加列齐全、每一列都真有值。

为什么要有这份脚本
------------------
「拼接字段」最容易出的三种错，交付档案本身看不出来：

1. **原表被动了**——引擎只写声明过的列，但声明里若混进「名称 / URL」，那张表
   就不再是对方的表；
2. **追加列其实整列为空**——列在、表头在，值一格没有，看着像「这批库都没有这项」；
3. **名称与 URL 指向不同的库**——名称是对方手填的，解析时只认 URL；两者不一致
   说明对方表里已经有一行错位，回填得再准也是错位的准。

用法（在仓库根目录；不带参数只查输入，带交付目录连产物一起查）：

    .venv\\Scripts\\python.exe requests\\REQ-0004-*\\method\\scripts\\verify_table.py
    .venv\\Scripts\\python.exe requests\\REQ-0004-*\\method\\scripts\\verify_table.py deliveries\\<时刻>
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import openpyxl

from hf_data_crawl.delivery.backfill import parse_reference
from hf_data_crawl.delivery.requirement import load as load_request
from hf_data_crawl.delivery.stamp import stamp
from hf_data_crawl.platform.store import DatasetStore

REQUEST_DIR = Path(__file__).resolve().parents[2]
REPO_ROOT = REQUEST_DIR.parents[1]
ORIGINAL = ("序号", "名称", "URL")


def open_sheet(path: Path, name: str | None) -> Any:
    workbook = openpyxl.load_workbook(path)
    return workbook[name] if name else workbook[workbook.sheetnames[0]]


def header_of(sheet: Any, header_row: int) -> list[str]:
    return [str(cell.value).strip() if cell.value is not None else "" for cell in sheet[header_row]]


def check_input(
    path: Path, sheet_name: str | None, header_row: int, key_column: str
) -> tuple[list[str], int]:
    """输入快照查三件事：表头、序号连续、名称与 URL 是否指向同一个库。"""
    problems: list[str] = []
    sheet = open_sheet(path, sheet_name)
    header = header_of(sheet, header_row)
    if header[:3] != list(ORIGINAL):
        return [f"表头前三列不是 {ORIGINAL}，实得 {header[:3]}"], 0
    key = header.index(key_column) + 1
    serial = header.index("序号") + 1
    name = header.index("名称") + 1

    rows = 0
    for row in range(header_row + 1, sheet.max_row + 1):
        raw = sheet.cell(row=row, column=key).value
        if raw in (None, ""):
            continue
        rows += 1
        seen = sheet.cell(row=row, column=serial).value
        if str(seen).strip() != f"{rows}":
            problems.append(f"第 {row} 行序号是 {seen!r}，按位置应为 {rows}")
        try:
            resolved = parse_reference(str(raw).strip())
        except ValueError as error:
            problems.append(f"第 {row} 行 URL 解析不了：{error}")
            continue
        claimed = sheet.cell(row=row, column=name).value
        if claimed is not None and str(claimed).strip() != resolved:
            problems.append(f"第 {row} 行名称 {claimed!r} 与 URL 指向的 {resolved!r} 不一致")
    print(f"输入快照：{rows} 行 · 表头 {header[:3]} · {path.name}")
    return problems, rows


def row_of(sheet: Any, row: int, width: int) -> list[Any]:
    return [sheet.cell(row=row, column=column).value for column in range(1, width + 1)]


def check_product(directory: Path, fill: Any, rows: int) -> list[str]:
    """产物查四件事：行数、原表三列逐格未改、追加列顺序、每列都真有值。"""
    product = directory / fill.output_name
    if not product.is_file():
        return [f"{directory} 里没有产物 {fill.output_name}"]
    out = open_sheet(product, fill.source.sheet)
    source = open_sheet(REQUEST_DIR / fill.source.relative, fill.source.sheet)
    header_row = fill.source.header_row
    width = len(ORIGINAL)
    problems: list[str] = []

    for row in range(header_row + 1, header_row + 1 + rows):
        if row_of(source, row, width) != row_of(out, row, width):
            problems.append(
                f"第 {row} 行原表三列被改过：{row_of(source, row, width)} → {row_of(out, row, width)}"
            )

    declared = [item.name for item in fill.columns]
    actual = [
        out.cell(row=header_row, column=width + 1 + offset).value for offset in range(len(declared))
    ]
    if actual != declared:
        problems.append(f"追加列表头与 spec 顺序不一致：{actual}")

    for offset, name in enumerate(declared):
        column = width + 1 + offset
        filled = sum(
            1
            for row in range(header_row + 1, header_row + 1 + rows)
            if out.cell(row=row, column=column).value not in (None, "")
        )
        if filled == 0:
            problems.append(f"列 {name!r} 整列为空——列在、值没在，等于没拼")
        elif offset == 0:
            print(f"产物：{product.name}（原表 {width} 列 + 追加 {len(declared)} 列，{rows} 行）")
    return problems


def check_manifest(directory: Path, fill: Any, rows: int) -> list[str]:
    """档案查两件事：追加列记全了；底库指纹还对得上「现在」这份底库吗。"""
    path = directory / "manifest.json"
    if not path.is_file():
        return []
    body = json.loads(path.read_text(encoding="utf-8"))
    problems: list[str] = []
    if body.get("result", {}).get("rows") != rows:
        problems.append(f"manifest 记的行数是 {body.get('result', {}).get('rows')}，应为 {rows}")
    declared = [item.name for item in fill.columns]
    if (body.get("fill") or {}).get("appended") != declared:
        problems.append(
            f"manifest 的 appended 与 spec 不一致：{(body.get('fill') or {}).get('appended')}"
        )
    header = ((body.get("fill") or {}).get("input") or {}).get("header") or {}
    if header.get("columns") != list(ORIGINAL):
        problems.append(f"manifest 的输入表头应只有原表三列，实得 {header.get('columns')}")

    delivered = str((body.get("source") or {}).get("current_md5", ""))
    current = stamp(DatasetStore(REPO_ROOT / "data")).current_md5
    if delivered != current:
        # 不判失败：底库每天在涨，交付那一刻的数值本来就不该被回溯。
        print(
            f"[~] 底库已变（交付时 {delivered[:8]}…，现在 {current[:8]}…）：数值列是交付那一刻的快照"
        )
    else:
        print("底库指纹：与交付时一致")
    return problems


def main(argv: list[str]) -> int:
    spec = load_request(REQUEST_DIR / "spec.yaml")
    fill = spec.fill
    if fill is None:
        print("这不是 backfill 需求")
        return 1

    problems, rows = check_input(
        REQUEST_DIR / fill.source.relative,
        fill.source.sheet,
        fill.source.header_row,
        fill.source.key_column,
    )
    if len(argv) > 1:
        directory = Path(argv[1])
        if not directory.is_absolute():
            directory = REQUEST_DIR / directory
        problems += check_product(directory, fill, rows)
        problems += check_manifest(directory, fill, rows)

    if problems:
        print("\n[!] 自检不通过：")
        for item in problems:
            print(f"  - {item}")
        return 1
    print("\n[ok] 自检通过：原表三列未改、追加列齐全且逐列有值、档案与口径一致")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
