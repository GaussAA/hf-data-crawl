"""底库 74 列 → 四桶划分，并当场核对 spec 声明的列正好是「该拼的那些」。

为什么要有这份脚本
------------------
「把能拿到的字段都拼进来」是句人话，落到 `spec.yaml` 必须是几十条明确的规则。
手抄一份列名清单，底库一加列、一补采卡片，它就悄悄过期了；而交付档案里钉住的
是那份过期清单。这里每次现算，并把「拼 / 不拼」的判据摊成看得见的四桶：

  A 原表已有对应列（id / url）           ── 拼进来就是重复
  B 平台与管道记账                       ── 描述「我们怎么采的」，不是数据集
  C 这批 N 行整列为空                    ── 拼进来只多一片空格
  D 其余                                 ── 全部拼进来

核对不过就退出码 1：spec 与底库对不上时，先改 spec，再交付。

用法（在仓库根目录）：
    .venv\\Scripts\\python.exe requests\\REQ-0004-*/method/scripts/columns.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import openpyxl
import polars as pl

from hf_data_crawl.delivery.backfill import parse_reference
from hf_data_crawl.delivery.requirement import load as load_request
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.query.gold import GoldCatalog

REQUEST_DIR = Path(__file__).resolve().parents[2]
REPO_ROOT = REQUEST_DIR.parents[1]

#: A 桶：原表已经有这两列（序号 / 名称 / URL），再拼一遍就是同义重复。
ALREADY = ("id", "url")

#: B 桶：平台标记与管道记账。判据是「它描述数据集本身，还是描述我们怎么采的」——
#: 同一套判据写在 `delivery/fields.py` 的四条取舍里，这里只列名单。
PIPELINE = (
    "first_seen_at",
    "last_seen_at",
    "status",
    "private",
    "disabled",
    "xet_enabled",
    "resource_group",
    "fetched_at",
    "size_status",
    "files_status",
    "truncated",
    "card_source_sha",
)


def read_ids(path: Path, sheet: str | None, header_row: int, key_column: str) -> list[str]:
    """从输入快照读出标识列并归一成 `作者/名称`，与回填引擎同一套解析。"""
    # 不开 read_only：只读模式下按行号取表头不可靠（行索引要另建），而这张表只有 28 行。
    workbook = openpyxl.load_workbook(path)
    worksheet = workbook[sheet] if sheet else workbook[workbook.sheetnames[0]]
    header = [
        str(cell.value).strip() if cell.value is not None else "" for cell in worksheet[header_row]
    ]
    column = header.index(key_column) + 1
    ids: list[str] = []
    for row in range(header_row + 1, worksheet.max_row + 1):
        raw = worksheet.cell(row=row, column=column).value
        if raw in (None, ""):
            continue
        ids.append(parse_reference(str(raw).strip()))
    return ids


def filled(frame: pl.DataFrame, column: str) -> int:
    """这一列在这批行里有几行「有内容」。

    「有内容」不等于非 NULL：列表列要非空、文本列排除空串。空列表在这套库里表示
    「问过、答了没有」，与 NULL 一样不可用——照 `count(col)` 算会把整列为空算成 100%。
    """
    series = frame[column]
    if series.dtype.is_nested():
        return sum(1 for item in series.to_list() if item)
    if series.dtype == pl.String:
        return sum(1 for item in series.to_list() if item is not None and str(item).strip())
    return int(series.is_not_null().sum())


#: 有值但不拼的列：各有理由，且理由能用数据当场复核（复核见 `_evidence`）。
#: 「D 桶全拼」是默认，破例这三列，是因为它们要么塞不进一格，要么与已拼的列
#: 逐行同一个数——重复两列挨着放，读表的人只会问「到底信哪个」。
SKIPPED: dict[str, str] = {
    "server_configs": "逐 config 的明细 JSON，一格塞不下也读不了；要看明细去底库同名列表",
    "server_num_bytes_original": "与 server_num_bytes_parquet 逐行同值（同批实测），重复",
    "total_bytes": "与 main_size 逐行同值（除 1 个文件树截断的库），重复",
}


def _evidence(frame: pl.DataFrame) -> list[str]:
    """把 SKIPPED 里「重复」的断言当场验一遍，不靠人记得当时比过。"""
    lines: list[str] = []
    pairs = {
        "server_num_bytes_original": "server_num_bytes_parquet",
        "total_bytes": "main_size",
    }
    for left, right in pairs.items():
        both = frame.filter(pl.col(left).is_not_null() & pl.col(right).is_not_null())
        same = both.filter(pl.col(left) == pl.col(right)).height
        lines.append(
            f"  {left} == {right}：{same}/{both.height} 行（都为空的 {frame.height - both.height} 行不计）"
        )
    return lines


def main() -> int:
    spec = load_request(REQUEST_DIR / "spec.yaml")
    fill = spec.fill
    if fill is None:
        print("这不是 backfill 需求，本脚本无从核对")
        return 1

    ids = read_ids(
        REQUEST_DIR / fill.source.relative,
        fill.source.sheet,
        fill.source.header_row,
        fill.source.key_column,
    )
    catalog = GoldCatalog(DatasetStore(REPO_ROOT / "data"))
    frame = catalog.run(
        "SELECT * FROM datasets WHERE id IN (" + ",".join("?" * len(ids)) + ")", ids
    )
    if frame.height != len(ids):
        print(f"[!] 底库命中 {frame.height} 行，输入表 {len(ids)} 行——先查缺了哪些 id")
        return 1

    counts = {column: filled(frame, column) for column in frame.columns}
    buckets: dict[str, list[str]] = {"A": list(ALREADY), "B": list(PIPELINE), "C": [], "D": []}
    declared = {item.field for item in fill.columns if item.kind == "field"}
    templates = [item.name for item in fill.columns if item.kind != "field"]
    for column in frame.columns:
        if column in buckets["A"] or column in buckets["B"]:
            continue
        buckets["C" if counts[column] == 0 else "D"].append(column)

    covered = 74 == len(buckets["A"]) + len(buckets["B"]) + len(buckets["C"]) + len(buckets["D"])
    print(
        f"底库 {len(frame.columns)} 列 → A {len(buckets['A'])} · B {len(buckets['B'])} · "
        f"C {len(buckets['C'])} · D {len(buckets['D'])}（四桶互斥且合计等于总数：{covered}）"
    )

    print("\n## D 桶：拼进来的列（覆盖率 = 这 28 行里有内容的比例）")
    for column in buckets["D"]:
        if column in SKIPPED:
            print(f"  {column:24s} {counts[column]:2d}/{len(ids)}  × 不拼：{SKIPPED[column]}")
            continue
        mark = "✓" if column in declared else "[!] spec 没声明"
        print(f"  {column:24s} {counts[column]:2d}/{len(ids)}  {mark}")
    print("\n## 不拼的理由复核（D 桶里的破例项）")
    print("\n".join(_evidence(frame)))

    print("\n## C 桶：这批整列为空，不拼")
    print("  " + "、".join(buckets["C"]))
    print("\n## B 桶：平台与管道记账，不拼")
    print("  " + "、".join(buckets["B"]))
    print("\n## A 桶：原表已有，不拼")
    print("  " + "、".join(buckets["A"]))
    if templates:
        print("\n## 非底库列的派生列（模板 / 常量 / 判读）")
        print("  " + "、".join(templates))

    problems: list[str] = []
    if not covered:
        problems.append("四桶合计与底库列数不一致")
    for column in buckets["D"]:
        if column in SKIPPED:
            continue
        if column not in declared:
            problems.append(f"底库有值但 spec 没拼：{column}")
    for column in declared:
        if column not in buckets["D"]:
            problems.append(f"spec 声明了 {column}，但它不在 D 桶（无值 / 属 A、B、C）")
        elif column in SKIPPED:
            problems.append(f"spec 声明的 {column} 在「不拼」名单里，两者只能留一个")

    if problems:
        print("\n[!] spec 与底库对不上：")
        for item in problems:
            print(f"  - {item}")
        return 1
    print(
        f"\n[ok] spec 声明的 {len(declared)} 个底库列 == D 桶 − 不拼 {len(SKIPPED)} 列；"
        "交付前口径与底库一致"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
