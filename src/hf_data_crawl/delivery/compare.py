"""两份交付的逐列比对：这次变更究竟动了哪些格。

为什么要有
----------
`manifest` 只记「这次多少行」。「行数没变」不等于「结果没变」：口径从 `> 1000` 改成
`> 800`，行数完全可能一样；回填换了一版表，行数更是必然一样。没有逐列比对，
「变更影响面」只能靠人打开两份 Excel 肉眼比。

比什么、按什么对齐
------------------
- 查询型：按 `id` 对齐（每张表都有它，且交付档案里它就是唯一键）。
- 回填型：按标识列对齐。产物里那一列是原始链接，归一成 `作者/名称` 再比——
  与引擎匹配底库的口径一致，否则 `https://huggingface.co/datasets/a/b` 与
  `a/b` 会被当成两个不同的键。
- 只比**两侧都有的列**；列集不同单独报出来，不静默取交集。

读完即弃
--------
比对是诊断动作，不是交付的一部分：不写任何文件，也不进 `runs/`。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import openpyxl
import polars as pl

from .backfill import FillError, parse_reference
from .delivery import read_manifest
from .header import HeaderError
from .requirement import MANIFEST_NAME, Kind
from .sheet import read_header

#: 每列给出的样例条数。再多就成了又一份表，人不会看。
SAMPLE_SIZE = 3


class CompareError(ValueError):
    """两份交付没法比：形态不同、产物缺失、列类型对不上。"""


@dataclass(frozen=True)
class ColumnDiff:
    """一列的改值情况。`samples` 是 (键, 旧值, 新值)。"""

    name: str
    changed: int
    samples: tuple[tuple[str, str, str], ...] = ()


@dataclass(frozen=True)
class Comparison:
    kind: str
    left: str
    right: str
    left_rows: int
    right_rows: int
    added: tuple[str, ...]
    removed: tuple[str, ...]
    left_only_columns: tuple[str, ...]
    right_only_columns: tuple[str, ...]
    columns: tuple[ColumnDiff, ...]

    @property
    def common_rows(self) -> int:
        return self.left_rows - len(self.removed)

    @property
    def changed_cells(self) -> int:
        return sum(item.changed for item in self.columns)


def _manifest(directory: Path) -> dict[str, Any]:
    """读交付的 manifest；「缺」与「坏」分开报，处置不同。

    判据与文件名都来自 `delivery.read_manifest`——这里只负责把 None 翻译成带
    目录名的 `CompareError`。原先自己拼 `"manifest.json"`，档案改名不会有谁
    通知这个文件，比对会在一个「明明存在」的目录上报「没有 manifest」。
    """
    body = read_manifest(directory)
    if body is not None:
        return body
    if not (directory / MANIFEST_NAME).is_file():
        raise CompareError(f"{directory.name} 没有 {MANIFEST_NAME}，不是一次交付")
    raise CompareError(f"{directory.name} 的 {MANIFEST_NAME} 读不出来或顶层不是对象")


def compare_deliveries(left_dir: Path, right_dir: Path) -> Comparison:
    """比两份交付目录。形态不同直接拒绝——「换了形态」不是口径变更，是另一件事。

    分派是**穷尽**的：新加一种形态时这里会当场报错，而不是静默掉进最后那条 `else`
    拿 parquet 的口径去比一张 xlsx。想让形态只在一处登记（`Form(execute, compare)`）
    走不通：比对器住在本模块，而执行体已被 `delivery` 点名导入，互相 import 会成环。
    取舍写在 GLOSSARY #105。
    """
    left_body, right_body = _manifest(left_dir), _manifest(right_dir)
    left_kind, right_kind = left_body.get("kind"), right_body.get("kind")
    if left_kind != right_kind:
        raise CompareError(f"两份交付形态不同（{left_kind} → {right_kind}），无从逐列比较")
    try:
        kind = Kind(str(left_kind))
    except ValueError as error:
        # manifest 是档案的事实来源；认不出的 kind 说明有人手改过或版本不匹配，
        # 两种都不该猜。
        raise CompareError(f"manifest 里的形态认不出来：{left_kind!r}") from error

    if kind is Kind.BACKFILL:
        return _compare_workbooks(left_body, right_body, left_dir, right_dir)
    if kind is Kind.QUERY:
        return _compare_frames(left_dir, right_dir)
    raise CompareError(f"{kind.value} 型交付还没有比对实现")


# ---------------------------------------------------------------- 查询型


def _frame(directory: Path) -> pl.DataFrame:
    parquet = directory / "data.parquet"
    if parquet.is_file():
        return pl.read_parquet(parquet)
    csv = directory / "data.csv"
    if csv.is_file():
        # 全按字符串读：CSV 是展示形态，`001` 与 `1` 本来就不是同一个东西。
        return pl.read_csv(csv, infer_schema_length=0)
    raise CompareError(f"{directory.name} 里没有 data.parquet / data.csv")


def _comparable(frame: pl.DataFrame, name: str) -> pl.Expr:
    """把一列摊成可比形式：列表先合并，其余转字符串，空值统一成空串。

    两处必须这么办：① 直接拿 List 列做 `!=`，只要列表里含空元素，比较结果就可能是
    null——「两侧都没写过」会被算成「变了」；② 空列表与空值在存储上是两回事、在
    含义上是同一件事（都没值），不统一就会满屏假差异。实测 23 行 × 30 列里凭空冒出
    79 格「改值」，就是这么来的。
    """
    expr = (
        # 先逐元素转字符串再 join：整列为空时 dtype 会退化成 `List(Null)`，
        # 直接 `list.join` 会报「list join with non-string dtype」。
        pl.col(name).list.eval(pl.element().cast(pl.Utf8)).list.join("\u001f")
        if isinstance(frame.schema[name], pl.List)
        else pl.col(name).cast(pl.Utf8)
    )
    return expr.fill_null("")


def _show(value: Any) -> str:
    """样例里的取值写法：把「空值」与「空列表」分开，否则样例全是「（空）→（空）」。"""
    if value is None:
        return "（空）"
    if isinstance(value, list | tuple):
        return "〔" + "、".join(_text(item) for item in value) + "〕" if value else "〔空列表〕"
    text = _text(value)
    return text if text else "（空）"


def _compare_frames(left_dir: Path, right_dir: Path) -> Comparison:
    left, right = _frame(left_dir), _frame(right_dir)
    for frame, side in ((left, "旧"), (right, "新")):
        if "id" not in frame.columns:
            raise CompareError(f"{side}交付没有 id 列，无法按主键对齐")

    left_ids = set(left["id"].to_list())
    right_ids = set(right["id"].to_list())
    added = tuple(sorted(str(item) for item in right_ids - left_ids))
    removed = tuple(sorted(str(item) for item in left_ids - right_ids))
    common = left_ids & right_ids

    left_columns = [name for name in left.columns if name != "id"]
    right_columns = [name for name in right.columns if name != "id"]
    shared = [name for name in left_columns if name in set(right_columns)]

    joined = (
        left.filter(pl.col("id").is_in(list(common)))
        .sort("id")
        .join(right.filter(pl.col("id").is_in(list(common))), on="id", how="inner", suffix="__r")
    )
    diffs: list[ColumnDiff] = []
    for name in shared:
        other = f"{name}__r"
        try:
            pair = joined.select(
                _comparable(joined, name).alias("a"), _comparable(joined, other).alias("b")
            )
        except pl.exceptions.InvalidOperationError as error:
            raise CompareError(f"列 {name} 两版类型不一致，无法逐格比较：{error}") from None
        # 两侧的「没值」（空值 / 空列表）已经统一成空串，余下的差异都是真的。
        mask = pair["a"] != pair["b"]
        changed = int(mask.sum())
        samples: tuple[tuple[str, str, str], ...] = ()
        if changed:
            rows = joined.filter(mask).select("id", name, other).head(SAMPLE_SIZE).rows()
            samples = tuple((str(key), _show(old_v), _show(new_v)) for key, old_v, new_v in rows)
        diffs.append(ColumnDiff(name=name, changed=changed, samples=samples))

    return Comparison(
        kind="query",
        left=left_dir.name,
        right=right_dir.name,
        left_rows=left.height,
        right_rows=right.height,
        added=added,
        removed=removed,
        left_only_columns=tuple(name for name in left_columns if name not in set(right_columns)),
        right_only_columns=tuple(name for name in right_columns if name not in set(left_columns)),
        columns=tuple(diffs),
    )


# ---------------------------------------------------------------- 回填型


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list | tuple):
        return "、".join(_text(item) for item in value)
    return str(value).strip()


def _identifier(value: Any) -> str:
    """对齐用的键：链接归一成 `作者/名称`，认不出来就原样留着。"""
    text = _text(value)
    if not text:
        return ""
    try:
        return parse_reference(text)
    except FillError:
        return text


def _cells(directory: Path, body: dict[str, Any]) -> tuple[dict[str, dict[str, str]], list[str]]:
    """把产物读成 {标识: {列名: 值}} 与列名顺序。只读数据页，不碰「补全说明」。"""
    files = (body.get("result") or {}).get("files") or []
    names = [item.get("name") for item in files if isinstance(item, dict)]
    product = next(
        (directory / str(name) for name in names if str(name).endswith((".xlsx", ".xlsm"))), None
    )
    if product is None or not product.is_file():
        raise CompareError(f"{directory.name} 里找不到产物表格")

    fill = body.get("fill") or {}
    source = fill.get("source") or {}
    key_column = str(source.get("key_column") or "")
    header_row = int(source.get("header_row") or 1)
    source_sheet = source.get("sheet")

    # 读的是**自己的产物**，不是对方给的输入表：产物是交付出去的文件，人可能手工动过，
    # 所以取缓存值（`data_only=True`）而不是公式文本。输入表那侧相反，见 `sheet.read_value`。
    workbook = openpyxl.load_workbook(product, data_only=True)
    sheet = (
        workbook[source_sheet] if source_sheet in workbook.sheetnames else workbook.worksheets[0]
    )
    try:
        parsed = read_header(sheet, header_row=header_row)
    except HeaderError as error:
        raise CompareError(f"{directory.name} 的产物表头读不出来：{error}") from error
    if key_column not in parsed.columns:
        raise CompareError(f"{directory.name} 的产物里没有标识列 {key_column!r}")
    key_index = parsed.columns[key_column]

    rows: dict[str, dict[str, str]] = {}
    for row in range(header_row + 1, sheet.max_row + 1):
        key = _identifier(sheet.cell(row=row, column=key_index).value)
        if not key:
            continue
        # 归一化会把 `https://huggingface.co/datasets/a/b` 与 `a/b` 压成同一个键。
        # 静默后写覆盖前写时，丢掉的是一整行数据，而 `left_rows` 是按字典长度报的
        # ——报 100 行、实际 101 行，两边都不对，且没有任何信号。宁可报错让人看表。
        if key in rows:
            raise CompareError(
                f"{directory.name} 的标识列归一后出现重复：{key!r}"
                f"（第 {row} 行与前面某行指向同一个库，可能是两种 URL 写法）"
            )
        rows[key] = {
            name: _text(sheet.cell(row=row, column=column).value)
            for name, column in parsed.columns.items()
        }
    return rows, list(parsed.names)


def _compare_workbooks(
    left_body: dict[str, Any], right_body: dict[str, Any], left_dir: Path, right_dir: Path
) -> Comparison:
    left, left_columns = _cells(left_dir, left_body)
    right, right_columns = _cells(right_dir, right_body)

    added = tuple(sorted(set(right) - set(left)))
    removed = tuple(sorted(set(left) - set(right)))
    shared = [name for name in left_columns if name in set(right_columns)]

    diffs: list[ColumnDiff] = []
    for name in shared:
        samples: list[tuple[str, str, str]] = []
        changed = 0
        for key in sorted(set(left) & set(right)):
            before, after = left[key].get(name, ""), right[key].get(name, "")
            if before == after:
                continue
            changed += 1
            if len(samples) < SAMPLE_SIZE:
                samples.append((key, before, after))
        diffs.append(ColumnDiff(name=name, changed=changed, samples=tuple(samples)))

    return Comparison(
        kind="backfill",
        left=left_dir.name,
        right=right_dir.name,
        left_rows=len(left),
        right_rows=len(right),
        added=added,
        removed=removed,
        left_only_columns=tuple(name for name in left_columns if name not in set(right_columns)),
        right_only_columns=tuple(name for name in right_columns if name not in set(left_columns)),
        columns=tuple(diffs),
    )
