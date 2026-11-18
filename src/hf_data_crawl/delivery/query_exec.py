"""查询形态的执行体：`spec` → 一条 SQL → 一张表 → 一份 `Payload`。

为什么单独一个模块
------------------
这段原先住在 `delivery.py` 的「执行体：查询」一节，与「档案怎么落盘、README 怎么写」
挤在同一个文件里。它谈的全是 SQL、词表与字段契约，跟档案无关；放在档案层里，读者
要翻过四百行编排才找得到「口径是怎么变成 SQL 的」，而要加一种新形态又得往同一个
文件再塞两百行。

现在档案层只认 `Payload`（见 `contract.py`），本模块只认 `Spec` 与 `GoldCatalog`。
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from ..platform.atomic import write_csv, write_parquet
from ..query.gold import GoldCatalog, column_coverage, filled_count, share_text
from .contract import EXECUTORS, DeliveryBlocked, Payload, RunContext
from .fields import (
    COLUMN_NOTES,
    CONTRACT_VERSION,
    DEFAULT_COLUMNS,
    DISPLAY_COLUMNS,
    EXTENDED_FIELDS,
    EXTENDED_NOTE,
    GROUPS,
    PAGE_FIELDS,
    display_expr,
)
from .metrics import lookup
from .requirement import Kind, Mode, OutputFormat, Spec, compile_sql
from .stamp import file_stamp


def payload(ctx: RunContext) -> Payload:
    """跑这条口径，产出交付物与它在档案里的两段说明。"""
    frame = query(ctx.spec, ctx.catalog)
    definition = definition_of(ctx.spec, frame.columns)
    lines = detail_lines(ctx.spec, frame, ctx.catalog)
    if ctx.dry_run:
        # 预演不写盘：只报行列数，`files` 留空。档案层据此也能跑通同一条路径。
        return Payload(
            rows=frame.height,
            columns=tuple(frame.columns),
            definition=definition,
            detail_lines=lines,
        )
    written = write_data(ctx.staging, ctx.spec, frame)
    return Payload(
        rows=frame.height,
        columns=tuple(frame.columns),
        files=written,
        definition=definition,
        detail_lines=lines,
    )


def definition_of(spec: Spec, result_columns: Sequence[str]) -> dict[str, Any]:
    """查询形态的机器读口径。`criteria` 带定义，交出去后不必回来翻词表。

    `mode: sql` 的列集由手写 SELECT 决定（`compile_sql` 原样透传，见该处注释）。
    这里若照旧写 `source=default` 并钉上 `CONTRACT_VERSION`，档案就会声称按 31 列
    默认契约投影、实际只有两列——下游照 projection 复原口径必然得出错误结论。
    手写 SQL 只记真实列集，不冒充契约。
    """
    criteria = [
        {
            "metric": item.metric,
            "field": lookup(item.metric).field,
            "op": item.op,
            "value": _jsonable(item.value),
            "definition": lookup(item.metric).definition,
        }
        for item in spec.criteria
    ]
    if spec.mode is Mode.SQL:
        return {
            "sql": compile_sql(spec),
            "projection": {"source": "sql", "columns": list(result_columns)},
            "criteria": criteria,
        }
    using_default = not spec.output.columns
    return {
        "sql": compile_sql(spec),
        "projection": {
            "source": "default" if using_default else "spec",
            "default_columns_version": CONTRACT_VERSION,
            "columns": list(DEFAULT_COLUMNS) if using_default else list(spec.output.columns),
        },
        "criteria": criteria,
    }


def detail_lines(
    spec: Spec,
    frame: pl.DataFrame,
    catalog: GoldCatalog | None = None,
) -> list[str]:
    """查询形态的人读口径：条件表、投影来源、以及默认契约下逐列是什么。

    `frame` 是本次结果，用来当真算「这次交出去的列有多少有内容」——交付方拿到
    一份全空的 `server_*` 会以为数据坏了，而管线里没有任何地方会告诉他这是
    「size 还没采集」。`catalog` 用来算扩展字段的**全库**覆盖：那是底库属性，
    与本次筛选无关，不能拿结果的覆盖率顶替。
    """
    lines: list[str] = []
    if spec.mode is Mode.SQL:
        lines += ["手写 SQL：", "", "```sql", (spec.sql or "").strip(), "```", ""]
    else:
        lines += ["| 词条 | 落成字段 | 条件 | 定义 |", "| --- | --- | --- | --- |"]
        for item in spec.criteria:
            metric = lookup(item.metric)
            lines.append(
                f"| {item.metric} | `{metric.field}` | {item.op} {item.value} | {metric.definition} |"
            )
        lines.append("")
    if spec.output.format is not OutputFormat.PARQUET:
        lines.append(
            "CSV 里列表列以 `;` 连接成一格；`languages` / `task_categories` / "
            "`size_categories` 另按页面形式展平（如 `ko` → `Korean`）。"
            "要保留机器码与原始结构请用 parquet"
        )
    if not spec.output.columns and spec.mode is not Mode.SQL:
        lines += [
            "",
            f"### 默认交付字段（契约 v{CONTRACT_VERSION}，共 {len(DEFAULT_COLUMNS)} 列）",
            "",
            "未指定 `output.columns`，按 `data/exports/datasets.parquet` 的字段契约投影；",
            "该文件即默认参考底库，列序与此处完全一致。",
            "",
            "| 分组 | 字段 | 说明 |",
            "| --- | --- | --- |",
        ]
        for group, columns in GROUPS:
            for index, column in enumerate(columns):
                label = group if index == 0 else ""
                lines.append(f"| {label} | `{column}` | {COLUMN_NOTES[column]} |")
        lines += [
            "",
            "### 详情页标签 ↔ 库列",
            "",
            "页面标签行与库里列名不同、值也多为机器码，按此表对照；",
            "空值表示作者未在卡片里声明，不代表该数据集没有这一属性。",
            "",
            "| 页面 | 库列 | 值的显示形式 |",
            "| --- | --- | --- |",
        ]
        for label, columns, note in PAGE_FIELDS:
            refs = " / ".join(f"`{column}`" for column in columns)
            lines.append(f"| {label} | {refs} | {note} |")
        extended_coverage = (
            column_coverage(catalog, [column for column, _definition in EXTENDED_FIELDS])
            if catalog is not None
            else {}
        )
        lines += [
            "",
            "### 扩展字段（不进默认面，按需取用）",
            "",
            EXTENDED_NOTE,
            "",
            "| 字段 | 定义 | 全库覆盖 |",
            "| --- | --- | --- |",
        ]
        for column, definition in EXTENDED_FIELDS:
            share = extended_coverage.get(column)
            text = "—" if share is None else share_text(share)
            lines.append(f"| `{column}` | {definition} | {text} |")
        lines.append("")
    lines += _coverage_lines(frame)
    return lines


def _coverage_lines(frame: pl.DataFrame) -> list[str]:
    """本次结果的列覆盖：哪几列整列为空、哪几列几乎为空。

    判据来自 `query.gold.filled_count`，与 `column_coverage` 共用同一套分类
    （`fill_kind`）：非 NULL，且列表非空、文本非空串——空列表表示「问过、答了
    没有」，对使用者与空值同样不可用。原先这里自己抄了一条 dtype 分支，加一种
    类型时两边会各改一半。
    """
    if frame.height == 0:
        return [
            "",
            "### 本次输出的列覆盖",
            "",
            "结果为空（0 行），各列覆盖率无从谈起；先确认口径是否筛得过紧。",
            "",
        ]
    pairs: list[tuple[str, float]] = []
    for column in frame.columns:
        pairs.append((column, filled_count(frame[column]) / frame.height))

    empty = [column for column, share in pairs if share == 0]
    lowest = sorted((item for item in pairs if item[1] > 0), key=lambda item: item[1])[:5]
    lines = ["", "### 本次输出的列覆盖", ""]
    lines.append(
        f"- 行数 {frame.height:,}；{len(frame.columns)} 列中 "
        f"{len(frame.columns) - len(empty)} 列有内容。"
    )
    if empty:
        lines.append(f"- **整列为空**：{'、'.join(f'`{column}`' for column in empty)}")
    if lowest:
        lowest_text = " · ".join(f"`{column}` {share_text(share)}" for column, share in lowest)
        lines.append(f"- 覆盖最低：{lowest_text}")
    lines += [
        "",
        "「有内容」= 非 NULL 且列表非空。空值表示作者未声明，不代表该数据集没有这一属性。",
        "",
    ]
    return lines


def query(spec: Spec, catalog: GoldCatalog) -> pl.DataFrame:
    sql = compile_sql(spec)
    try:
        return catalog.run(sql)
    except duckdb.Error as error:
        # 包一层：裸的 BinderException 看不出是哪条需求、哪条 SQL 出的问题。
        raise DeliveryBlocked(f"{spec.id} 查询失败：{error}\nSQL：\n{sql}") from None


def write_data(directory: Path, spec: Spec, frame: pl.DataFrame) -> list[dict[str, Any]]:
    """写数据文件并算校验和。返回每个文件的元数据，按写入顺序。"""
    plans: list[tuple[str, str]] = []
    if spec.output.format is OutputFormat.CSV:
        plans.append(("data.csv", "csv"))
    elif spec.output.format is OutputFormat.BOTH:
        plans.extend([("data.parquet", "parquet"), ("data.csv", "csv")])
    else:
        plans.append(("data.parquet", "parquet"))

    written: list[dict[str, Any]] = []
    for name, kind in plans:
        target = directory / name
        # 原子写：交付产物是给人取走的文件，中断留下半截 parquet/csv 时对方会当成
        # 「这次交付的内容」读进去。`platform.atomic` 一直在，只是这里绕开了它。
        if kind == "csv":
            write_csv(_flat_for_csv(frame), target)
        else:
            write_parquet(frame, target)
        # 与回填产物、输入快照走同一份判据（`stamp.file_stamp`：原字节 + sha256）。
        # 这里曾内联一遍 sha256——同一份字节若有两套算法，两个数会写进档案的不同
        # 字段，谁都不会报错，只有比对时才发现「同一份东西，两处记的不一样」。
        written.append({"name": name, **file_stamp(target)})
    return written


def _flat_for_csv(frame: pl.DataFrame) -> pl.DataFrame:
    """CSV 不支持嵌套类型，列表列先拍平成 `;` 连接的字符串。

    宽表里有十余个列表列（`tags`、`languages`、`plain_tags`…），不拍平的话
    `write_csv` 直接抛 `CSV format does not support nested data`，一份 csv 都交不出去。
    选择 `;` 而非 JSON 数组：CSV 是给人拿 Excel 打开的，`a;b` 比 `["a","b"]`
    少一层引号转义，肉眼也认得出来。空列表留空串。

    语言 / 任务 / 规模三列额外走 `fields.display_expr` 换成页面形式（CSV 是交付给
    人读的）；parquet 与 SQL 结果仍保留机器码，便于筛算。映射用 Polars 原生表达式，
    不走逐格 Python 回调——大表交付不能被展示层拖慢。
    """
    exprs: list[pl.Expr] = []
    for name, dtype in frame.schema.items():
        if isinstance(dtype, pl.List):
            exprs.append(
                display_expr(name)
                if name in DISPLAY_COLUMNS
                else pl.col(name).list.join(";").alias(name)
            )
        else:
            exprs.append(pl.col(name))
    return frame.select(exprs)


def _jsonable(value: Any) -> Any:
    """条件值转成 JSON 可写的形态：日期与时间戳走 ISO，其余原样交给 `json`。

    这里**不**兜底 `str()`：遇到 JSON 写不了的类型就该当场抛，不能悄悄变成字符串
    写进档案——那会让人以为「原本就是文本」。
    """
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


EXECUTORS[Kind.QUERY] = payload
