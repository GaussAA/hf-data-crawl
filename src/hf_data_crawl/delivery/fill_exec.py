"""回填形态的执行体：URL 清单 → 填好的表格 → 一份 `Payload`。

为什么单独一个模块
------------------
这段原先住在 `delivery.py` 的「执行体：回填」一节。它谈的是表格、列规则、判读表，
与「档案怎么落盘」无关。搬出来之后，档案层不再需要认识回填的存在——连
`backfill.FillError` 也不必认识：执行失败在这里就翻译成 `DeliveryBlocked`。

（`backfill.py` 仍是执行引擎，本模块只管「引擎跑完之后，档案里该写什么」。）
"""

from __future__ import annotations

from typing import Any

from . import backfill
from .contract import EXECUTORS, DeliveryBlocked, Payload, RunContext
from .header import header_digest
from .requirement import Kind
from .stamp import file_stamp


def payload(ctx: RunContext) -> Payload:
    """跑回填，产出产物与它在档案里的两段说明。"""
    fill = ctx.spec.fill
    if fill is None:  # `_validate` 已挡住，这里只为把类型收窄
        raise DeliveryBlocked(f"{ctx.spec.id}: kind=backfill 却没有 fill 段")
    try:
        report = backfill.run(
            ctx.store,
            fill,
            target_dir=ctx.staging,
            request_id=ctx.spec.id,
            title=ctx.spec.title,
            catalog=ctx.catalog,
            source=ctx.source,
            dry_run=ctx.dry_run,
        )
    except (backfill.FillError, OSError) as error:
        # 在这里翻译，而不是让档案层捕获回填的异常类型：档案层一旦认识
        # `FillError`，加第三种形态时又得回去改它——「只认 Payload」就成了空话。
        raise DeliveryBlocked(f"{ctx.spec.id} 执行失败：{error}") from error
    input_stamp = file_stamp(fill.source.path)
    return Payload(
        rows=report.rows,
        columns=report.columns(),
        files=report.files,
        definition=_definition(fill, report, input_stamp),
        detail_lines=_lines(fill, report, ctx.dry_run, input_stamp),
        header=report.header,
    )


def _definition(
    fill: backfill.FillSpec, report: backfill.FillReport, input_stamp: dict[str, Any]
) -> dict[str, Any]:
    """回填形态的机器读口径：哪一列从哪来、这一轮实际写了多少。"""
    return {
        "fill": {
            # 输入快照的相对路径与指纹：目录搬走后仍能回答「基于哪一版清单」。
            # 表头单独记一份：只记 sha256 的话，换了一列确实对不上，可对不上在
            # 哪一列没人知道；列名与顺序记下来，「字段不一样了」才可核对。
            "input": {
                "path": fill.source.relative,
                **input_stamp,
                "header": {
                    "sha256": header_digest(report.header),
                    "columns": list(report.header),
                },
            },
            "source": {
                "path": fill.source.relative,
                "sheet": fill.source.sheet,
                "header_row": fill.source.header_row,
                "key_column": fill.source.key_column,
            },
            "output": fill.output_name,
            # 追加列单列一段：`input.header` 说的是对方那张表，`appended` 说的是
            # 我们往右加了什么。两者混成一份，「原表长什么样」就再也答不出来。
            "appended": list(report.appended),
            # 与 `input.path` 同口径：记 spec 里的相对路径，不记 basename。
            "overrides": fill.overrides_relative,
            # 判读表对不上账的部分也记进档案：写错库名或列名时，产物上只表现为
            # 「这一格空着」，与「没判」无法区分——只能靠这行字点名。
            "overrides_unmatched": {
                "ids": list(report.override_unknown_ids),
                "columns": list(report.override_unknown_columns),
            },
            "overwrite": fill.overwrite,
            "stamp_note": fill.stamp_note,
            "columns": [
                {
                    "name": item.name,
                    "kind": item.kind,
                    "field": item.field,
                    "template": item.template,
                    "const": item.const,
                    "reason": item.reason,
                    "format": item.fmt,
                }
                for item in fill.columns
            ],
            "written": report.written(),
            "tallies": {
                name: {
                    "written": item.written,
                    "from_override": item.from_override,
                    "kept": item.kept,
                    "empty": item.empty,
                }
                for name, item in report.tallies.items()
            },
            "missing_ids": report.missing_ids,
            "undeclared": report.undeclared,
            "manual": [{"column": name, "reason": reason} for name, reason in report.manual],
            "notes": [{"title": title, "text": text} for title, text in fill.notes],
        }
    }


def _lines(
    fill: backfill.FillSpec,
    report: backfill.FillReport,
    dry_run: bool,
    input_stamp: dict[str, Any],
) -> list[str]:
    """回填形态的人读口径：来源、规则、覆盖、以及「哪些是引擎故意没填」。"""
    digest = str(input_stamp["sha256"])[:16]
    lines = [
        f"- 形态：按 URL 清单回填表格（`kind: backfill`）{' · 预演，未写盘' if dry_run else ''}",
        f"- 输入快照：`{fill.source.relative}`（需求目录内，"
        f"{input_stamp['bytes']:,} 字节，sha256 `{digest}…`）",
        f"- 表结构：sheet {fill.source.sheet or '首个'}，"
        f"表头第 {fill.source.header_row} 行，标识列 `{fill.source.key_column}`",
        f"- 产物：`deliveries/<时刻>/{fill.output_name}`，只写在需求目录内；外部文件一律不动",
        f"- 取值：只填空格{'（本份允许覆盖已有内容）' if fill.overwrite else ''}；"
        "未命中底库的行整行留空；需要判读的列由 `overrides.json` 提供，引擎不猜",
        f"- 声明 {len(fill.columns)} 列，本轮写入 {report.written()} 格，"
        f"原有内容保留 {sum(item.kept for item in report.tallies.values())} 格，"
        f"空 {sum(item.empty for item in report.tallies.values())} 格",
    ]
    if report.appended:
        # 追加列是本次产物的结构变化，不是填充量变化；混进上面那行数字里，
        # 「表比上次多了几列」就没人看得见。
        lines.append(
            f"- 追加列 {len(report.appended)} 列（原表没有，写在最右侧，原表原有列一字未动）："
            + "、".join(f"`{name}`" for name in report.appended)
        )
    if report.override_unknown_ids:
        lines.append(
            f"- [!] 判读表里 {len(report.override_unknown_ids)} 个标识对不上这批清单"
            "（库名拼错或不在表里，这些值没有生效）："
            + "、".join(f"`{name}`" for name in report.override_unknown_ids[:8])
            + ("…" if len(report.override_unknown_ids) > 8 else "")
        )
    if report.override_unknown_columns:
        lines.append(
            f"- [!] 判读表里 {len(report.override_unknown_columns)} 个列名不在 spec 声明里"
            "（列名拼错，整列判读白写）："
            + "、".join(f"`{name}`" for name in report.override_unknown_columns[:8])
            + ("…" if len(report.override_unknown_columns) > 8 else "")
        )
    lines += [
        "",
        "### 逐列规则",
        "",
        "| 列名 | 来源 | 写入 | 人工判读 | 保留 | 空 |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for item in fill.columns:
        tally = report.tallies[item.name]
        origin = {
            "field": f"`{item.field}`"
            + (f" · {item.fmt}" if item.fmt else "")
            + (f" · 拼接 `{item.join}`" if item.join else ""),
            "template": f"模板 `{item.template}`",
            "const": f"常量 `{item.const}`",
            "manual": f"人工判读：{item.reason}",
        }[item.kind]
        lines.append(
            f"| {item.name}{'（新增）' if item.name in report.appended else ''} "
            f"| {origin} | {tally.written} | {tally.from_override} "
            f"| {tally.kept} | {tally.empty} |"
        )
    lines.append("")
    if report.manual:
        lines += ["### 需人工判读（引擎不猜）", ""]
        lines += [f"- **{name}**：{reason}" for name, reason in report.manual]
        lines.append("")
    if report.missing_ids:
        sample = "、".join(report.missing_ids[:8])
        tail = "" if len(report.missing_ids) <= 8 else f" 等 {len(report.missing_ids)} 条"
        lines += [f"### 未命中底库 {len(report.missing_ids)} 条", "", f"{sample}{tail}", ""]
    if report.undeclared:
        lines += [
            # 「保持空白」只说对了一半：对方表里未声明的列可能本来就有内容
            # （序号、名称、URL 这类），引擎一个都没碰——「不动」才是判据。
            "### 未声明的列（不填、原样保留）",
            "",
            "、".join(report.undeclared[:20]),
            "",
        ]
    if fill.notes:
        lines += ["### 口径补充说明", ""]
        lines += [f"- **{title}**：{text}" for title, text in fill.notes]
        lines.append("")
    if fill.stamp_note:
        lines += [f"> 数据来源说明：{fill.stamp_note}", ""]
    return lines


EXECUTORS[Kind.BACKFILL] = payload
