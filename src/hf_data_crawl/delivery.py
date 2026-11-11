"""交付：把一份需求跑成一份**能自证来源的档案**。

为什么不是「跑一条 SQL 存个文件」
--------------------------------
文件一旦离开这台机器，就只剩一份数据。三个月后对方问「这数从哪来的、怎么算的」，
没有档案就只能回来翻记录。所以每份交付固定三件：

- 产物本体（query 是 `data.parquet` / `data.csv`，backfill 是补全后的表格）
- `manifest.json`：机器读——底库指纹、口径、行数、列、sha256、健康标记
- `README.md`：人读——要什么、怎么算的、基于哪天的底库、几行

`manifest.source.current_md5` 是整份档案的锚点。底库每天都变，所以一份交付
只在「那一版底库」下成立；有这个 md5，任何一份历史交付都能回答它基于什么。

**不变与变**（这套体系的关键）
------------------------------
受理、定口径、按节奏跑、护栏、标疑、原子落盘、留档、可复现——对所有需求都一样，
全在本文件里走一条链。真正会变的只有「这次到底产出什么」，它被收进两处：

- `spec.kind`：形态的名字，只是一个枚举值；
- `EXECUTORS`：形态对应的执行体，产出统一的 `Payload`。

将来多一种交付形态（对账报表、按名单核验失效…），写一个执行体 + 注册一行即可，
不必再开一个目录、一套编号、一套命令。`Payload` 是执行体与档案层之间唯一的契约：
档案层只认「行数、列、产物清单、口径明细」，不认背后是 SQL 还是Excel。

三条硬规矩
----------
1. **0 行不交付**。空结果交出去没有意义，且通常意味着口径写错了。
2. **超上限不交付**。一条写错的需求不该把百万行倒给对方。
3. **判疑不阻断**。行数突变时照常交付，只在 manifest 与 README 顶部标记——
   「每周一给人一份」这种需求，当天交不出货比数字可疑更糟。但「这次不对劲」
   必须留在纸面上，不能靠人回忆上次是多少。

原子性：先写 `.tmp-*` 目录，三件齐了再整体改名。中途失败绝不留半份交付。

**自包含**：输入快照、口径、判读、历次交付**以及做法本身**（`method/`：步骤、
脚本原件、逐份指纹）全在需求目录内。产物绝不外写——一经外写，「哪一份才是正式的」
就说不清，而外部那份还会被随手改动。方法缺失时**不交付**：数据说不清来历，交出去
就永久补不回来了（见 `method.py`）。
"""

from __future__ import annotations

import hashlib
import json
import shutil
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from . import backfill, method
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
from .gold import GoldCatalog, GoldNotReady
from .metrics import lookup
from .platform.atomic import replace
from .platform.store import DatasetStore
from .requirement import (
    DELIVERIES_DIR,
    MANIFEST_NAME,
    README_NAME,
    SPEC_NAME,
    Cadence,
    Kind,
    Mode,
    OutputFormat,
    Spec,
    SpecError,
    compile_sql,
    resolve_dir,
    snapshot_history,
    spec_digest,
    version_conflict,
)
from .requirement import (
    load as load_spec,
)
from .source import SourceMissing, SourceStamp, file_stamp, stamp

#: 交付目录名的时刻格式。到秒：同一分钟内跑两个需求是常态。
_STAMP_FORMAT = "%Y%m%dT%H%M%S"


class DeliveryBlocked(RuntimeError):
    """按护栏不该交付（0 行、超上限、底库缺失、执行失败）。一律不落盘。"""


@dataclass(frozen=True)
class Payload:
    """执行体的产出，也是**执行体与档案层之间唯一的契约**。

    档案与 README 只认这个结构，不认「这次跑的是 SQL 还是回填」。它们交付的东西
    不一样，可「交付这件事」是一样的——这正是两类需求能合成一套体系的原因。
    """

    rows: int
    columns: tuple[str, ...]
    #: 产物元数据：name / bytes / sha256。预演时为空。
    files: list[dict[str, Any]] = field(default_factory=list)
    #: 写进 manifest 顶层的形态特有口径（query 的 sql/criteria、backfill 的 fill）。
    definition: dict[str, Any] = field(default_factory=dict)
    #: 写进 README「口径」段的若干行（markdown）。
    detail_lines: list[str] = field(default_factory=list)
    #: 输入表的列名（回填形态有；查询形态没有）。档案据此比对「这次的表和上次一样吗」。
    header: tuple[str, ...] = ()


@dataclass(frozen=True)
class Health:
    """与上一次交付相比，这次的结果是否可疑。

    `status` 只有四值：`first` 首次（无基线）、`ok` 正常、`suspect` 波动超阈值、
    `unknown` 上次的 manifest 读不出来（不比较，但要说出来，不能假装正常）。
    """

    status: str
    baseline_rows: int | None = None
    delta_pct: float | None = None
    delta_abs: int | None = None
    #: 上一次交付的口径版本；与本版不同时不比较行数（改了口径必然大变）。
    supersedes: int | None = None
    note: str = ""
    #: 输入表头与上一份交付的差异。变了不是错，但必须留在纸面上——字段悄悄换了
    #: 是这类需求最容易踩的坑，而它不会让任何数字变红。
    header_note: str = ""


@dataclass(frozen=True)
class DeliveryResult:
    request_id: str
    directory: Path
    rows: int
    columns: tuple[str, ...]
    files: tuple[dict[str, Any], ...]
    health: Health
    spec_version: int
    kind: Kind


@dataclass(frozen=True)
class Preview:
    """预演结果：只算不写，不建目录、不留档案，也不受护栏阻断。

    预演的用途恰恰是「看看会不会 0 行」，此时套护栏等于自相矛盾。
    """

    request_id: str
    kind: Kind
    rows: int
    columns: tuple[str, ...]
    lines: list[str]


@dataclass(frozen=True)
class RunContext:
    """执行体拿到的全部上下文。执行体不自己开 catalog、不算指纹、不定目录。"""

    store: DatasetStore
    spec: Spec
    staging: Path
    catalog: GoldCatalog
    source: SourceStamp
    dry_run: bool = False


#: 形态 → 执行体。**新形态只在这里加一行**，其余生命周期一律复用。
Executor = Callable[[RunContext], Payload]
EXECUTORS: dict[Kind, Executor] = {}


def _stamp(store: DatasetStore) -> SourceStamp:
    """把「底库不在」翻译成本模块的失败类型，调用方看到的语义不变。"""
    try:
        return stamp(store)
    except SourceMissing as error:
        raise DeliveryBlocked(str(error)) from None


def _catalog(store: DatasetStore, catalog: GoldCatalog | None) -> GoldCatalog:
    if catalog is not None:
        return catalog
    try:
        return GoldCatalog(store)
    except GoldNotReady as error:
        raise DeliveryBlocked(str(error)) from error


def _method_stamp(request_dir: Path, spec: Spec) -> dict[str, Any]:
    """交付前闸门：过程记录不合规就不出货。

    与 `min_rows` 同级，拦的理由却不同——0 行是「这份数据不可信」，缺方法是
    「这份数据说不清来历」。后者当下不影响收数，可一旦交出去就永久补不回来：
    三个月后再想补写，当时的判据早已忘了，剩下的只是复述结论。

    翻译成 `DeliveryBlocked` 而非原样抛 `MethodError`：`run-due` 只认前者，
    一份缺方法的需求该记一条失败，不该让整批交付一起崩。
    """
    try:
        record = method.read(request_dir)
    except method.MethodError as error:
        raise DeliveryBlocked(
            f"{error}\n\n补齐后重跑；骨架用 `hf-data-crawl request method sync {spec.id}` 生成。"
        ) from None
    return method.stamp(request_dir, record)


def deliver(
    store: DatasetStore,
    requests_root: Path,
    request_id: str,
    *,
    catalog: GoldCatalog | None = None,
    source: SourceStamp | None = None,
    now: datetime | None = None,
) -> DeliveryResult:
    """跑一次交付。任何护栏不通过都抛 `DeliveryBlocked`，且不留任何目录。"""
    request_dir = resolve_dir(requests_root, request_id)
    spec_path = request_dir / SPEC_NAME
    spec = load_spec(spec_path)
    # 版本号不可复用：口径变了却还用 v1，档案会指着一份「当时说过、现在已经不是」
    # 的定义。这比缺方法更隐蔽——缺方法至少会报错，这一种只会让人拿着错前提核对。
    conflict = version_conflict(spec_path, spec.version)
    if conflict:
        raise DeliveryBlocked(f"{spec.id}: {conflict}")
    spec_sha = spec_digest(spec_path)
    # 先验方法再算数：方法是「这份交付凭什么成立」的一部分，缺它连预留目录都不该做。
    method_stamp = _method_stamp(request_dir, spec)
    moment = now or datetime.now().astimezone()
    stamp_value = source or _stamp(store)
    runner = _catalog(store, catalog)

    directory = _reserve_dir(request_dir, moment)
    staging = directory.parent / f".tmp-{directory.name}"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    try:
        payload = _execute(
            RunContext(
                store=store,
                spec=spec,
                staging=staging,
                catalog=runner,
                source=stamp_value,
            )
        )
        _check_guard(spec, payload.rows)
        health = _health(request_dir, spec, payload.rows, payload.header)
        manifest = _manifest(
            spec=spec,
            moment=moment,
            stamp_value=stamp_value,
            payload=payload,
            health=health,
            previous=_last_delivery_name(request_dir, exclude=directory.name),
            method_stamp=method_stamp,
            spec_sha=spec_sha,
        )
        (staging / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (staging / README_NAME).write_text(
            _readme(spec, moment, stamp_value, payload, health, spec_sha), encoding="utf-8"
        )
        # 三件齐了才换名：中途失败最多留下一个 .tmp-* 目录，不会出现「有数据没说明」。
        replace(staging, directory)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    # 首次交付落一份口径快照——只留「真的用过」的版本，没用过的口径没有保留价值。
    snapshot_history(request_dir / SPEC_NAME, spec.version)

    return DeliveryResult(
        request_id=spec.id,
        directory=directory,
        rows=payload.rows,
        columns=payload.columns,
        files=tuple(payload.files),
        health=health,
        spec_version=spec.version,
        kind=spec.kind,
    )


def preview(
    store: DatasetStore,
    requests_root: Path,
    request_id: str,
    *,
    catalog: GoldCatalog | None = None,
    source: SourceStamp | None = None,
) -> Preview:
    """只算不写：看这次会出多少行、每列能填多少，不产生任何交付档案。"""
    request_dir = resolve_dir(requests_root, request_id)
    spec = load_spec(request_dir / SPEC_NAME)
    payload = _execute(
        RunContext(
            store=store,
            spec=spec,
            # 预演下执行体不得写盘；给一个明确的哨兵路径，误写也会立刻暴露在
            # 这个不存在的目录上，而不是悄悄落到当前工作目录。
            staging=request_dir / DELIVERIES_DIR / ".preview",
            catalog=_catalog(store, catalog),
            source=source or _stamp(store),
            dry_run=True,
        )
    )
    return Preview(
        request_id=spec.id,
        kind=spec.kind,
        rows=payload.rows,
        columns=payload.columns,
        lines=_preview_lines(request_dir, payload.detail_lines),
    )


def _preview_lines(request_dir: Path, detail: list[str]) -> list[str]:
    """预演不拦，但要把「正式交付会被拦」写在最上面。

    预演的唯一用途就是「看看会不会 0 行」，此时也被方法闸门挡住等于自相矛盾；
    可若一声不吭地跑通，人就会以为可以交付了，然后在正式那一刻才撞墙。
    """
    found = method.problems(request_dir)
    if not found:
        return detail
    return [
        "> ⚠ 过程记录不合规，正式交付会被拦下：",
        *[f"> - {text}" for text in found],
        "",
        *detail,
    ]


def _execute(ctx: RunContext) -> Payload:
    """按 kind 分派，并把执行体的失败统一翻译成 `DeliveryBlocked`。

    翻译是给 `run-due` 用的：一份坏 spec 或打不开的输入表只该记一条失败，
    不该让整批交付一起崩。编程错误（TypeError 之类）不在此列，要照常抛出来。
    """
    executor = EXECUTORS.get(ctx.spec.kind)
    if executor is None:
        raise DeliveryBlocked(f"{ctx.spec.id}: 未注册的执行体 kind={ctx.spec.kind.value}")
    try:
        return executor(ctx)
    except (backfill.FillError, OSError) as error:
        raise DeliveryBlocked(f"{ctx.spec.id} 执行失败：{error}") from error


# ---------------------------------------------------------------- 执行体：查询


def _query_payload(ctx: RunContext) -> Payload:
    frame = _query(ctx.spec, ctx.catalog)
    definition = _query_definition(ctx.spec)
    lines = _query_lines(ctx.spec)
    if ctx.dry_run:
        return Payload(
            rows=frame.height,
            columns=tuple(frame.columns),
            definition=definition,
            detail_lines=lines,
        )
    written = _write_data(ctx.staging, ctx.spec, frame)
    return Payload(
        rows=frame.height,
        columns=tuple(frame.columns),
        files=written,
        definition=definition,
        detail_lines=lines,
    )


def _query_definition(spec: Spec) -> dict[str, Any]:
    """查询形态的机器读口径。`criteria` 带定义，交出去后不必回来翻词表。"""
    using_default = not spec.output.columns
    return {
        "sql": compile_sql(spec),
        "projection": {
            "source": "default" if using_default else "spec",
            "default_columns_version": CONTRACT_VERSION,
            "columns": list(DEFAULT_COLUMNS) if using_default else list(spec.output.columns),
        },
        "criteria": [
            {
                "metric": item.metric,
                "field": lookup(item.metric).field,
                "op": item.op,
                "value": _jsonable(item.value),
                "definition": lookup(item.metric).definition,
            }
            for item in spec.criteria
        ],
    }


def _query_lines(spec: Spec) -> list[str]:
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
        lines += [
            "",
            "### 扩展字段（不进默认面，按需取用）",
            "",
            EXTENDED_NOTE,
            "",
            "| 字段 | 定义 | 覆盖档 |",
            "| --- | --- | --- |",
        ]
        for column, definition, tier in EXTENDED_FIELDS:
            lines.append(f"| `{column}` | {definition} | {tier} |")
        lines.append("")
    return lines


def _query(spec: Spec, catalog: GoldCatalog) -> pl.DataFrame:
    sql = compile_sql(spec)
    try:
        return catalog.run(sql)
    except duckdb.Error as error:
        # 包一层：裸的 BinderException 看不出是哪条需求、哪条 SQL 出的问题。
        raise DeliveryBlocked(f"{spec.id} 查询失败：{error}\nSQL：\n{sql}") from None


# ---------------------------------------------------------------- 执行体：回填


def _backfill_payload(ctx: RunContext) -> Payload:
    fill = ctx.spec.fill
    if fill is None:  # `_validate` 已挡住，这里只为把类型收窄
        raise DeliveryBlocked(f"{ctx.spec.id}: kind=backfill 却没有 fill 段")
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
    input_stamp = file_stamp(fill.source.path)
    definition = _fill_definition(fill, report, input_stamp)
    lines = _fill_lines(fill, report, ctx.dry_run, input_stamp)
    return Payload(
        rows=report.rows,
        columns=report.columns(),
        files=report.files,
        definition=definition,
        detail_lines=lines,
        header=report.header,
    )


def _fill_definition(
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
                    "sha256": backfill.header_digest(report.header),
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
            "overrides": fill.overrides.name if fill.overrides else None,
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


def _fill_lines(
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
            f"| {item.name} | {origin} | {tally.written} | {tally.from_override} "
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
            "### 未声明的列（保持空白）",
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


EXECUTORS.update({Kind.QUERY: _query_payload, Kind.BACKFILL: _backfill_payload})


# ---------------------------------------------------------------- 护栏


def _check_guard(spec: Spec, rows: int) -> None:
    if rows < spec.guard.min_rows:
        raise DeliveryBlocked(
            f"{spec.id} 只得到 {rows} 行，低于 min_rows={spec.guard.min_rows}。"
            "空结果交付出去没有意义，通常意味着口径写错了——先 `request show` 核对一遍。"
        )
    if spec.guard.max_rows is not None and rows > spec.guard.max_rows:
        raise DeliveryBlocked(
            f"{spec.id} 得到 {rows} 行，超过 max_rows={spec.guard.max_rows}。"
            "这么大的量交出去很可能不是对方要的；确认无误请调高 spec 里的 max_rows。"
        )


# ---------------------------------------------------------------- 与上次比较


def _last_delivery(request_dir: Path) -> Path | None:
    base = request_dir / DELIVERIES_DIR
    if not base.is_dir():
        return None
    done = sorted(
        path for path in base.iterdir() if path.is_dir() and not path.name.startswith(".")
    )
    return done[-1] if done else None


def _last_delivery_name(request_dir: Path, *, exclude: str) -> str | None:
    last = _last_delivery(request_dir)
    if last is None or last.name == exclude:
        return None
    return last.name


def _read_manifest(directory: Path) -> dict[str, Any] | None:
    path = directory / MANIFEST_NAME
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return body if isinstance(body, dict) else None


def _health(request_dir: Path, spec: Spec, rows: int, header: tuple[str, ...] = ()) -> Health:
    """与上一次交付比：同一口径版本内才比行数；表头则只在有基线时比。

    改了口径（版本变了）必然行数大变，那时比较只会天天发假警报——这类误报最能
    训练人忽略告警，所以宁可显式跳过并记下 `supersedes`。

    表头单独比，**跨版本也比**：换一份字段不同的表，恰恰是改口径的常见原因。行数
    可以不比，但「这次的表和上次不一样」必须留在纸面上。
    """
    last = _last_delivery(request_dir)
    if last is None:
        return Health(status="first")

    body = _read_manifest(last)
    if body is None:
        return Health(
            status="unknown", note=f"上一次交付（{last.name}）的 manifest 读不出来，本次未比较"
        )

    header_note = _header_note(body, header)

    baseline = (body.get("result") or {}).get("rows")
    if not isinstance(baseline, int):
        return Health(
            status="unknown",
            note=f"上一次交付（{last.name}）没有记行数，本次未比较",
            header_note=header_note,
        )

    previous_version = body.get("spec_version")
    if previous_version != spec.version:
        return Health(
            status="ok",
            baseline_rows=baseline,
            supersedes=previous_version if isinstance(previous_version, int) else None,
            note=f"口径已从 v{previous_version} 改到 v{spec.version}，行数变化不计入标疑",
            header_note=header_note,
        )

    delta_abs = rows - baseline
    delta_pct = (delta_abs / baseline * 100) if baseline else 0.0
    # 两个条件必须同时满足：只用百分比，小结果集会被误报（12 行变 15 行就是 +25%）。
    suspect = (
        abs(delta_pct) > spec.alert.row_delta_pct and abs(delta_abs) >= spec.alert.row_delta_abs
    )
    return Health(
        status="suspect" if suspect else "ok",
        baseline_rows=baseline,
        delta_pct=round(delta_pct, 2),
        delta_abs=delta_abs,
        header_note=header_note,
    )


def _header_note(body: dict[str, Any], header: tuple[str, ...]) -> str:
    """把表头差异说成人话。任一侧没有记录就明说「没比」，不假装一致。"""
    if not header:
        return ""
    recorded = ((body.get("fill") or {}).get("input") or {}).get("header") or {}
    columns = recorded.get("columns")
    if not isinstance(columns, list) or not columns:
        return "上一份档案没记输入表头，本次未比较（早期档案没有这一项）"
    old = tuple(str(name) for name in columns)
    if old == header:
        return ""
    change = backfill.header_change(old, header)
    parts: list[str] = []
    if change["added"]:
        parts.append("新增 " + "、".join(change["added"]))
    if change["removed"]:
        parts.append("少了 " + "、".join(change["removed"]))
    if change["reordered"]:
        parts.append("共同列次序变了")
    return "输入表头与上一份交付不同：" + "；".join(parts)


# ---------------------------------------------------------------- 落盘


def _reserve_dir(request_dir: Path, moment: datetime) -> Path:
    base = request_dir / DELIVERIES_DIR
    base.mkdir(parents=True, exist_ok=True)
    stem = moment.strftime(_STAMP_FORMAT)
    candidate = base / stem
    index = 2
    while candidate.exists():
        candidate = base / f"{stem}-{index}"
        index += 1
    return candidate


def _write_data(directory: Path, spec: Spec, frame: pl.DataFrame) -> list[dict[str, Any]]:
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
        if kind == "csv":
            _flat_for_csv(frame).write_csv(target)
        else:
            frame.write_parquet(target)
        payload = target.read_bytes()
        written.append(
            {
                "name": name,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
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


def _manifest(
    *,
    spec: Spec,
    moment: datetime,
    stamp_value: SourceStamp,
    payload: Payload,
    health: Health,
    previous: str | None,
    method_stamp: dict[str, Any],
    spec_sha: str,
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "request_id": spec.id,
        "title": spec.title,
        "requester": spec.requester,
        "kind": spec.kind.value,
        "spec_version": spec.version,
        # 口径的语义指纹。只记版本号不够：版本号只是人写的一个数字，改了口径
        # 忘了加一，读档的人无从分辨。有了指纹，「这份档案对应哪一版口径」才可核对。
        "spec_sha256": spec_sha,
        "delivered_at": moment.isoformat(timespec="seconds"),
        "source": {
            "current_md5": stamp_value.current_md5,
            "current_rows": stamp_value.current_rows,
            "newest_snapshot_date": (
                stamp_value.newest_snapshot_date.isoformat()
                if stamp_value.newest_snapshot_date
                else None
            ),
        },
        "result": {
            "rows": payload.rows,
            "columns": list(payload.columns),
            "files": payload.files,
        },
        "health": {
            "status": health.status,
            "baseline_rows": health.baseline_rows,
            "delta_pct": health.delta_pct,
            "delta_abs": health.delta_abs,
            "supersedes": health.supersedes,
            "note": health.note,
            "header_note": health.header_note,
        },
        "previous": previous,
        # 过程自包含：这份结果由哪版方法、哪版脚本、哪版代码产出。缺了它，
        # 「基于哪一版底库」只钉住了原材料，钉不住做法。
        "method": method_stamp,
    }
    # 形态特有的口径进顶层，便于「读 manifest 就知道这次怎么算的」；公共键不许被
    # 覆盖——一旦覆盖，「这份交付基于哪版底库」就可能被某个执行体悄悄改写。
    overlap = manifest.keys() & payload.definition.keys()
    if overlap:
        raise DeliveryBlocked(f"{spec.id}: 执行体口径与档案公共字段冲突：{sorted(overlap)}")
    manifest.update(payload.definition)
    return manifest


def _jsonable(value: Any) -> Any:
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


def _readme(
    spec: Spec,
    moment: datetime,
    stamp_value: SourceStamp,
    payload: Payload,
    health: Health,
    spec_sha: str,
) -> str:
    lines: list[str] = [f"# {spec.id} {spec.title}", ""]
    if health.status == "suspect":
        lines += [
            f"> ⚠️ **本次结果与上次相比行数变动 {health.delta_pct:+.1f}%"
            f"（{health.baseline_rows} → {payload.rows}），已标疑。**"
            "交出去之前请先确认是底库正常波动，还是口径或上游出了问题。",
            "",
        ]
    elif health.status == "unknown":
        lines += [f"> 上一次交付的档案读不出来，本次未做行数比较（{health.note}）。", ""]
    if health.header_note:
        lines += [f"> 注意：{health.header_note}。行数没变不等于结果没变，请先核对这一批。", ""]

    lines += [f"- 提出人：{spec.requester}"]
    if spec.purpose:
        lines.append(f"- 用途：{spec.purpose}")
    lines += [
        f"- 交付时刻：{moment.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 需求形态：`{spec.kind.value}`",
        f"- 需求版本：v{spec.version}（口径指纹 `{spec_sha[:16]}…`）",
        f"- 交付节奏：{spec.cadence.value}",
        "",
        "## 口径",
        "",
        *payload.detail_lines,
        "",
        "## 基于哪一版底库",
        "",
        f"- `current.parquet` md5：`{stamp_value.current_md5}`",
        f"- 底库总行数：{stamp_value.current_rows:,}",
        f"- 最新观测日：{_day_text(stamp_value)}",
        "",
        "底库每天都在变，所以这份结果只在「这一版底库」下成立。要核对来源，",
        f"跑 `hf-data-crawl request show {spec.id}` 看交付台账。",
        "",
        "## 结果",
        "",
        f"- 行数：{payload.rows:,}",
        f"- 列数：{len(payload.columns)}",
    ]
    for entry in payload.files:
        lines.append(
            f"- `{entry['name']}`：{entry['bytes']:,} 字节，sha256 `{entry['sha256'][:16]}…`"
        )
    lines += [
        "",
        "## 复现",
        "",
        f"- 做法：`{method.METHOD_DIR}/{method.METHOD_DOC}`（步骤、判据、哪步机器哪步人判）",
        f"- 脚本：`{method.METHOD_DIR}/{method.SCRIPTS_DIR}/`，逐份 sha256 与所依赖的代码版本",
        "  见 `manifest.json` 的 `method` 段",
        "",
        "```bash",
        f"hf-data-crawl request run {spec.id} --force",
        "```",
        "",
        "同一版底库、同一份输入表下重跑，sha256 应当与上面一致。若不一致，",
        "说明底库或输入表已经变过。",
        "",
    ]
    return "\n".join(lines)


def _day_text(stamp_value: SourceStamp) -> str:
    return (
        stamp_value.newest_snapshot_date.isoformat() if stamp_value.newest_snapshot_date else "未知"
    )


# ---------------------------------------------------------------- 到期判断


#: 各节奏要求的最小间隔（日历日）。`daily` 用 1 天而非 24 小时：昨晚 23:50 交过，
#: 今早 07:00 该不该再交一次？按「每天一份」的语义，该——昨天那份属于昨天。
_CADENCE_DAYS: dict[Cadence, int] = {
    Cadence.DAILY: 1,
    Cadence.WEEKLY: 7,
    Cadence.MONTHLY: 30,
}


@dataclass(frozen=True)
class DueReport:
    delivered: list[DeliveryResult]
    skipped: list[tuple[str, str]]
    failed: list[tuple[str, str]]


def last_delivered_at(request_dir: Path) -> datetime | None:
    """上一次交付的时刻。读不出来就返回 None——调用方会当成「从未交付」。"""
    last = _last_delivery(request_dir)
    if last is None:
        return None
    body = _read_manifest(last)
    if body is None:
        return None
    raw = body.get("delivered_at")
    if not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def due_state(spec: Spec, request_dir: Path, now: datetime) -> tuple[bool, str]:
    """这份需求今天该不该跑，以及为什么。第二个返回值直接进日志。"""
    if spec.cadence is Cadence.MANUAL:
        return False, "manual：只等人跑"
    moment = last_delivered_at(request_dir)
    if moment is None:
        return True, "从未交付"
    if spec.cadence is Cadence.ONCE:
        return False, f"once 已交付过（{moment:%Y-%m-%d %H:%M}）"

    need = _CADENCE_DAYS[spec.cadence]
    elapsed = (now.date() - moment.date()).days
    if elapsed >= need:
        return True, f"距上次 {elapsed} 天，满 {need} 天"
    return False, f"距上次 {elapsed} 天，未满 {need} 天"


def run_due(
    store: DatasetStore,
    requests_root: Path,
    *,
    now: datetime | None = None,
    catalog: GoldCatalog | None = None,
    source: SourceStamp | None = None,
) -> DueReport:
    """把所有到期的需求各跑一次。

    一个需求失败不挡别的：坏 spec、0 行、执行出错都只记一条，继续下一个。
    但底库本身不成立（`current` 缺失）时整体失败——那不是某一条需求的问题，
    报十遍同样的错只会把真正的原因埋掉。
    """
    moment = now or datetime.now().astimezone()
    stamp_value = source or _stamp(store)
    runner = _catalog(store, catalog)

    delivered: list[DeliveryResult] = []
    skipped: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []

    for spec_path in sorted(requests_root.glob(f"*/{SPEC_NAME}")):
        # 目录名带语义后缀，交付时必须把「目录名」换成 spec 里的需求号——
        # `deliver` 靠编号去解析目录，传目录名会找不到家。
        try:
            spec = load_spec(spec_path)
        except SpecError as error:
            failed.append((spec_path.parent.name, str(error)))
            continue
        request_id = spec.id

        due, reason = due_state(spec, spec_path.parent, moment)
        if not due:
            skipped.append((spec.id, reason))
            continue

        try:
            result = deliver(
                store,
                requests_root,
                request_id,
                catalog=runner,
                source=stamp_value,
                now=moment,
            )
        except (DeliveryBlocked, SpecError) as error:
            failed.append((request_id, str(error)))
            continue
        delivered.append(result)

    return DueReport(delivered=delivered, skipped=skipped, failed=failed)
