"""需求定义：spec 的读取与校验，以及口径编译成 SQL。

三层职责，只此一处
------------------
1. **读**：YAML → `Spec`。校验全部在 `_validate` 里做一次，之后各层拿到的
   `Spec` 一定是自洽的——不必在编译、执行、交付各处重复检查。
2. **编译**：`criteria` → SQL。字段名只来自词表白名单，运算符只来自按类型定
   的白名单，值走转义。三者都不接受调用方直接拼字符串，所以「需求 SQL」不可能
   变成注入入口。
3. **确定性**：编译结果末尾永远有一条唯一键排序。没有它，同一条需求两次跑出
   两个字节不同的文件，`sha256` 比对（交付可复现的判据）就永远失败。

为什么 spec 用 YAML 手写模板而非 `yaml.safe_dump`
-------------------------------------------------
`spec.yaml` 是给人改的：注释要留得住（「下载量」取 30 天还是累计，就写在旁边），
`dump` 会把注释冲掉。所以骨架用手写模板，历史版本直接复制原文件字节——
复制比重新生成更忠实，也不会因将来新增字段而让旧快照悄悄变样。
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from .fields import projection_sql
from .metrics import Metric, MetricKind, lookup

if TYPE_CHECKING:
    from .backfill import FillSpec

#: 需求目录下的固定文件名。
SPEC_NAME = "spec.yaml"
HISTORY_DIR = "history"
DELIVERIES_DIR = "deliveries"
#: 输入快照目录。外部给的表受理时复制进来，之后需求目录不再依赖任何外部路径。
INPUT_DIR = "input"
#: 每次交付目录里的三件套。`data.*` 是字节（不进 git），后两个是说明（进 git）。
MANIFEST_NAME = "manifest.json"
README_NAME = "README.md"


def _slug(title: str, limit: int = 24) -> str:
    """标题短名：只留中英文、数字、连字符，去掉路径分隔符与文件系统禁用的字符。"""
    text = re.sub(r"[\s/\\:*?\"<>|]+", "-", title.strip())
    text = re.sub(r"-{2,}", "-", text).strip("-")
    return text[:limit].rstrip("-")


def directory_name(request_id: str, title: str, kind: Kind) -> str:
    """语义化目录名：`REQ-0002-backfill-具身感知数据集信息补充`。

    编号保唯一与排序；`kind` 让人和模型一眼看出该走哪条执行路径；标题短名让
    人不打开文件就知道这单是什么。**改名不搬目录**：目录名是受理那一刻的标签，
    正式标题以 `spec.yaml` 里的 `title` 为准，搬目录会扯断交付台账的路径。
    """
    slug = _slug(title)
    return f"{request_id}-{kind.value}-{slug}" if slug else f"{request_id}-{kind.value}"


def resolve_dir(root: Path, request_id: str) -> Path:
    """用需求号定位目录。目录名带语义后缀，不能再靠字符串拼接。

    兼容旧的纯编号目录（`REQ-0001/`）：先精确匹配，再找 `<id>-*` 前缀。命中多个
    说明编号撞车，直接报错——猜一个会用错口径，而口径错了整份交付都白做。
    """
    if not root.is_dir():
        raise SpecError(f"找不到需求目录：{root}")
    hits = sorted(
        path
        for path in root.iterdir()
        if path.is_dir()
        and (path.name == request_id or path.name.startswith(f"{request_id}-"))
        and (path / SPEC_NAME).is_file()
    )
    if not hits:
        raise SpecError(f"找不到需求 {request_id}：{root} 下没有对应的 {SPEC_NAME}")
    if len(hits) > 1:
        names = "、".join(path.name for path in hits)
        raise SpecError(f"需求号 {request_id} 对应多个目录：{names}；请合并或改名")
    return hits[0]


#: 需求号的形状。四位起步，够用一千个需求；真要超了再放宽，届时会逼人正视编号。
_ID_PATTERN = r"^REQ-\d{4,}$"

#: 不经词表的标识列。`id` 是每张表的主键（排序兜底全靠它），`url` 是它的派生物。
#: 它们没有「口径」含义，不需要一套定义，但 `order_by` 与 `output.columns` 会用到。
PLAIN_COLUMNS: frozenset[str] = frozenset({"id", "url"})


class SpecError(ValueError):
    """spec 不合法。信息要说清「哪一项、为什么不行」，别只说「格式错误」。"""


class _StrictLoader(yaml.SafeLoader):
    """拒绝重复键。

    YAML 默认「后写覆盖前写」，而 spec 里出现两处同名几乎总是笔误：改了下面
    那处、上面还留着，跑起来用的却是后者——人看着自己写的那行明明对，结果
    就是不对，最难查。两类需求共用一份 spec 后，这条防线必须放在读入处，
    不能只挂在回填口径体上。
    """


def _strict_mapping(
    loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise SpecError(f"重复的键 {key!r}（第 {key_node.start_mark.line + 1} 行）")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _strict_mapping)


class Kind(StrEnum):
    """需求的产出形态。

    这是整套体系里**唯一**的「变」。受理、定口径、按节奏跑、交付留档、可追溯
    这些环节对每一种形态都一样，所以它们必须共用一套容器、一套编号、一套台账。
    按形态分目录、分命令，等于把「不变」的那部分复制若干份——而形态是列不完的：
    今天查询、明天回填，后天会有对账报表、按名单核验失效。
    """

    QUERY = "query"
    BACKFILL = "backfill"


class Cadence(StrEnum):
    """交付节奏。一次性不是另一种东西，只是 `once`。"""

    ONCE = "once"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    MANUAL = "manual"


class Mode(StrEnum):
    """口径的写法。

    `criteria` 是默认：结构化的条件，可校验、可自动生成人读说明。
    `sql` 是逃生舱：join、聚合、窗口函数这些 criteria 表达不了的，手写 SQL。
    """

    CRITERIA = "criteria"
    SQL = "sql"


class OutputFormat(StrEnum):
    PARQUET = "parquet"
    CSV = "csv"
    BOTH = "both"


#: 运算符白名单，按词条类型分。文本列上的 `>` 在 DuckDB 里合法，但那是字典序比较，
#: 几乎从来不是提问者想要的——宁可在编译期拒绝，也不要交出一份「看起来对」的结果。
_OPERATORS: dict[MetricKind, frozenset[str]] = {
    MetricKind.NUMBER: frozenset({"=", "!=", ">", ">=", "<", "<="}),
    MetricKind.TIME: frozenset({"=", "!=", ">", ">=", "<", "<="}),
    MetricKind.TEXT: frozenset({"=", "!="}),
    MetricKind.FLAG: frozenset({"=", "!="}),
}

#: 接受但归一化的写法。
_OPERATOR_ALIASES: dict[str, str] = {
    "<>": "!=",
    "==": "=",
    "≥": ">=",
    "≤": "<=",
    "＞": ">",
    "＜": "<",
}


@dataclass(frozen=True)
class Criterion:
    """一条筛选条件。`metric` 保留词条原文，供 README 与 manifest 记录。"""

    metric: str
    op: str
    value: Any


@dataclass(frozen=True)
class OrderKey:
    metric: str
    descending: bool = False


@dataclass(frozen=True)
class Output:
    format: OutputFormat = OutputFormat.PARQUET
    #: 空表示按默认交付字段契约（`fields.DEFAULT_COLUMNS`）投列；写了才按词条
    #: 或视图列名逐列指定。「全都给」不是口径，所以默认值必须是一份定义好的清单。
    columns: tuple[str, ...] = ()


@dataclass(frozen=True)
class Guard:
    """交付前后的硬性护栏。"""

    #: 低于此值即判失败。默认 1：空结果交付出去毫无意义，且通常意味着口径写错。
    min_rows: int = 1
    #: 高于此值即判失败，防一条写错的需求把百万行倒给对方。
    max_rows: int | None = 500_000


@dataclass(frozen=True)
class Alert:
    """与上一次交付相比，多大波动要标疑。两个条件同时满足才标。"""

    row_delta_pct: float = 20.0
    row_delta_abs: int = 100


@dataclass(frozen=True)
class Spec:
    """一份需求的全部定义。构造后不可变：改口径要升 `version`，不是就地改。"""

    id: str
    title: str
    requester: str
    version: int
    cadence: Cadence = Cadence.ONCE
    criteria: tuple[Criterion, ...] = ()
    order_by: tuple[OrderKey, ...] = ()
    output: Output = field(default_factory=Output)
    guard: Guard = field(default_factory=Guard)
    alert: Alert = field(default_factory=Alert)
    mode: Mode = Mode.CRITERIA
    sql: str | None = None
    purpose: str = ""
    notes: str = ""
    kind: Kind = Kind.QUERY
    #: `kind: backfill` 时的口径体；由 `backfill.from_mapping` 解析校验。
    fill: FillSpec | None = None

    @property
    def slug(self) -> str:
        """语义化目录名：`REQ-0002-backfill-具身感知数据集信息补充`。"""
        return directory_name(self.id, self.title, self.kind)


def load(path: Path, *, base_dir: Path | None = None) -> Spec:
    """读一个 spec.yaml。任何不合法的字段都在这里一次性报出来。

    `base_dir` 指定 `fill.source.path` 等相对路径的基准目录，默认取 spec 所在目录。
    历史快照（`history/v1.yaml`）必须显式传**需求目录**：它自己躺在 `history/` 下，
    按默认基准去解析 `input/清单.xlsx`，会指向 `history/input/` 这个不存在的地方——
    于是「那一版真的用过」的档案，恰恰打不开了。
    """
    try:
        body = yaml.load(path.read_text(encoding="utf-8"), Loader=_StrictLoader)
    except FileNotFoundError:
        raise SpecError(f"找不到 {path}") from None
    except SpecError as error:
        raise SpecError(f"{path}: {error}") from None
    except yaml.YAMLError as error:
        raise SpecError(f"{path} 不是合法 YAML：{error}") from None
    if not isinstance(body, dict):
        raise SpecError(f"{path} 顶层应是一个映射（key: value），实得 {type(body).__name__}")
    return _from_mapping(body, path, base_dir)


def _from_mapping(body: dict[str, Any], path: Path, base_dir: Path | None = None) -> Spec:
    def need(key: str) -> Any:
        if key not in body or body[key] in (None, "", ()):
            raise SpecError(f"{path}: 缺少必填项 {key!r}")
        return body[key]

    kind = _enum(Kind, body.get("kind", Kind.QUERY), "kind", path)
    fill = _fill_body(body, path, base_dir) if kind is Kind.BACKFILL else None
    criteria = tuple(_criterion(item, path) for item in _as_list(body.get("criteria")))
    mode = _enum(Mode, body.get("mode", Mode.CRITERIA), "mode", path)

    spec = Spec(
        id=str(need("id")),
        title=str(need("title")),
        requester=str(need("requester")),
        version=_positive_int(body.get("version", 1), "version", path),
        cadence=_enum(Cadence, body.get("cadence", Cadence.ONCE), "cadence", path),
        criteria=criteria,
        order_by=tuple(_order_key(item, path) for item in _as_list(body.get("order_by"))),
        output=_output(body.get("output"), path),
        guard=_guard(body.get("guard"), path),
        alert=_alert(body.get("alert"), path),
        mode=mode,
        sql=_optional_text(body.get("sql")),
        purpose=str(body.get("purpose") or ""),
        notes=str(body.get("notes") or ""),
        kind=kind,
        fill=fill,
    )
    _validate(spec, path)
    return spec


def _fill_body(body: dict[str, Any], path: Path, base_dir: Path | None = None) -> FillSpec:
    """解析回填口径体。运行期才导入 `backfill`——它要算指纹、又要被本模块的类型引用。"""
    from .backfill import FillError
    from .backfill import from_mapping as fill_from_mapping

    try:
        return fill_from_mapping(body.get("fill"), base_dir or path.parent, origin=str(path))
    except FillError as error:
        raise SpecError(str(error)) from None


def _validate(spec: Spec, path: Path) -> None:
    if not re.match(_ID_PATTERN, spec.id):
        raise SpecError(f"{path}: id 应形如 REQ-0001，实得 {spec.id!r}")
    if not spec.title.strip():
        raise SpecError(f"{path}: title 不能为空白")
    if not spec.requester.strip():
        raise SpecError(f"{path}: requester 不能为空白——交付单要能回答「给谁」")
    if spec.kind is Kind.BACKFILL:
        # 回填的护栏在口径体里（输出文件名、必填列）；criteria / order_by 对它无意义。
        if spec.fill is None:
            raise SpecError(f"{path}: kind=backfill 时必须有 fill 段")
        return
    if spec.mode is Mode.CRITERIA and not spec.criteria:
        raise SpecError(f"{path}: mode=criteria 时 criteria 不能为空（要手写 SQL 请写 mode: sql）")
    if spec.mode is Mode.SQL and not (spec.sql or "").strip():
        raise SpecError(f"{path}: mode=sql 时必须给 sql")
    # 词条在这里查一次，而不是等编译：受理时就该发现「热度」不在词表里，
    # 而不是等交付跑到一半。条件与排序分开遍历，两者的类型不同，混在一个
    # 循环里只能推断成宽联合。
    for term in [item.metric for item in (*spec.criteria,)] + [key.metric for key in spec.order_by]:
        if term in PLAIN_COLUMNS:
            continue
        try:
            lookup(term)
        except ValueError as error:
            raise SpecError(f"{path}: {error}") from None


def _criterion(item: Any, path: Path) -> Criterion:
    if not isinstance(item, dict):
        raise SpecError(f"{path}: criteria 的每一项应是映射，实得 {type(item).__name__}")
    metric = str(item.get("metric") or "").strip()
    if not metric:
        raise SpecError(f"{path}: 条件缺少 metric")
    # 词条存在性由 `_validate` 统一报，这里只归一化运算符的写法。
    return Criterion(
        metric=metric, op=_normalize_op(item.get("op", "="), metric, path), value=item.get("value")
    )


def _order_key(item: Any, path: Path) -> OrderKey:
    if isinstance(item, str):
        return OrderKey(metric=item)
    if not isinstance(item, dict):
        raise SpecError(f"{path}: order_by 的每一项应是词条或映射")
    metric = str(item.get("metric") or item.get("field") or "").strip()
    if not metric:
        raise SpecError(f"{path}: order_by 缺少 metric")
    direction = str(item.get("direction") or item.get("dir") or "asc").lower()
    if direction not in {"asc", "desc"}:
        raise SpecError(f"{path}: order_by 的 direction 只能是 asc 或 desc，实得 {direction!r}")
    return OrderKey(metric=metric, descending=direction == "desc")


def _output(raw: Any, path: Path) -> Output:
    if raw is None:
        return Output()
    if not isinstance(raw, dict):
        raise SpecError(f"{path}: output 应是映射")
    columns = raw.get("columns")
    if columns is not None and not isinstance(columns, list):
        raise SpecError(f"{path}: output.columns 应是列表")
    return Output(
        format=_enum(OutputFormat, raw.get("format", OutputFormat.PARQUET), "output.format", path),
        columns=tuple(str(name) for name in (columns or ())),
    )


def _guard(raw: Any, path: Path) -> Guard:
    if raw is None:
        return Guard()
    if not isinstance(raw, dict):
        raise SpecError(f"{path}: guard 应是映射")
    guard = Guard(
        min_rows=_non_negative_int(raw.get("min_rows", 1), "guard.min_rows", path),
        max_rows=(
            None
            if raw.get("max_rows") is None
            else _non_negative_int(raw["max_rows"], "guard.max_rows", path)
        ),
    )
    if guard.max_rows is not None and guard.max_rows < guard.min_rows:
        raise SpecError(
            f"{path}: guard.max_rows（{guard.max_rows}）小于 min_rows（{guard.min_rows}），"
            "任何结果都过不去"
        )
    return guard


def _alert(raw: Any, path: Path) -> Alert:
    if raw is None:
        return Alert()
    if not isinstance(raw, dict):
        raise SpecError(f"{path}: alert 应是映射")
    pct = raw.get("row_delta_pct", 20.0)
    if not isinstance(pct, int | float) or pct < 0:
        raise SpecError(f"{path}: alert.row_delta_pct 应是非负数字，实得 {pct!r}")
    return Alert(
        row_delta_pct=float(pct),
        row_delta_abs=_non_negative_int(raw.get("row_delta_abs", 100), "alert.row_delta_abs", path),
    )


def _normalize_op(raw: Any, metric: str, path: Path) -> str:
    text = str(raw).strip()
    op = _OPERATOR_ALIASES.get(text, text)
    if not op:
        raise SpecError(f"{path}: 条件 {metric!r} 缺少运算符")
    allowed = set().union(*_OPERATORS.values())
    if op not in allowed:
        raise SpecError(f"{path}: 不支持的运算符 {text!r}（可用：{' '.join(sorted(allowed))}）")
    return op


def _as_list(raw: Any) -> list[Any]:
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    return [raw]


def _optional_text(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    return text or None


def _enum(kind: type[StrEnum], raw: Any, key: str, path: Path) -> Any:
    try:
        return kind(raw)
    except ValueError:
        allowed = "、".join(member.value for member in kind)
        raise SpecError(f"{path}: {key} 只能是 {allowed}，实得 {raw!r}") from None


def _positive_int(raw: Any, key: str, path: Path) -> int:
    value = _int(raw, key, path)
    if value < 1:
        raise SpecError(f"{path}: {key} 应 >= 1，实得 {value}")
    return value


def _non_negative_int(raw: Any, key: str, path: Path) -> int:
    value = _int(raw, key, path)
    if value < 0:
        raise SpecError(f"{path}: {key} 应 >= 0，实得 {value}")
    return value


def _int(raw: Any, key: str, path: Path) -> int:
    if isinstance(raw, bool) or not isinstance(raw, int):
        raise SpecError(f"{path}: {key} 应是整数，实得 {raw!r}")
    return raw


# ---------------------------------------------------------------- 编译


def compile_sql(spec: Spec) -> str:
    """把 spec 编译成一条只读 SQL。

    输出三处固定：`SELECT` 的列、`WHERE` 的条件、`ORDER BY` 的唯一键兜底。
    三者都由这里决定，调用方不再拼字符串。
    """
    if spec.mode is Mode.SQL:
        return (spec.sql or "").strip().rstrip(";")

    where = "\n  AND ".join(_clause(item) for item in spec.criteria)
    return (
        f"SELECT {_select_list(spec)}\n"
        f"FROM datasets\n"
        f"WHERE {where}\n"
        f"ORDER BY {_order_clause(spec.order_by)}"
    )


def _clause(item: Criterion) -> str:
    metric = lookup(item.metric)
    op = _OPERATORS[metric.kind]
    if item.op not in op:
        raise SpecError(
            f"条件「{item.metric}」是{_kind_note(metric.kind)}，只能用 {' '.join(sorted(op))}，"
            f"不能用 {item.op!r}"
        )
    return f'"{metric.field}" {item.op} {_literal(item.value, metric)}'


def _kind_note(kind: MetricKind) -> str:
    return {
        MetricKind.NUMBER: "数字",
        MetricKind.TIME: "时间",
        MetricKind.TEXT: "文本",
        MetricKind.FLAG: "布尔值",
    }[kind]


def _literal(value: Any, metric: Metric) -> str:
    """值的安全转义。数字与布尔的形态先收紧，文本一律单引号转义。"""
    if metric.kind is MetricKind.NUMBER:
        number = _number(value, metric)
        return repr(int(number)) if float(number).is_integer() else repr(float(number))
    if metric.kind is MetricKind.TIME:
        return _time_literal(value, metric)
    if metric.kind is MetricKind.FLAG:
        return "TRUE" if _flag(value, metric) else "FALSE"
    return _quote(str(value))


def _number(value: Any, metric: Metric) -> int | float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise SpecError(f"条件「{metric.term}」需要一个数字，实得 {value!r}")
    if isinstance(value, float) and (value != value or value in (float("inf"), float("-inf"))):
        raise SpecError(f"条件「{metric.term}」不接受 NaN 或无穷")
    return value


def _flag(value: Any, metric: Metric) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in {"true", "false"}:
        return value.strip().lower() == "true"
    raise SpecError(f"条件「{metric.term}」需要 true 或 false，实得 {value!r}")


def _time_literal(value: Any, metric: Metric) -> str:
    if isinstance(value, datetime):
        return f"TIMESTAMP '{value.isoformat(sep=' ')}'"
    if isinstance(value, date):
        return f"DATE '{value.isoformat()}'"
    text = str(value).strip()
    if not text:
        raise SpecError(f"条件「{metric.term}」需要日期，实得空值")
    # YAML 未加引号的 `2026-01-01` 会被解析成 date；加了引号则留成字符串。
    # 只有日期就按 DATE，带时间就按 TIMESTAMP——DuckDB 与 DATE 比较时会隐式提升。
    keyword = "TIMESTAMP" if ("T" in text or ":" in text) else "DATE"
    return f"{keyword} {_quote(text)}"


def _quote(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _select_list(spec: Spec) -> str:
    """未指定字段时投默认交付契约；指定时按 spec 逐列投（词条先翻成字段名）。

    不再是 `SELECT *`：宽视图 72 列里混着水印、抓取状态与近乎全空的补抓列，
    默认全给等于没定义口径。契约列与 `export-wide` 产物逐列一致，由 doctor 勾稽。
    """
    if not spec.output.columns:
        return projection_sql()
    return ", ".join(f'"{_column(name)}"' for name in spec.output.columns)


def _column(name: str) -> str:
    """列名既可写词条，也可写视图列名。词条优先，写错由执行期对照视图报出。"""
    try:
        return lookup(name).field
    except ValueError:
        return name


def _order_clause(keys: Sequence[OrderKey]) -> str:
    """末尾永远补 `"id" ASC`：没有唯一键兜底，两次交付的字节可能不同。"""
    parts = [f'"{_column(key.metric)}" {"DESC" if key.descending else "ASC"}' for key in keys]
    if not any(_column(key.metric) == "id" for key in keys):
        parts.append('"id" ASC')
    return ", ".join(parts)


# ---------------------------------------------------------------- 骨架与快照


_SKELETON = """\
# 需求 {id}：{title}
#
# 这份文件是「口径」——需求到底要什么，全写在这里。
# 改动它必须同时把 version 加一：交付档案会钉住当时的版本号，
# 否则同一份需求的两份交付会显得「口径没变却行数大变」。
id: {id}
title: {title}
requester: {requester}
purpose: "{purpose}"
version: 1

# 交付节奏：once（只跑一次）/ daily / weekly / monthly / manual（只等人跑）
cadence: {cadence}

# 口径：metric 只写词表里的词条（见 GLOSSARY 的「交付口径」一节）
# 最常见的歧义是「下载量」——它指近 30 天，不是累计值。要累计请写「累计下载量」。
criteria:
  - metric: 下载量
    op: ">"
    value: 0
  - metric: 点赞数
    op: ">"
    value: 0

# 交付物里的行序。不写则按 id 升序——顺序固定，两次交付才能逐字节比对。
# order_by:
#   - {metric: 下载量, direction: desc}

output:
  format: parquet      # parquet / csv / both
  # columns: [id, url, 下载量, 点赞数]   # 不写则按默认交付底库字段（fields.DEFAULT_COLUMNS）

# 硬性护栏：0 行不交付（几乎总是口径写错了）；超过上限不交付（防误写倒出百万行）。
guard:
  min_rows: 1
  max_rows: 500000

# 与上一次交付相比，行数波动超过「百分比且绝对值」两个条件才标疑，不阻断交付。
alert:
  row_delta_pct: 20
  row_delta_abs: 100

# 逃生舱：join / 聚合 / 窗口函数这类 criteria 表达不了的，改 mode 为 sql 并把
# 整条查询写在下面。SQL 必须从 datasets 或其它登记视图出发，连接层只读。
# mode: sql
# sql: |
#   SELECT ...
"""

#: 回填形态的骨架。公共头一模一样，只有 `kind` 与 `fill:` 段不同——这正是
#: 「变只在 kind 与口径体」落到文件上的样子。
_FILL_SKELETON = """\
# 需求 {id}：{title}
#
# 公共头、节奏、护栏、标疑与查询型需求完全相同；改口径必须把 version 加一。
# 不同的只有下面两处：kind 与它对应的 fill: 口径体。
id: {id}
title: {title}
requester: {requester}
purpose: "{purpose}"
version: 1

# 交付节奏：once / daily / weekly / monthly / manual
cadence: {cadence}
kind: backfill

# 护栏与标疑对所有形态同义：rows 是这次交付的记录数（回填即输入表数据行数）。
guard:
  min_rows: 1
  max_rows: 500000
alert:
  row_delta_pct: 20
  row_delta_abs: 100

fill:
  source:
    path: "input/{source_name}"    # 受理时复制进来的输入快照；绝不指向外部文件
    sheet: Sheet1            # 不写则取第一个 sheet
    header_row: 2            # 列名所在行；其上可有多行分组标题
    key_column: URL          # 用列名定位，不用列号
  output:
    name: 清单-已补全.xlsx    # 交付档案里的文件名
    # copy_to: D:/somewhere/清单-已补全.xlsx   # 可选：档案落定后另存一份
  # overrides: overrides.json # 逐库人工判读结果；引擎只合并、不生成
  overwrite: false           # 只填空格；表里已有内容一律不动

  # 一列只能用一种取值方式：
  #   field    取底库某列（可配 join / translate / format / width / wrap）
  #   template 按底库列拼字符串，占位符缺值就整格留空，不拼半截链接
  #   const    常量
  #   manual   引擎不猜，值留空、原因进「补全说明」页；真值从 overrides 来
  columns:
    "数据集名称": {field: id}
    "下载量": {field: downloads}
    "来源": {const: HuggingFace}
    "训练阶段": {manual: 须读 README 判定，引擎不猜}
"""


def skeleton(
    *,
    request_id: str,
    title: str,
    requester: str,
    purpose: str = "",
    cadence: Cadence = Cadence.ONCE,
    kind: Kind = Kind.QUERY,
    source_path: str = "",
) -> str:
    """生成一份带注释的 spec 骨架。注释是给人看的，所以不走 YAML dump。

    用逐项 `replace` 而非 `str.format`：模板里本来就带 YAML 的花括号
    （`{metric: 下载量}`、`{field: id}`），`format` 会把它们当占位符，直接抛 KeyError。
    """
    values = {
        "{id}": request_id,
        "{title}": title,
        "{requester}": requester,
        "{purpose}": purpose,
        "{cadence}": cadence.value,
    }
    if kind is Kind.BACKFILL:
        # 骨架只写文件名：输入表由 `request new` 复制进 input/，spec 里不留任何
        # 绝对路径，整个需求目录才能搬走。文件必须真实存在，新建完的回读才过得去。
        values["{source_name}"] = Path(source_path).name if source_path else "清单.xlsx"
    text = _FILL_SKELETON if kind is Kind.BACKFILL else _SKELETON
    for placeholder, value in values.items():
        text = text.replace(placeholder, value)
    return text


def snapshot_history(spec_path: Path, version: int) -> Path:
    """把当前 spec 原样存一份到 `history/vN.yaml`，已存在则不动。

    复制字节而非重新生成：重新生成会带上「今天」的默认值与格式，将来改了默认值，
    旧快照就会悄悄变样，而它本该是当时那一刻的忠实记录。
    """
    target = spec_path.parent / HISTORY_DIR / f"v{version}.yaml"
    target.parent.mkdir(parents=True, exist_ok=True)
    if not target.exists():
        shutil.copyfile(spec_path, target)
    return target


def _plain(value: Any) -> Any:
    """把 YAML 解析出的值摊平成可稳定序列化的形状。

    `date` / `datetime` 要转字符串（`json.dumps` 不认它们），键一律转 str，
    免得 `1:` 与 `"1":` 被当成两种东西。
    """
    if isinstance(value, date | datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _plain(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_plain(item) for item in value]
    return value


def _spec_body(spec: Spec) -> dict[str, Any]:
    """把 Spec 摊成「决定产物长什么样」的那部分。

    只收口径，不收展示：标题、提出人、用途、节奏改来改去都不该算一版变更——
    改标题本来就不搬目录，若还逼人升版本，那条规矩就自相矛盾了。`version`
    也不收：它标识「第几版」，不标识「这一版说了什么」。
    """
    body: dict[str, Any] = {
        "kind": spec.kind.value,
        "mode": spec.mode.value,
        "sql": spec.sql,
        "criteria": [
            {"metric": item.metric, "op": item.op, "value": _plain(item.value)}
            for item in spec.criteria
        ],
        "order_by": [{"metric": key.metric, "descending": key.descending} for key in spec.order_by],
        "output": {"format": spec.output.format.value, "columns": list(spec.output.columns)},
        "guard": {"min_rows": spec.guard.min_rows, "max_rows": spec.guard.max_rows},
        "alert": {
            "row_delta_pct": spec.alert.row_delta_pct,
            "row_delta_abs": spec.alert.row_delta_abs,
        },
    }
    fill = spec.fill
    if fill is not None:
        body["fill"] = {
            # 输入只认 spec 里写的相对路径：`source.path` 是解析后的绝对路径，
            # 进了指纹就成了「这台机器专属」的哈希，目录一搬全对不上。
            "source": {
                "path": fill.source.relative,
                "sheet": fill.source.sheet,
                "header_row": fill.source.header_row,
                "key_column": fill.source.key_column,
            },
            "output": fill.output_name,
            "overrides": fill.overrides.name if fill.overrides else None,
            "overwrite": fill.overwrite,
            "lexicon": dict(fill.lexicon),
            "notes": [list(item) for item in fill.notes],
            "stamp_note": fill.stamp_note,
            "columns": [
                {
                    "name": item.name,
                    "kind": item.kind,
                    "field": item.field,
                    "template": item.template,
                    "const": _plain(item.const),
                    "reason": item.reason,
                    "format": item.fmt,
                    "join": item.join,
                    "translate": item.translate,
                    "width": item.width,
                    "wrap": item.wrap,
                }
                for item in fill.columns
            ],
        }
    return body


def spec_digest(spec_path: Path, *, base_dir: Path | None = None) -> str:
    """口径的**语义**指纹：只认内容，不认注释、空行、键序与展示字段。

    为什么不是文件字节的哈希：REQ-0001 的 spec 后来补了一行 `kind: query`（默认值
    本就是 query），字节变了、口径一分没变。按字节算，它立刻会被判成「版本号复用」
    而拦下——那是冤枉。反过来，把 `下载量 > 1000` 改成 `> 800`，语义一定变。

    也不只按原始 YAML 映射算：那样「补一个默认值」与「真的换了默认值」分不开。
    故先 `load()` 成 `Spec`（默认值全部补齐、枚举归一），再摊成 `_spec_body` 比。
    """
    return spec_body_digest(_spec_body(load(spec_path, base_dir=base_dir)))


def spec_body_digest(body: dict[str, Any]) -> str:
    """给已经摊平的 Spec 口径算指纹。拆出来只为让测试能直接喂结构。"""
    canonical = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def version_conflict(spec_path: Path, version: int) -> str | None:
    """当前内容是否占用了**别的**口径的版本号。有冲突返回人读说明，否则 None。

    `history/vN.yaml` 是「那一版真的用过」的忠实记录。若它已存在、语义却与当前
    spec 不同，说明有人就地改了口径没升版本：档案会记 `spec_version: N`，读档的人
    以为这就是当时那一版，而它已经不是了。这种错不会报错，只会让人拿着错的前提
    核对三个月——所以必须拦。
    """
    target = spec_path.parent / HISTORY_DIR / f"v{version}.yaml"
    if not target.is_file():
        return None
    # 两边都按需求目录解析相对路径：历史快照自己躺在 history/ 下，用它的父目录
    # 当基准，`input/清单.xlsx` 就会被读成 `history/input/清单.xlsx`。
    request_dir = spec_path.parent
    if spec_digest(target, base_dir=request_dir) == spec_digest(spec_path, base_dir=request_dir):
        return None
    return (
        f"v{version} 已被 {HISTORY_DIR}/{target.name} 占用，当前 spec 的口径却与它不同 —— "
        f"版本号不能复用。请把 version 改成 {version + 1}，"
        "并在 spec 顶部记一句「改了什么、为什么改」。"
    )


def history_versions(spec_path: Path) -> list[int]:
    """已快照的口径版本，升序。"""
    directory = spec_path.parent / HISTORY_DIR
    if not directory.is_dir():
        return []
    found: list[int] = []
    for path in directory.glob("v*.yaml"):
        tail = path.stem.removeprefix("v")
        if tail.isdigit():
            found.append(int(tail))
    return sorted(found)
