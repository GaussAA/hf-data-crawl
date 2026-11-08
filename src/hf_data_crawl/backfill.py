"""按「给定的 URL 清单」回填表格字段。

这类需求会反复出现：对方给一张表，只有一列 URL（或数据集标识），其余列要按
约定口径填上。手填一次可以，填到第五次必然出错——尤其「下载量取 30 天还是
累计」这种口径，每换一个人就会重新裁一遍。

本模块把它拆成两件事，边界守得很死：

- **机械的归引擎**：URL 解析、批量取底库、枚举中文化、字节折 GB、按模板拼
  链接、写回表格、出覆盖报告。
- **判断的归人**：README 里「训练阶段是微调还是后训练」「构造方式是仿真还是
  真机」这类只能读原文才能定的列，由人（或助手）写成 `overrides.json` 一并
  进来。引擎自己不读 README，也不替人下结论。

**引擎永不猜测。** 判读不出来的列写 `manual`，值留空、原因进报告。一份看着
齐整、实则编造的表，比一份诚实地空着几格表危险得多——空着会有人来问，编造
不会。

spec 形状（`fills/<编号>/spec.yaml`）：

```yaml
name: FILL-0001
title: 具身感知数据集信息补充
source:
  path: C:/Users/me/Downloads/清单.xlsx
  sheet: Sheet1
  header_row: 2            # 列名所在行；其上可有多行分组标题
  key_column: URL          # 用列名定位，不用列号
output:
  path: C:/Users/me/Downloads/清单-已补全.xlsx
columns:
  来源:      {const: HuggingFace}
  数据集名称: {field: id}
  数据大小(GB): {field: main_size, format: gb}
  数据模态:   {field: modalities, join: "、", translate: true}
  论文URL:   {template: "https://arxiv.org/abs/{arxiv_id}"}
  训练阶段:   {manual: README 未明写训练阶段}
overrides: overrides.json   # 可选，人工判读结果
```

四条只写不说的规矩：

1. **绝不改原件**：`output.path` 与 `source.path` 相同则直接报错。
2. **默认不覆盖已有内容**：只填空格。要覆盖须显式 `overwrite: true`。
3. **未声明的列一律不碰**，并在报告里列出来，让人看得见还剩多少活。
4. **未命中底库的行不编值**，整行留空并点名。
"""

from __future__ import annotations

import json
import re
import zipfile
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal

import openpyxl
import yaml
from openpyxl.styles import Alignment, Font

from .atomic import replace
from .delivery import SourceStamp, stamp
from .gold import GoldCatalog
from .store import DatasetStore

#: 底库缺省端口。与 `gold.datasets` 视图同名，换视图时只改这一处。
VIEW = "datasets"

#: HF 的标签是英文枚举，回填进中文表要归一；词表外的新枚举原样保留，不硬猜。
#: spec 里可用 `lexicon:` 追加，但追加项只对本份 spec 生效。
LEXICON: dict[str, str] = {
    "image": "图像",
    "text": "文本",
    "video": "视频",
    "audio": "音频",
    "tabular": "表格",
    "timeseries": "时序",
    "depth": "深度",
    "pointcloud": "点云",
    "3d": "三维",
    "robotics": "机器人",
    "question-answering": "问答",
    "visual-question-answering": "视觉问答",
    "video-text-to-text": "视频-文本",
    "image-to-text": "图像-文本",
    "text-to-image": "文本-图像",
    "gated": "受限",
    "false": "否",
    "true": "是",
}

#: 数据集的合法标识：`作者/名称`。HF 的 id 不允许再分层，故只认两段。
_ID = re.compile(r"^[A-Za-z0-9][\w.\-]*/[\w.\-]+$")

#: 报告里「没写」的三类原因，措辞固定，便于统计时区分。
MISS_BASE = "底库无此行"
MISS_MANUAL = "需人工判读"
MISS_EMPTY = "底库此列为空"


class FillError(ValueError):
    """spec 写错了、表与 spec 对不上、或产物路径不安全。"""


@dataclass(frozen=True)
class TableSource:
    """输入表：路径、sheet、列名所在行、标识列名。"""

    path: Path
    sheet: str | None
    header_row: int
    key_column: str


@dataclass(frozen=True)
class ColumnRule:
    """一列怎么填。四种形态互斥，加载期就校验，执行期不再判类型。"""

    name: str
    kind: Literal["field", "template", "const", "manual"]
    field: str | None = None
    template: str | None = None
    const: Any = None
    reason: str | None = None
    fmt: str | None = None
    join: str | None = None
    translate: bool = False
    width: int | None = None
    wrap: bool = False


@dataclass(frozen=True)
class FillSpec:
    name: str
    title: str
    source: TableSource
    output: Path
    columns: tuple[ColumnRule, ...]
    overrides: Path | None = None
    overwrite: bool = False
    lexicon: dict[str, str] = field(default_factory=dict)
    notes: tuple[tuple[str, str], ...] = ()
    stamp_note: str | None = None

    def rule_for(self, name: str) -> ColumnRule | None:
        return next((item for item in self.columns if item.name == name), None)


@dataclass
class ColumnTally:
    """一列的账：写了多少、跳了多少、空了多少。"""

    name: str
    written: int = 0
    kept: int = 0  # 原有内容非空，按「不覆盖」跳过
    empty: int = 0
    from_override: int = 0


@dataclass
class FillReport:
    spec_name: str
    output: Path | None
    rows: int
    tallies: dict[str, ColumnTally]
    missing_ids: list[str]
    undeclared: list[str]
    manual: list[tuple[str, str]]
    stamp: SourceStamp | None = None

    def written(self) -> int:
        return sum(item.written for item in self.tallies.values())


# ---------------------------------------------------------------- URL 解析


def parse_reference(raw: str) -> str:
    """把一行里的「标识」解析成 `作者/名称`。

    认三种写法：完整 HF 链接（可带 `/tree/main`、查询串、尾斜杠）、`hf.co`
    短链、以及直接写 `作者/名称`。别的一律报错——猜一个像是标识的片段出来，
    比报错危险：它会安静地匹配到另一个库。
    """
    text = (raw or "").strip()
    if not text:
        raise FillError("标识为空")
    is_url = False
    for marker in ("huggingface.co/datasets/", "hf.co/datasets/"):
        if marker in text:
            text = text.split(marker, 1)[1]
            is_url = True
            break
    # 先掐掉查询串与锚点，再按段切：id 本身就是「作者/名称」两段，
    # 在第一个斜杠处截断会把 `nvidia/xxx` 切成 `nvidia`——看起来像解析成功，
    # 实际每一行都匹配不到，而报告只会说「未命中」。
    text = re.split(r"[?#]", text, maxsplit=1)[0]
    parts = [item for item in text.split("/") if item]
    if len(parts) < 2:
        raise FillError(f"不是 HuggingFace 数据集链接或 `作者/名称`：{raw!r}")
    if not is_url and len(parts) > 2:
        # 链接可以有 `/tree/main` 这类尾巴；裸标识没有——多一段基本是写错了，
        # 悄悄取前两段会把人指到另一个库上。
        raise FillError(f"裸标识应为「作者/名称」两段，实得 {len(parts)} 段：{raw!r}")
    candidate = f"{parts[0]}/{parts[1]}"
    if not _ID.fullmatch(candidate):
        raise FillError(f"不是 HuggingFace 数据集链接或 `作者/名称`：{raw!r}")
    return candidate


# ---------------------------------------------------------------- spec


class _StrictLoader(yaml.SafeLoader):
    """拒绝重复键。

    YAML 的默认行为是「后写覆盖前写」，而 spec 里出现两处同名，几乎总是笔误：
    改了下面那处、上面那处还留着，跑起来用的却是后者——人看着自己写的那行
    明明对，结果就是不对，最难查。宁可加载期直接拒绝。
    """


def _strict_mapping(
    loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise FillError(f"重复的键 {key!r}（第 {key_node.start_mark.line + 1} 行）")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _strict_mapping)


def load_spec(path: Path) -> FillSpec:
    try:
        body = yaml.load(path.read_text(encoding="utf-8"), Loader=_StrictLoader)
    except FileNotFoundError:
        raise FillError(f"找不到 spec：{path}") from None
    except yaml.YAMLError as error:
        raise FillError(f"{path}: YAML 解析失败：{error}") from None
    except FillError as error:
        raise FillError(f"{path}: {error}") from None
    if not isinstance(body, dict):
        raise FillError(f"{path}: 顶层必须是映射")

    name = _text(body.get("name"), "name", path)
    title = _text(body.get("title", name), "title", path)
    source = _source(body.get("source"), path)
    output = _output(body.get("output"), path, source)
    columns = tuple(
        _rule(key, value, path) for key, value in _mapping(body.get("columns"), path).items()
    )
    if not columns:
        raise FillError(f"{path}: columns 为空，没有可填的列")

    seen: set[str] = set()
    for item in columns:
        if item.name in seen:
            raise FillError(f"{path}: 列 {item.name!r} 声明了两次")
        seen.add(item.name)

    overrides = body.get("overrides")
    overrides_path = None if overrides in (None, "") else (path.parent / str(overrides)).resolve()
    lexicon_raw = body.get("lexicon") or {}
    lexicon = (
        {str(k): str(v) for k, v in lexicon_raw.items()} if isinstance(lexicon_raw, dict) else {}
    )
    notes = _notes(body.get("notes"), path)

    return FillSpec(
        name=name,
        title=title,
        source=source,
        output=output,
        columns=columns,
        overrides=overrides_path,
        overwrite=bool(body.get("overwrite", False)),
        lexicon=lexicon,
        notes=notes,
        stamp_note=None if body.get("stamp_note") in (None, "") else str(body["stamp_note"]),
    )


def _notes(raw: Any, path: Path) -> tuple[tuple[str, str], ...]:
    """说明页的补充条目：`notes: [{标题: 正文}]` 或 `notes: [[标题, 正文]]`。"""
    if raw in (None, ""):
        return ()
    if isinstance(raw, dict):
        return tuple((str(k), str(v)) for k, v in raw.items())
    if isinstance(raw, list):
        out: list[tuple[str, str]] = []
        for item in raw:
            if isinstance(item, dict) and len(item) == 1:
                key, value = next(iter(item.items()))
                out.append((str(key), str(value)))
            elif isinstance(item, (list, tuple)) and len(item) == 2:
                out.append((str(item[0]), str(item[1])))
            else:
                raise FillError(f"{path}: notes 的条目须是单键映射或二元组")
        return tuple(out)
    raise FillError(f"{path}: notes 须是映射或列表")


def _source(raw: Any, path: Path) -> TableSource:
    if not isinstance(raw, dict):
        raise FillError(f"{path}: source 必须是映射（path / sheet / header_row / key_column）")
    table = Path(str(raw.get("path", ""))).expanduser()
    if not table.is_absolute():
        table = (path.parent / table).resolve()
    if not table.exists():
        raise FillError(f"{path}: 输入表不存在：{table}")
    header_row = int(raw.get("header_row", 1))
    if header_row < 1:
        raise FillError(f"{path}: header_row 从 1 起算")
    sheet = raw.get("sheet")
    return TableSource(
        path=table,
        sheet=None if sheet in (None, "") else str(sheet),
        header_row=header_row,
        key_column=_text(raw.get("key_column"), "source.key_column", path),
    )


def _output(raw: Any, path: Path, source: TableSource) -> Path:
    if isinstance(raw, dict):
        target = raw.get("path")
        sheet = raw.get("sheet")
    else:
        target, sheet = raw, None
    del sheet  # 输出 sheet 名保留原样，不需要额外声明
    if target in (None, ""):
        raise FillError(f"{path}: output.path 必填")
    out = Path(str(target)).expanduser()
    if not out.is_absolute():
        out = (path.parent / out).resolve()
    if out.resolve() == source.path.resolve():
        # 补全是「产出」，不是「就地修改」。原件是唯一可回退的凭据，不能被写掉。
        raise FillError(f"{path}: output 不能与 source 相同（{out}），补全请另存")
    return out


def _mapping(raw: Any, path: Path) -> dict[str, Any]:
    if not isinstance(raw, dict):
        raise FillError(f"{path}: columns 必须是「列名 → 规则」的映射")
    return {str(key): value for key, value in raw.items()}


def _rule(name: str, raw: Any, path: Path) -> ColumnRule:
    if not isinstance(raw, dict):
        raise FillError(f"{path}: 列 {name!r} 的规则必须是映射")
    kinds = [key for key in ("field", "template", "const", "manual") if key in raw]
    if len(kinds) != 1:
        raise FillError(
            f"{path}: 列 {name!r} 必须且只能用 field / template / const / manual 之一，实得 {kinds or '无'}"
        )
    kind = kinds[0]
    join = raw.get("join")
    width = raw.get("width")
    return ColumnRule(
        name=name,
        kind=kind,  # type: ignore[arg-type]
        field=None if kind != "field" else _text(raw["field"], f"{name}.field", path),
        template=None if kind != "template" else str(raw["template"]),
        const=raw.get("const"),
        reason=None if kind != "manual" else (str(raw["manual"]) or "未说明原因"),
        fmt=None if raw.get("format") in (None, "") else str(raw["format"]),
        join=None if join in (None, "") else str(join),
        translate=bool(raw.get("translate", False)),
        width=None if width in (None, "") else int(str(width)),
        wrap=bool(raw.get("wrap", False)),
    )


def _text(raw: Any, key: str, path: Path) -> str:
    if raw in (None, ""):
        raise FillError(f"{path}: {key} 必填")
    return str(raw)


# ---------------------------------------------------------------- 取值


def _lexicon(spec: FillSpec) -> dict[str, str]:
    return {**LEXICON, **spec.lexicon}


def _translate(value: Any, table: dict[str, str]) -> Any:
    if isinstance(value, list):
        return [_translate(item, table) for item in value]
    if not isinstance(value, str):
        return value
    return table.get(value.strip(), value)


def _format(value: Any, fmt: str | None) -> Any:
    if value is None:
        return None
    if fmt in (None, "", "raw"):
        return value
    if fmt == "gb":
        # 十进制 GB：与 HuggingFace 页面、README 自述（4.5TB）同一口径。
        # 用 1024³ 会系统性偏小 7%，对不上任何一处公开数字。
        return round(float(value) / 1e9, 1)
    if fmt == "date":
        if isinstance(value, datetime):
            return value.date().isoformat()
        if isinstance(value, date):
            return value.isoformat()
        return str(value)[:10]
    if fmt == "int":
        return int(value)
    if fmt == "text":
        return str(value)
    if fmt == "prose":
        # 底库的 description 是卡片原文，夹着制表符、连续空行与 HTML 残片；
        # 不回炉就写进表里，等于把一堆空白格交给对方。这里只做「压平」：
        # 逐行去空白、丢空行、保留行结构，不删正文、不改措辞——改写属于判断。
        text = re.sub(r"<[^>]+>", " ", str(value))
        lines = [re.sub(r"[ \t\u3000]+", " ", item).strip() for item in text.splitlines()]
        return "\n".join(item for item in lines if item)
    raise FillError(f"未知 format：{fmt!r}（可用 raw / gb / date / int / text / prose）")


_PLACEHOLDER = re.compile(r"\{(\w+)\}")


def render(rule: ColumnRule, row: dict[str, Any], spec: FillSpec) -> Any:
    """按规则算出一格的值；算不出来返回 None（上层记为「空」）。

    None 的语义是「不知道」，不是「空字符串」——写成空串会让下游以为这个人
    字段真就是空的，而它可能只是没抓到。
    """
    if rule.kind == "const":
        return rule.const
    if rule.kind == "manual":
        return None
    if rule.kind == "field":
        value = row.get(rule.field or "")
        if rule.translate:
            value = _translate(value, _lexicon(spec))
        if rule.join and isinstance(value, list):
            value = rule.join.join(str(item) for item in value)
        return _format(value, rule.fmt)
    # template：占位符缺值就整格留空，不拼出半截 URL。
    template = rule.template or ""
    names = _PLACEHOLDER.findall(template)
    if not names:
        return template
    parts: dict[str, str] = {}
    for key in names:
        value = row.get(key)
        if isinstance(value, list):
            # 占位符是单值语境；`arxiv_ids` 这类列表取首项——一篇论文一个链接。
            value = value[0] if value else None
        if value in (None, "", []):
            return None
        parts[key] = str(_format(value, rule.fmt)) if rule.fmt else str(value)
    return template.format(**parts)


# ---------------------------------------------------------------- 取数


def resolve_ids(values: list[str]) -> tuple[list[str | None], list[str]]:
    """把标识列逐行解析成 `作者/名称`；认不出的记 None 并入坏行清单。

    返回与入参**等长**的列表：短一截就会让后面每一行对错号，而错号的表看起来
    完全正常。坏行不阻塞整批——二十行、坏一行，剩下十九行照样该填。
    """
    resolved: list[str | None] = []
    bad: list[str] = []
    for raw in values:
        try:
            resolved.append(parse_reference(raw))
        except FillError:
            resolved.append(None)
            bad.append(raw)
    return resolved, bad


def fetch_rows(catalog: GoldCatalog, ids: list[str]) -> dict[str, dict[str, Any]]:
    """一次取回全部行。逐条查会把 72 列 112 万行的视图点开上千遍。"""
    if not ids:
        return {}
    unique = sorted({item for item in ids})
    quoted = ",".join("'" + item.replace("'", "''") + "'" for item in unique)
    frame = catalog.run(f"SELECT * FROM {VIEW} WHERE id IN ({quoted})")
    # id 不 pop：它本身就是要填的「数据集名称」列。
    return {str(row["id"]): row for row in frame.to_dicts()}


def load_overrides(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    if not path.exists():
        raise FillError(f"找不到 overrides：{path}")
    body = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(body, dict):
        raise FillError(f"{path}: overrides 必须是「标识 → {{列: 值}}」的映射")
    return {str(key): dict(value) for key, value in body.items() if isinstance(value, dict)}


# ---------------------------------------------------------------- 表格读写


def _open_sheet(spec: FillSpec) -> tuple[Any, Any]:
    workbook = openpyxl.load_workbook(spec.source.path)
    name = spec.source.sheet or workbook.sheetnames[0]
    if name not in workbook.sheetnames:
        raise FillError(f"{spec.source.path}: 没有 sheet {name!r}（现有 {workbook.sheetnames}）")
    return workbook, workbook[name]


def _column_index(sheet: Any, header_row: int) -> dict[str, int]:
    """列名 → 列号。空列名的占位列跳过，重名报错。"""
    index: dict[str, int] = {}
    for column in range(1, sheet.max_column + 1):
        value = sheet.cell(row=header_row, column=column).value
        if value in (None, ""):
            continue
        name = str(value).strip()
        if name in index:
            raise FillError(f"表头第 {header_row} 行有重名列 {name!r}，无法定位")
        index[name] = column
    return index


def _data_rows(sheet: Any, key_column: int, header_row: int) -> list[int]:
    """标识列非空的行，即数据区。表尺寸常报到几百行，空行不能算进去。"""
    rows: list[int] = []
    for row in range(header_row + 1, sheet.max_row + 1):
        value = sheet.cell(row=row, column=key_column).value
        if value not in (None, ""):
            rows.append(row)
    return rows


def run(
    store: DatasetStore,
    spec: FillSpec,
    *,
    catalog: GoldCatalog | None = None,
    source: SourceStamp | None = None,
    dry_run: bool = False,
) -> FillReport:
    workbook, sheet = _open_sheet(spec)
    header_row = spec.source.header_row
    index = _column_index(sheet, header_row)
    if spec.source.key_column not in index:
        raise FillError(
            f"表头第 {header_row} 行没有 {spec.source.key_column!r} 列（现有：{'、'.join(list(index)[:8])}…）"
        )
    key_column = index[spec.source.key_column]
    rows = _data_rows(sheet, key_column, header_row)
    if not rows:
        raise FillError(
            f"{spec.source.path}: 标识列 {spec.source.key_column!r} 全空，没有可处理的行"
        )

    declared = [item.name for item in spec.columns]
    for name in declared:
        if name not in index:
            raise FillError(f"表里没有 {name!r} 列——spec 与表头对不上，先核对列名")

    raw_keys = [str(sheet.cell(row=row, column=key_column).value).strip() for row in rows]
    resolved, bad = resolve_ids(raw_keys)
    ids = [item for item in resolved if item is not None]
    runner = catalog or GoldCatalog(store)
    stamp_value = source or stamp(store)
    rows_by_id = fetch_rows(runner, ids)
    overrides = load_overrides(spec.overrides)

    tallies = {name: ColumnTally(name) for name in declared}
    missing: list[str] = list(bad)
    for excel_row, raw, ds_id in zip(rows, raw_keys, resolved, strict=True):
        record = rows_by_id.get(ds_id) if ds_id else None
        if record is None:
            missing.append(raw)
        for rule in spec.columns:
            tally = tallies[rule.name]
            column = index[rule.name]
            existing = sheet.cell(row=excel_row, column=column).value
            if existing not in (None, "") and not spec.overwrite:
                # 原有内容优先：人家手填的，可能比底库新，也可能有底库没有的判断。
                tally.kept += 1
                continue
            manual = overrides.get(ds_id, {}).get(rule.name) if ds_id else None
            if manual not in (None, ""):
                value = manual
                tally.from_override += 1
            elif record is None:
                value = None
            else:
                value = render(rule, record, spec)
            if value in (None, "", []):
                tally.empty += 1
                continue
            if not dry_run:
                sheet.cell(row=excel_row, column=column, value=value)
            tally.written += 1

    manual_columns = [
        (item.name, item.reason or "") for item in spec.columns if item.kind == "manual"
    ]
    undeclared = [name for name in index if name not in set(declared)]

    if not dry_run:
        _annotate(sheet, header_row, index, declared, tallies, spec, len(rows), missing)
        anchor = _anchor(stamp_value)
        _fix_properties(workbook, anchor)
        spec.output.parent.mkdir(parents=True, exist_ok=True)
        temp = spec.output.with_suffix(spec.output.suffix + ".tmp")
        workbook.save(temp)
        _normalize_zip(temp, anchor)
        replace(temp, spec.output)

    return FillReport(
        spec_name=spec.name,
        output=None if dry_run else spec.output,
        rows=len(rows),
        tallies=tallies,
        missing_ids=missing,
        undeclared=undeclared,
        manual=manual_columns,
        stamp=stamp_value,
    )


def _normalize_zip(path: Path, anchor: datetime) -> None:
    """把 xlsx 内每个条目的时间戳钉死，并改写 `dcterms:modified`。

    xlsx 是个 zip，openpyxl 存盘时把每个条目的 mtime 写成「此刻」，`modified`
    属性也在存盘时被它重写——内容全同、每次 md5 却不同，等于「可复现」只做到
    一半，而这一半恰是最容易被当成「他改了东西」的那一半。
    """
    with zipfile.ZipFile(path) as source:
        members = [(info.filename, source.read(info.filename)) for info in source.infolist()]
    stamp = anchor.strftime("%Y-%m-%dT%H:%M:%SZ").encode()
    temp = path.with_suffix(path.suffix + ".zip")
    with zipfile.ZipFile(temp, "w", zipfile.ZIP_DEFLATED) as target:
        for name, payload in members:
            if name == "docProps/core.xml":
                payload = re.sub(
                    rb"<dcterms:modified[^>]*>[^<]*</dcterms:modified>",
                    b'<dcterms:modified xsi:type="dcterms:W3CDTF">'
                    + stamp
                    + b"</dcterms:modified>",
                    payload,
                )
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            target.writestr(info, payload)
    replace(temp, path)


def _anchor(stamp_value: SourceStamp) -> datetime:
    """文档时间戳的锚点：底库快照日零点。与运行时刻无关，故可复现。"""
    moment = stamp_value.newest_snapshot_date or date.today()
    return datetime.combine(moment, datetime.min.time())


def _fix_properties(workbook: Any, anchor: datetime) -> None:
    """把文档时间戳锚到底库快照日，使「同输入 → 同字节」。

    xlsx 里唯一会随运行时刻变化的就是 `docProps/core.xml` 的两个时间戳；不锚住，
    同一份表连跑两次 md5 都不同，「这次和上次是不是一模一样」就只能靠人肉比对。
    真正的运行时刻另有 `runs/*.json` 记着，不靠文档属性承担。
    """
    workbook.properties.created = anchor
    workbook.properties.modified = anchor
    workbook.properties.creator = "hf-data-crawl"
    workbook.properties.lastModifiedBy = "hf-data-crawl"


def _annotate(
    sheet: Any,
    header_row: int,
    index: dict[str, int],
    declared: list[str],
    tallies: dict[str, ColumnTally],
    spec: FillSpec,
    rows: int,
    missing: list[str],
) -> None:
    """写「补全说明」页，并把长文本列开换行——否则填进去的字会被整片截断。

    说明页单独一页而非塞进原表：原表是对方给的表，它只该有它的列。
    """
    name = "补全说明"
    if name in sheet.parent.sheetnames:
        del sheet.parent[name]
    note = sheet.parent.create_sheet(name)
    note.column_dimensions["A"].width = 22
    note.column_dimensions["B"].width = 88

    lines: list[tuple[str, str]] = [
        ("任务", f"{spec.name} · {spec.title}"),
        ("范围", f"第 {header_row + 1}–{header_row + rows} 行、共 {rows} 条记录"),
        (
            "写入",
            f"{sum(item.written for item in tallies.values())} 格；"
            f"原有内容保留 {sum(item.kept for item in tallies.values())} 格"
            + ("（本份 spec 关闭了覆盖）" if not spec.overwrite else ""),
        ),
    ]
    if spec.stamp_note:
        lines.append(("数据来源", spec.stamp_note))
    for item in spec.columns:
        if item.kind == "manual":
            lines.append((f"留空 · {item.name}", f"需人工判读：{item.reason}"))
    if missing:
        sample = "、".join(missing[:8])
        tail = "" if len(missing) <= 8 else f" 等 {len(missing)} 条"
        lines.append(("未命中", f"{sample}{tail}"))
    lines.extend(spec.notes)

    for row, (key, text) in enumerate(lines, start=1):
        note.cell(row=row, column=1, value=key).font = Font(bold=True)
        note.cell(row=row, column=2, value=text).alignment = Alignment(
            wrap_text=True, vertical="top"
        )

    # 长文本不开换行就会被右邻格整片盖住：内容其实在，眼睛看不见，等于没填。
    # 只在 spec 明确声明 wrap 或 width 的列上动手，别去猜哪些列「看起来长」。
    for item in spec.columns:
        if not (item.wrap or item.width):
            continue
        column = index[item.name]
        if item.width:
            sheet.column_dimensions[
                sheet.cell(row=header_row, column=column).column_letter
            ].width = item.width
        for row in range(header_row + 1, header_row + rows + 1):
            sheet.cell(row=row, column=column).alignment = Alignment(
                wrap_text=True, vertical="top", horizontal="left"
            )
