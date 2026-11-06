"""生成 `docs/DATA.md`：data 目录的结构、各文件作用与字段含义。

为什么由代码生成
----------------
这份文档最容易腐烂的地方不是文笔，是**清单**：哪几张表、每张表有哪些列。
手写的话，加一列就得记得改文档；忘了也不报错，读者照着过期文档查字段，
查不到还以为是数据有问题。

所以结构与统计一律从这里算：表清单来自 `tables.TABLES`，列来自各表 schema，
行数与体积来自实际文件。人只需要维护 `fielddocs` 里的「这一列是什么意思」。

同步由两部分保证：`hf-data-crawl docs` 随时可重新生成；
`tests/test_docs.py` 断言文档与当前 schema 一致——忘记重新生成会变红。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .fielddocs import describe
from .tables import TABLES, Origin, TableSpec

#: 由 raw 分块构成、不是登记表的东西。文档里单列一节说明它们的作用。
RAW_KINDS: tuple[tuple[str, str, str], ...] = (
    ("part-*.json.gz", "日增量扫描的分块（按 `lastModified` 倒序）", "bronze → silver → state"),
    ("top-*.json.gz", "顶部池刷新的分块（按 `downloads` 倒序）", "同上，让增速榜每天有新读数"),
    ("card-*.json.gz", "卡片补齐的分块（带 `cardData`）", "carddata · card_configs"),
)

#: data/ 下不属于任何表的文件与目录。
SIDE_FILES: tuple[tuple[str, str, str], ...] = (
    ("logs/", "计划任务的输出日志，按运行日期命名", "不参与任何派生"),
    (
        "runs/",
        "每轮运行摘要，成功与失败都写；`status` 与 `reason` 由命令结局推出",
        "只读，供对比两轮退化",
    ),
    ("reports/", "对账差异清单，只保留最近 8 份", "只读，供人工核对"),
    ("ALERT.txt", "体检发现异常时写入，正常时不存在", "`doctor` 写"),
    (
        "state/reconcile-seen.csv",
        "对账会话的已见集合，续跑时靠它避免把前半段误判下架",
        "对账完成后删除",
    ),
    ("state/*-checkpoint.json", "扫描游标，记录下一页从哪开始", "走到末页后清空"),
)


def _human_bytes(size: int) -> str:
    value = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.0f} {unit}" if unit == "B" else f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def _row_count(path: Path) -> int | None:
    """取行数。`scan + count` 走 parquet 元数据，不真读列数据（实测 0.02s/文件）。"""
    try:
        return int(pl.scan_parquet(path).select(pl.len()).collect().item())
    except (OSError, pl.exceptions.PolarsError):
        return None


def _type_name(dtype: Any) -> str:
    """类型名中文化。全表统一，避免「Float64」与「字符串」并排出现。"""
    names = {
        "String": "字符串",
        "Boolean": "布尔",
        "Int64": "整数",
        "Float64": "浮点",
        "Date": "日期",
        "Datetime(time_unit='us', time_zone='UTC')": "日期时间(UTC)",
        "List(String)": "字符串数组",
    }
    text = str(dtype)
    return names.get(text, text)


def _table_stats(root: Path, spec: TableSpec) -> str:
    """真实落盘情况：分区数、总行数、体积。文件不存在时明说「尚无数据」。"""
    if spec.partitioned:
        parents = sorted({path.parent for path in root.glob(spec.glob())})
        if not parents:
            return "尚无数据"
        files = [path for path in root.glob(spec.glob())]
    else:
        files = [root / spec.path]
        files = [path for path in files if path.exists()]
        parents = [root / spec.path] if files else []
        if not files:
            return "尚无数据"

    total_rows = sum(_row_count(path) or 0 for path in files)
    total_bytes = sum(path.stat().st_size for path in files)
    span = f"{len(parents)} 个分区 · " if spec.partitioned else ""
    return f"{span}{total_rows:,} 行 · {_human_bytes(total_bytes)}"


def _origin_note(origin: Origin) -> str:
    return {
        Origin.RAW: "由 `raw/` 派生，可随时重建",
        Origin.NETWORK: "来自外部接口，**删掉就要重抓**",
        Origin.STATE: "运行态，由派生过程维护",
    }[origin]


def missing_docs() -> list[tuple[str, str]]:
    """列出「有列但没有说明」的字段。测试据此断言文档不会漏。"""
    gaps: list[tuple[str, str]] = []
    for name, spec in TABLES.items():
        for column in spec.schema:
            if not describe(name, column):
                gaps.append((name, column))
    return gaps


def render(store_root: Path, now: datetime | None = None) -> str:
    """生成整份文档。"""
    stamp = (now or datetime.now()).strftime("%Y-%m-%d %H:%M")
    lines: list[str] = [
        "# data 目录说明",
        "",
        f"> 本文件由 `uv run hf-data-crawl docs` 生成于 {stamp}，**请勿手工编辑**。",
        "> 表结构与行数由代码和实际文件算出；字段含义维护在 `src/hf_data_crawl/fielddocs.py`。",
        "> 改完代码后重新生成即可，`tests/test_docs.py` 会在忘记生成时报错。",
        "",
        "## 一、数据是怎么流过来的",
        "",
        "```mermaid",
        "flowchart TD",
        '  API["Hugging Face API"] -->|"翻页扫描"| RAW["raw/<br/>原始响应"]',
        '  DS["datasets-server /size"] -->|"按需补齐"| SIZE["enrich/size.parquet"]',
        '  RAW -->|"解析·去重"| BRONZE["bronze/<br/>标量快照"]',
        '  BRONZE -->|"拆维度·只记变更"| SILVER["silver/<br/>属性流水"]',
        '  BRONZE -->|"折叠指标"| STATE["state/<br/>当前态 + 观测流水"]',
        '  RAW -->|"卡片解析"| ENRICH["enrich/carddata<br/>+ card_configs"]',
        '  SILVER --> GOLD["Gold 视图<br/>datasets / tag_values / 榜单"]',
        "  STATE --> GOLD",
        "  ENRICH --> GOLD",
        "  SIZE --> GOLD",
        "```",
        "",
        "**Raw 是唯一不可重算的一层**：其余各层删掉都能从它重新推出来，",
        "所以 `build` 可以断网重跑，而 `raw/` 一旦丢失就真的没有了。",
        "",
        "## 二、目录一览",
        "",
        "```",
        "data/",
        f"├── raw/dt=YYYY-MM-DD/          # 原始响应分块，永久保留（当前 {_dir_summary(store_root, 'raw')}）",
        f"├── bronze/dt=YYYY-MM-DD/       # 标量快照，只保留最新一天（当前 {_dir_summary(store_root, 'bronze')}）",
        f"├── silver/dt=YYYY-MM-DD/       # 属性变更流水（当前 {_dir_summary(store_root, 'silver')}）",
        f"├── state/                      # 当前态与观测流水（当前 {_dir_summary(store_root, 'state')}）",
        f"├── enrich/                     # 卡片与规模（当前 {_dir_summary(store_root, 'enrich')}）",
        f"├── meta/                       # 重建中断时留存的状态快照（当前 {_dir_summary(store_root, 'meta')}）",
        f"├── reports/                    # 对账差异清单（当前 {_dir_summary(store_root, 'reports')}）",
        f"├── runs/                       # 每轮运行摘要（当前 {_dir_summary(store_root, 'runs')}）",
        "└── logs/                       # 计划任务日志",
        "```",
        "",
        "## 三、各张表",
        "",
    ]

    for name, spec in TABLES.items():
        lines += [
            f"### `{spec.path}`",
            "",
            f"- 逻辑名：`{name}`",
            f"- 来源：{_origin_note(spec.origin)}",
            f"- 分区：{'按 `snapshot_date` 分目录，`dt=` 后即日期' if spec.partitioned else '单文件，不分区'}",
            f"- 现状：{_table_stats(store_root, spec)}",
            "",
            "| 字段 | 类型 | 含义 |",
            "| --- | --- | --- |",
        ]
        for column, dtype in spec.schema.items():
            note = describe(name, column) or "**待补**"
            lines.append(f"| `{column}` | {_type_name(dtype)} | {note} |")
        lines.append("")

    lines += [
        "## 四、不属于表的东西",
        "",
        "### raw 分块",
        "",
        "文件名形如 `<批次>-<页号>-<内容摘要>.json.gz`，摘要由内容算出：",
        "同一页同一内容重写为同一个文件（幂等），内容变了则另存（绝不覆盖丢数据）。",
        "排序依据是 payload 里的 `written_at`，**不是文件名、也不是文件修改时间**。",
        "",
        "| 前缀 | 批次 | 派生去向 |",
        "| --- | --- | --- |",
    ]
    for pattern, note, destination in RAW_KINDS:
        lines.append(f"| `{pattern}` | {note} | {destination} |")

    lines += [
        "",
        "### 其他",
        "",
        "| 路径 | 作用 | 生命周期 |",
        "| --- | --- | --- |",
    ]
    for path, note, life in SIDE_FILES:
        lines.append(f"| `{path}` | {note} | {life} |")

    lines += [
        "",
        "## 五、几个容易误解的地方",
        "",
        "1. **`downloads` 是近 30 天，不是累计**。累计值在 `downloads_all_time`，只增不减。",
        "2. **`card_num_rows` 与 `num_rows` 是两个口径**：前者是作者在卡片里声明的，后者是",
        "   datasets-server 实测的。两个都留着，就是为了让它们能对不上时被看见。",
        "3. **`silver` 是属性流水，不是每日快照**。一个 id 只在属性变化时追加一行，",
        "   所以它的行数会少于 `bronze` 的累计行数；指标历史在 `state/observations`。",
        "4. **`bronze` 只保留最新一天**。历史快照没有保留价值——它可由 `raw` 重算。",
        "5. **`status` 的 `restricted` 不是删除**。它表示数据集转为私有，数据还在，",
        "   只是不再公开。真正删除的 `disappeared` 在全站 111 万条里曾长期为 0。",
        "6. **`main_size` 是仓库字节，不等于数据本身的体积**。仓库含历史版本与 LFS 开销。",
        "7. **每张表都带 `url`，虽然它是算出来的**。HF 接口不返回地址——原始响应里只有",
        "   一个 `_id`，那是 MongoDB 内部主键，不是网页地址。地址由 `id` 唯一确定，所以",
        "   每张表都存一列：翻表时拿到 id 就能点开，不必先想起「要 join 哪个视图」。",
        "   这是一处有意的冗余，实测 111.7 万行只多 12.4 MB。",
        "8. **有几个「看着像结构、实际存成文本」的列**：`resource_group`、`features`、",
        "   `data_files`、`configs`。它们的内容是嵌套 JSON，但形态不固定——`features` 的键",
        "   随数据集而变，写成固定 struct 必然有一半字段落空。存文本是刻意的选择；",
        "   需要拆开时用 DuckDB 的 `json_extract` 或 polars 的 `str.json_decode`，",
        "   不必为了少数查询把结构硬编码进 schema。",
        "",
        "## 六、HF 页面上的数字，在表里是哪个字段",
        "",
        "数据集页面上那两张卡片（行数、文件大小）不是同一个来源，对应到表里也是不同字段。",
        "",
        "| 页面显示 | 表字段 | 覆盖 | 说明 |",
        "| --- | --- | --- | --- |",
        "| Number of rows | `size.num_rows` | 84.4% | datasets-server 实测行数 |",
        "| Total file size | `current.main_size` | ~100% | **仓库字节**，含 LFS 与历史版本 |",
        "| Estimated rows（预估行数） | `size.estimated_num_rows` | 2.9% | 上游按体积估算，精度差 |",
        "| Columns | `size.num_columns_max` | 67.5% | 各 config 中列数的最大值 |",
        "",
        "**两处必须留意的差异**：",
        "",
        "1. `Total file size` 取的是 `main_size`（仓库字节），**不是** `size.num_bytes_original`。",
        "   两者在 58% 的数据集上相等，但四分之一以上差 4 倍——多 config 的大数据集尤其明显",
        "   （实测 `permutans/wdc-common-crawl-embedded-jsonld`：页面 3.79 TB = `main_size`，",
        "   而 `num_bytes_original` 只有 1.48 TB）。要「和页面对得上」就用 `main_size`。",
        "2. `estimated_num_rows` 只在数据集太大、上游放弃精确统计时才出现，且**极不准**：",
        "   实测有 1.64 亿行被估成 289 亿（差 175 倍）的例子。它只适合做「量级判断」，",
        "   需要准确行数时看 `num_rows`，宁可空着也不要拿估算值冒充。",
        "",
        "页面上的卡片只在有数据时才显示——我们抓不到，通常意味着上游也没算出来。",
        "",
    ]
    return "\n".join(lines)


def _dir_summary(root: Path, name: str) -> str:
    """一个顶层目录的规模概览，用于目录树旁注。"""
    target = root / name
    if not target.exists():
        return "空"
    files = [path for path in target.rglob("*") if path.is_file()]
    if not files:
        return "空"
    size = sum(path.stat().st_size for path in files)
    return f"{len(files):,} 个文件 · {_human_bytes(size)}"


def write(store_root: Path, target: Path) -> Path:
    """生成并落盘。返回写入路径。"""
    from .atomic import write_text

    write_text(target, render(store_root))
    return target
