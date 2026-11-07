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
from .gold import wide_column_names
from .raw import BRONZE_LABELS, CARD_LABEL
from .tables import TABLES, Origin, TableSpec

#: 各类 Raw 分块的作用与派生去向。类别清单取自 `raw` 的常量，避免文档与
#: 「谁进 Bronze、谁是卡片批次」这两条约定各写一份。
_LABEL_KINDS: dict[str, tuple[str, str]] = {
    "part": ("日增量扫描的分块（按 `lastModified` 倒序）", "bronze → silver → state"),
    "top": ("顶部池刷新的分块（按 `downloads` 倒序）", "同上，让增速榜每天有新读数"),
    CARD_LABEL: ("卡片补齐的分块（带 `cardData`）", "carddata · card_configs"),
}

#: 由 raw 分块构成、不是登记表的东西。文档里单列一节说明它们的作用。
RAW_KINDS: tuple[tuple[str, str, str], ...] = tuple(
    (f"{label}-*.json.gz", *_LABEL_KINDS[label]) for label in [*sorted(BRONZE_LABELS), CARD_LABEL]
)

#: 各表的粒度与它和 current 的关系。第七节据此逐行生成，行数实时计算。
_TABLE_GRAIN: dict[str, tuple[str, str]] = {
    "current": ("一个数据集一行", "**基准**——全站数据集总数"),
    "silver": ("一次属性变更一行", "**多对一**：改过几次就有几行，比基准多"),
    "observations": ("一次指标变化一行", "**多对一**：指标变过才追加"),
    "bronze": ("某天扫到的一条记录", "**子集**：只留最新一天，历史由 `raw` 重算"),
    "carddata": ("一个数据集一行", "**子集**：按下载量取头部补齐；长尾多数没有卡片"),
    "card_configs": ("一个 config/split 一行", "**展开**：卡片里的 config 逐条拆开"),
    "size": ("一个数据集一行", "**全量**：每库一条，其中约七成有实测行数"),
    "files": ("一个数据集一行", "**子集**：按下载量取头部补文件树，长尾没有"),
}

#: data/ 下不属于任何表的文件与目录。
SIDE_FILES: tuple[tuple[str, str, str], ...] = (
    ("logs/", "计划任务的输出日志，按运行日期命名", "不参与任何派生"),
    (
        "runs/",
        "每轮运行摘要，成功与失败都写（保留最近 `RUNS_KEEP` 条）；"
        "`status` 与 `reason` 由命令结局推出",
        "只读，供对比两轮退化",
    ),
    (
        "reports/",
        "对账差异清单（`reconcile-YYYY-MM-DD.csv`，保留最近 `REPORT_KEEP` 份）"
        "与榜单页 `dashboard.html`",
        "只读，供人工核对",
    ),
    ("ALERT.txt", "体检发现异常时写入，正常时不存在", "`doctor` 写"),
    (
        "state/reconcile-seen.csv",
        "对账会话的已见集合，续跑时靠它避免把前半段误判下架",
        "对账完成后删除",
    ),
    ("state/*-checkpoint.json", "扫描游标，记录下一页从哪开始", "走到末页后清空"),
    (
        "exports/",
        "宽表导出：`gold.datasets` 视图的单文件 parquet，供分析工具直接打开",
        "**可重建**：不参与派生，删掉后重跑 `export-wide`",
    ),
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
        '  SILVER --> GOLD["Gold 视图<br/>datasets / tag_values / tag_index / 榜单"]',
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
        f"├── raw/dt=YYYY-MM-DD/          # 原始响应：热区原样，7 天后压缩移出（当前 {_dir_summary(store_root, 'raw')}）",
        "├── cold/raw/                   # 冷区基线（跨天去重）+ manifest.json",
        "├── backup/raw/dt=YYYY-MM-DD/   # 归档/压缩移出的原分块，人工确认后清理",
        f"├── bronze/dt=YYYY-MM-DD/       # 标量快照，只保留最新一天（当前 {_dir_summary(store_root, 'bronze')}）",
        f"├── silver/dt=YYYY-MM-DD/       # 属性变更流水（当前 {_dir_summary(store_root, 'silver')}）",
        f"├── state/                      # 当前态与观测流水（当前 {_dir_summary(store_root, 'state')}）",
        f"├── enrich/                     # 卡片与规模（当前 {_dir_summary(store_root, 'enrich')}）",
        f"├── meta/                       # 重建中断时留存的状态快照（当前 {_dir_summary(store_root, 'meta')}）",
        f"├── reports/                    # 对账清单与榜单页（当前 {_dir_summary(store_root, 'reports')}）",
        f"├── runs/                       # 每轮运行摘要（当前 {_dir_summary(store_root, 'runs')}）",
        f"├── exports/                    # 宽表导出（可重建，当前 {_dir_summary(store_root, 'exports')}）",
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
        "文件名形如 `<类别>-<来源>-<页号>-<内容摘要>.json.gz`；旧格式与旧归档分片省略 `<来源>`。",
        "来源由 `ScanSpec` 的 mode + sort 派生，日增量、顶部刷新、卡片补齐各有独立页码空间。",
        "归档后变为 `<类别>-<来源>-archive-<序号>-<摘要>.json.gz`，分片内仍按原分段保留排序键。",
        "`cold/raw/baseline-*.json.gz` 是跨天去重的冷区基线，与热区分块一起回放。",
        "摘要由内容算出：同一页同一内容重写为同一个文件（幂等），内容变了则另存（绝不覆盖丢数据）。",
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
        "9. **`resource_group` 整列为空，不是抓漏了**。字段名在接口的 expand 白名单里，",
        "   我们也确实在请求里带了它，但列表接口收到请求也不返回（实测 2 万条里连键都不存在）。",
        "   它只在单库详情里出现，逐库请求 112 万次不现实——所以这一列保留但为空，",
        "   等上游哪天在列表里开放。",
        "10. **`current` 只有 19 列不是抓漏了**：语义字段在 `silver` / `carddata` / `size`，",
        "    由 `gold.datasets` 视图拼成宽表；需要单文件宽表见第八节。",
        "11. **白名单合法不等于接口会返回**：`siblings` / `resourceGroup` 请求了也拿不到，",
        "    实测数据与替代方案见第九节。",
        "",
        "## 六、HF 页面上的数字，在表里是哪个字段",
        "",
        "数据集页面上那两张卡片（行数、文件大小）不是同一个来源，对应到表里也是不同字段。",
        "",
        "| 页面显示 | 表字段 | 覆盖 | 说明 |",
        "| --- | --- | --- | --- |",
        f"| Number of rows | `size.num_rows` | {_coverage(store_root, 'size', 'num_rows')} | datasets-server 实测行数 |",
        f"| Total file size | `current.main_size` | {_coverage(store_root, 'current', 'main_size')} | **仓库字节**，含 LFS 与历史版本 |",
        f"| Estimated rows（预估行数） | `size.estimated_num_rows` | {_coverage(store_root, 'size', 'estimated_num_rows')} | 上游按体积估算，精度差 |",
        f"| Columns | `size.num_columns_max` | {_coverage(store_root, 'size', 'num_columns_max')} | 各 config 中列数的最大值 |",
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
        "## 七、各表行数为什么不一样",
        "",
        f"`current` 的 {_total_rows(store_root, 'current'):,} 行是全站数据集总数。看到别的表"
        "「只有几万或十几万」时，",
        "先看它是**什么粒度**——不是缺数据，是每行代表的不是同一个东西：",
        "",
        "| 表 | 行数 | 粒度 | 与 `current` 的关系 |",
        "| --- | --- | --- | --- |",
        *_grain_rows(store_root),
        "",
        "归纳起来只有三种：**一对一**（`current`，唯一全量）、**多对一**（流水，比基准多）、",
        "**子集**（按天或按需补齐，比基准少）。想知道某个数据集的全部信息，",
        "以 `current` 为主表 `LEFT JOIN` 其余各表即可——所有表的 id 都能在 `current` 里找到；",
        "不想自己拼 join 时，直接导出第八节那份宽表即可。",
        "",
        "## 八、宽表导出（可重建，不是事实源）",
        "",
        "`hf-data-crawl export-wide` 把 `gold.datasets` 视图原样写成一份 parquet，",
        f"默认落在 `data/exports/datasets.parquet`，当前共 {len(wide_column_names())} 列、",
        "行数与 `current` 相同。列清单不在这里手抄：`gold.wide_columns()` 是唯一真相源，",
        "视图 SQL、导出校验与本节列数都从它取，加一个附表字段三处一起跟上。",
        "",
        "**它是可重建的投影，不是第五张状态表**：",
        "",
        "- 不登记进 `TABLES`，不参与 `state.update` / `build` / `compact`；",
        "- 删掉不影响任何派生，重跑一次 `export-wide` 就能重建；",
        "- 事实源仍是 `raw/`，权威层仍是 `silver` / `observations` / `current`。",
        "",
        "为什么不直接给 `current` 加列：每轮采集只扫约二十万行，其余九十万行靠",
        "`_rebuild_current` 沿用旧行，而 `_align_frame` 对旧表缺列一律补 null；",
        "加列后约九成行的新字段会长期为空，且 `silver` 与 `current` 会各存一份",
        "「当前属性」。单独导出则状态表保持轻，宽表想加多少列都随视图走。",
        "",
        "写入先落同目录临时文件，校验视图列、导出列与行数后才原子替换；任一项",
        "不符就撤销，不会让半截文件顶替上一版。`--out` 只接受数据目录内的路径。",
        "",
        "## 九、实测：列表接口拿不到什么",
        "",
        "2026-11-07 对 383 万条 Raw 记录做全量普查，并逐项探接口。结论是：",
        "`expand[]` 白名单只保证「参数合法、不会 400」，**不保证列表接口会返回该字段**。",
        "把两者混为一谈，会白烧一整轮采集而拿不到任何新数据。",
        "",
        "| 字段 | 列表接口 | 单库详情 | 库内覆盖 | 结论 |",
        "| --- | --- | --- | --- | --- |",
        f"| `citation` | 返回但稀疏 | 有 | {_coverage(store_root, 'carddata', 'citation')} | 已在 `IDENTITY_EXPAND`，近乎零带宽 |",
        f"| `paperswithcode_id` | 返回但稀疏 | — | {_coverage(store_root, 'carddata', 'paperswithcode_id')} | 同上 |",
        f"| `cardData` | 返回（头部约七成有） | 有 | {_coverage(store_root, 'carddata', 'license_name')}（license_name） | 4.1 KB/条，全量重扫约 4.6 GB/轮 |",
        "| `siblings` | **请求也不返回** | 有，详情约 1.2 MB/次 | 0 | 只在详情接口，逐库补 112 万不现实 |",
        "| `resourceGroup` | 请求也不返回 | — | 0 | 历史占位列，整列为空 |",
        "",
        "带 `expand[]=siblings` 与 `full=true` 的列表响应都没有文件清单；轻量替代",
        "`/tree/main?recursive=true` 一次约 110-230 KB 且**上限 1000 条**，全量仍需数百 GB。",
        "但只补**头部**完全可行：`fetch-files` 默认每库取一页，有后续页就标 `truncated`，",
        "拿不到全库真值时如实承认是下限；单库详情头部实测 13-25 MB / 44-76 s，已弃用。",
        "文件级元数据只在榜单头部按需补，不进日常扫描。边界由",
        "`api.LIST_UNRETURNED_FIELDS` 与 `tests/test_schema_contract.py` 钉死。",
        "",
        "`citation` 与 `paperswithcode_id` 进基座只为让各来源 Raw 留下可回溯副本：",
        "它们不对应 Bronze 列、也不进宽表；`carddata` 表仍只由 `card-*` 分块解析，",
        "覆盖率受 `enrich --top` 限制。数值稀疏是作者没填，不是抓漏了。",
        "",
        "文件清单走 `fetch-files`（`/tree/main` 递归树）按下载量取头部补",
        f"`enrich/files.parquet`（当前 {_total_rows(store_root, 'files'):,} 行）。",
        "它是 `Origin.NETWORK` 表，重建不清；只覆盖头部，不能拿它算全站分布。",
        "默认每库一页（1000 条），`truncated=true` 表示还有后续页未取，",
        "此时 `file_count` / `total_bytes` / `formats` / `has_readme` 都只是下限；",
        "要完整清单用 `--max-pages`；已标截断的行默认被断点续跑跳过，补全须显式",
        "`--refresh-truncated`。目录条目不计入文件数，只统计 `type=file`。",
        "`recursive` 按路径排序、目录在前，头几页可能一个文件都没有；这种「没见到",
        "文件」不等于空仓库，此时四个指标一律留空，不拿 0 或 False 冒充。",
        "配额是固定窗口（匿名 500 请求 / 300 秒，带 `HF_TOKEN` 为 1000），"
        "429 按响应头 `Retry-After` 退避；",
        "临时失败不落库，重跑本命令即由断点续跑补回，不必整批重来。",
        "",
    ]
    return "\n".join(lines)


def _total_rows(root: Path, name: str) -> int:
    spec = TABLES[name]
    files = [root / spec.path] if not spec.partitioned else list(root.glob(spec.glob()))
    return sum(_row_count(path) or 0 for path in files if path.exists())


def _coverage(root: Path, name: str, column: str) -> str:
    """某列非空的真实占比。无文件、空表或读不到 schema 时返回占位符。"""
    spec = TABLES[name]
    files = [root / spec.path] if not spec.partitioned else list(root.glob(spec.glob()))
    files = [path for path in files if path.exists()]
    if not files:
        return "—"

    try:
        frame = pl.scan_parquet(files)
        total = int(frame.select(pl.len()).collect().item())
        if total == 0:
            return "—"
        filled = int(frame.select(pl.col(column).is_not_null().sum()).collect().item())
    except (OSError, pl.exceptions.PolarsError):
        return "—"

    return f"{filled / total * 100:.1f}%"


def _grain_rows(root: Path) -> list[str]:
    """第七节的表格行：行数实时算，粒度与关系取自常量。"""
    rows = []
    for name in _TABLE_GRAIN:
        grain, relation = _TABLE_GRAIN[name]
        rows.append(f"| `{name}` | {_total_rows(root, name):,} | {grain} | {relation} |")
    return rows


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
