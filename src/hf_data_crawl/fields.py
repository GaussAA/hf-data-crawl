"""默认交付字段契约：不指定字段时，数据集筛选需求交什么。

为什么要有这份契约
------------------
`datasets` 宽视图有 72 列，但它是**管道全景**，不是给人看的交付面：水印
（`first_seen_at`）、抓取状态（`files_status` / `truncated`）、卡片凭证
（`source_sha`）、内部标记（`xet_enabled` / `resource_group`）全在里面；还有一批
只对头部补过、`card_num_rows` 1.6% / `card_dataset_bytes` 1.4% / `arxiv_ids` 3.7%
的近乎全空列。默认 `SELECT *` 把这些一股脑倒给对方，等于**没有定义口径**——
对方得自己猜哪列能用，而「下载量」与「累计下载量」并列出现又不附定义，歧义
照样会发生。

于是把「默认交什么」收敛成一份显式契约，三处共用：

- `wide.export_wide` 按它写 `data/exports/datasets.parquet`——这份文件即契约的
  物化，可直接交给 BI / DuckDB / Polars 当默认参考底库；
- `requirement.compile_sql` 在 `output.columns` 为空时按它投影；
- `doctor._artifact_freshness` 勾稽导出文件的列集与它是否一致，防漂移。

**为什么不让查询运行时去读导出文件的 schema**：导出物每天才刷新一次，可能滞后于
底库。若口径依赖它，就会出现「数据来自 A 版导出、manifest 记的是 B 版 current
指纹」的不一致。真源是本文件；导出物与查询都从它派生，doctor 负责盯住两者。

四条取舍判据
------------
1. **描述数据集本身**，不是采集/派生管道的内部状态。
2. **覆盖可用**：只对头部补过、九成行是空的列不进默认面。用户点名要的字段即使
   覆盖偏低也保留，并在说明里写明「未声明则为空，不代表没有」。
3. **同一语义只留主口径**，避免两张表都叫「大小」却互不相干；确需并列的两个
   字节口径，在 `COLUMN_NOTES` 里写清区别。
4. **词表登记过的口径优先**（`metrics.py`），它们自带可照抄进 README 的定义。

v2：把详情页那行标签补齐
------------------------
v1 只按覆盖率与语义裁列，没拿 Hugging Face 详情页逐项对值，结果把**无前缀标签**
（`Tags: keyword / reranking / persona …`）整组裁掉了——页面上看得见、库里找不着。
`tags` 是 API 返回的原样全量标签（带 `task_categories:` 之类前缀），`plain_tags`
是拆出来的自定义标签，正是页面红圈里那串。两列都补回来。

同时补三件事，专治「页面有、库里像没有」：

- `PAGE_FIELDS`：页面标签名 ↔ 库列名 ↔ 值的显示形式，照抄进交付 README；
- `FALLBACKS`：少数数据集只在卡片 YAML 里声明、没同步进 `tags` 的，
  默认投影用 `card_*` 兜底（实测 1,795 行任务、894 行规模、11 行语言）；
- `display_expr`：CSV 是给人看的，列表值按页面形式展平；parquet / 查询仍存机器码，
  便于筛算。数据与展示分开，谁都不将就。

v3：默认底库扁平化，打开就能看见值
----------------------------------
v2 把标签补回来了，但 parquet 里它们是 LIST（三层嵌套 `col → list → element`）。
主流 parquet 预览器 / Excel / BI 不认这个结构：有的把内部节点拼成
`LICENSES.LIST.ELEMENT` 当列名，取值失败后一律显示 NULL；有的整列留白。
大帅实测「预览看不见值」——一份**给人参考的默认底库**若连预览都读不出，
就没达成目的。

于是默认投影对 9 个标签列用 `array_to_string(..., ';')` 压成字符串：列集与别名
一字不改，导出物与 `compile_sql` 结果同源，doctor 的列集勾稽照旧；
`gold.datasets` 视图仍保留 LIST 原值，要按单个标签精确筛就回视图查。
CSV 交付另用 `display_text_expr` 把机器码换成页面形式——人读给文本，机读给码。

v4：克制扩充，另立扩展字段清单
------------------------------
大帅问「地区、论文、应用能不能也加」。实测两条反直觉的事实：`regions` 虽
100% 非空，但 **99.94% 是 HF 自动推断的 `us`**，零信息量；论文类 `arxiv_ids`
3.67% / `dois` 0.42% / `paperswithcode_id` 0.17%，九成以上库没声明。
HF 列表接口也不提供「被哪些模型 / Space 使用」这类应用字段。

默认面只加 5 列有实际信息量的：`pretty_name`（展示名 14.17%）、
`multilinguality`、`arxiv_ids`（论文）、`source_datasets` / `task_ids`（用途与来源）。
其余约 15 个语义字段不进默认面，改由 `EXTENDED_FIELDS` 登记定义与覆盖档：
它们一直都在 72 列视图里，`spec.output.columns` 点名即可投列——**默认面管的是
「不指定时给什么」，不是「总共有什么」**。低覆盖列塞回默认面，等于把 v1 裁掉的
「九成空」问题请回来。

改这份清单要升 `CONTRACT_VERSION`：交付档案会钉住当时的版本号，否则同一份
需求的两份交付会显得「口径没变却列数变了」。
"""

from __future__ import annotations

import polars as pl

from .gold import wide_column_names

#: 契约版本。增删字段、改列序、改列类型都要加一；只改说明文字不必。
CONTRACT_VERSION = 4

#: 分组 → 列。顺序即交付列序：标识在前、画像居中、数值在后，便于人读。
GROUPS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "标识与来源",
        ("id", "url", "author", "created_at", "last_modified"),
    ),
    ("内容描述", ("description",)),
    (
        "热度",
        ("downloads", "downloads_all_time", "likes"),
    ),
    (
        "可用性与许可",
        ("status", "gated", "license_name", "licenses"),
    ),
    (
        "语言与模态",
        ("languages", "modalities"),
    ),
    (
        "任务与数据类型",
        ("task_categories", "formats", "libraries"),
    ),
    (
        "标签",
        ("tags", "plain_tags"),
    ),
    (
        "补充画像",
        ("pretty_name", "multilinguality"),
    ),
    (
        "学术关联与用途",
        ("arxiv_ids", "source_datasets", "task_ids"),
    ),
    (
        "规模",
        ("size_categories", "server_num_rows", "server_num_columns", "server_config_count"),
    ),
    (
        "大小",
        ("main_size", "server_num_bytes_parquet"),
    ),
)

#: 默认交付字段，按 GROUPS 顺序展开。别在别处手抄第二份。
DEFAULT_COLUMNS: tuple[str, ...] = tuple(column for _, columns in GROUPS for column in columns)

#: 每列一句人话定义。会进交付 README 与数据说明，所以要说清口径与歧义。
COLUMN_NOTES: dict[str, str] = {
    "id": "数据集唯一标识（作者/名称），也是交付结果的排序兜底键",
    "url": "Hugging Face 数据集页面地址，可直接点开",
    "author": "仓库所属用户名或组织",
    "created_at": "仓库创建时刻（UTC）",
    "last_modified": "最后一次提交的时刻（UTC）",
    "description": "数据集简介，取自卡片原文；未写卡片则为空",
    "downloads": "近 30 天下载量，不是累计值；累计值见 downloads_all_time",
    "downloads_all_time": "历史累计下载量，只增不减；与近 30 天下载量是两个口径",
    "likes": "点赞总数",
    "status": "active 在架 / disappeared 已删除 / restricted 转私有 / renamed 已改名",
    "gated": "门禁：false 公开 / manual 需人工审批 / auto 自动授权",
    "license_name": "卡片声明的许可证名称（页面 License）；未声明则为空，不代表无许可",
    "licenses": "许可证标签列表（HF 结构化枚举），与 license_name 可能口径不同",
    "languages": "数据集语言标签列表（页面 Languages）；存 ISO 码如 ko，页面显示 Korean；未声明为空",
    "modalities": "数据模态标签（页面 Modalities，图像/文本/音频/视频等）；未声明则为空",
    "task_categories": "任务类别标签（页面 Tasks，即目标用途）；存 text-generation，页面显示 Text Generation",
    "formats": "文件格式标签列表（页面 Formats，parquet/json/csv 等）",
    "libraries": "支持库标签列表（页面 Libraries，datasets、pandas 等）",
    "size_categories": "规模档位标签（页面 Size）；存 100K<n<1M，页面显示 100K - 1M",
    "tags": "API 返回的原样标签全量（含 task_categories: 之类前缀），可追溯、可按任意前缀再拆",
    "plain_tags": "无前缀的自定义标签（页面 Tags，如 keyword / reranking）；本仓库拆分所得",
    "pretty_name": "卡片里的展示名（可能与作者/名称完全不同）；未声明则为空",
    "multilinguality": "多语言性标记（单语 / 多语 / 翻译等）；未声明则为空",
    "arxiv_ids": "关联的 arXiv 论文编号，如 2507.02259；仅少数库在卡片里声明",
    "source_datasets": "该数据集派生自哪些上游数据集；取值由作者填写，未必是 HF 数据集 id",
    "task_ids": "具体任务标识，比 task_categories 大类更细；未声明则为空",
    "server_num_rows": "datasets-server 实测行数；与卡片声明行数是两个口径",
    "server_num_columns": "各 config 中列数的最大值",
    "server_config_count": "datasets-server 返回的 config 数量",
    "main_size": "仓库主分支字节数（页面 Total file size 的机器值），十进制换算 MB 请除以 1e6",
    "server_num_bytes_parquet": "datasets-server 转成 parquet 后的字节数，是数据文件体积而非仓库体积",
}

#: 详情页标签行 → 库列 → 值的显示形式。交付 README 照抄，回答「页面那一项在哪一列」。
#: 一行可以对应多列（Tags 既有原样全量，也有拆出的自定义标签）。
PAGE_FIELDS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("Tasks", ("task_categories",), "Text Generation ↔ `text-generation`"),
    ("Modalities", ("modalities",), "Tabular, Text ↔ `tabular` / `text`"),
    ("Formats", ("formats",), "json ↔ `json`"),
    ("Languages", ("languages",), "Korean ↔ `ko`（ISO 码）"),
    ("Size", ("size_categories",), "100K - 1M ↔ `100K<n<1M`（档位，非精确行数）"),
    ("Tags", ("plain_tags", "tags"), "keyword / reranking … ↔ `plain_tags`；`tags` 另存含前缀全量"),
    ("Libraries", ("libraries",), "Datasets, pandas, Polars, +1 ↔ 列表四项，`+1` 是末项折叠显示"),
    ("License", ("license_name", "licenses"), "cc-by-nc-4.0 ↔ 标量 + 标签列表"),
    ("Downloads last month", ("downloads",), "12 ↔ 快照时点的近 30 天下载量，会随时间变化"),
    ("Total file size", ("main_size",), "729 MB ↔ 字节数；页面按文件大小统计，口径可能略有出入"),
)

#: 不进默认面、但一直都在 72 列视图里的语义字段。
#:
#: 登记在这里是为了回答「总共有什么、怎么拿」：默认面管「不指定时给什么」，
#: 不是「总共有什么」。覆盖档刻意写区间而非精确百分比——百分比会随每日采集漂移，
#: 档位不会。精确值以 `doctor` / 实测为准（本表定档于 2026-11-08，112 万行）。
EXTENDED_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("regions", "数据覆盖地区标签", "全库有值但 99.94% 是 HF 自动推断的 us，无区分度"),
    ("dois", "关联的 DOI 编号", "低（<1%）"),
    ("paperswithcode_id", "Papers with Code 条目 id", "极低（<0.2%）"),
    ("arxiv", "卡片里声明的 arXiv 编号", "极低（<0.1%）"),
    ("citation", "作者给出的引用格式（BibTeX / 纯文本）", "极低（<0.5%，长文本）"),
    ("license_link", "许可证全文链接", "低（<1%）"),
    ("card_homepage", "项目主页", "极低（<0.1%）"),
    ("card_description", "卡片正文的开头摘要，与 description 不是一回事", "极低（<0.1%）"),
    ("annotations_creators", "标注者类型（专家 / 众包 / 机器生成）", "低（约 1%）"),
    ("language_creators", "语料来源（原始采集 / 翻译 / 机器生成）", "低（约 1%）"),
    ("card_multilinguality", "卡片声明的多语言性；与 multilinguality 近重复", "低（约 4%）"),
    ("card_source_datasets", "卡片声明的上游数据集；与 source_datasets 近重复", "低（约 1%）"),
    ("card_num_rows", "卡片声明的行数；与 datasets-server 实测行数是两个口径", "低（约 1.6%）"),
    ("card_dataset_bytes", "卡片声明的数据体积", "低（约 1.4%）"),
    ("card_config_count", "卡片里列出的 config 个数", "中（约 43%）"),
    ("server_num_bytes_original", "datasets-server 统计的原始文件体积", "中（约 66%）"),
    ("server_configs", "datasets-server 返回的 config 明细（JSON 文本）", "中（约 67%）"),
    ("file_formats", "文件清单试点得到的扩展名计数（JSON 文本）", "极低（约 0.08%，头部子集）"),
    ("total_bytes", "文件清单试点得到的总字节数（下限）", "极低（约 0.08%，头部子集）"),
)

#: 交付 README 与数据说明里对扩展字段的统一说明。
EXTENDED_NOTE = (
    "以下字段一直都在 `gold.datasets` 视图里，只是**不进默认交付面**：低覆盖列塞回"
    "默认面会让九成行重新变空。需要时在 spec 里写 `output.columns: [id, <列名>]` "
    "即可投列（列名既认词条也认视图列名）。空值表示作者未声明，不代表没有。"
    "覆盖档基于 2026-11-08 全库 112 万行实测，精确值以当期 `doctor` 为准。"
)

#: 默认投影的兜底：主列为空时取卡片 YAML 声明的同义列。
#:
#: 只在这四列做，因为实测差集极小（1,795 / 894 / 11 / 64 行，合计不到 0.25%）；
#: 不改 `gold.datasets` 视图语义——内部查询仍能看到「标签列」与「卡片列」各自原值，
#: 便于追查到底哪一侧缺了。默认交付面才做合并。
FALLBACKS: dict[str, str] = {
    "task_categories": "card_task_categories",
    "languages": "card_languages",
    "size_categories": "card_size_categories",
    "plain_tags": "card_tags",
}

#: 默认投影里要压成字符串的 LIST 列。
#:
#: 不是「为了好看」：parquet 的 LIST 是三层嵌套，预览器把内部节点 `list` /
#: `element` 当列名取值，失败后整列显示 NULL。默认底库是交给人看的文件，
#: 必须让任何工具打开都能看见值。视图里的 LIST 原值不动，结构化筛选不受影响。
FLATTEN_COLUMNS: frozenset[str] = frozenset(
    {
        "licenses",
        "languages",
        "modalities",
        "task_categories",
        "formats",
        "libraries",
        "size_categories",
        "tags",
        "plain_tags",
        "multilinguality",
        "arxiv_ids",
        "source_datasets",
        "task_ids",
    }
)

#: 交付说明里对扁平列的一句统一口径，README 与数据说明都从这里取。
FLATTEN_NOTE = (
    f"上述 {len(FLATTEN_COLUMNS)} 个多值列在默认底库里是 `;` 连接的字符串"
    "（空列表为空串、未声明为 NULL）；要按单个标签精确筛选，请查 "
    "`gold.datasets` 视图里同名的 LIST 原值。"
)

#: CSV 展平时做「机器码 → 页面形式」变换的列。parquet 与 SQL 结果不变。
DISPLAY_COLUMNS: frozenset[str] = frozenset({"languages", "task_categories", "size_categories"})

#: 常见 ISO 639-1 → 英文名。未收录的（yue / ceb / cmn …）保留原码，不乱猜。
_LANGUAGE_NAMES: dict[str, str] = {
    "ar": "Arabic",
    "bn": "Bengali",
    "ca": "Catalan",
    "cs": "Czech",
    "da": "Danish",
    "de": "German",
    "el": "Greek",
    "en": "English",
    "es": "Spanish",
    "eu": "Basque",
    "fa": "Persian",
    "fi": "Finnish",
    "fr": "French",
    "he": "Hebrew",
    "hi": "Hindi",
    "hu": "Hungarian",
    "id": "Indonesian",
    "it": "Italian",
    "ja": "Japanese",
    "ko": "Korean",
    "ms": "Malay",
    "nl": "Dutch",
    "no": "Norwegian",
    "pl": "Polish",
    "pt": "Portuguese",
    "ro": "Romanian",
    "ru": "Russian",
    "sv": "Swedish",
    "sw": "Swahili",
    "ta": "Tamil",
    "te": "Telugu",
    "th": "Thai",
    "tr": "Turkish",
    "uk": "Ukrainian",
    "ur": "Urdu",
    "vi": "Vietnamese",
    "zh": "Chinese",
}


def default_columns() -> tuple[str, ...]:
    """默认交付列序。导出、编译、勾稽三处都从这里取。"""
    return DEFAULT_COLUMNS


def quoted_columns() -> str:
    """纯列名清单（带双引号），供文档与自检使用；实际投影走 `projection_sql`。"""
    return ", ".join(f'"{column}"' for column in DEFAULT_COLUMNS)


def projection_sql() -> str:
    """默认投影的 SQL 列清单：列序与别名不变，标签列压成 `;` 字符串。

    两层变换叠在一处：先按 `FALLBACKS` 补卡片声明，再按 `FLATTEN_COLUMNS`
    用 `array_to_string` 压平。别名一律取契约列名，所以导出文件、`compile_sql`
    结果与契约三者的列集逐列一致，`doctor` 的勾稽不必另写一套；变的只是列类型
    （LIST → VARCHAR）。`gold.datasets` 视图本身不改，内部仍可按标签精确筛。
    """
    parts: list[str] = []
    for column in DEFAULT_COLUMNS:
        fallback = FALLBACKS.get(column)
        if fallback is None:
            expression = f'"{column}"'
        else:
            expression = (
                f'CASE WHEN coalesce(array_length("{column}"), 0) > 0 '
                f'THEN "{column}" ELSE "{fallback}" END'
            )
        if column in FLATTEN_COLUMNS:
            parts.append(f"array_to_string({expression}, ';') AS \"{column}\"")
        elif fallback is None:
            parts.append(expression)
        else:
            parts.append(f'{expression} AS "{column}"')
    return ", ".join(parts)


def display_value(column: str, value: str) -> str:
    """单个标签值 → 页面上的可读形式。未知取值原样返回，绝不猜。"""
    if column == "languages":
        return _LANGUAGE_NAMES.get(value.lower(), value)
    if column == "task_categories":
        return value.replace("-", " ").title()
    if column == "size_categories":
        if "<n<" in value:
            low, high = value.split("<n<", 1)
            return f"{low} - {high}"
        if value.startswith("n<"):
            return f"< {value[2:]}"
        if value.startswith("n>"):
            return f"> {value[2:]}"
    return value


def _display_item(column: str) -> pl.Expr:
    """逐元素映射表达式；必须用在 `list.eval` 里（`pl.element()` 的上下文）。

    刻意全用 Polars 原生表达式而非 Python UDF：一次交付可能几十万行，逐格回调
    会把分钟级查询拖成小时级。
    """
    item = pl.element()
    if column == "languages":
        item = item.replace_strict(_LANGUAGE_NAMES, default=item)
    elif column == "task_categories":
        item = item.str.replace_all("-", " ").str.to_titlecase()
    elif column == "size_categories":
        item = item.str.replace_all("<n<", " - ").str.replace("^n<", "< ").str.replace("^n>", "> ")
    return item


def display_expr(column: str) -> pl.Expr:
    """LIST 列 → `;` 字符串，并换成页面形式（spec 显式投 LIST 列时用）。"""
    return pl.col(column).list.eval(_display_item(column)).list.join(";").alias(column)


def display_text_expr(column: str) -> pl.Expr:
    """已是 `;` 字符串的列 → 逐元素换成页面形式（v3 默认投影走这条）。

    `str.split` 对 NULL 返回 NULL，`list.eval` / `list.join` 保持为 NULL；空串仍是
    空串，不会凭空造出一个空标签。
    """
    return (
        pl.col(column).str.split(";").list.eval(_display_item(column)).list.join(";").alias(column)
    )


def contract_errors() -> list[str]:
    """契约自检；返回问题描述列表，空列表表示通过。

    放在这里而不是散在各消费方：拼错列名、重复列、漏写说明、丢了 `id` 兜底、
    对照表指向不存在的列，都该在「改完就发现」。
    """
    problems: list[str] = []
    known = set(wide_column_names())
    unknown = [column for column in DEFAULT_COLUMNS if column not in known]
    if unknown:
        problems.append(f"契约里有宽视图不存在的列：{unknown}")
    if len(DEFAULT_COLUMNS) != len(set(DEFAULT_COLUMNS)):
        problems.append("契约里有重复列")
    if not DEFAULT_COLUMNS or DEFAULT_COLUMNS[0] != "id":
        problems.append("契约第一列必须是 id（排序兜底与对人的可读性都靠它）")
    missing_notes = [column for column in DEFAULT_COLUMNS if column not in COLUMN_NOTES]
    if missing_notes:
        problems.append(f"这些列缺中文说明：{missing_notes}")
    extra_notes = sorted(set(COLUMN_NOTES) - set(DEFAULT_COLUMNS))
    if extra_notes:
        problems.append(f"说明里有契约之外的列：{extra_notes}")
    missing_fallbacks = [
        source
        for column, source in FALLBACKS.items()
        if column not in DEFAULT_COLUMNS or source not in known
    ]
    if missing_fallbacks:
        problems.append(f"兜底列在契约或宽视图里找不到：{missing_fallbacks}")
    missing_page = [
        column
        for _label, columns, _note in PAGE_FIELDS
        for column in columns
        if column not in DEFAULT_COLUMNS
    ]
    if missing_page:
        problems.append(f"页面对照表指向契约之外的列：{missing_page}")
    if not DISPLAY_COLUMNS <= set(DEFAULT_COLUMNS):
        problems.append("CSV 展平映射里有契约之外的列")
    if not FLATTEN_COLUMNS <= set(DEFAULT_COLUMNS):
        problems.append("扁平化清单里有契约之外的列")
    if not set(FALLBACKS) <= FLATTEN_COLUMNS:
        problems.append("卡片兜底列必须同时扁平化，否则导出物会混着 LIST 与 VARCHAR")
    extended = [column for column, _definition, _tier in EXTENDED_FIELDS]
    if len(extended) != len(set(extended)):
        problems.append("扩展字段清单里有重复列")
    unknown_extended = [column for column in extended if column not in known]
    if unknown_extended:
        problems.append(f"扩展字段清单里有视图不存在的列：{unknown_extended}")
    overlap = sorted(set(extended) & set(DEFAULT_COLUMNS))
    if overlap:
        problems.append(f"扩展字段清单与默认面重复：{overlap}")
    return problems
