"""字段语义：每一列「是什么意思」。

为什么单独一份
--------------
`tables.py` 记的是**结构**（有哪些列、什么类型），机器读得懂；这一份记的是
**含义**，机器猜不出来。两者分开，`docs` 命令才能把「结构自动、语义人工」
拼成一份完整的数据字典。

维护规则
--------
- 只写「这一列回答什么问题」，不写「它是怎么算出来的」——后者属于代码注释。
- 通用列放 `COMMON`，各表共用；表内同名列覆盖通用说明。
- 新增列却忘了写说明时，`docs` 命令会把它标成「待补」并计数，
  `tests/ops/test_docs.py` 会因此变红——不会悄悄漏掉。
"""

from __future__ import annotations

#: 多张表共用的列。
COMMON: dict[str, str] = {
    "id": "数据集标识，形如 `作者/名称`，全库唯一",
    "url": "数据集页面地址，形如 `https://huggingface.co/datasets/作者/名称`；"
    "由 `id` 拼出，可直接点开",
    "author": "拥有者账号（`id` 的前半段）",
    "sha": "主分支最新提交哈希；仓库内容一变它就变",
    "created_at": "仓库创建时刻（UTC，带时区的时间类型，可直接做时间运算）",
    "last_modified": "最后一次提交的时刻（UTC）；增量扫描按它排序",
    "snapshot_date": "快照日期，也是分区键——同一条记录落在哪个 `dt=` 目录",
    "downloads": "**近 30 天**下载量。不是累计值，也不是当日增量",
    "downloads_all_time": "历史累计下载量，只增不减",
    "likes": "点赞数",
    "trending_score": "HF 计算的趋势分，可为空；它随时间漂移，**不能用来排序翻页**",
    "gated": "访问门槛：`false` 公开、`manual` 需人工审批、`auto` 自动授权",
    "private": "是否私有。私有库对外不可见，但公开索引里可能仍留一条记录",
    "disabled": "是否被平台禁用",
    "main_size": "主分支的仓库字节数（含 LFS 指针指向的实际大小）",
    "xet_enabled": "是否使用 Xet 存储后端",
    "resource_group": "所属资源组；原值是复合结构，按 JSON 文本存储。**列表接口从不返回此字段**（实测请求了也拿不到），故整列为空",
    "description": "仓库简介（作者填写，非卡片正文）",
    "tags": "原始标签全集，形如 `license:mit`、`language:en`；维度列都由它拆出",
}


#: 各表专属列。与 `COMMON` 同名的以这里为准。
FIELD_DOCS: dict[str, dict[str, str]] = {
    "bronze": {
        "raw": "该条记录对应的原始 JSON 整行——其余各层都能重算，只有它能回溯上游",
        "page_file": "来源分块文件名，用于把一行追回到具体的 raw 文件",
    },
    "silver": {
        "task_categories": "任务大类（`text-classification`、`image-segmentation` 等）",
        "task_ids": "具体任务标识，比大类更细",
        "languages": "语言代码，已把三字码折成两字码（`eng`→`en`）；无 ISO 639-1 码的语言（`yue`、`ceb`）保持原样",
        "size_categories": "规模档位（`n<1K`、`1K<n<10K` … `n>1T`）",
        "licenses": "许可证（`mit`、`apache-2.0`、`cc-by-4.0` 等）",
        "libraries": "适配的框架库（`datasets`、`pandas`、`polars` …）",
        "modalities": "模态（`text`/`image`/`audio`/`video`/`3d`/`tabular` 等；无前缀的 `multimodal` 也会并入，属本仓库扩展取值）",
        "formats": "数据文件格式（`parquet`、`csv`、`json`、`arrow` …）",
        "regions": "数据覆盖的地区",
        "multilinguality": "多语言性标记（单语 / 多语）",
        "source_datasets": "该数据集派生自哪些上游数据集",
        "annotations_creators": "标注者类型（专家、众包、机器生成等）",
        "language_creators": "语料来源（原始采集、翻译、机器生成等）",
        "arxiv_ids": "关联的 arXiv 论文编号",
        "dois": "关联的 DOI",
        "plain_tags": "不带前缀的标签（旧式 `text`/`image` 标记都在这里）",
        "snapshot_date": "该行**生效**的日期；同 id 取最新一行即为当前属性",
    },
    "observations": {
        "snapshot_date": "这次观测发生的日期",
    },
    "current": {
        "first_seen_at": "首次出现在库里的日期",
        "last_seen_at": "最后一次被扫描到的日期；停更不等于下架",
        "status": "`active` 在架 / `disappeared` 已删除 / `restricted` 转私有 / `renamed` 已改名",
    },
    "carddata": {
        "source_sha": (
            "抓这张卡时该库的 commit sha。拿它与 `sha`（仓库当前版本）对照，"
            "就能判断卡片是否落后——`snapshot_date` 只说什么时候抓的，不说内容对应哪一版。"
            "加列之前抓的行为空，属预期"
        ),
        "pretty_name": "卡片里的展示名（可能与 `作者/名称` 完全不同）",
        "paperswithcode_id": "Papers with Code 上的条目 id",
        "arxiv": "卡片声明的 arXiv 编号",
        "license_name": "卡片声明的许可证名称",
        "license_link": "许可证全文链接",
        "citation": "作者给出的引用格式（BibTeX 或纯文本）",
        "card_description": "**卡片正文**的开头摘要，与 `description` 字段不是一回事",
        "card_tags": "卡片 YAML 里声明的标签（作者口径，非平台规范化）",
        "card_task_categories": "卡片声明的任务大类",
        "card_languages": "卡片声明的语言",
        "card_size_categories": "卡片声明的规模档",
        "card_multilinguality": "卡片声明的多语言性",
        "card_annotations_creators": "卡片声明的标注者类型",
        "card_language_creators": "卡片声明的语料来源",
        "card_source_datasets": "卡片声明的上游数据集",
        "card_homepage": "项目主页",
        "card_config_count": "卡片里列出的 config 个数",
        "card_num_rows": "**作者声明**的总行数（跨 config 累加），未必准",
        "card_dataset_bytes": "作者声明的数据体积",
        "card_download_bytes": "作者声明的下载体积（压缩后）",
    },
    "card_configs": {
        "config_name": "配置名（一个数据集可以有多个 config，如不同语言子集）",
        "split": "切分名（`train`、`test`、`validation`）",
        "num_bytes": "该 split 的原始字节数",
        "num_examples": "该 split 的行数",
        "dataset_size": "生成 arrow 后的内存占用（卡片口径）",
        "download_size": "下载体积（卡片口径）",
        "features": "列名与数据类型的 JSON 描述——整条管线里唯一能回答「有哪些列」的来源",
        "data_files": "该 split 由哪些文件构成（JSON 数组，可能是通配符）",
    },
    "size": {
        "status": "`ok` 拿到数据 / `unavailable` 上游没有这份数据 / `error` 临时失败",
        "num_rows": "datasets-server **实测**行数，与 `card_num_rows` 是两个口径",
        "estimated_num_rows": "上游给出的估算行数（未做精确统计时才有）",
        "num_bytes_original": "原始文件字节数",
        "num_bytes_parquet": "转成 parquet 后的字节数",
        "num_bytes_memory": "载入内存后的估算占用",
        "config_count": "上游报告的 config 个数",
        "num_columns_max": "各 config 中列数的最大值",
        "configs": "各 config 的明细（JSON 文本）",
        "snapshot_date": "最近一次成功补抓的日期",
    },
    "files": {
        "status": "`ok` 拿到文件树 / `unavailable` 仓库不可访问 / `error` 临时失败",
        "file_count": "本页 tree 里 type=file 的条目数；truncated 时只是下限，不是全库真值",
        "total_bytes": "本页文件 size 之和；全部缺失时留空，不拿 0 冒充",
        "formats": "本页扩展名计数（JSON 文本）；无扩展名的归入 (none)",
        "has_readme": "本页是否存在以 README 开头的文件",
        "truncated": "是否还有未取的下一页；为 true 时上面四项都只是下限",
        "snapshot_date": "最近一次补抓文件清单的日期",
    },
}


def describe(table_name: str, column: str) -> str | None:
    """取一列的含义：表内说明优先，其次通用说明。"""
    return FIELD_DOCS.get(table_name, {}).get(column) or COMMON.get(column)
