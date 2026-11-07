"""表登记表：每张表在这里登记一次，其余各处都从这里取。

为什么需要它
------------
「一张表住哪、叫什么、什么 schema、能否从 raw 重建」这门知识，原本散在五处：
store 里的文件名常量、各模块的 schema 常量、gold 里的硬编码路径、rebuild 里
的删除清单、README 的目录树。任一处改名都不会报错——

- gold 的 glob 落空时会**静默退化成一个空视图**，查询照常返回 0 行；
- rebuild 曾整目录删除 enrich，把不可重算的 size.parquet 一并删掉（实测丢了
  两万条、约 13 分钟抓取），因为「该表不从 raw 派生」这个事实只写在 sizes.py。

登记一次之后，新增一张表只需在此处添加一行，读写、视图、重建清理同时生效。

三条来源语义
------------
`Origin` 决定了重建时的待遇，这是本模块存在的核心理由：

- **RAW**：由 `raw/*.json.gz` 派生，删掉能重算 → rebuild 应当清除。
- **NETWORK**：来自外部接口，raw 里没有对应数据 → rebuild 必须保留。
- **STATE**：运行态（当前态、观测流水），由派生过程维护 → rebuild 重建。
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import polars as pl

#: polars 的 `pl.Utf8` 这类类对象在类型存根里不算 `DataType` 实例，直接标注会满屏误报。
#: 统一用这个别名，运行时无影响。
PolarsType = pl.DataType | type[pl.DataType]

#: 时间列的统一类型：带 UTC 时区、微秒精度。
#:
#: 用真正的类型而不是字符串：字符串排序只在格式永远规整时才等价于时间序，
#: 一旦上游某天少写毫秒位，排序就会静默错位。
TIMESTAMP = pl.Datetime(time_unit="us", time_zone="UTC")

#: 数据集页面地址前缀。
#:
#: HF 接口**不返回 url**（原始响应里只有 `_id`，那是 MongoDB 内部主键）。
#: 但地址由 `id` 唯一确定，所以每一层都可以直接带上一列——查看表的人
#: 拿到 id 就能点开，不必再关心「视图里有没有」。
#:
#: 存这一列是有意的冗余：实测 111.7 万行只多 12.4 MB，而换来的是
#: 「任何一张表、任何查看工具（DuckDB / polars / WhoDB）都能直接定位到网页」。
DATASET_URL = "https://huggingface.co/datasets/"


def url_for(dataset_id: str) -> str:
    """数据集页面地址。"""
    return f"{DATASET_URL}{dataset_id}"


class Origin(StrEnum):
    """表的来源，决定重建时该不该清除。"""

    RAW = "raw"
    NETWORK = "network"
    STATE = "state"


@dataclass(frozen=True)
class TableSpec:
    """一张表的身份。"""

    #: 逻辑名，代码里引用它而不是文件名。
    name: str
    #: 相对 data/ 的路径模板；`{dt}` 表示按快照日期分区。
    path: str
    #: 列定义。读写两侧都以它为准，不再各自维护一份。
    schema: dict[str, PolarsType]
    origin: Origin

    @property
    def partitioned(self) -> bool:
        return "{dt}" in self.path

    @property
    def filename(self) -> str:
        return self.path.rsplit("/", 1)[-1]

    def relative(self, snapshot: str | None = None) -> str:
        """把路径模板填成具体相对路径。分区表必须给 snapshot。"""
        if self.partitioned:
            if snapshot is None:
                raise ValueError(f"{self.name} 是按日期分区的，缺少 snapshot")
            return self.path.replace("{dt}", snapshot)
        return self.path

    def glob(self) -> str:
        """给视图用的通配路径。"""
        return self.path.replace("{dt}", "*")


# ------------------------------------------------------------------ 列定义
#
# 全部集中在此，是为了让「一张表长什么样」只有一个答案。各模块仍可从原位置
# 导入（它们做了 re-export），但定义只在这里。

BRONZE_SCHEMA: dict[str, PolarsType] = {
    "id": pl.Utf8,
    "url": pl.Utf8,
    "author": pl.Utf8,
    "sha": pl.Utf8,
    "created_at": TIMESTAMP,
    "last_modified": TIMESTAMP,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
    "trending_score": pl.Float64,
    "gated": pl.Utf8,
    "private": pl.Boolean,
    "disabled": pl.Boolean,
    "main_size": pl.Int64,
    "xet_enabled": pl.Boolean,
    "resource_group": pl.Utf8,
    "tags": pl.List(pl.Utf8),
    "description": pl.Utf8,
    "raw": pl.Utf8,
    "page_file": pl.Utf8,
    "snapshot_date": pl.Date,
}

#: 由本地派生、不对应 HF 响应字段的列。契约测试据此反推源字段清单。
BRONZE_DERIVED_COLUMNS: frozenset[str] = frozenset({"raw", "page_file", "snapshot_date", "url"})

#: Bronze 列名 → HF 响应字段名。**契约的唯一真相源**。
#:
#: 存在的理由：HF 的 `expand[]` 是白名单裁剪，列与请求清单是两份会各自漂移的
#: 声明。2026-11-04 实测事故即由此而来——`private` 列已声明、请求里却没带，
#: 于是 111.5 万行整列为空且无任何报错。
BRONZE_SOURCE_FIELDS: dict[str, str] = {
    "id": "id",
    "author": "author",
    "sha": "sha",
    "created_at": "createdAt",
    "last_modified": "lastModified",
    "downloads": "downloads",
    "downloads_all_time": "downloadsAllTime",
    "likes": "likes",
    "trending_score": "trendingScore",
    "gated": "gated",
    "private": "private",
    "disabled": "disabled",
    "main_size": "mainSize",
    "xet_enabled": "xetEnabled",
    "resource_group": "resourceGroup",
    "tags": "tags",
    "description": "description",
}

#: 列表接口无条件返回、无需写进 `expand[]` 的字段。
UNCONDITIONAL_FIELDS: frozenset[str] = frozenset({"id"})

# Silver 是「属性变更流水」：一个 id 只在首次出现或属性变了才追加一行。
# 指标（downloads / downloads_all_time / likes）不在其中，它们每天有一半的行在变，
# 历史由 observations 负责；放进来会让每天重复存一份近乎全量的快照。
SILVER_SCHEMA: dict[str, PolarsType] = {
    "id": pl.Utf8,
    "url": pl.Utf8,
    "author": pl.Utf8,
    "sha": pl.Utf8,
    "created_at": TIMESTAMP,
    "last_modified": TIMESTAMP,
    "trending_score": pl.Float64,
    "gated": pl.Utf8,
    "private": pl.Boolean,
    "disabled": pl.Boolean,
    "main_size": pl.Int64,
    "xet_enabled": pl.Boolean,
    "resource_group": pl.Utf8,
    "description": pl.Utf8,
    "tags": pl.List(pl.Utf8),
    "task_categories": pl.List(pl.Utf8),
    "task_ids": pl.List(pl.Utf8),
    "languages": pl.List(pl.Utf8),
    "size_categories": pl.List(pl.Utf8),
    "licenses": pl.List(pl.Utf8),
    "libraries": pl.List(pl.Utf8),
    "modalities": pl.List(pl.Utf8),
    "formats": pl.List(pl.Utf8),
    "regions": pl.List(pl.Utf8),
    "multilinguality": pl.List(pl.Utf8),
    "source_datasets": pl.List(pl.Utf8),
    "annotations_creators": pl.List(pl.Utf8),
    "language_creators": pl.List(pl.Utf8),
    "arxiv_ids": pl.List(pl.Utf8),
    "dois": pl.List(pl.Utf8),
    "plain_tags": pl.List(pl.Utf8),
    "snapshot_date": pl.Date,
}

OBSERVATIONS_SCHEMA: dict[str, PolarsType] = {
    "id": pl.Utf8,
    "url": pl.Utf8,
    "snapshot_date": pl.Date,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
}

CURRENT_SCHEMA: dict[str, PolarsType] = {
    "id": pl.Utf8,
    "url": pl.Utf8,
    "author": pl.Utf8,
    "sha": pl.Utf8,
    "created_at": TIMESTAMP,
    "last_modified": TIMESTAMP,
    "downloads": pl.Int64,
    "downloads_all_time": pl.Int64,
    "likes": pl.Int64,
    "trending_score": pl.Float64,
    "gated": pl.Utf8,
    "private": pl.Boolean,
    "disabled": pl.Boolean,
    "main_size": pl.Int64,
    "xet_enabled": pl.Boolean,
    "resource_group": pl.Utf8,
    "first_seen_at": pl.Date,
    "last_seen_at": pl.Date,
    "status": pl.Utf8,
}

ENRICH_SCHEMA: dict[str, PolarsType] = {
    "id": pl.Utf8,
    "url": pl.Utf8,
    "snapshot_date": pl.Date,
    "pretty_name": pl.Utf8,
    "paperswithcode_id": pl.Utf8,
    "arxiv": pl.Utf8,
    "license_name": pl.Utf8,
    "license_link": pl.Utf8,
    "citation": pl.Utf8,
    "card_description": pl.Utf8,
    # 带 card_ 前缀：来自卡片 YAML，与 Silver 里由 tags 拆出的同名维度是两套
    # 口径（作者声明 vs 平台规范化）。不加前缀会在 datasets 视图 join 时撞名。
    "card_tags": pl.List(pl.Utf8),
    "card_task_categories": pl.List(pl.Utf8),
    "card_languages": pl.List(pl.Utf8),
    "card_size_categories": pl.List(pl.Utf8),
    "card_multilinguality": pl.Utf8,
    "card_annotations_creators": pl.List(pl.Utf8),
    "card_language_creators": pl.List(pl.Utf8),
    "card_source_datasets": pl.List(pl.Utf8),
    "card_homepage": pl.Utf8,
    # 汇总指标：由 dataset_info 跨 config 累加，取不到则为空而非 0。
    "card_config_count": pl.Int64,
    "card_num_rows": pl.Int64,
    "card_dataset_bytes": pl.Int64,
    "card_download_bytes": pl.Int64,
}

CARD_CONFIGS_SCHEMA: dict[str, PolarsType] = {
    "id": pl.Utf8,
    "url": pl.Utf8,
    "snapshot_date": pl.Date,
    "config_name": pl.Utf8,
    "split": pl.Utf8,
    "num_bytes": pl.Int64,
    "num_examples": pl.Int64,
    "dataset_size": pl.Int64,
    "download_size": pl.Int64,
    "features": pl.Utf8,
    "data_files": pl.Utf8,
}

SIZE_SCHEMA: dict[str, PolarsType] = {
    "id": pl.Utf8,
    "url": pl.Utf8,
    "snapshot_date": pl.Date,
    "status": pl.Utf8,
    "num_rows": pl.Int64,
    "estimated_num_rows": pl.Int64,
    "num_bytes_original": pl.Int64,
    "num_bytes_parquet": pl.Int64,
    "num_bytes_memory": pl.Int64,
    "config_count": pl.Int64,
    "num_columns_max": pl.Int64,
    "configs": pl.Utf8,
}

#: 文件清单汇总：一行一个数据集，来自单库详情接口的 `siblings`。
#:
#: 只存汇总不存明细，是 B 案试点的刻意收窄：详情接口一次可达 1.2 MB，
#: 全量逐库不可行，按下载量取头部时明细表也会比汇总表大几个数量级。
#: `formats` 是扩展名计数的 JSON 文本——键随数据集而变，写成固定 struct
#: 会让大多数列落空（与 `features` / `configs` 同一理由）。
FILES_SCHEMA: dict[str, PolarsType] = {
    "id": pl.Utf8,
    "url": pl.Utf8,
    "snapshot_date": pl.Date,
    "status": pl.Utf8,
    "file_count": pl.Int64,
    "total_bytes": pl.Int64,
    "formats": pl.Utf8,
    "has_readme": pl.Boolean,
    "truncated": pl.Boolean,
}


# ------------------------------------------------------------------ 登记

TABLES: dict[str, TableSpec] = {
    "bronze": TableSpec("bronze", "bronze/dt={dt}/bronze.parquet", BRONZE_SCHEMA, Origin.RAW),
    "silver": TableSpec("silver", "silver/dt={dt}/silver.parquet", SILVER_SCHEMA, Origin.RAW),
    "current": TableSpec("current", "state/current.parquet", CURRENT_SCHEMA, Origin.STATE),
    "observations": TableSpec(
        "observations", "state/observations/dt={dt}/obs.parquet", OBSERVATIONS_SCHEMA, Origin.STATE
    ),
    "carddata": TableSpec("carddata", "enrich/carddata.parquet", ENRICH_SCHEMA, Origin.RAW),
    "card_configs": TableSpec(
        "card_configs", "enrich/card_configs.parquet", CARD_CONFIGS_SCHEMA, Origin.RAW
    ),
    # NETWORK 表来自外部接口，raw 里没有对应数据，删掉就要重抓：
    # size 走 datasets-server，files 走单库详情接口（按下载量取头部）。
    "size": TableSpec("size", "enrich/size.parquet", SIZE_SCHEMA, Origin.NETWORK),
    "files": TableSpec("files", "enrich/files.parquet", FILES_SCHEMA, Origin.NETWORK),
}


def table(name: str) -> TableSpec:
    """按逻辑名取表。写错名字时立刻失败，而不是拼出一条错误的路径。"""
    try:
        return TABLES[name]
    except KeyError as error:
        raise KeyError(f"未登记的表：{name!r}，已登记：{sorted(TABLES)}") from error
