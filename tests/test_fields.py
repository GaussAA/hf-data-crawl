"""默认交付字段契约的行为测试。

守的是「默认交付面」的四条取舍：描述数据集本身、覆盖可用、语义不重复、
每列都有可照抄进 README 的定义。契约本身若写错（拼错列名、漏写说明、
丢了 id 兜底），`contract_errors()` 必须在改完当场报出来。
"""

from __future__ import annotations

import polars as pl

from hf_data_crawl.fields import (
    COLUMN_NOTES,
    CONTRACT_VERSION,
    DEFAULT_COLUMNS,
    DISPLAY_COLUMNS,
    EXTENDED_FIELDS,
    EXTENDED_NOTE,
    FALLBACKS,
    GROUPS,
    PAGE_FIELDS,
    contract_errors,
    default_columns,
    display_expr,
    display_value,
    projection_sql,
    quoted_columns,
)
from hf_data_crawl.query.gold import wide_column_names


def test_契约自检通过() -> None:
    assert contract_errors() == []


def test_默认列序以id开头且无重复() -> None:
    assert DEFAULT_COLUMNS[0] == "id"
    assert len(DEFAULT_COLUMNS) == len(set(DEFAULT_COLUMNS))
    assert default_columns() == DEFAULT_COLUMNS


def test_契约列都在宽视图里() -> None:
    """拼错列名要让契约自检当场炸，而不是等交付跑出一份少列的表。"""
    assert set(DEFAULT_COLUMNS) <= set(wide_column_names())


def test_每列都有中文说明且分组不漏列() -> None:
    assert set(COLUMN_NOTES) == set(DEFAULT_COLUMNS)
    grouped = tuple(column for _, columns in GROUPS for column in columns)
    assert grouped == DEFAULT_COLUMNS
    assert all(COLUMN_NOTES[column].strip() for column in DEFAULT_COLUMNS)


def test_SQL列清单与契约逐列一致() -> None:
    assert quoted_columns() == ", ".join(f'"{column}"' for column in DEFAULT_COLUMNS)
    assert quoted_columns().count('"') == len(DEFAULT_COLUMNS) * 2


def test_内部状态列不进默认交付面() -> None:
    """水印、抓取状态、卡片凭证、内部标记属于管道，不属于数据集画像。"""
    internal = {
        "sha",
        "first_seen_at",
        "last_seen_at",
        "source_sha",
        "resource_group",
        "xet_enabled",
        "files_status",
        "truncated",
        "private",
        "disabled",
        "trending_score",
    }
    assert not (internal & set(DEFAULT_COLUMNS))


def test_低覆盖补抓列不进默认交付面() -> None:
    """九成空的补抓列仍排除；v4 只放行了用户点名的论文 / 用途两列。"""
    sparse = {
        "card_num_rows",
        "card_dataset_bytes",
        "card_download_bytes",
        "citation",
        "dois",
        "has_readme",
        "total_bytes",
        "file_count",
    }
    assert not (sparse & set(DEFAULT_COLUMNS))
    # 这两列覆盖也低（3.67% / 0.72%），但语义明确、用户点名，故进默认面。
    assert {"arxiv_ids", "task_ids"} <= set(DEFAULT_COLUMNS)


def test_用户点名的画像字段都在契约里() -> None:
    required = {
        "languages",
        "modalities",
        "formats",
        "task_categories",
        "size_categories",
        "server_num_rows",
        "main_size",
        "license_name",
        "licenses",
        "description",
    }
    assert required <= set(DEFAULT_COLUMNS)


def test_契约版本为正整数() -> None:
    """改清单要升版本；交付档案靠它回答「哪一版默认口径」。"""
    assert isinstance(CONTRACT_VERSION, int) and CONTRACT_VERSION >= 2


# ---------------------------------------------------------------- v2：详情页标签


def test_补回页面红圈里那组Tags() -> None:
    """v1 只裁列没对值，把无前缀标签整组丢了——页面看得见、库里找不着。"""
    assert "tags" in DEFAULT_COLUMNS
    assert "plain_tags" in DEFAULT_COLUMNS
    assert DEFAULT_COLUMNS.index("plain_tags") == DEFAULT_COLUMNS.index("tags") + 1
    assert "Tags" in COLUMN_NOTES["plain_tags"] or "页面 Tags" in COLUMN_NOTES["plain_tags"]


def test_页面标签行逐项都有对照() -> None:
    labels = {label for label, _columns, _note in PAGE_FIELDS}
    assert {
        "Tasks",
        "Modalities",
        "Formats",
        "Languages",
        "Size",
        "Tags",
        "Libraries",
        "License",
    } <= labels
    assert all(columns and note for _label, columns, note in PAGE_FIELDS)


def test_卡片兜底只作用于四列且别名不变() -> None:
    """兜底是「默认交付面」的事；视图语义不改，内部仍可分列追溯。"""
    sql = projection_sql()
    assert sql.count("CASE WHEN") == len(FALLBACKS) == 4
    for column, source in FALLBACKS.items():
        assert column in DEFAULT_COLUMNS
        assert source in wide_column_names()
        assert f'ELSE "{source}" END' in sql
        assert f'AS "{column}"' in sql
    assert not set(FALLBACKS.values()) & set(DEFAULT_COLUMNS)


def test_可读值映射不猜未知值() -> None:
    assert display_value("languages", "ko") == "Korean"
    assert display_value("languages", "zh") == "Chinese"
    assert display_value("languages", "yue") == "yue"  # 无 639-1 码，原样留
    assert display_value("task_categories", "text-generation") == "Text Generation"
    assert display_value("size_categories", "100K<n<1M") == "100K - 1M"
    assert display_value("size_categories", "n<1K") == "< 1K"
    assert display_value("size_categories", "n>1T") == "> 1T"
    assert display_value("formats", "json") == "json"  # 无映射的列不动


def test_CSV展平用原生表达式换成页面形式() -> None:
    frame = pl.DataFrame(
        {
            "languages": [["ko", "en"]],
            "task_categories": [["text-generation"]],
            "size_categories": [["100K<n<1M"]],
            "tags": [["keyword", "reranking"]],
        }
    )
    exprs = [
        display_expr(name) if name in DISPLAY_COLUMNS else pl.col(name).list.join(";").alias(name)
        for name in frame.columns
    ]
    out = frame.select(exprs)
    assert out["languages"][0] == "Korean;English"
    assert out["task_categories"][0] == "Text Generation"
    assert out["size_categories"][0] == "100K - 1M"
    assert out["tags"][0] == "keyword;reranking"  # 无映射的列保持机器码


def test_契约自检盯住新增的两张表() -> None:
    """对照表 / 兜底表指错列名，也要在改完当场炸。"""
    assert contract_errors() == []
    assert quoted_columns().count('"') == len(DEFAULT_COLUMNS) * 2
    assert not set(DISPLAY_COLUMNS) - set(DEFAULT_COLUMNS)


# ---------------------------------------------------------------- v5：撤销扁平化


def test_默认投影不再压平列表() -> None:
    """v3 为绕开预览器缺陷把标签列压成字符串；缺陷修好后必须撤掉，否则成了隐性契约。"""
    sql = projection_sql()

    assert "array_to_string" not in sql
    for column in ("languages", "task_categories", "tags", "plain_tags", "arxiv_ids"):
        assert f'"{column}"' in sql
    assert CONTRACT_VERSION >= 5


def test_卡片兜底仍保留且不改变列类型() -> None:
    """兜底只是取另一列的同义值，两边都是 LIST，不引入类型转换。"""
    sql = projection_sql()

    for column, source in FALLBACKS.items():
        assert f'ELSE "{source}" END AS "{column}"' in sql


def test_列表列仍能展平成页面形式交给CSV() -> None:
    """压平撤的是导出物，不是展示层：CSV 里仍要人读。"""
    frame = pl.DataFrame(
        {
            "languages": [["ko", "en"], None, []],
            "task_categories": [["text-generation", "question-answering"], None, []],
            "size_categories": [["100K<n<1M", "n<1K"], None, []],
            "tags": [["keyword", "reranking"], None, []],
        }
    )
    exprs = [
        display_expr(name) if name in DISPLAY_COLUMNS else pl.col(name).list.join(";").alias(name)
        for name in frame.columns
    ]
    out = frame.select(exprs)
    assert out["languages"].to_list() == ["Korean;English", None, ""]
    assert out["task_categories"][0] == "Text Generation;Question Answering"
    assert out["size_categories"][0] == "100K - 1M;< 1K"
    assert out["tags"][0] == "keyword;reranking"


# ---------------------------------------------------------------- v4：克制扩充 + 扩展清单


def test_v4新增五列都在默认面() -> None:
    added = {"pretty_name", "multilinguality", "arxiv_ids", "source_datasets", "task_ids"}
    assert added <= set(DEFAULT_COLUMNS)
    assert len(DEFAULT_COLUMNS) == 31
    assert CONTRACT_VERSION >= 4


def test_地区因无区分度不进默认面但进扩展清单() -> None:
    assert "regions" not in DEFAULT_COLUMNS
    extended = {column for column, _definition, _tier in EXTENDED_FIELDS}
    assert "regions" in extended
    assert {"dois", "paperswithcode_id", "citation", "card_homepage"} <= extended


def test_扩展清单与默认面不重叠且列都存在() -> None:
    extended = [column for column, _definition, _tier in EXTENDED_FIELDS]
    assert not set(extended) & set(DEFAULT_COLUMNS)
    assert set(extended) <= set(wide_column_names())
    assert all(definition.strip() and tier.strip() for _c, definition, tier in EXTENDED_FIELDS)


def test_扩展说明写明按需取用方式() -> None:
    assert "output.columns" in EXTENDED_NOTE
    assert "gold.datasets" in EXTENDED_NOTE
