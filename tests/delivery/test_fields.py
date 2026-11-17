"""默认交付字段契约的行为测试。

守的是「默认交付面」的四条取舍：描述数据集本身、覆盖可用、语义不重复、
每列都有可照抄进 README 的定义。契约本身若写错（拼错列名、漏写说明、
丢了 id 兜底），`contract_errors()` 必须在改完当场报出来。
"""

from __future__ import annotations

import re

import polars as pl

from hf_data_crawl.delivery.fields import (
    COLUMN_NOTES,
    COLUMN_TYPES,
    CONTRACT_VERSION,
    DEFAULT_COLUMNS,
    DISPLAY_COLUMNS,
    EXTENDED_FIELDS,
    EXTENDED_NOTE,
    FALLBACKS,
    GROUPS,
    PAGE_FIELDS,
    contract_errors,
    display_expr,
    display_value,
    projection_sql,
    type_family,
)
from hf_data_crawl.query.gold import wide_column_names


def test_契约自检通过() -> None:
    assert contract_errors() == []


def test_默认列序以id开头且无重复() -> None:
    assert DEFAULT_COLUMNS[0] == "id"
    assert len(DEFAULT_COLUMNS) == len(set(DEFAULT_COLUMNS))


def test_契约列都在宽视图里() -> None:
    """拼错列名要让契约自检当场炸，而不是等交付跑出一份少列的表。"""
    assert set(DEFAULT_COLUMNS) <= set(wide_column_names())


def test_每列都有中文说明且分组不漏列() -> None:
    assert set(COLUMN_NOTES) == set(DEFAULT_COLUMNS)
    grouped = tuple(column for _, columns in GROUPS for column in columns)
    assert grouped == DEFAULT_COLUMNS
    assert all(COLUMN_NOTES[column].strip() for column in DEFAULT_COLUMNS)


def test_SQL投影覆盖全部契约列() -> None:
    """`projection_sql()` 才是导出与交付真正走的投影，漏一列等于交付少一列。

    原先这里断言的是一个只被测试使用的 `quoted_columns()`——它等于一句 join 表达式，
    改坏了也没有任何生产代码受影响，测的其实是「另一个函数写得对不对」。
    """
    sql = projection_sql()
    missing = [column for column in DEFAULT_COLUMNS if f'"{column}"' not in sql]

    assert not missing, f"投影里缺列：{missing}"


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
        assert f'ELSE coalesce("{source}", CAST([] AS VARCHAR[])) END AS "{column}"' in sql
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
        assert f'ELSE coalesce("{source}", CAST([] AS VARCHAR[])) END AS "{column}"' in sql


def test_兜底未命中时保持空列表而非塌成NULL() -> None:
    """2026-11-17 修正：空列表被兜底吞成 NULL 后，`NOT list_contains` 会静默漏行。

    同一列在导出物里是 NULL、在视图里是空列表，是「导出物与视图同构」唯一没对上的
    地方。三条分支必须各自成立，因为它们是三件事：

    - 主列空列表 + 兜底未命中 → 空列表（作者未声明）
    - 主列空列表 + 兜底有值 → 兜底值（只在卡片 YAML 里声明过）
    - 主列为 NULL + 兜底未命中 → 仍是 NULL（该 id 在 silver 里没有行，属「不知道」）
    """
    import duckdb

    expression = re.search(r'CASE WHEN .*?END AS "languages"', projection_sql())
    assert expression is not None, "契约里应当有 languages 的兜底表达式"

    with duckdb.connect() as connection:
        connection.execute("CREATE TABLE t(languages VARCHAR[], card_languages VARCHAR[])")
        connection.execute(
            "INSERT INTO t VALUES ([], NULL), ([], ['y']), (NULL, NULL), (['x'], ['z'])"
        )
        rows = connection.execute(f"SELECT {expression.group(0)} FROM t").fetchall()

    assert rows == [([],), (["y"],), (None,), (["x"],)]


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


def test_每列都有类型声明且不越界() -> None:
    """契约原先只声明列名，于是「列名没变、类型变了」没有绊线。

    `tags` 从 VARCHAR 改回 LIST（契约 v3→v5）正是这种变更：doctor 与
    export_wide 三处勾稽全都只比列名元组。
    """
    assert not [column for column in DEFAULT_COLUMNS if column not in COLUMN_TYPES]
    assert not sorted(set(COLUMN_TYPES) - set(DEFAULT_COLUMNS))


def test_类型族只认常量与未知() -> None:
    assert type_family(pl.Utf8) == "text"
    assert type_family(pl.List(pl.Utf8)) == "list"
    assert type_family(pl.Int64) == "number"
    assert type_family(pl.Datetime("us")) == "time"
    # 空列只出现在手工构造的空表里；当通配，避免夹具天天假红。
    assert type_family(pl.Null) == "unknown"


def test_列表列的类型声明不会退化成文本() -> None:
    """v3 曾把 13 个多值列压成 `;` 字符串以绕开预览器缺陷，v5 撤销。

    契约里的族声明是那道回退的绊线：谁要把 `tags` 改回 `text`，得先改这里。
    """
    multi = [name for name, family in COLUMN_TYPES.items() if family == "list"]
    assert "tags" in multi and "plain_tags" in multi
    assert COLUMN_TYPES["tags"] == "list"
