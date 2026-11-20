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
    INJECTED_COLUMNS,
    PAGE_FIELDS,
    VIEW_COLUMNS,
    contract_errors,
    display_expr,
    projection_sql,
    type_family,
)
from hf_data_crawl.query.gold import wide_column_names


def test_契约自检通过() -> None:
    assert contract_errors() == []


def test_默认列序以id开头且无重复() -> None:
    assert DEFAULT_COLUMNS[0] == "id"
    assert len(DEFAULT_COLUMNS) == len(set(DEFAULT_COLUMNS))


def test_契约列都在宽视图或注入清单里() -> None:
    """拼错列名要让契约自检当场炸，而不是等交付跑出一份少列的表。

    五个注入列（readme / split / features / paper_url / citation_bibtex）不在视图里，
    由 `export_wide` 的第二步现场 join——它们体积太大或粒度不同。
    """
    known = set(wide_column_names()) | set(INJECTED_COLUMNS)
    assert set(DEFAULT_COLUMNS) <= known


def test_每列都有中文说明且分组不漏列() -> None:
    assert set(COLUMN_NOTES) == set(DEFAULT_COLUMNS)
    grouped = tuple(column for _, columns in GROUPS for column in columns)
    assert grouped == DEFAULT_COLUMNS
    assert all(COLUMN_NOTES[column].strip() for column in DEFAULT_COLUMNS)


def test_SQL投影覆盖全部视图列() -> None:
    """`projection_sql()` 是导出第一步真正走的投影，漏一列等于交付少一列。

    注入列刻意不在投影里：它们由导出的第二步 join，写进 `COPY (...)` 会找不到来源。
    """
    sql = projection_sql()
    missing = [column for column in VIEW_COLUMNS if f'"{column}"' not in sql]

    assert not missing, f"投影里缺列：{missing}"
    assert not [column for column in INJECTED_COLUMNS if f'"{column}"' in sql]


def test_内部状态列不进默认交付面() -> None:
    """水印、抓取状态、卡片凭证、内部标记属于管道，不属于数据集画像。"""
    internal = {
        "sha",
        "source_sha",
        "resource_group",
        "files_status",
        "truncated",
        "private",
        "disabled",
        "trending_score",
    }
    assert not (internal & set(DEFAULT_COLUMNS))
    # v6 起 first_seen_at / last_seen_at / xet_enabled 进默认面：前者答「这个库什么时候
    # 出现」，后者答「怎么下载」，都是客户问题，不是管道内部状态。
    assert {"first_seen_at", "last_seen_at", "xet_enabled"} <= set(DEFAULT_COLUMNS)


def test_低覆盖补抓列不进默认交付面() -> None:
    """九成空的补抓列仍排除；v4 只放行了用户点名的论文 / 用途两列。"""
    sparse = {
        "card_num_rows",
        "card_dataset_bytes",
        "card_download_bytes",
        "citation",
        "dois",
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
        "licenses",
        "readme",
        "has_readme",
    }
    assert required <= set(DEFAULT_COLUMNS)


def test_契约版本为正整数() -> None:
    """改清单要升版本；交付档案靠它回答「哪一版默认口径」。"""
    assert isinstance(CONTRACT_VERSION, int) and CONTRACT_VERSION >= 2


# ---------------------------------------------------------------- v2：详情页标签


def test_标签列保留原始全量_无前缀子集可自滤() -> None:
    """v1 只裁列没对值，把无前缀标签整组丢了；v6 反过来删掉 `plain_tags`。

    理由是后者本就是 `tags` 按「无前缀」拆出来的子集，删了可从 `tags` 还原；
    保留原始全量，才不丢 `task_categories:` / `region:` 这些前缀维度。
    """
    assert "tags" in DEFAULT_COLUMNS
    assert "plain_tags" not in DEFAULT_COLUMNS
    assert "Tags" in COLUMN_NOTES["tags"] or "页面 Tags" in COLUMN_NOTES["tags"]


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


def test_卡片兜底只作用于三列且别名不变() -> None:
    """兜底是「默认交付面」的事；视图语义不改，内部仍可分列追溯。"""
    sql = projection_sql()
    assert sql.count("CASE WHEN") == len(FALLBACKS) == 3
    for column, source in FALLBACKS.items():
        assert column in DEFAULT_COLUMNS
        assert source in wide_column_names()
        assert f'ELSE coalesce("{source}", CAST([] AS VARCHAR[])) END AS "{column}"' in sql
    assert not set(FALLBACKS.values()) & set(DEFAULT_COLUMNS)


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


# ---------------------------------------------------------------- v5：撤销扁平化


def test_默认投影不再压平列表() -> None:
    """v3 为绕开预览器缺陷把标签列压成字符串；缺陷修好后必须撤掉，否则成了隐性契约。"""
    sql = projection_sql()

    assert "array_to_string" not in sql
    for column in ("languages", "task_categories", "tags", "arxiv_ids"):
        assert f'"{column}"' in sql
    assert CONTRACT_VERSION >= 6


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


def test_v4新增五列与v6扩充都在默认面() -> None:
    added = {"pretty_name", "multilinguality", "arxiv_ids", "source_datasets", "task_ids"}
    assert added <= set(DEFAULT_COLUMNS)
    # v6：31 → 35（删 5：description / license_name / plain_tags / server_config_count /
    # server_num_columns；加 9：readme 等）。
    assert len(DEFAULT_COLUMNS) == 35
    assert CONTRACT_VERSION >= 6
    v6 = {
        "readme",
        "has_readme",
        "first_seen_at",
        "last_seen_at",
        "xet_enabled",
        "split",
        "features",
        "paper_url",
        "citation_bibtex",
    }
    assert v6 <= set(DEFAULT_COLUMNS)
    dropped = {
        "description",
        "license_name",
        "plain_tags",
        "server_config_count",
        "server_num_columns",
    }
    assert not (dropped & set(DEFAULT_COLUMNS))


def test_地区因无区分度不进默认面但进扩展清单() -> None:
    assert "regions" not in DEFAULT_COLUMNS
    extended = {column for column, _definition in EXTENDED_FIELDS}
    assert "regions" in extended
    assert {"dois", "paperswithcode_id", "citation", "card_homepage"} <= extended


def test_扩展清单与默认面不重叠且列都存在() -> None:
    extended = [column for column, _definition in EXTENDED_FIELDS]
    assert not set(extended) & set(DEFAULT_COLUMNS)
    assert set(extended) <= set(wide_column_names())
    assert all(definition.strip() for _c, definition in EXTENDED_FIELDS)
    # 覆盖率是按当期底库现算的（DATA.md 与交付 README 各算一次），不进源码：
    # 写死一个百分比，读者无法判断它是哪天的，也没人会记得回来改。
    assert all(len(item) == 2 for item in EXTENDED_FIELDS)


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
    assert "tags" in multi and "split" in multi
    assert COLUMN_TYPES["tags"] == "list"


def test_与数据字典共用的列两边都要有说明() -> None:
    """同一列在交付契约与内部数据字典里各有一份文案，写给两个读者。

    两份不同是有意的（一个讲客户口径、一个讲管道语义，见 DECISIONS #107）。这条钉的是
    **列名漂移**：谁的列改了名或删了，交集就悄悄变小，而两边各自的测试都不会出声——
    `COLUMN_NOTES` 只查自己的 31 列，`fielddocs` 只查自己表里的列。
    """
    from hf_data_crawl.ops.fielddocs import COMMON

    shared = sorted(set(COMMON) & set(COLUMN_NOTES))

    assert len(shared) >= 12, f"两边同名的列只剩 {len(shared)} 个，像是有人改了列名：{shared}"
    for column in shared:
        assert COMMON[column].strip(), f"{column} 在 fielddocs 里没有说明"
        assert COLUMN_NOTES[column].strip(), f"{column} 在 COLUMN_NOTES 里没有说明"
