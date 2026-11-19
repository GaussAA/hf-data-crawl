"""默认交付底库导出：把 `gold.datasets` 视图按字段契约裁成一份可重建的 parquet。

定位
----
这是**只读投影的物化快照**，不是事实源，也不是状态表。它同时是「默认交付
字段」的物化：`fields.DEFAULT_COLUMNS` 是唯一契约，导出物与查询交付都按它投列，
`doctor` 勾稽两者不漂移。视图本身仍保留全部 72 列供内部查询用。

- 不登记进 `tables.TABLES`，不参与 `state.update` / `rebuild` / `compact` 的状态机；
- 随时可删、随时可由 `export-wide` 重建，删它不影响任何派生；
- 口径唯一来源是 `gold.datasets` 视图，这里绝不重新拼一次 join。

为什么不扩 current
------------------
`current` 只有 19 列，但语义字段早已在 `silver` / `carddata` / `size` 里，
`gold.datasets` 视图已把它们 LEFT JOIN 好（当前 65 列，全库 112 万行）。
把它们搬进 `current` 有三处代价：

1. 每轮 `state.update` 都要重写 112 万行全量表；
2. 每轮实际只扫约 22 万行，其余沿用 previous，而 `_align_frame` 对旧表缺列
   一律**补 null 而非重算**，于是约九成行的新字段会长期为空，直到各自被扫到
   ——与 2026-11-04 `private` 整列静默变空同类；
3. `silver` 本就是「同 id 取最新一条即当前属性」，再存一份就出现双真相源。

单独导出则状态表保持轻，分析者拿到单文件宽表，两者兼得。

安全边界
--------
`GoldCatalog` 的连接关闭了外部访问，只留数据目录这一处例外（视图要读
parquet），且不再对外暴露连接本身：导出只经 `copy_wide()` 这一条受控通道
流式 COPY，不必把 112 万行读进内存；`copy_wide()` 与这里各验一次目标确实
在数据目录内：万一将来有人放宽连接配置，`--out` 也不该写到别处。
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from ..platform.store import DatasetStore
from ..query.gold import GoldCatalog, wide_column_names
from .fields import (
    COLUMN_TYPES,
    DEFAULT_COLUMNS,
    FALLBACKS,
    INJECTED_COLUMNS,
    contract_errors,
    projection_sql,
    type_family,
)

#: 默认产物路径，相对 `store.root`。放在 `exports/` 而非登记表管理的目录，
#: 是为了让「它不是一张表」在目录结构上一眼可见。
DEFAULT_RELATIVE = Path("exports") / "datasets.parquet"

#: 注入列 → 第二步 SQL 里的别名。`split` 与 `features` 同来自一次 `card_configs`
#: 聚合，故共用一个别名。
_INJECT_ALIAS: dict[str, str] = {
    "readme": "rc",
    "split": "ca",
    "features": "ca",
    "paper_url": "rf",
    "citation_bibtex": "rf",
}

#: README 正文清洗：去 YAML 头、图片徽章、HTML 标签、链接只留文字、空行压缩。
#:
#: 顺序有意义：`frontmatter` 先去掉（它含 `---` 与大量 `:`，先处理后两个正则的
#: 成本更低）；图片与徽章必须在「链接只留文字」之前——徽章是 `[![alt](img)](url)`，
#: 先转链接会留下 `![alt](img)` 这种半截。
#:
#: **换行一律写成 `\r?\n`**：上游 README 里 CRLF 与 LF 混着（2026-11-19 实测，
#: `farwew/subsetTemplateName` 这类整篇都是 `\r\n`），只认 `\n` 会让整段正则空转。
#:
#: 实测（2026-11-19 抽 2000 条）：frontmatter 占 26.8%、代码块 25.9%、HTML 7.5%。
#: 这里只清格式噪音，**代码块一个字不动**——加载示例与字段示例是有用信息。
_README_CLEAN = (
    "regexp_replace("
    "regexp_replace("
    "regexp_replace("
    "regexp_replace("
    "regexp_replace(rm.text, '(?s)^---\\r?\\n.*?---\\r?\\n', ''),"
    " '!\\[[^\\]]*\\]\\([^)]*\\)', '', 'g'),"
    " '<[^>]+>', '', 'g'),"
    " '\\[([^\\]]*)\\]\\([^)]*\\)', '\\1', 'g'),"
    " '(?:\\r?\\n){3,}', chr(10) || chr(10), 'g')"
)

#: 只去 YAML 头，其余原样。它是**回退**用的底稿：清洗后不足 100 字符时截它前
#: 500 字符，而不是截原始 `rm.text`——否则回退会把刚去掉的 frontmatter 又还回去
#: （2026-11-19 实测：`sravaniayyagari/apr2` 这类只有 YAML 的 README 正是这个形状）。
_STRIP_YAML = "regexp_replace(rm.text, '(?s)^---\\r?\\n.*?---\\r?\\n', '')"


def _sql_path(path: Path) -> str:
    """SQL 字面量里的绝对路径：正斜杠 + 单引号转义。"""
    return path.resolve().as_posix().replace("'", "''")


#: 富化表还没采时的空 CTE。导出不该因为「这批富化还没跑」而整个失败：注入列
#: 退化成 NULL，行数与列集仍然正确，`doctor` 的整列为空检查会如实报「来源表没数据」。
_EMPTY_CTE: dict[str, str] = {
    "readme_clean": "readme_clean AS (SELECT CAST(NULL AS VARCHAR) AS id, "
    "CAST(NULL AS VARCHAR) AS readme WHERE false)",
    "configs_agg": "configs_agg AS (SELECT CAST(NULL AS VARCHAR) AS id, "
    "CAST(NULL AS VARCHAR[]) AS split, CAST(NULL AS VARCHAR) AS features WHERE false)",
    "fields_agg": "fields_agg AS (SELECT CAST(NULL AS VARCHAR) AS id, "
    "CAST(NULL AS VARCHAR) AS paper_url, CAST(NULL AS VARCHAR) AS citation_bibtex WHERE false)",
}


def _injection_sql(store: DatasetStore, base: Path) -> str:
    """导出第二步：把视图里那 30 列与五个注入列拼成最终契约列序。

    `readme` / `split` / `features` / `paper_url` / `citation_bibtex` 不在
    `datasets` 视图里（体积或粒度原因，见 `fields.INJECTED_COLUMNS`），这里现场
    join：`readme` 清洗正文、`split` 去重列表、`features` 取第一个非空 config、
    `paper_url`/`citation_bibtex` 取 `readme_fields`。
    """
    columns = ", ".join(
        f'{_INJECT_ALIAS[column]}."{column}"' if column in _INJECT_ALIAS else f'base."{column}"'
        for column in DEFAULT_COLUMNS
    )

    readme_cte = (
        f"""
        readme_clean AS (
            SELECT c.id,
                   CASE WHEN char_length(c.body) < 100
                        THEN substr(c.stripped, 1, 500) ELSE c.body END AS readme
            FROM (
                SELECT rm.id, rm.text AS raw,
                       {_STRIP_YAML} AS stripped,
                       {_README_CLEAN} AS body
                FROM read_parquet('{_sql_path(store.path("readme"))}') rm
                WHERE rm.text IS NOT NULL
            ) c
        ),"""
        if store.path("readme").exists()
        else _EMPTY_CTE["readme_clean"] + ","
    )
    configs = store.path("card_configs")
    if configs.exists():
        # `split` 与 `features` 一次聚合里出：两次扫 186 万行只为两列不值得。
        configs_cte = f"""
        configs_agg AS (
            SELECT id,
                   list(DISTINCT split) FILTER (WHERE split IS NOT NULL) AS split,
                   first(features) FILTER (WHERE features IS NOT NULL) AS features
            FROM read_parquet('{_sql_path(configs)}')
            GROUP BY id
        ),"""
    else:
        configs_cte = _EMPTY_CTE["configs_agg"] + ","
    fields_cte = (
        f"""
        fields_agg AS (
            SELECT id, first(paper_url) AS paper_url, first(citation_bibtex) AS citation_bibtex
            FROM read_parquet('{_sql_path(store.path("readme_fields"))}')
            GROUP BY id
        )"""
        if store.path("readme_fields").exists()
        else _EMPTY_CTE["fields_agg"]
    )

    return f"""
        WITH base AS (SELECT * FROM read_parquet('{_sql_path(base)}')),
        {readme_cte}
        {configs_cte}
        {fields_cte}
        SELECT {columns}
        FROM base
        LEFT JOIN readme_clean rc USING (id)
        LEFT JOIN configs_agg ca USING (id)
        LEFT JOIN fields_agg rf USING (id)
    """


class WideExportMismatch(RuntimeError):
    """导出结果不满足契约（视图列、静态清单、行数三者对不上），已撤销临时文件。"""


class WideExportPath(ValueError):
    """导出目标越出了数据目录。

    单独一个类型是为了让命令层把它转成一句能照做的失败，而不是把堆栈摊给人看：
    这是**输入写错了**，不是程序坏了。`ValueError` 本身太宽，捕获它会连同真正的
    编程错误一起吞掉。
    """


@dataclass(frozen=True)
class WideExportSummary:
    """一次导出的结果，供运行摘要使用。"""

    path: Path
    rows: int
    columns: int
    bytes: int


def export_path(store: DatasetStore) -> Path:
    """宽表导出的默认落点。

    这是「导出物在哪」的唯一答案：`export_wide` 用它决定写哪里，`ops` 用它决定
    读哪里。曾经 `ops.doctor` 自己拼了一遍 `root / "exports" / "datasets.parquet"`，
    两处各有一份；那天有人改了 `DEFAULT_RELATIVE`，体检就会报「导出不存在」并让人
    去跑 `export-wide`，而重跑只会写到新位置——提示永远修不好它指的那个病。
    """
    return store.root / DEFAULT_RELATIVE


def _assert_inside(root: Path, target: Path) -> None:
    """目标必须落在数据目录内；用 resolve 消掉 `..` 与符号链接再判。"""
    if not target.resolve().is_relative_to(root.resolve()):
        raise WideExportPath(f"导出目标必须在数据目录内：{target}（数据目录 {root}）")


def _current_rows(store: DatasetStore) -> int:
    """基准行数。scan + count 只读 parquet 元数据，不把整表读进内存。"""
    return int(pl.scan_parquet(store.path("current")).select(pl.len()).collect().item())


def export_wide(store: DatasetStore, target: Path | None = None) -> WideExportSummary:
    """按默认交付字段契约，把 `datasets` 视图流式写成 parquet。

    投的是契约列而非 `SELECT *`：这份文件是给人当**默认参考底库**用的，不是
    管道全景。视图仍保留全部 72 列供内部查询，两者不必相同。

    先在同目录写 `.tmp`，再做四项校验（视图/静态清单、契约自洽、导出列集、
    行数），任一不符就抛 `WideExportMismatch` 并在 `finally` 里撤销——绝不让
    半截或错口径的文件顶替上一版。校验在替换**之前**做，是这里最关键的顺序。
    """
    destination = Path(target) if target is not None else export_path(store)
    _assert_inside(store.root, destination)

    catalog = GoldCatalog(store)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp = destination.with_suffix(destination.suffix + ".tmp")
    temp.unlink(missing_ok=True)
    # 第一步的中间文件。定义在 `try` 之前：视图校验失败时异常会走 `finally`，
    # 那时 `base` 必须已经绑定，否则清理语句本身抛 NameError、把原异常盖掉。
    base = destination.with_suffix(destination.suffix + ".base.tmp")
    base.unlink(missing_ok=True)

    try:
        expected = wide_column_names()
        view_columns = tuple(catalog.run("SELECT * FROM datasets LIMIT 0").columns)
        if view_columns != expected:
            raise WideExportMismatch(
                f"视图实际列与静态清单不一致（{len(view_columns)} vs {len(expected)}），"
                "多半是改了 _wide_view_sql 却没同步 wide_columns()"
            )

        problems = contract_errors()
        if problems:
            raise WideExportMismatch(f"默认交付字段契约不自洽：{'；'.join(problems)}")
        needed = [
            column
            for column in (*DEFAULT_COLUMNS, *FALLBACKS.values())
            if column not in INJECTED_COLUMNS
        ]
        outside = [column for column in needed if column not in view_columns]
        if outside:
            raise WideExportMismatch(f"契约列或卡片兜底列在视图里找不到：{outside}")

        # 第一步：视图里那 30 列先落一个临时文件。第二步再与 `readme` 等五张附表
        # 现场 join——它们体积太大或粒度不同，不能进视图（见 INJECTED_COLUMNS）。
        catalog.copy_wide(base, projection=projection_sql())
        catalog.copy_query(temp, _injection_sql(store, base))
        base.unlink(missing_ok=True)

        frame = pl.scan_parquet(temp)
        actual = tuple(frame.collect_schema().names())
        if actual != DEFAULT_COLUMNS:
            raise WideExportMismatch(
                f"导出文件列与默认交付契约不一致：缺 {sorted(set(DEFAULT_COLUMNS) - set(actual))}，"
                f"多 {sorted(set(actual) - set(DEFAULT_COLUMNS))}"
            )

        # 列名对了还不够：`tags` 从 VARCHAR 改回 LIST 那次，列名一个没变。
        # 契约现在声明了类型族，导出物必须与它一致——不一致说明「改了类型却没同步
        # 契约」（也就没升 CONTRACT_VERSION），这正是 2026 年那轮变更漏掉的一步。
        schema = frame.collect_schema()
        wrong_types = [
            (name, type_family(schema[name]), COLUMN_TYPES[name])
            for name in DEFAULT_COLUMNS
            if type_family(schema[name]) not in (COLUMN_TYPES[name], "unknown")
        ]
        if wrong_types:
            raise WideExportMismatch(
                f"导出列类型与默认交付契约不一致（列名、类型族、契约族）：{wrong_types}；"
                "改列类型须同步 COLUMN_TYPES 并升 CONTRACT_VERSION"
            )

        rows = int(frame.select(pl.len()).collect().item())
        baseline = _current_rows(store)
        if rows != baseline:
            raise WideExportMismatch(
                f"导出行数 {rows} 与 current 基准 {baseline} 不等："
                "附表有重复 id，LEFT JOIN 把行放大了"
            )

        size = temp.stat().st_size
        os.replace(temp, destination)
        return WideExportSummary(destination, rows, len(actual), size)
    finally:
        temp.unlink(missing_ok=True)
        base.unlink(missing_ok=True)
        catalog.close()
