"""Gold 层：DuckDB 视图。榜单是视图不是实体表——口径改了立刻生效，不必重跑采集。

视图直接读 parquet 文件，不做物化拷贝；新增分区后同一连接也能立即查到。
"""

from __future__ import annotations

from typing import Any

import duckdb
import polars as pl

from .silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS
from .store import DatasetStore
from .tables import table

#: 宽视图里需要改名的列：来源表 → {原列名: 视图里的名字}。
#:
#: 只写真正需要区分的：`size.num_rows` 若原样进视图，与卡片口径的
#: `card_num_rows` 并排时，读者分不清哪个是作者声明的、哪个是 datasets-server
#: 统计的。其余同名列由「先到先得」自动跳过。
_VIEW_RENAMES: dict[str, dict[str, str]] = {
    "size": {
        "num_rows": "server_num_rows",
        "num_columns_max": "server_num_columns",
        "num_bytes_memory": "server_num_bytes_memory",
        "estimated_num_rows": "server_estimated_rows",
        "config_count": "server_config_count",
        "num_bytes_original": "server_num_bytes_original",
        "num_bytes_parquet": "server_num_bytes_parquet",
        "configs": "server_configs",
    }
}

#: 宽视图里刻意不带的列。
#:
#: `raw` 是整行原始 JSON（每行数 KB），带进来会让每一次查询都拖着它走；
#: `snapshot_date` 在各表重复且对查询无意义——它属于分区，不属于分析维度。
_VIEW_EXCLUDED: dict[str, frozenset[str]] = {
    "silver": frozenset({"snapshot_date"}),
    "carddata": frozenset({"snapshot_date"}),
    "size": frozenset({"snapshot_date"}),
}

#: 宽视图的附表：(别名, 登记表名, 视图名)，按此顺序拼接，重名列先到先得。
_VIEW_SOURCES: tuple[tuple[str, str, str], ...] = (
    ("s", "silver", "latest_silver"),
    ("cd", "carddata", "card"),
    ("ds", "size", "dataset_size"),
)

#: 视图名 → 登记表名。两者不必同名（`card` 视图读的是 `carddata` 表）。
#:
#: 路径、schema 一律取自登记表：此前这里硬编码了五个相对路径，一旦某张表改名，
#: `_create_file_view` 的 glob 落空就会**静默退化成一个空视图**——查询照常返回
#: 0 行，没有任何报错。
_VIEWS: tuple[tuple[str, str], ...] = (
    ("current", "current"),
    ("observations", "observations"),
    ("silver", "silver"),
    ("card", "carddata"),
    ("card_config", "card_configs"),
    ("dataset_size", "size"),
)

#: 内置榜单。`?` 为条数上限占位。
BOARDS: dict[str, str] = {
    "growth": """
        SELECT id, downloads_start, downloads_now, growth, span_days, from_date, to_date
        FROM v_growth
        WHERE growth > 0
        ORDER BY growth DESC
        LIMIT ?
    """,
    "growth_pct": """
        SELECT id, downloads_start, downloads_now, growth,
               round(growth_rate * 100, 1) AS growth_pct, span_days
        FROM v_growth
        WHERE growth > 0 AND downloads_start >= 100
        ORDER BY growth_rate DESC
        LIMIT ?
    """,
    "downloads": """
        SELECT id, downloads, downloads_all_time, likes, gated
        FROM current
        ORDER BY downloads DESC NULLS LAST
        LIMIT ?
    """,
    "alltime": """
        SELECT id, downloads_all_time, downloads, likes
        FROM current
        ORDER BY downloads_all_time DESC NULLS LAST
        LIMIT ?
    """,
    "categories": """
        SELECT unnest(task_categories) AS category, count(*) AS datasets, sum(downloads) AS downloads
        FROM datasets
        GROUP BY 1
        ORDER BY downloads DESC NULLS LAST
        LIMIT ?
    """,
    "ranking": """
        SELECT d.id, d.pretty_name, d.downloads, d.downloads_all_time, d.likes, d.license_name
        FROM datasets AS d
        ORDER BY d.downloads DESC NULLS LAST
        LIMIT ?
    """,
    "licenses": """
        SELECT license_name, count(*) AS datasets
        FROM card WHERE license_name IS NOT NULL
        GROUP BY 1 ORDER BY datasets DESC LIMIT ?
    """,
    "volume": """
        SELECT d.id, ds.num_rows, ds.num_columns_max,
               round(ds.num_bytes_memory / 1024.0 / 1024 / 1024) AS mem_gb
        FROM dataset_size AS ds
        JOIN datasets AS d ON d.id = ds.id
        WHERE ds.status = 'ok' AND ds.num_rows IS NOT NULL
        ORDER BY ds.num_rows DESC NULLS LAST
        LIMIT ?
    """,
    "modalities": """
        SELECT unnest(modalities) AS modality, count(*) AS datasets,
               sum(downloads) AS downloads
        FROM datasets GROUP BY 1 ORDER BY datasets DESC NULLS LAST LIMIT ?
    """,
}

_DUCKDB_TYPES: dict[pl.DataType, str] = {
    pl.Utf8: "VARCHAR",
    pl.Int64: "BIGINT",
    pl.Float64: "DOUBLE",
    pl.Boolean: "BOOLEAN",
    pl.Date: "DATE",
}


def _duckdb_type(dtype: pl.DataType) -> str:
    if dtype == pl.List(pl.Utf8):
        return "VARCHAR[]"
    try:
        return _DUCKDB_TYPES[dtype]
    except KeyError as error:
        # 新增字段类型时在此显式失败，好过让它静默退化成 VARCHAR 再在查询时出错。
        raise ValueError(f"Gold 空视图未映射的 dtype: {dtype}") from error


def _empty_view(schema: dict[str, pl.DataType]) -> str:
    """按 schema 自动生成空视图。

    手写空视图是第三份会漂移的清单（另两份是表结构与请求字段）：扩列时忘了
    同步，表现为「库为空时查询报列不存在，有数据时却正常」，只在初次安装或
    清库后暴露。改为从 schema 生成，从根上消除这一类漏改。
    """
    columns = ",\n           ".join(
        f"CAST(NULL AS {_duckdb_type(dtype)}) AS {name}" for name, dtype in schema.items()
    )
    return f"SELECT {columns}\n    WHERE false"


class GoldNotReady(RuntimeError):
    """尚无采集数据，无法建立视图。"""


class GoldCatalog:
    """在本地 parquet 上挂一组视图，供 SQL 直接查。"""

    def __init__(self, store: DatasetStore) -> None:
        if not store.has("current"):
            raise GoldNotReady("尚无采集数据，请先运行 `hf-data-crawl collect`")
        self._store = store
        self._connection = duckdb.connect()
        self._register()

    @property
    def store(self) -> DatasetStore:
        return self._store

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        return self._connection

    def run(self, sql: str, params: list[Any] | None = None) -> pl.DataFrame:
        return self._connection.sql(sql, params=params or []).pl()

    def board(self, name: str, limit: int = 50) -> pl.DataFrame:
        sql = BOARDS.get(name)
        if sql is None:
            raise ValueError(f"未知榜单 {name!r}，可选：{sorted(BOARDS)}")
        return self.run(sql, [limit])

    def close(self) -> None:
        self._connection.close()

    def _register(self) -> None:
        # 视图的连接路径与空视图 schema 全部取自登记表，不再硬编码。
        for view, name in _VIEWS:
            spec = table(name)
            self._create_file_view(view, spec.glob(), _empty_view(spec.schema))
        self._connection.execute(
            """
            CREATE VIEW latest_silver AS
            SELECT * FROM silver
            QUALIFY row_number() OVER (PARTITION BY id ORDER BY snapshot_date DESC) = 1
            """
        )
        self._connection.execute(self._wide_view_sql())
        # 长表视图：一行一个 (数据集, 维度, 取值)。宽表便于 join 榜单，长表便于
        # 任意维度聚合而不必改 SQL——「按模态统计」「按格式统计」都是同一句。
        # 由 TAG_DIMENSIONS 生成，新增维度自动进入，不必再手写一段 UNION。
        #
        # 单次扫描：每行先打包成「维度 → 取值列表」的结构体数组再展开。此前是 16 路
        # UNION ALL，每一路都重新算一遍 latest_silver 的窗口函数，全量 count(*) 要
        # 8~9 秒；单次扫描实测 3.6 秒，结果逐行相同（tests/test_gold.py 对照旧写法）。
        packed = ", ".join(
            f"{{'dimension': '{column}', 'vals': {column}}}"
            for column in (*TAG_DIMENSIONS.values(), PLAIN_TAG_COLUMN)
        )
        self._connection.execute(
            f"""
            CREATE VIEW tag_values AS
            SELECT id, d.dimension AS dimension, unnest(d.vals) AS value
            FROM (SELECT id, unnest([{packed}]) AS d FROM latest_silver)
            """
        )
        # 增速取「最近两个观测点」，而不是固定 7 日窗口：
        # 日增量只扫最新更新的万把条，全站指标靠周对账刷新，7 日窗口在周节奏下几乎取不到两点。
        # 间隔上限 35 天，避免跨季度比较；跨度以 span_days 明示，读榜的人自己判断。
        self._connection.execute(
            """
            CREATE VIEW v_growth AS
            WITH ranked AS (
                SELECT id, snapshot_date, downloads,
                       row_number() OVER (PARTITION BY id ORDER BY snapshot_date DESC) AS rn
                FROM observations
            ),
            last_two AS (
                SELECT id,
                       max(CASE WHEN rn = 2 THEN snapshot_date END) AS from_date,
                       max(CASE WHEN rn = 1 THEN snapshot_date END) AS to_date,
                       max(CASE WHEN rn = 2 THEN downloads END) AS downloads_start,
                       max(CASE WHEN rn = 1 THEN downloads END) AS downloads_now
                FROM ranked
                WHERE rn <= 2
                GROUP BY id
            )
            SELECT id, from_date, to_date, downloads_start, downloads_now,
                   downloads_now - downloads_start AS growth,
                   CASE WHEN downloads_start > 0
                        THEN (downloads_now - downloads_start)::DOUBLE / downloads_start
                        ELSE NULL END AS growth_rate,
                   date_diff('day', from_date, to_date) AS span_days
            FROM last_two
            WHERE downloads_start IS NOT NULL
              AND date_diff('day', from_date, to_date) <= 35
            """
        )

    def _wide_view_sql(self) -> str:
        """由登记表拼出宽视图，而不是手抄一份列清单。

        手抄的代价是漂移：加一个卡片字段，`card` 视图由 schema 生成、自动就有，
        `datasets` 却安静地少一列——查询照常跑，只是那一列不存在。同一份文件里
        `_empty_view` 与 `tag_values` 早就示范了正确做法，唯独最常被查的这个视图
        是例外。

        规则：current 的列原样作基底；附表按 `_VIEW_SOURCES` 顺序追加，**重名先到
        先得**；需要区分口径的列在 `_VIEW_RENAMES` 里改名；大列与分区列在
        `_VIEW_EXCLUDED` 里排除。
        """
        used: set[str] = set(table("current").schema)
        parts: list[str] = ["c.*"]

        for alias, name, _view in _VIEW_SOURCES:
            spec = table(name)
            excluded = _VIEW_EXCLUDED.get(name, frozenset())
            renames = _VIEW_RENAMES.get(name, {})
            for column in spec.schema:
                if column in excluded or column in used:
                    continue
                target = renames.get(column, column)
                if target in used:
                    continue
                parts.append(f"{alias}.{column} AS {target}")
                used.add(target)

        columns = ",\n                   ".join(parts)
        joins = "\n            ".join(
            f"LEFT JOIN {view} AS {alias} ON c.id = {alias}.id"
            for alias, _name, view in _VIEW_SOURCES
        )
        return (
            "CREATE VIEW datasets AS\n"
            f"            SELECT {columns}\n"
            "            FROM current AS c\n"
            f"            {joins}\n"
        )

    def _create_file_view(self, name: str, relative_pattern: str, empty_sql: str) -> None:
        """无分区时挂空视图，让「刚装好还没采集」也能建连并给出空结果而非报错。"""
        if any(self._store.root.glob(relative_pattern)):
            pattern = f"{self._store.root.as_posix()}/{relative_pattern}"
            source = f"SELECT * FROM read_parquet('{pattern}', union_by_name = true)"
        else:
            source = empty_sql
        self._connection.execute(f"CREATE VIEW {name} AS {source}")
