"""Gold 层：DuckDB 视图。榜单是视图不是实体表——口径改了立刻生效，不必重跑采集。

视图直接读 parquet 文件，不做物化拷贝；新增分区后同一连接也能立即查到。
"""

from __future__ import annotations

from typing import Any

import duckdb
import polars as pl

from .card import CARD_CONFIGS_SCHEMA, ENRICH_SCHEMA
from .silver import SILVER_SCHEMA
from .store import OBSERVATIONS_SCHEMA, DatasetStore

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
        if not (store.state_dir / "current.parquet").exists():
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
        root = self._store.root.as_posix()
        self._connection.execute(
            f"CREATE VIEW current AS SELECT * FROM read_parquet('{root}/state/current.parquet')"
        )
        self._create_file_view(
            "observations", "state/observations/dt=*/obs.parquet", _empty_view(OBSERVATIONS_SCHEMA)
        )
        self._create_file_view("silver", "silver/dt=*/silver.parquet", _empty_view(SILVER_SCHEMA))
        self._create_file_view("card", "enrich/carddata.parquet", _empty_view(ENRICH_SCHEMA))
        self._create_file_view(
            "card_config", "enrich/card_configs.parquet", _empty_view(CARD_CONFIGS_SCHEMA)
        )
        self._connection.execute(
            """
            CREATE VIEW latest_silver AS
            SELECT * FROM silver
            QUALIFY row_number() OVER (PARTITION BY id ORDER BY snapshot_date DESC) = 1
            """
        )
        self._connection.execute(
            """
            CREATE VIEW datasets AS
            SELECT c.*, s.task_categories, s.languages, s.size_categories,
                   s.licenses, s.libraries, s.tags, s.description,
                   cd.pretty_name, cd.paperswithcode_id, cd.arxiv,
                   cd.license_name, cd.license_link, cd.citation, cd.card_description,
                   cd.card_tags, cd.card_task_categories, cd.card_languages,
                   cd.card_size_categories, cd.card_multilinguality,
                   cd.card_annotations_creators, cd.card_language_creators,
                   cd.card_source_datasets, cd.card_homepage,
                   cd.card_config_count, cd.card_num_rows,
                   cd.card_dataset_bytes, cd.card_download_bytes
            FROM current AS c
            LEFT JOIN latest_silver AS s ON c.id = s.id
            LEFT JOIN card AS cd ON c.id = cd.id
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

    def _create_file_view(self, name: str, relative_pattern: str, empty_sql: str) -> None:
        """无分区时挂空视图，让「刚装好还没采集」也能建连并给出空结果而非报错。"""
        if any(self._store.root.glob(relative_pattern)):
            pattern = f"{self._store.root.as_posix()}/{relative_pattern}"
            source = f"SELECT * FROM read_parquet('{pattern}', union_by_name = true)"
        else:
            source = empty_sql
        self._connection.execute(f"CREATE VIEW {name} AS {source}")
