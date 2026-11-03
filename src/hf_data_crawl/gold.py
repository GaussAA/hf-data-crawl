"""Gold 层：DuckDB 视图。榜单是视图不是实体表——口径改了立刻生效，不必重跑采集。

视图直接读 parquet 文件，不做物化拷贝；新增分区后同一连接也能立即查到。
"""

from __future__ import annotations

from typing import Any

import duckdb
import polars as pl

from .store import DatasetStore

#: 内置榜单。`?` 为条数上限占位。
BOARDS: dict[str, str] = {
    "growth": """
        SELECT id, downloads_start, downloads_now, growth,
               round(growth_rate * 100, 1) AS growth_pct, from_date, to_date
        FROM v_growth_7d
        ORDER BY growth DESC
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

_EMPTY_OBSERVATIONS = """
    SELECT CAST(NULL AS VARCHAR) AS id,
           CAST(NULL AS DATE) AS snapshot_date,
           CAST(NULL AS BIGINT) AS downloads,
           CAST(NULL AS BIGINT) AS downloads_all_time,
           CAST(NULL AS BIGINT) AS likes
    WHERE false
"""

_EMPTY_SILVER = """
    SELECT CAST(NULL AS VARCHAR) AS id,
           CAST(NULL AS VARCHAR[]) AS task_categories,
           CAST(NULL AS VARCHAR[]) AS languages,
           CAST(NULL AS VARCHAR[]) AS size_categories,
           CAST(NULL AS VARCHAR[]) AS licenses,
           CAST(NULL AS VARCHAR[]) AS libraries,
           CAST(NULL AS VARCHAR[]) AS tags,
           CAST(NULL AS VARCHAR) AS description,
           CAST(NULL AS DATE) AS snapshot_date
    WHERE false
"""


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
        self._create_file_view("observations", "state/observations/dt=*/obs.parquet", _EMPTY_OBSERVATIONS)
        self._create_file_view("silver", "silver/dt=*/silver.parquet", _EMPTY_SILVER)
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
                   s.licenses, s.libraries, s.tags, s.description
            FROM current AS c
            LEFT JOIN latest_silver AS s ON c.id = s.id
            """
        )
        self._connection.execute(
            """
            CREATE VIEW v_growth_7d AS
            WITH bounds AS (SELECT max(snapshot_date) AS as_of FROM observations),
            recent AS (
                SELECT o.id, o.snapshot_date, o.downloads
                FROM observations AS o, bounds AS b
                WHERE o.snapshot_date >= b.as_of - INTERVAL 7 DAY
            ),
            agg AS (
                SELECT id,
                       count(*) AS points,
                       min(snapshot_date) AS from_date,
                       max(snapshot_date) AS to_date,
                       arg_min(downloads, snapshot_date) AS downloads_start,
                       arg_max(downloads, snapshot_date) AS downloads_now
                FROM recent
                GROUP BY id
            )
            SELECT id, points, from_date, to_date, downloads_start, downloads_now,
                   downloads_now - downloads_start AS growth,
                   CASE WHEN downloads_start > 0
                        THEN (downloads_now - downloads_start)::DOUBLE / downloads_start
                        ELSE NULL END AS growth_rate
            FROM agg
            WHERE points >= 2
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
