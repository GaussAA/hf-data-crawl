"""Gold 层：DuckDB 视图。榜单是视图不是实体表——口径改了立刻生效，不必重跑采集。

视图直接读 parquet 文件，不做物化拷贝；新增分区后同一连接也能立即查到。
"""

from __future__ import annotations

import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from ..pipeline.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS
from ..platform.store import DatasetStore
from ..platform.tables import DATASET_URL, TIMESTAMP, PolarsType, table

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
    },
    # files.status 与 current.status 是两回事：前者只说这次文件清单补采成功与否。
    # files.formats 是扩展名计数，与 silver.formats（标签维度）同名不同义。
    "files": {"status": "files_status", "formats": "file_formats"},
    # 卡片表里只有这一个 sha，表内不必限定；但并进宽表后它左边就是 `sha`（仓库
    # 当前版本），不限定会被读成「另一个 sha」而不是「抓卡片时的 sha」。
    "carddata": {"source_sha": "card_source_sha"},
}

#: 宽视图里刻意不带的列。
#:
#: `raw` 是整行原始 JSON（每行数 KB），带进来会让每一次查询都拖着它走；
#: `snapshot_date` 在各表重复且对查询无意义——它属于分区，不属于分析维度。
_VIEW_EXCLUDED: dict[str, frozenset[str]] = {
    "silver": frozenset({"snapshot_date"}),
    "carddata": frozenset({"snapshot_date"}),
    "size": frozenset({"snapshot_date"}),
    "files": frozenset({"snapshot_date"}),
}

#: 宽视图的附表：(别名, 登记表名, 视图名)，按此顺序拼接，重名列先到先得。
_VIEW_SOURCES: tuple[tuple[str, str, str], ...] = (
    ("s", "silver", "latest_silver"),
    ("cd", "carddata", "card"),
    ("ds", "size", "dataset_size"),
    ("fs", "files", "files"),
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
    ("files", "files"),
)


def wide_columns() -> tuple[tuple[str, str], ...]:
    """宽视图在 `c.*` 之后追加的列：(SELECT 表达式, 视图里的列名)。

    抽成模块级函数，是为了让「宽视图有哪些列」只有一个答案：拼 SQL 的
    `_wide_view_sql`、导出时的列校验、生成数据说明的 `datadoc` 都从这里取。
    此前这份清单埋在方法内部，多一个消费方就难免手抄第二份——而手抄的
    代价项目里已经吃过一次（加卡片字段后 `datasets` 视图静默少一列）。
    """
    used: set[str] = set(table("current").schema)
    parts: list[tuple[str, str]] = []
    for alias, name, _view in _VIEW_SOURCES:
        spec = table(name)
        excluded = _VIEW_EXCLUDED.get(name, frozenset())
        renames = _VIEW_RENAMES.get(name, {})
        for column in spec.schema:
            if column in excluded:
                continue
            target = renames.get(column, column)
            # 改名过的列按**新名字**判重。此前先拿原列名查 used，于是
            # files.status 因与 current.status 撞名被跳过，连改成 files_status
            # 的机会都没有——一条本该进宽视图的状态列就这样静默消失。
            if target in used or (target == column and column in used):
                continue
            parts.append((f"{alias}.{column} AS {target}", target))
            used.add(target)
    return tuple(parts)


def wide_column_names() -> tuple[str, ...]:
    """`datasets` 视图的完整列序：current 原列 + 附表追加列。"""
    return (*table("current").schema, *(name for _, name in wide_columns()))


def url_expr(column: str = "id") -> str:
    """生成 `... AS url` 的 SQL 片段。传入带别名的列名即可（如 `d.id`）。"""
    return f"'{DATASET_URL}' || {column} AS url"


#: 内置榜单。`?` 为条数上限占位。
#:
#: 每个榜都以唯一键收尾（数据集榜用 `id`，聚合榜用分组键）：只按计数排序时，
#: 并列行的次序由 DuckDB 的哈希聚合顺序决定，同一份数据两次运行可能给出不同
#: 页面，报告产物就不再可复现。
#:
#: 带数据集列表的榜单都附 `url` 列：报告页据此把名字渲染成链接，查询者也能
#: 直接拿到可点开的地址。聚合类榜单（分类、许可证、模态）没有单一数据集，不加。
BOARDS: dict[str, str] = {
    "growth": f"""
        SELECT id, {url_expr()}, downloads_start, downloads_now, growth, span_days, from_date, to_date
        FROM v_growth
        WHERE growth > 0
        ORDER BY growth DESC, id
        LIMIT ?
    """,
    "growth_pct": f"""
        SELECT id, {url_expr()}, downloads_start, downloads_now, growth,
               round(growth_rate * 100, 1) AS growth_pct, span_days
        FROM v_growth
        WHERE growth > 0 AND downloads_start >= 100
        ORDER BY growth_rate DESC, id
        LIMIT ?
    """,
    "downloads": f"""
        SELECT id, {url_expr()}, downloads, downloads_all_time, likes, gated
        FROM current
        ORDER BY downloads DESC NULLS LAST, id
        LIMIT ?
    """,
    "alltime": f"""
        SELECT id, {url_expr()}, downloads_all_time, downloads, likes
        FROM current
        ORDER BY downloads_all_time DESC NULLS LAST, id
        LIMIT ?
    """,
    "categories": """
        SELECT unnest(task_categories) AS category, count(*) AS datasets, sum(downloads) AS downloads
        FROM datasets
        GROUP BY 1
        ORDER BY downloads DESC NULLS LAST, category
        LIMIT ?
    """,
    "ranking": f"""
        SELECT d.id, {url_expr("d.id")}, d.pretty_name, d.downloads, d.downloads_all_time, d.likes, d.license_name
        FROM datasets AS d
        ORDER BY d.downloads DESC NULLS LAST, d.id
        LIMIT ?
    """,
    # 与 ranking 列结构完全一致，只换排序键。页面上「累计」若只是把 30 天榜的
    # 那几行重排，看到的是「30 天头部里的累计排序」而非累计榜——两者成员都不同。
    "ranking_alltime": f"""
        SELECT d.id, {url_expr("d.id")}, d.pretty_name, d.downloads, d.downloads_all_time, d.likes, d.license_name
        FROM datasets AS d
        ORDER BY d.downloads_all_time DESC NULLS LAST, d.id
        LIMIT ?
    """,
    "licenses": """
        SELECT license_name, count(*) AS datasets
        FROM card WHERE license_name IS NOT NULL
        GROUP BY 1 ORDER BY datasets DESC, license_name LIMIT ?
    """,
    "volume": f"""
        SELECT d.id, {url_expr("d.id")}, ds.num_rows, ds.num_columns_max,
               round(ds.num_bytes_memory / 1024.0 / 1024 / 1024) AS mem_gb
        FROM dataset_size AS ds
        JOIN datasets AS d ON d.id = ds.id
        WHERE ds.status = 'ok' AND ds.num_rows IS NOT NULL
        ORDER BY ds.num_rows DESC NULLS LAST, d.id
        LIMIT ?
    """,
    "modalities": """
        SELECT unnest(modalities) AS modality, count(*) AS datasets,
               sum(downloads) AS downloads
        FROM datasets GROUP BY 1 ORDER BY datasets DESC NULLS LAST, modality LIMIT ?
    """,
}

#: 报告首屏的整库概览：单行聚合。不放 `BOARDS`——榜单返回多行、概览返回单行，
#: 也不该出现在 `query --board` 的可选值里。SQL 与 `report` 原先执行的一字不差。
OVERVIEW_SQL = """
    SELECT (SELECT count(*) FROM current) AS datasets,
           (SELECT count(*) FROM current WHERE status <> 'active') AS gone,
           (SELECT coalesce(sum(CASE WHEN status = 'active' THEN 1 ELSE 0 END), 0)
              FROM current) AS active,
           (SELECT coalesce(sum(CASE WHEN status = 'disappeared' THEN 1 ELSE 0 END), 0)
              FROM current) AS disappeared,
           (SELECT coalesce(sum(CASE WHEN status = 'restricted' THEN 1 ELSE 0 END), 0)
              FROM current) AS restricted,
           (SELECT coalesce(sum(CASE WHEN status = 'renamed' THEN 1 ELSE 0 END), 0)
              FROM current) AS renamed,
           (SELECT count(*) FROM card) AS cards,
           (SELECT count(*) FROM dataset_size WHERE status = 'ok') AS sized,
           (SELECT count(*) FROM v_growth) AS growth_ready,
           (SELECT count(*) FROM observations
             WHERE snapshot_date = (SELECT max(snapshot_date) FROM observations)) AS moved,
           (SELECT max(snapshot_date) FROM observations) AS as_of,
           (SELECT max(last_seen_at) FROM current) AS last_seen,
           (SELECT coalesce(sum(downloads), 0) FROM current) AS total_downloads
    """

_DUCKDB_TYPES: dict[PolarsType, str] = {
    pl.Utf8: "VARCHAR",
    pl.Int64: "BIGINT",
    pl.Float64: "DOUBLE",
    pl.Boolean: "BOOLEAN",
    pl.Date: "DATE",
    # 带时区的时间在 DuckDB 里是 TIMESTAMPTZ，不是 TIMESTAMP
    TIMESTAMP: "TIMESTAMPTZ",
}


def _duckdb_type(dtype: PolarsType) -> str:
    if dtype == pl.List(pl.Utf8):
        return "VARCHAR[]"
    try:
        return _DUCKDB_TYPES[dtype]
    except KeyError as error:
        # 新增字段类型时在此显式失败，好过让它静默退化成 VARCHAR 再在查询时出错。
        raise ValueError(f"Gold 空视图未映射的 dtype: {dtype}") from error


def _empty_view(schema: dict[str, PolarsType]) -> str:
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


class UnsafeQuery(RuntimeError):
    """查询被安全策略拒绝：非只读语句，或越出数据目录访问文件。"""


#: 拒绝执行时的统一说明。DuckDB 自己给出的 PermissionException 只讲
#: 「filesystem operations are disabled」，读者看不出这是刻意的安全限制。
SECURITY_NOTE = (
    "出于安全限制，`query --sql` 只允许读取本工具的采集数据："
    "禁止写文件、附加数据库、加载扩展，也不能读取数据目录以外的本机文件。"
)


def _guard(sql: str) -> None:
    """只放行单条 SELECT；其余语句在进 DuckDB 之前就拒绝。

    连接层已经关闭外部文件访问（见 `_lock_down`），这里再补一层语句限制：
    `CREATE` / `SET` 之类虽只改内存态、不落盘，但会让同一条 `--sql` 表现出
    与「只读查询」不符的能力；一次也只允许一条语句，避免用分号夹带。
    """
    statements = duckdb.extract_statements(sql)
    if len(statements) != 1:
        raise UnsafeQuery(f"{SECURITY_NOTE}一次只能执行一条语句（收到 {len(statements)} 条）。")
    kind = statements[0].type
    if kind != duckdb.StatementType.SELECT:
        raise UnsafeQuery(f"{SECURITY_NOTE}拒绝执行 {kind} 语句，只接受 SELECT。")


class GoldCatalog:
    """在本地 parquet 上挂一组视图，供 SQL 直接查。"""

    def __init__(self, store: DatasetStore) -> None:
        if not store.has("current"):
            raise GoldNotReady("尚无采集数据，请先运行 `hf-data-crawl collect`")
        self._store = store
        # 内存库不支持 read_only，故先把视图写进一个临时文件库，再以只读方式
        # 重新打开给查询用：查询连接既改不了目录（不能 CREATE/DROP），也解除
        # 不了下面的外部访问限制。
        self._tempdir = tempfile.TemporaryDirectory(prefix="hf-data-crawl-gold-")
        database = Path(self._tempdir.name) / "catalog.duckdb"
        writer = duckdb.connect(str(database))
        try:
            self._lock_down(writer)
            self._register(writer)
        finally:
            writer.close()
        self._connection = duckdb.connect(str(database), read_only=True)
        self._lock_down(self._connection)
        # 视图建完后锁死配置：之后任何 `SET`（包括把外部访问重新打开）都会被拒。
        self._connection.execute("SET lock_configuration = true")

    def _lock_down(self, connection: duckdb.DuckDBPyConnection) -> None:
        """连接层只读：外部文件访问只允许数据目录，其余一律关闭。

        只读连接挡得住对目录本身的写操作（CREATE/DROP/INSERT 都会报
        "attached in read-only mode"）；这里再把外部访问关掉，数据目录以外的
        读写文件、ATTACH、INSTALL/LOAD 都会报 PermissionException。`allowed_
        directories` 是视图读取 parquet 的唯一例外，范围就是数据目录本身；
        目录内仍可写文件，故 `run()` 另有「只允许 SELECT」的语句限制兜底。
        配置在视图就绪后锁死，查询无法自行解除。
        """
        directory = self._store.root.as_posix().replace("'", "''")
        connection.execute(f"SET allowed_directories = ['{directory}']")
        connection.execute("SET enable_external_access = false")

    @property
    def store(self) -> DatasetStore:
        return self._store

    def run(self, sql: str, params: list[Any] | None = None) -> pl.DataFrame:
        _guard(sql)
        try:
            return self._connection.sql(sql, params=params or []).pl()
        except duckdb.PermissionException as error:
            raise UnsafeQuery(f"{SECURITY_NOTE}具体原因：{error}") from error

    def board(self, name: str, limit: int = 50) -> pl.DataFrame:
        sql = BOARDS.get(name)
        if sql is None:
            raise ValueError(f"未知榜单 {name!r}，可选：{sorted(BOARDS)}")
        return self.run(sql, [limit])

    def overview(self) -> dict[str, Any]:
        """整库概览：单行聚合，返回列名到值。"""
        return self.run(OVERVIEW_SQL).row(0, named=True)

    def rows(self, ids: Iterable[str]) -> dict[str, dict[str, Any]]:
        """按 id 取 `datasets` 整行；参数化 `IN`，不拼 SQL 字符串。

        空集合直接返回空字典：`id IN ()` 在 DuckDB 里是语法错误，而「没要任何
        行」本就是一个正常答案。
        """
        unique = sorted({str(item) for item in ids})
        if not unique:
            return {}
        placeholders = ", ".join("?" for _ in unique)
        frame = self.run(f"SELECT * FROM datasets WHERE id IN ({placeholders})", list(unique))
        return {str(row["id"]): row for row in frame.to_dicts()}

    def copy_wide(self, target: Path, *, projection: str | None = None) -> None:
        """把 `datasets` 按投影写成 parquet；目标必须在数据目录内。

        `COPY` 不是 SELECT，进不了 `_guard` 的口子，所以走内部连接——这正是
        这条通道必须收进接口里的原因。`allowed_directories` 仍只放开数据目录，
        这里再把目标校一次：接口自己就不可越界，不靠调用方守规矩。

        `projection` 是一段列表达式（来自 `fields.projection_sql()`），不是第二
        条 SQL：它嵌在 `COPY (...)` 的括号里，无法提前结束语句。
        """
        destination = Path(target)
        root = self._store.root.resolve()
        if not destination.resolve().is_relative_to(root):
            raise ValueError(f"导出目标必须在数据目录内：{destination}（数据目录 {root}）")
        escaped = destination.resolve().as_posix().replace("'", "''")
        self._connection.execute(
            f"COPY (SELECT {projection or '*'} FROM datasets) TO '{escaped}' (FORMAT PARQUET)"
        )

    def close(self) -> None:
        self._connection.close()
        self._tempdir.cleanup()

    def _register(self, connection: duckdb.DuckDBPyConnection) -> None:
        # 视图的连接路径与空视图 schema 全部取自登记表，不再硬编码。
        for view, name in _VIEWS:
            spec = table(name)
            self._create_file_view(connection, view, spec.glob(), _empty_view(spec.schema))
        connection.execute(
            """
            CREATE VIEW latest_silver AS
            SELECT * FROM silver
            QUALIFY row_number() OVER (PARTITION BY id ORDER BY snapshot_date DESC) = 1
            """
        )
        connection.execute(self._wide_view_sql())
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
        connection.execute(
            f"""
            CREATE VIEW tag_values AS
            SELECT id, d.dimension AS dimension, unnest(d.vals) AS value
            FROM (SELECT id, unnest([{packed}]) AS d FROM latest_silver)
            """
        )
        # 任意前缀视图：直接展开原始 tags，不依赖 TAG_DIMENSIONS 白名单。
        # tag_values 只覆盖已拆的 15 个维度 + plain_tags；被丢弃的长尾前缀
        # （`model:` / `eval:` 之类，合计 0.06%）不在其中。这里按第一个冒号原样切分，
        # 任何前缀都能查——是视图不是表：112 万 × 平均 11 条 ≈ 1,200 万行，物化不值当。
        connection.execute(
            """
            CREATE VIEW tag_index AS
            SELECT
                id,
                CASE WHEN strpos(tag, ':') > 0 THEN split_part(tag, ':', 1) ELSE '' END
                    AS prefix,
                CASE WHEN strpos(tag, ':') > 0
                     THEN substr(tag, strpos(tag, ':') + 1)
                     ELSE trim(tag) END AS value
            FROM (SELECT id, unnest(tags) AS tag FROM latest_silver)
            """
        )
        # 增速取「最近两个观测点」，而不是固定 7 日窗口：
        # 日增量只扫最新更新的万把条，全站指标靠周对账刷新，7 日窗口在周节奏下几乎取不到两点。
        # 间隔上限 35 天，避免跨季度比较；跨度以 span_days 明示，读榜的人自己判断。
        connection.execute(
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
        columns = ",\n                   ".join(
            ["c.*", *(expression for expression, _ in wide_columns())]
        )
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

    def _create_file_view(
        self,
        connection: duckdb.DuckDBPyConnection,
        name: str,
        relative_pattern: str,
        empty_sql: str,
    ) -> None:
        """无分区时挂空视图，让「刚装好还没采集」也能建连并给出空结果而非报错。"""
        if any(self._store.root.glob(relative_pattern)):
            pattern = f"{self._store.root.as_posix()}/{relative_pattern}"
            source = f"SELECT * FROM read_parquet('{pattern}', union_by_name = true)"
        else:
            source = empty_sql
        connection.execute(f"CREATE VIEW {name} AS {source}")
