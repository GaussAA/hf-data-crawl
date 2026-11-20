"""S7 · Gold 视图与 query 的行为测试。

金标值一律写死字面量，不用被测代码的算法反推，否则测试只是自证。
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import CURRENT_SCHEMA
from hf_data_crawl.query.gold import (
    BOARDS,
    FillKind,
    GoldCatalog,
    column_coverage,
    fill_kind,
    filled_count,
)

DAY1 = date(2026, 11, 1)
DAY4 = date(2026, 11, 4)
DAY5 = date(2026, 11, 5)


def record(
    index: int,
    *,
    downloads: int,
    licenses: list[str] | None = None,
    languages: list[str] | None = None,
    gated: bool = False,
) -> dict[str, Any]:
    tags = [f"license:{item}" for item in (licenses or ["mit"])]
    tags += [f"language:{item}" for item in (languages or ["en"])]
    tags.append("task_categories:text-classification")
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 10,
        "likes": 1,
        "gated": gated,
        "private": False,
        "description": "d",
        "tags": tags,
    }


def seed(store: DatasetStore, snapshot: date, records: list[dict[str, Any]]) -> None:
    raw.write(store, snapshot_date=snapshot, page_index=1, records=records, meta={})
    derive_day(store, snapshot)


@pytest.fixture()
def catalog(tmp_path: Path) -> GoldCatalog:
    store = DatasetStore(tmp_path / "data")
    seed(
        store,
        DAY1,
        [
            record(1, downloads=100, licenses=["mit"]),
            record(2, downloads=50, licenses=["apache-2.0"], languages=["zh", "en"]),
            record(3, downloads=1000, licenses=["gpl-3.0"], gated=True),
            record(4, downloads=200, licenses=["cc0-1.0"]),
        ],
    )
    seed(
        store,
        DAY4,
        [
            record(1, downloads=260, licenses=["mit"]),
            record(2, downloads=60, licenses=["apache-2.0"], languages=["zh", "en"]),
            record(4, downloads=150, licenses=["cc0-1.0"]),
        ],
    )
    return GoldCatalog(store)


def test_增速榜只计有两次读数的集(catalog: GoldCatalog) -> None:
    rows = catalog.board("growth", limit=50)

    assert rows["id"].to_list() == ["acme/ds-1", "acme/ds-2"]


def test_下跌的集不出现在增速榜(catalog: GoldCatalog) -> None:
    rows = catalog.board("growth", limit=50)

    assert "acme/ds-4" not in rows["id"].to_list()


def test_增速数值与金标一致(catalog: GoldCatalog) -> None:
    rows = catalog.board("growth", limit=50)

    assert rows["growth"].to_list() == [160, 10]
    assert rows["span_days"].to_list() == [3, 3]


def test_百分比榜排除基数过小的集(catalog: GoldCatalog) -> None:
    """起始下载不足 100 时百分比会被小数放大，噪声盖过真实信号。"""
    rows = catalog.board("growth_pct", limit=50)

    assert rows["id"].to_list() == ["acme/ds-1"]
    assert rows["growth_pct"].to_list() == [160.0]


def test_当前态榜按三十天下载降序(catalog: GoldCatalog) -> None:
    rows = catalog.board("downloads", limit=2)

    assert rows["id"].to_list() == ["acme/ds-3", "acme/ds-1"]


def test_累计榜按累计下载降序(catalog: GoldCatalog) -> None:
    rows = catalog.board("alltime", limit=1)

    assert rows["id"].to_list() == ["acme/ds-3"]


def test_可按许可证与gated过滤(catalog: GoldCatalog) -> None:
    mit_rows = catalog.run("SELECT id FROM datasets WHERE list_contains(licenses, 'mit')")
    gated_rows = catalog.run("SELECT id FROM datasets WHERE gated = 'true'")

    assert mit_rows["id"].to_list() == ["acme/ds-1"]
    assert gated_rows["id"].to_list() == ["acme/ds-3"]


def test_分类榜按任务类别聚合(catalog: GoldCatalog) -> None:
    """分类榜走 `board("categories")`，一行是一个任务类别。

    原先这里另写一条 SQL，`board` 的 SQL 改坏也不会红——测的与跑的不是同一条。
    """
    frame = catalog.board("categories", 5)

    assert set(frame.columns) == {"category", "datasets", "downloads"}
    assert frame["category"].to_list() == ["text-classification"]
    assert frame["datasets"][0] >= 1


def test_新增数据在同一连接内即时可见(catalog: GoldCatalog) -> None:
    """视图读的是 parquet 文件，非物化副本：补数后无需重建视图即可查到。"""
    before = catalog.board("growth", limit=50)
    assert before.filter(before["id"] == "acme/ds-1")["growth"].to_list() == [160]

    seed(catalog.store, DAY5, [record(1, downloads=300, licenses=["mit"])])

    after = catalog.board("growth", limit=50)
    ds1 = after.filter(after["id"] == "acme/ds-1").row(0, named=True)
    # 新口径取最近两点：DAY4(260) → DAY5(300)，而非首次读数起的累计差
    assert ds1["growth"] == 40
    assert ds1["span_days"] == 1


def test_并列时按唯一键稳定排序(tmp_path: Path) -> None:
    """并列行不能交给哈希顺序：同一份数据两次运行会给出不同的榜单与报告。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(2, downloads=100), record(1, downloads=100)])
    catalog = GoldCatalog(store)
    try:
        rows = catalog.board("downloads", limit=10)
    finally:
        catalog.close()

    assert rows["id"].to_list() == ["acme/ds-1", "acme/ds-2"]


def test_无数据时给出可读错误(tmp_path: Path) -> None:
    from hf_data_crawl.query.gold import GoldNotReady

    with pytest.raises(GoldNotReady, match="collect"):
        GoldCatalog(DatasetStore(tmp_path / "empty"))


# ---------------------------------------------------------------- 宽视图由 schema 拼装


def test_宽视图不含原始JSON与分区列(catalog: GoldCatalog) -> None:
    """`raw` 是整行 JSON（每行数 KB），带进视图会让每次查询都拖着它走。"""
    columns = catalog.run("SELECT * FROM datasets LIMIT 0").columns

    assert "raw" not in columns
    assert "snapshot_date" not in columns


def test_宽视图包含附表的全部有效列(catalog: GoldCatalog) -> None:
    """手抄列清单的代价是漂移：附表加了列，宽视图安静地少一列。"""
    from hf_data_crawl.platform.tables import table
    from hf_data_crawl.query.gold import _VIEW_RENAMES

    columns = set(catalog.run("SELECT * FROM datasets LIMIT 0").columns)
    for name, excluded in (
        ("silver", {"snapshot_date"}),
        ("carddata", {"snapshot_date"}),
        ("size", {"snapshot_date"}),
        ("files", {"snapshot_date"}),
    ):
        renames = _VIEW_RENAMES.get(name, {})
        expected = {renames.get(column, column) for column in set(table(name).schema) - excluded}
        # 与 current 重名的列由先到先得跳过，不算缺失
        missing = {
            column
            for column in expected
            if column not in columns and column not in table("current").schema
        }
        assert not missing, f"{name} 的这些列没进宽视图：{sorted(missing)}"


def test_宽视图列名唯一(catalog: GoldCatalog) -> None:
    """两张附表有同名列时，不加区分会让查询者拿到不确定的那一个。"""
    columns = catalog.run("SELECT * FROM datasets LIMIT 0").columns

    assert len(columns) == len(set(columns))


def test_规模口径带server前缀以免与卡片口径混淆(catalog: GoldCatalog) -> None:
    columns = set(catalog.run("SELECT * FROM datasets LIMIT 0").columns)

    assert "server_num_rows" in columns
    assert "card_num_rows" in columns, "卡片口径的行数应同时存在，两者含义不同"


def test_规模抓取状态不与库状态撞名(catalog: GoldCatalog) -> None:
    """`size.status` 是 ok/unavailable，`current.status` 是 active/disappeared。

    两列同名时先到先得，`size` 那列被静默丢弃，于是按 `status = 'ok'` 过滤会
    命中库状态而不是抓取状态——查出来是空也不报错。`files` 早先修过同一处。
    """
    columns = set(catalog.run("SELECT * FROM datasets LIMIT 0").columns)

    assert "size_status" in columns
    assert "files_status" in columns


def test_重名列以实际数据为准取current的那一份(catalog: GoldCatalog) -> None:
    """status 在 current 与 size 里都有，视图里必须是 current 的。"""
    row = catalog.run("SELECT status FROM datasets LIMIT 1").row(0, named=True)

    assert row["status"] == "active"


# ---------------------------------------------------------------- 最新一行只有一个定义


def test_current与latest_silver对同一时刻取值一致(catalog: GoldCatalog) -> None:
    """「最新一行」有两套实现：物化的 current 与窗口视图 latest_silver。

    两者必须永远一致，否则同一数据集在不同视图里会给出不同的数字，
    而没有任何东西会报错。
    """
    current = catalog.run("SELECT * FROM current ORDER BY id")
    latest = catalog.run("SELECT * FROM latest_silver ORDER BY id")
    common = [column for column in current.columns if column in latest.columns]

    assert common, "两者没有可比列，说明视图结构已严重漂移"
    assert current.select(common).equals(latest.select(common))


# ---------------------------------------------------------------- tag_index 任意前缀


def test_tag_index覆盖未拆分的长尾前缀(tmp_path: Path) -> None:
    """tag_values 只认白名单；被丢弃的长尾前缀（`model:` 之类）得另有出口。"""
    store = DatasetStore(tmp_path / "data")
    item = record(1, downloads=10)
    item["tags"] = [*item["tags"], "model:qwen-canary", "bare-tag"]
    seed(store, DAY1, [item])
    catalog = GoldCatalog(store)
    try:
        long_tail = catalog.run("SELECT value FROM tag_index WHERE prefix = 'model'")
        plain = catalog.run("SELECT value FROM tag_index WHERE prefix = ''")
        total = catalog.run("SELECT count(*) AS n FROM tag_index")["n"].to_list()
    finally:
        catalog.close()

    assert long_tail["value"].to_list() == ["qwen-canary"]
    assert "bare-tag" in plain["value"].to_list(), "无前缀标签的 prefix 应为空串"
    assert total == [5], "5 条标签各展开一行，不多不少"


def test_tag_index在已知维度上与tag_values一致(catalog: GoldCatalog) -> None:
    """同一份数据两条路径不该给出不同答案（夹具里只有两字码，故折叠无差异）。"""
    indexed = catalog.run(
        "SELECT id, value FROM tag_index WHERE prefix = 'language' ORDER BY id, value"
    )
    dimensioned = catalog.run(
        "SELECT id, value FROM tag_values WHERE dimension = 'languages' ORDER BY id, value"
    )

    assert indexed.height > 0
    assert indexed.equals(dimensioned)


# ---------------------------------------------------------------- tag_values 单次扫描


def _legacy_tag_values_sql() -> str:
    """改写前的 16 路 UNION ALL，留作参考实现。"""
    from hf_data_crawl.pipeline.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS

    return " UNION ALL ".join(
        f"SELECT id, '{column}' AS dimension, unnest({column}) AS value FROM latest_silver"
        for column in (*TAG_DIMENSIONS.values(), PLAIN_TAG_COLUMN)
    )


def test_tag_values与旧的UNION写法结果相同(catalog: GoldCatalog) -> None:
    new = catalog.run("SELECT * FROM tag_values ORDER BY id, dimension, value")
    old = catalog.run(f"SELECT * FROM ({_legacy_tag_values_sql()}) ORDER BY id, dimension, value")

    assert new.height > 0
    assert new.equals(old)


def test_tag_values覆盖全部维度列(catalog: GoldCatalog) -> None:
    from hf_data_crawl.pipeline.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS

    dimensions = set(
        catalog.run("SELECT DISTINCT dimension FROM tag_values")["dimension"].to_list()
    )

    assert dimensions <= {*TAG_DIMENSIONS.values(), PLAIN_TAG_COLUMN}
    assert "task_categories" in dimensions


def test_tag_values每个维度都有数据时仍与旧写法一致(tmp_path: Path) -> None:
    """夹具只带三类标签时，少一个维度也看不出来；这里让每个维度都有取值。"""
    from hf_data_crawl.pipeline.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS

    store = DatasetStore(tmp_path / "data")
    tags = [f"{prefix}:v-{prefix}" for prefix in TAG_DIMENSIONS] + ["text", "image"]
    payload = record(1, downloads=10)
    payload["tags"] = tags
    seed(store, DAY1, [payload])
    full = GoldCatalog(store)
    try:
        new = full.run("SELECT * FROM tag_values ORDER BY id, dimension, value")
        old = full.run(f"SELECT * FROM ({_legacy_tag_values_sql()}) ORDER BY id, dimension, value")
        dimensions = set(new["dimension"].to_list())
    finally:
        full.close()

    assert dimensions == {*TAG_DIMENSIONS.values(), PLAIN_TAG_COLUMN}
    assert new.equals(old)


# ---------------------------------------------------------------- 数据集页面地址


def test_宽视图带可直接打开的地址(catalog: GoldCatalog) -> None:
    """HF 不返回 url 字段，地址由 id 派生——但必须真能拼出来且拼得对。"""
    row = catalog.run("SELECT id, url FROM datasets ORDER BY id LIMIT 1").row(0, named=True)

    assert row["url"] == f"https://huggingface.co/datasets/{row['id']}"


def test_带数据集列表的榜单都附地址(catalog: GoldCatalog) -> None:
    for name in ("downloads", "alltime", "growth", "ranking", "ranking_alltime"):
        frame = catalog.board(name, 5)
        assert frame.height > 0, f"{name} 应有数据"
        assert "url" in frame.columns, f"{name} 缺 url 列"
        assert frame["url"][0].startswith("https://huggingface.co/datasets/")


def test_聚合类榜单不需要地址(catalog: GoldCatalog) -> None:
    """分类榜一行是一个任务类别，没有单一数据集可指向。"""
    frame = catalog.board("categories", 5)

    assert "url" not in frame.columns


# ---------------------------------------------------------------- 能力接口


def test_overview_returns_single_named_row(catalog: GoldCatalog) -> None:
    """概览是单行聚合，字段固定；它不是榜单，也不走 board。"""
    row = catalog.overview()

    assert set(row) == {
        "datasets",
        "gone",
        "active",
        "disappeared",
        "restricted",
        "renamed",
        "cards",
        "sized",
        "growth_ready",
        "moved",
        "as_of",
        "last_seen",
        "total_downloads",
    }
    assert row["datasets"] == 4
    assert row["active"] == 4


def test_rows_are_parameterized_and_tolerate_missing_ids(catalog: GoldCatalog) -> None:
    """按 id 取行不拼 SQL 字符串：带引号的 id 只是「没查到」，不是语法错误。"""
    found = catalog.rows(["acme/ds-1", "acme/ds-404", "x'y"])

    assert set(found) == {"acme/ds-1"}
    assert found["acme/ds-1"]["id"] == "acme/ds-1"
    assert catalog.rows([]) == {}


def test_copy_wide_rejects_target_outside_store(catalog: GoldCatalog, tmp_path: Path) -> None:
    """导出通道自己守住目录边界，不依赖调用方的检查。"""
    outside = tmp_path / "outside.parquet"

    with pytest.raises(ValueError, match="必须在数据目录内"):
        catalog.copy_wide(outside)

    assert not outside.exists()


# ---------------------------------------------------------------- 只读查询的安全边界


def test_任意SQL只接受单条SELECT(catalog: GoldCatalog) -> None:
    """非 SELECT 与分号夹带都在进 DuckDB 之前被拒，报错要点明是安全限制。"""
    from hf_data_crawl.query.gold import UnsafeQuery

    with pytest.raises(UnsafeQuery, match="安全限制"):
        catalog.run("CREATE TABLE stolen AS SELECT 1")
    with pytest.raises(UnsafeQuery, match="安全限制"):
        catalog.run("SELECT 1; SELECT 2")


def test_不能读取数据目录以外的文件(catalog: GoldCatalog, tmp_path: Path) -> None:
    outside = tmp_path / "secret.csv"
    outside.write_text("value\n42\n", encoding="utf-8")

    from hf_data_crawl.query.gold import UnsafeQuery

    with pytest.raises(UnsafeQuery, match="安全限制"):
        catalog.run(f"SELECT * FROM read_csv('{outside.as_posix()}')")


def test_连接层只读且禁止写文件(catalog: GoldCatalog, tmp_path: Path) -> None:
    """查询连接本身是只读的，且即便绕过公开 API 也写不了数据目录外的文件。

    这里刻意取私有 `_connection`：要验的是连接层的护栏，公开接口已不再暴露它。
    """
    import duckdb

    with pytest.raises(duckdb.InvalidInputException, match="read-only"):
        catalog._connection.execute("CREATE TABLE stolen AS SELECT 1")

    target = tmp_path / "copied.csv"

    with pytest.raises(duckdb.PermissionException):
        catalog._connection.execute(f"COPY (SELECT 1) TO '{target.as_posix()}'")
    assert not target.exists()


def test_外部访问配置已锁死(catalog: GoldCatalog) -> None:
    """查询执行期间不能把 `enable_external_access` 重新打开。"""
    import duckdb

    with pytest.raises(duckdb.InvalidInputException):
        catalog._connection.execute("SET enable_external_access = true")


def test_query命令行对非只读语句返回失败(tmp_path: Path, capsys: Any) -> None:
    from hf_data_crawl.ops.cli import main

    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, downloads=100)])

    code = main(["--data-dir", str(store.root), "query", "--sql", "CREATE TABLE x AS SELECT 1"])

    assert code == 1
    assert "安全限制" in capsys.readouterr().out


def test_自定义SQL交付口径仍受单条SELECT约束(catalog: GoldCatalog, tmp_path: Path) -> None:
    """`mode: sql` 是逃生舱，但编译层原样透传不等于绕过安全边界。

    端到端钉死分工：requirement 不自行加过滤，GoldCatalog._guard 负责拦多语句；
    两处都别「顺手补一刀」，否则边界被劈成两半、两边都不再可信。
    """
    import yaml

    from hf_data_crawl.delivery.requirement import compile_sql, load
    from hf_data_crawl.query.gold import UnsafeQuery

    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(
        yaml.safe_dump(
            {
                "id": "REQ-9001",
                "title": "手写 SQL",
                "requester": "tester",
                "version": 1,
                "mode": "sql",
                "sql": "SELECT 1; SELECT 2",
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    compiled = compile_sql(load(spec_path))

    assert compiled == "SELECT 1; SELECT 2"
    with pytest.raises(UnsafeQuery, match="安全限制"):
        catalog.run(compiled)


# ---------------------------------------------------------------- 文档契约


def test_README列出全部内置榜单() -> None:
    """榜单清单不能有手抄副本：README 的表格必须与 BOARDS 逐项对齐。

    新增一榜只改 gold.py 时，query 的候选项会自动跟着变，README 却不会——
    读文档的人于是查不到新榜。这条断言把那处静默漂移改成显式失败。
    """
    readme = (Path(__file__).resolve().parents[2] / "README.md").read_text(encoding="utf-8")
    section = readme.split("内置榜单（", 1)[1].split("**两个下载口径", 1)[0]
    listed = set(re.findall(r"^\| `([a-z_]+)` \|", section, flags=re.MULTILINE))
    expected = set(BOARDS)

    assert listed == expected, (
        f"README 榜单清单与 BOARDS 不一致：缺 {sorted(expected - listed)}，"
        f"多 {sorted(listed - expected)}"
    )


# ---------------------------------------------------------------- 旧库兼容


def test_旧结构表文件缺新列时视图仍能建立(tmp_path: Path) -> None:
    """加列之后，磁盘上没重写过的旧文件不该让整个视图建不起来。

    `gold` 的视图直读 parquet，而列对齐只发生在 `DatasetStore.read` 里。远程给
    `size` 加 `fetched_at` 后，本机那份 12 列的旧 `enrich/size.parquet` 让
    `CREATE VIEW datasets` 直接 BinderException——CI 里没有 `data/`，那条真实库
    对照测试是 skip，于是「加列」在本机炸、在 CI 全绿。这里造一份旧结构文件钉住：
    视图要照常建立，缺的列补 null，而不是整个视图建不起来。
    """
    import polars as pl

    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, downloads=100)])

    legacy = store.path("size")
    legacy.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "id": ["acme/ds-1"],
            "url": ["https://huggingface.co/datasets/acme/ds-1"],
            "snapshot_date": [DAY1],
            "status": ["ok"],
            "num_rows": [10],
        }
    ).write_parquet(legacy)

    catalog = GoldCatalog(store)
    frame = catalog.run("SELECT id, fetched_at, server_num_rows FROM datasets")

    assert frame["server_num_rows"].to_list() == [10]
    assert frame["fetched_at"].to_list() == [None]

    # 补 null 而不是少一列：旧文件缺的列在视图里照样存在，只是全空。
    columns = set(catalog.run("SELECT * FROM datasets LIMIT 0").columns)
    assert {"fetched_at", "server_num_rows", "size_status"} <= columns


def test_覆盖率只数真有内容的行(catalog: GoldCatalog) -> None:
    """非 NULL 不等于有内容：空列表要算空。

    这是 `data/DATA.md` 与交付 README 里那个百分比的唯一算法。夹具里的
    `plain_tags` 是 `fill_null([])` 出来的空列表，按 `count(col)` 会报成 100%，
    而实际没有一个数据集真带了自定义标签。
    """
    coverage = column_coverage(catalog, ["downloads", "tags", "plain_tags"])

    assert coverage["downloads"] == 1.0
    assert coverage["tags"] == 1.0
    assert coverage["plain_tags"] == 0.0


def test_空底库的覆盖率是零而不是炸掉(tmp_path: Path) -> None:
    """装着还没采集时 `docs` 也要能生成。

    空视图上的 `count_if` 是 `sum(CASE …)`，输入为空返回 NULL——直接 `float()`
    会炸在「装好还没跑过采集」这一步，而那正是第一次生成文档的时刻。
    """
    store = DatasetStore(tmp_path / "empty")
    store.write("current", pl.DataFrame(schema=CURRENT_SCHEMA))
    catalog = GoldCatalog(store)
    try:
        assert column_coverage(catalog, ["downloads"]) == {"downloads": 0.0}
    finally:
        catalog.close()


def test_有内容的判据按类型分三种() -> None:
    """空列表与空串都不算内容：空列表表示「问过、答了没有」。

    SQL 侧（`column_coverage`）与 Polars 侧（交付 README 的本次结果覆盖）都从
    `fill_kind` 出发，这条钉的是**分类**本身——加一种 dtype 而只改一边，两个
    覆盖率就会开始说不同的话，而两边各自的测试都不会红。
    """
    assert fill_kind(pl.List(pl.Utf8)) is FillKind.LIST
    assert fill_kind(pl.Utf8) is FillKind.TEXT
    assert fill_kind(pl.Int64) is FillKind.OTHER

    frame = pl.DataFrame(
        {
            "tags": [["a"], [], None],
            "text": ["x", "", None],
            "num": [1, None, 3],
        }
    )

    assert filled_count(frame["tags"]) == 1
    assert filled_count(frame["text"]) == 1
    assert filled_count(frame["num"]) == 2


# ------------------------------------------------- README 侧栏字段（2026-11-18）


def test_侧栏字段表也挂了视图(catalog: GoldCatalog) -> None:
    """抽出 78 万条字段却查不到，是最难解释的那种缺口。

    `fielddocs` 里写着 `readme_fields` 的字段说明、抽取命令也照常跑成功，而
    `query` 里 `SELECT ... FROM readme_fields` 报 `CatalogException`：视图登记表
    （`_VIEWS`）漏了它，只能直读 parquet 才拿得到。文档有、数据有、接口没有——
    三者不一致时，读文档的人会以为是自己写错了 SQL。
    """
    from hf_data_crawl.platform.tables import table

    columns = catalog.run("SELECT * FROM readme_fields LIMIT 0").columns

    assert set(table("readme_fields").schema) <= set(columns)


def test_侧栏字段不进宽表(catalog: GoldCatalog) -> None:
    """宽表是交付契约，加列要单独议；这条钉住「没顺手把它们并进去」。"""
    columns = catalog.run("SELECT * FROM datasets LIMIT 0").columns

    assert "homepage" not in columns
    assert "citation_bibtex" not in columns


def test_宽表has_readme由readme派生而不是files(tmp_path: Path) -> None:
    """两份事实分开：`files` 只看前 1000 名的一页，`readme` 覆盖全库。

    2026-11-18 之前宽表的 `has_readme` 取自 `files`，全库只有 0.08% 有值——空值在
    读数据的人眼里就是「没有 README」。当时 `readme` 表已 100% 覆盖，却没人用它。
    """
    from hf_data_crawl.platform.tables import FILES_SCHEMA, README_SCHEMA

    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, downloads=100), record(2, downloads=50), record(3, downloads=10)])

    readme: dict[str, list[Any]] = {name: [None] * 3 for name in README_SCHEMA}
    readme["id"] = ["acme/ds-1", "acme/ds-2", "acme/ds-3"]
    readme["snapshot_date"] = [DAY1] * 3
    readme["source_sha"] = ["sha-1", "sha-2", "sha-3"]
    readme["status"] = ["ok", "absent", "denied"]
    readme["bytes"] = [10, 0, None]
    store.write("readme", pl.DataFrame(readme, schema=README_SCHEMA))

    files: dict[str, list[Any]] = {name: [None] for name in FILES_SCHEMA}
    files["id"] = ["acme/ds-1"]
    files["snapshot_date"] = [DAY1]
    files["status"] = ["ok"]
    files["has_readme"] = [True]
    store.write("files", pl.DataFrame(files, schema=FILES_SCHEMA))

    catalog = GoldCatalog(store)
    try:
        rows = catalog.run(
            "SELECT id, has_readme, files_has_readme FROM datasets ORDER BY id"
        ).rows()
    finally:
        catalog.close()

    # ok → true，absent → false，denied → NULL（「读不到」不等于「作者没写」）
    assert rows == [
        ("acme/ds-1", True, True),
        ("acme/ds-2", False, None),
        ("acme/ds-3", None, None),
    ]


# ------------------------------------------------- 增补层可查性（2026-11-20）


def test_预览试点也有视图(tmp_path: Path) -> None:
    """第四份增补资产注册了表、却没有视图，`query` 里报的是「表不存在」。

    2026-11-20 实测：`FROM preview` 抛 CatalogException，于是 `query`、体检、榜单页
    三处都看不见这份数据——表在磁盘上、字段说明里也写着，唯独接口没有。
    """
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, downloads=100)])

    catalog = GoldCatalog(store)
    try:
        assert catalog.run("SELECT count(*) AS n FROM preview")["n"][0] == 0
        assert "preview" in catalog.views()
    finally:
        catalog.close()


def test_规模榜剔除int64溢出报出的行数(tmp_path: Path) -> None:
    """溢出的量不是行数，却会稳居规模榜首位。

    2026-11-20 实测：`yethss/The-Quettaset` 的 `num_rows` 是 9223372036854775000
    （落盘时被浮点舍入过，**不等于** int64 上限本身），页面于是把 `9223372.04T`
    印成「规模上限」的头名。阈值只剔除这一族，作者自己报的大数照旧留着。
    """
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, downloads=100), record(2, downloads=50)])
    sizes = store.path("size")
    sizes.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            "id": ["acme/ds-1", "acme/ds-2"],
            "snapshot_date": [DAY1, DAY1],
            "status": ["ok", "ok"],
            "num_rows": [10_000_000, 9_223_372_036_854_775_000],
        }
    ).write_parquet(sizes)

    catalog = GoldCatalog(store)
    try:
        ids = catalog.board("volume", 10)["id"].to_list()
    finally:
        catalog.close()

    assert ids == ["acme/ds-1"]
