"""S7 · Gold 视图与 query 的行为测试。

金标值一律写死字面量，不用被测代码的算法反推，否则测试只是自证。
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl.derive import derive_day
from hf_data_crawl.gold import BOARDS, GoldCatalog
from hf_data_crawl.store import DatasetStore

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
    store.write_raw(snapshot_date=snapshot, page_index=1, records=records, meta={})
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
    rows = catalog.run(
        "SELECT unnest(task_categories) AS category, count(*) AS n FROM datasets GROUP BY 1"
    )

    assert rows.height >= 1


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


def test_无数据时给出可读错误(tmp_path: Path) -> None:
    from hf_data_crawl.gold import GoldNotReady

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
    from hf_data_crawl.gold import _VIEW_RENAMES
    from hf_data_crawl.tables import table

    columns = set(catalog.run("SELECT * FROM datasets LIMIT 0").columns)
    for name, excluded in (
        ("silver", {"snapshot_date"}),
        ("carddata", {"snapshot_date"}),
        ("size", {"snapshot_date"}),
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


# ---------------------------------------------------------------- tag_values 单次扫描


def _legacy_tag_values_sql() -> str:
    """改写前的 16 路 UNION ALL，留作参考实现。"""
    from hf_data_crawl.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS

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
    from hf_data_crawl.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS

    dimensions = set(
        catalog.run("SELECT DISTINCT dimension FROM tag_values")["dimension"].to_list()
    )

    assert dimensions <= {*TAG_DIMENSIONS.values(), PLAIN_TAG_COLUMN}
    assert "task_categories" in dimensions


def test_tag_values每个维度都有数据时仍与旧写法一致(tmp_path: Path) -> None:
    """夹具只带三类标签时，少一个维度也看不出来；这里让每个维度都有取值。"""
    from hf_data_crawl.silver import PLAIN_TAG_COLUMN, TAG_DIMENSIONS

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


# ---------------------------------------------------------------- 文档契约


def test_README列出全部内置榜单() -> None:
    """榜单清单不能有手抄副本：README 的表格必须与 BOARDS 逐项对齐。

    新增一榜只改 gold.py 时，query 的候选项会自动跟着变，README 却不会——
    读文档的人于是查不到新榜。这条断言把那处静默漂移改成显式失败。
    """
    readme = (Path(__file__).resolve().parents[1] / "README.md").read_text(encoding="utf-8")
    section = readme.split("内置榜单（", 1)[1].split("**两个下载口径", 1)[0]
    listed = set(re.findall(r"^\| `([a-z_]+)` \|", section, flags=re.MULTILINE))
    expected = set(BOARDS)

    assert listed == expected, (
        f"README 榜单清单与 BOARDS 不一致：缺 {sorted(expected - listed)}，"
        f"多 {sorted(listed - expected)}"
    )
