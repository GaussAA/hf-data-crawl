"""表登记表的契约测试。

登记表存在的理由，是让「一张表住哪、什么 schema、能否重建」只有一个答案。
这里的断言全部针对**漂移**——它不会让程序报错，只会让某处悄悄用上错的路径、
错的 schema，或者在重建时删掉不该删的东西。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from hf_data_crawl import bronze, gold
from hf_data_crawl.rebuild import _clear, clear_derived
from hf_data_crawl.store import DatasetStore
from hf_data_crawl.tables import TABLES, Origin, TableSpec, table

# ---------------------------------------------------------------- 登记完整性


def test_每张登记表的名字与键一致() -> None:
    """键名与 spec.name 分叉后，按名字取表会拿到另一张表。"""
    for key, spec in TABLES.items():
        assert spec.name == key, f"{key} 的 spec.name 是 {spec.name}"


def test_落盘路径互不重复() -> None:
    """两张表共用一个路径，后写的会静默覆盖前一张。"""
    paths = [spec.path for spec in TABLES.values()]
    assert len(paths) == len(set(paths))


def test_每张表的schema非空且列名唯一() -> None:
    for spec in TABLES.values():
        assert spec.schema, f"{spec.name} 没有列定义"
        assert len(spec.schema) == len(set(spec.schema)), f"{spec.name} 有重复列名"


def test_分区表的路径模板带日期占位() -> None:
    for spec in TABLES.values():
        if not spec.partitioned:
            continue
        assert "{dt}" in spec.path
        assert spec.relative("2026-11-04").endswith("dt=2026-11-04" + "/" + spec.filename)


def test_非分区表不需要日期() -> None:
    for spec in TABLES.values():
        if spec.partitioned:
            continue
        assert spec.relative() == spec.path


def test_未登记的表名立刻报错而非拼出错误路径() -> None:
    with pytest.raises(KeyError):
        table("不存在的表")


def test_网络来源的表只有规模表一张() -> None:
    """NETWORK 意味着「删了就得重抓」。多一张就该重新审视它是否真的不可重算。"""
    network = [spec.name for spec in TABLES.values() if spec.origin is Origin.NETWORK]
    assert network == ["size"]


def test_当前态与观测属于运行态而非裸数据() -> None:
    assert table("current").origin is Origin.STATE
    assert table("observations").origin is Origin.STATE


# ---------------------------------------------------------------- 消费者一致


def test_视图引用的表都已登记() -> None:
    """视图名单里出现未登记的表，等于又回到硬编码的老路。"""
    for view, name in gold._VIEWS:  # noqa: SLF001 - 契约测试需要看这份名单
        assert name in TABLES, f"视图 {view} 指向未登记的表 {name}"


def test_视图路径由登记表生成() -> None:
    spec = table("bronze")
    assert spec.glob() == "bronze/dt=*/bronze.parquet"


def test_重建只跳过网络来源的表(tmp_path: Path) -> None:
    """规模表删了要重抓两万条，重建必须放过它；其余一律清除重算。"""
    store = DatasetStore(tmp_path / "data")
    for spec in TABLES.values():
        target = (
            store.root / spec.relative("2026-11-04") if spec.partitioned else store.root / spec.path
        )
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"placeholder")

    clear_derived(store)

    surviving = [
        spec.name
        for spec in TABLES.values()
        if (
            store.root / spec.relative("2026-11-04") if spec.partitioned else store.root / spec.path
        ).exists()
    ]
    assert surviving == ["size"]


def test_清除分区表会连整个分区目录一起删(tmp_path: Path) -> None:
    """只删单个 parquet 会留下空的 dt 目录，下次 glob 仍把它当成有效分区。"""
    store = DatasetStore(tmp_path / "data")
    partition = store.root / "bronze" / "dt=2026-11-04"
    partition.mkdir(parents=True)
    (partition / "bronze.parquet").write_bytes(b"x")

    _clear(store, table("bronze"))

    assert not (store.root / "bronze").exists()


def test_登记表的schema与实际落盘列一致(tmp_path: Path) -> None:
    """登记了却拉不出这些列，等于表身份是假的。"""
    store = DatasetStore(tmp_path / "data")
    record = {
        "id": "acme/x",
        "author": "acme",
        "sha": "s",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "downloads": 1,
        "downloadsAllTime": 2,
        "likes": 3,
        "trendingScore": 4,
        "gated": False,
        "private": False,
        "disabled": False,
        "mainSize": 5,
        "xetEnabled": True,
        "resourceGroup": None,
        "tags": [],
        "description": "d",
    }
    store.write_raw(snapshot_date=date(2026, 11, 4), page_index=1, records=[record], meta={})
    bronze.build(store, date(2026, 11, 4))

    frame = store.read_bronze(date(2026, 11, 4))

    assert list(frame.columns) == list(table("bronze").schema)


@pytest.mark.parametrize("name", sorted(TABLES))
def test_每张表都能构造出空帧(name: str) -> None:
    """空库时读表不报错，是「刚装好就能连上」的前提。"""
    spec: TableSpec = table(name)
    empty = pl.DataFrame(schema=spec.schema)
    assert empty.height == 0
    assert list(empty.columns) == list(spec.schema)


# ---------------------------------------------------------------- 页面地址


def test_每张表都登记了url列() -> None:
    """翻表时拿到 id 就能点开，不必先想起「要 join 哪个视图」。"""
    for name, spec in TABLES.items():
        assert "url" in spec.schema, f"{name} 缺 url 列"


def test_url由id严格拼出() -> None:
    from hf_data_crawl.tables import DATASET_URL, url_for

    assert url_for("acme/x") == DATASET_URL + "acme/x"
    assert url_for("a/b-c_d") == "https://huggingface.co/datasets/a/b-c_d"


def test_落盘后url与实际id对得上(tmp_path: Path) -> None:
    """光有列不够，值必须真能拼对——这是「点开就是那个数据集」的前提。"""
    from hf_data_crawl.tables import DATASET_URL

    store = DatasetStore(tmp_path / "data")
    store.write_raw(
        snapshot_date=date(2026, 11, 4),
        page_index=1,
        records=[{"id": "acme/x", "author": "acme", "sha": "s"}],
        meta={},
    )
    from hf_data_crawl.bronze import build as build_bronze

    build_bronze(store, date(2026, 11, 4))

    row = store.read_bronze(date(2026, 11, 4)).row(0, named=True)

    assert row["url"] == f"{DATASET_URL}acme/x"
