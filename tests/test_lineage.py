"""派生戳记：Raw 被重写之后，必须能判断派生层是否还跟得上。"""

from __future__ import annotations

from datetime import date
from pathlib import Path

from hf_data_crawl import lineage
from hf_data_crawl.lineage import path_of
from hf_data_crawl.platform.store import DatasetStore

DAY = date(2026, 11, 7)
EARLIER = date(2026, 11, 6)


def put_raw(store: DatasetStore, day: date, names: tuple[str, ...] = ("part-1.json.gz",)) -> None:
    directory = store.raw_dir / f"dt={day.isoformat()}"
    directory.mkdir(parents=True, exist_ok=True)
    for name in names:
        (directory / name).write_bytes(b"x")


def test_同一批分块两次指纹相同(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY, ("part-1.json.gz", "part-2.json.gz"))

    assert lineage.source_fingerprint(store, DAY) == lineage.source_fingerprint(store, DAY)


def test_分块增减会改变指纹(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)
    before = lineage.source_fingerprint(store, DAY)

    put_raw(store, DAY, ("part-1.json.gz", "part-2.json.gz"))

    assert lineage.source_fingerprint(store, DAY) != before


def test_补抓批次也在指纹里(tmp_path: Path) -> None:
    """补抓批次会经 `bronze.day_frame` 直接补进 current 与观测。

    只盯 part/top 的话，重写卡片批次而派生没重跑，会留下「今天的 current 用了
    前天的补抓」，且没有任何痕迹。
    """
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)
    before = lineage.source_fingerprint(store, DAY)

    put_raw(store, DAY, ("part-1.json.gz", "card-1.json.gz"))

    assert lineage.source_fingerprint(store, DAY) != before


def test_归档分片被替换后指纹变化(tmp_path: Path) -> None:
    """归档分片是可重放输入，替换后必须判为派生滞后。"""
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY, ("part-archive-000001-aaaa.json.gz",))
    before = lineage.source_fingerprint(store, DAY)

    (store.raw_dir / f"dt={DAY.isoformat()}" / "part-archive-000001-aaaa.json.gz").unlink()
    put_raw(store, DAY, ("part-archive-000001-bbbb.json.gz",))

    assert lineage.source_fingerprint(store, DAY) != before


def test_没有分块的日期没有指纹(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")

    assert lineage.source_fingerprint(store, DAY) is None


def test_落戳记之后判为一致(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)
    lineage.stamp(store, DAY, bronze_rows=10, silver_rows=8, observations=6)

    status = lineage.compare(store, [DAY])[0]

    assert status.state == lineage.OK
    assert status.stamped is not None and status.stamped.bronze_rows == 10


def test_没有戳记按无凭证(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)

    status = lineage.compare(store, [DAY])[0]

    assert status.state == lineage.UNVERIFIED
    assert lineage.divergent(store, [DAY]) == [DAY]


def test_重写raw之后判为滞后(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)
    lineage.stamp(store, DAY, bronze_rows=10, silver_rows=8, observations=6)

    put_raw(store, DAY, ("part-1.json.gz", "part-2.json.gz"))

    status = lineage.compare(store, [DAY])[0]
    assert status.state == lineage.STALE
    assert lineage.divergent(store, [DAY]) == [DAY]


def test_restamp只换指纹不改行数(tmp_path: Path) -> None:
    """archive 是等价改写：分片换了名字，派生结果仍然有效。"""
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)
    original = lineage.stamp(store, DAY, bronze_rows=10, silver_rows=8, observations=6)
    assert original is not None

    put_raw(store, DAY, ("part-archive-000001-abcd.json.gz",))
    refreshed = lineage.restamp(store, DAY)

    assert refreshed is not None
    assert refreshed.source == lineage.source_fingerprint(store, DAY)
    assert refreshed.source != original.source
    assert (refreshed.bronze_rows, refreshed.silver_rows, refreshed.observations) == (10, 8, 6)
    assert refreshed.derived_at == original.derived_at
    assert lineage.compare(store, [DAY])[0].state == lineage.OK


def test_restamp没有旧戳记时什么都不做(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)

    assert lineage.restamp(store, DAY) is None
    assert lineage.read(store) == {}


def test_Raw已移出主目录的日子不参与比对(tmp_path: Path) -> None:
    """compact 之后无法重放，报「陈旧」只会给出一个执行不了的建议。"""
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)
    lineage.stamp(store, DAY, bronze_rows=1, silver_rows=1, observations=1)
    (store.raw_dir / f"dt={DAY.isoformat()}" / "part-1.json.gz").unlink()

    assert lineage.compare(store, [DAY]) == []
    assert lineage.divergent(store, [DAY]) == []


def test_档案损坏按无凭证处理而不崩(tmp_path: Path) -> None:
    """体检是只读诊断，不能因为一份坏档案自己变成故障点。"""
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)
    lineage.stamp(store, DAY, bronze_rows=1, silver_rows=1, observations=1)

    path_of(store).write_text("{ 坏档案", encoding="utf-8")

    assert lineage.read(store) == {}
    assert lineage.compare(store, [DAY])[0].state == lineage.UNVERIFIED


def test_版本不符按无凭证处理(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    put_raw(store, DAY)
    lineage.stamp(store, DAY, bronze_rows=1, silver_rows=1, observations=1)
    body = path_of(store).read_text(encoding="utf-8").replace('"version": 1', '"version": 99')
    path_of(store).write_text(body, encoding="utf-8")

    assert lineage.read(store) == {}


def test_多日各记各的(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    put_raw(store, EARLIER)
    put_raw(store, DAY)
    lineage.stamp(store, EARLIER, bronze_rows=1, silver_rows=1, observations=1)
    lineage.stamp(store, DAY, bronze_rows=2, silver_rows=2, observations=2)

    stamps = lineage.read(store)

    assert set(stamps) == {EARLIER, DAY}
    assert stamps[DAY].bronze_rows == 2
    assert [item.state for item in lineage.compare(store, [EARLIER, DAY])] == [
        lineage.OK,
        lineage.OK,
    ]
