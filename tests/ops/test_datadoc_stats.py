"""数据说明文档里那几个数字是怎么算出来的。

`test_docs.py` 验的是「文档与结构对得上」；这里验的是「数字本身对不对」。
两者失败代价不同：结构漏了看得见（少一节），数字错了看不见——文档照样通顺，
只是它说的是错的，而读它的人无从核对。

只测纯函数，不跑整篇 `render`：一个 700 行的文档比对不出「这行数字为什么是 5」。
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from hf_data_crawl.ops import datadoc
from hf_data_crawl.platform.tables import TABLES


def parquet(path: Path, rows: int) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"x": list(range(rows))}).write_parquet(path)
    return path


@pytest.mark.parametrize(
    ("size", "expected"),
    [
        (0, "0 B"),
        (512, "512 B"),
        (2048, "2.0 KB"),
        (5 * 1024 * 1024, "5.0 MB"),
        (3 * 1024**3, "3.0 GB"),
        (2 * 1024**4, "2.0 TB"),
        (1024**5, "1024.0 TB"),
    ],
)
def test_体积按人类可读换算(size: int, expected: str) -> None:
    """文档里写「1.2 GB」而不是「1288490188」，读的人才不用自己换算。

    边界取在 1024 而不是 1000：这些数来自字节数，换算是二进制口径。
    """
    assert datadoc._human_bytes(size) == expected


def test_分区表统计带分区数(tmp_path: Path) -> None:
    """分区数不说，「1200 万行」会被误读成一天的量。"""
    root = tmp_path / "data"
    parquet(root / "bronze" / "dt=2026-11-01" / "bronze.parquet", 3)
    parquet(root / "bronze" / "dt=2026-11-02" / "bronze.parquet", 2)

    text = datadoc._table_stats(root, TABLES["bronze"])

    assert "2 个分区" in text
    assert "5 行" in text, "行数要跨分区求和，不能只报第一个文件"


def test_非分区表没有文件时说尚无数据(tmp_path: Path) -> None:
    """「0 行」与「还没有这张表」是两件事：前者是跑过但空，后者是没跑。"""
    assert datadoc._table_stats(tmp_path / "data", TABLES["current"]) == "尚无数据"


def test_坏掉的文件按零行计而不是让整篇文档失败(tmp_path: Path) -> None:
    """一个 parquet 损坏就整篇生不出来，代价远大于那一格数字不准。"""
    root = tmp_path / "data"
    broken = root / "bronze" / "dt=2026-11-01" / "bronze.parquet"
    broken.parent.mkdir(parents=True)
    broken.write_bytes(b"not a parquet")

    text = datadoc._table_stats(root, TABLES["bronze"])

    assert "0 行" in text
    assert "1 个分区" in text, "分区数按目录算，与文件能不能读无关"


def test_未知类型原样输出而不是猜一个() -> None:
    """表里出现没登记的类型时，显示原文比显示一个猜的中文名安全。"""
    assert datadoc._type_name(pl.Utf8) == "字符串"
    assert datadoc._type_name(pl.Int32) == "Int32"


# ---------------------------------------------------------------- 缺说明能不能被抓出来


def test_缺说明的列会被点名(monkeypatch: pytest.MonkeyPatch) -> None:
    """`missing_docs` 若恒返回空表，「没有缺少说明的字段」那条断言就成了自证。"""
    real = datadoc.describe
    monkeypatch.setattr(
        datadoc,
        "describe",
        lambda table, column: None if column == "downloads" else real(table, column),
    )

    gaps = datadoc.missing_docs()

    assert any(column == "downloads" for _table, column in gaps), gaps


# ---------------------------------------------------------------- 读不到数据时的降级


def coverage_root(
    tmp_path: Path, frame: pl.DataFrame | None, *, raw_text: str | None = None
) -> Path:
    """按 `TABLES["current"]` 的真实路径造一份 parquet（或一份坏文件）。"""
    root = tmp_path / "data"
    target = root / TABLES["current"].path
    target.parent.mkdir(parents=True, exist_ok=True)
    if raw_text is not None:
        target.write_text(raw_text, encoding="utf-8")
    elif frame is not None:
        frame.write_parquet(target)
    return root


def test_没有文件时覆盖率写占位符(tmp_path: Path) -> None:
    """装好就跑的第一步命令，不能因为还没采集就抛错。"""
    assert datadoc._coverage(tmp_path / "data", "current", "id") == "—"


def test_空表覆盖率也写占位符(tmp_path: Path) -> None:
    """0 行时算不出占比，写 `0%` 会读成「全空」，写 `—` 才是「还无从谈起」。"""
    root = coverage_root(tmp_path, pl.DataFrame(schema=dict(TABLES["current"].schema)))

    assert datadoc._coverage(root, "current", "id") == "—"


def test_读不出文件时覆盖率不炸(tmp_path: Path) -> None:
    """半截文件、换了编码、被别的东西占着——都只该让这一格变成 `—`。"""
    root = coverage_root(tmp_path, None, raw_text="这不是 parquet")

    assert datadoc._coverage(root, "current", "id") == "—"


def test_覆盖率按非空占比算(tmp_path: Path) -> None:
    frame = pl.DataFrame(
        [{"id": "a/1"}, {"id": None}],
        schema=dict(TABLES["current"].schema),
        strict=False,
    )
    root = coverage_root(tmp_path, frame)

    assert datadoc._coverage(root, "current", "id") == "50%"


def test_底库还没建时扩展覆盖率是空字典(tmp_path: Path) -> None:
    """`current.parquet` 不在就没法算；返回空字典让文档写占位符。"""
    assert datadoc._extended_coverage(tmp_path / "data") == {}


def test_扩展覆盖率走视图并关掉连接(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """散在四张表的扩展字段只有视图拼得全；连接用完必须关，否则临时库留在盘上。"""
    root = tmp_path / "data"
    (root / "state").mkdir(parents=True)
    (root / "state" / "current.parquet").write_bytes(b"")
    seen: dict[str, object] = {}

    class FakeCatalog:
        def __init__(self, store: object) -> None:
            seen["store"] = store

        def close(self) -> None:
            seen["closed"] = True

    monkeypatch.setattr(datadoc, "GoldCatalog", FakeCatalog)
    monkeypatch.setattr(
        datadoc, "column_coverage", lambda _catalog, columns: dict.fromkeys(columns, 0.5)
    )

    result = datadoc._extended_coverage(root)

    assert result and set(result.values()) == {0.5}
    assert seen["closed"] is True


# ---------------------------------------------------------------- 目录树旁注


def test_目录不存在时写空(tmp_path: Path) -> None:
    assert datadoc._dir_summary(tmp_path / "data", "logs") == "空"


def test_目录里只有子目录时也写空(tmp_path: Path) -> None:
    """`rglob` 只数文件：只有目录没有文件，等于还没写进任何东西。"""
    (tmp_path / "data" / "logs" / "nested").mkdir(parents=True)

    assert datadoc._dir_summary(tmp_path / "data", "logs") == "空"


def test_目录旁注给出文件数与体积(tmp_path: Path) -> None:
    target = tmp_path / "data" / "logs"
    target.mkdir(parents=True)
    for name in ("a.log", "b.log"):
        (target / name).write_bytes(b"x" * 1024)

    text = datadoc._dir_summary(tmp_path / "data", "logs")

    assert text.startswith("2 个文件 · ")
    assert text.endswith("KB")


# ---------------------------------------------------------------- 单文件表与覆盖率写法


def test_单文件表的统计直接读那个文件(tmp_path: Path) -> None:
    """非分区表没有 `{dt}` 可展开：路径就是文件本身，存在则算行数与体积。"""
    spec = TABLES["current"]
    parquet(tmp_path / "data" / spec.path, 3)

    text = datadoc._table_stats(tmp_path / "data", spec)

    assert "3 行" in text
    assert "分区" not in text, "单文件表不该冒出分区数"


def test_覆盖率写法与全项目统一格式同源() -> None:
    """有底库时走 `share_text`，没有时写 `—`：两件事读者必须能分清。"""
    assert datadoc._share_text(None) == "—"
    assert datadoc._share_text(0.5) == "50%"
    assert datadoc._share_text(0.0005) == "<0.1%"
