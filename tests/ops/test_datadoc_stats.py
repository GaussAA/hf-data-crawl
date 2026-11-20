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
