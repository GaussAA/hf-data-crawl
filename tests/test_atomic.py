"""原子写的行为测试。

真正要守的不是「能写进去」，而是**写不进去时目标文件不会变成半截**。
散在六个写点里的纪律，只要有一处偷懒，用户就会拿到一张坏掉的页面。
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from hf_data_crawl.atomic import write_bytes, write_csv, write_parquet, write_text


def test_自动创建父目录(tmp_path: Path) -> None:
    target = tmp_path / "a" / "b" / "c.txt"

    write_text(target, "内容")

    assert target.read_text(encoding="utf-8") == "内容"


def test_不留下临时文件(tmp_path: Path) -> None:
    target = tmp_path / "x.txt"

    write_text(target, "内容")

    assert not list(tmp_path.glob("*.tmp"))


def test_覆盖时旧内容不会残留(tmp_path: Path) -> None:
    """直接以写模式打开会先截断；若中途失败，旧内容也没了。"""
    target = tmp_path / "x.txt"
    write_text(target, "很长的一段旧内容" * 50)

    write_text(target, "短")

    assert target.read_text(encoding="utf-8") == "短"


def test_写入失败时旧文件不受损(tmp_path: Path) -> None:
    """这是原子写的全部意义：失败发生在替换之前，目标仍是完整的旧版本。"""
    target = tmp_path / "x.txt"
    write_text(target, "旧内容")

    with pytest.raises(TypeError):
        write_bytes(target, "不是字节")  # type: ignore[arg-type]

    assert target.read_text(encoding="utf-8") == "旧内容"
    assert not list(tmp_path.glob("*.tmp"))


def test_parquet往返(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "t.parquet"
    frame = pl.DataFrame({"id": ["a", "b"], "n": [1, 2]})

    write_parquet(frame, target)

    assert pl.read_parquet(target).equals(frame)


def test_csv往返(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "t.csv"
    frame = pl.DataFrame({"id": ["a", "b"], "n": [1, 2]})

    write_csv(frame, target)

    assert pl.read_csv(target).equals(frame)


def test_二进制原样写入(tmp_path: Path) -> None:
    target = tmp_path / "blob.bin"
    payload = bytes(range(256))

    write_bytes(target, payload)

    assert target.read_bytes() == payload
