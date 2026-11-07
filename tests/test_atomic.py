"""原子写的行为测试。

真正要守的不是「能写进去」，而是**写不进去时目标文件不会变成半截**。
散在六个写点里的纪律，只要有一处偷懒，用户就会拿到一张坏掉的页面。
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

from hf_data_crawl import atomic
from hf_data_crawl.atomic import replace, write_bytes, write_csv, write_parquet, write_text


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


# --------------------------------------------------------------- 瞬态占用重试
#
# Windows 不允许替换正被另一个句柄打开的文件，而 Defender 实时扫描恰好在文件
# 刚写完时短暂持有它。2026-11-08 实测 reconcile 跑到第 10 分钟被 WinError 5
# 拒绝，整轮 30 分钟作废。这类占用是毫秒级的，重试即可；真占用必须如实失败。


def _flaky_replace(monkeypatch: pytest.MonkeyPatch, failures: int) -> list[int]:
    """让前 `failures` 次 os.replace 抛 PermissionError，记录尝试次数。"""
    real = os.replace
    calls: list[int] = []

    def fake(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> None:
        calls.append(1)
        if len(calls) <= failures:
            raise PermissionError(13, "拒绝访问")
        real(source, target)

    monkeypatch.setattr(atomic.os, "replace", fake)
    return calls


def test_瞬态占用重试后成功(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = _flaky_replace(monkeypatch, failures=2)
    target = tmp_path / "x.txt"

    write_text(target, "内容")

    assert target.read_text(encoding="utf-8") == "内容"
    assert len(calls) == 3
    assert not list(tmp_path.glob("*.tmp"))


def test_持续占用如实失败(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """重试不是无限容忍：真被别的进程锁住时要立刻把真因报出来。"""
    target = tmp_path / "x.txt"
    write_text(target, "旧内容")
    calls = _flaky_replace(monkeypatch, failures=99)

    with pytest.raises(PermissionError):
        replace(tmp_path / "x.txt.tmp", target)

    assert len(calls) == len(atomic.REPLACE_BACKOFF) + 1


def test_其他错误不重试(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """磁盘满、路径不存在这类失败重试没有意义，只会让真因迟到近一秒。"""
    calls: list[int] = []

    def fake(source: str | os.PathLike[str], target: str | os.PathLike[str]) -> None:
        calls.append(1)
        raise FileNotFoundError("没有这个文件")

    monkeypatch.setattr(atomic.os, "replace", fake)

    with pytest.raises(FileNotFoundError):
        replace(tmp_path / "x.txt.tmp", tmp_path / "x.txt")

    assert len(calls) == 1
