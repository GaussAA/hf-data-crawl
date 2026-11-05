"""原子写：先写同目录临时文件，再替换目标。

为什么统一
----------
崩溃或中断时不能留下半截文件。半张 HTML 页面比旧页面更糟——用户以为报告坏了；
半截 JSON 会让下一次读取直接抛错，而错误信息指向的是读取方，不是写入方。

这条纪律原本在六个写点里执行了三次（parquet、raw、checkpoint），遗漏的恰恰是
面向用户的那三个：榜单页、运行记录、对账清单。纪律散在各处时，是否执行取决于
写那段代码的人当天想没想到。

临时文件必须与目标同目录：`os.replace` 的原子性依赖同一文件系统，跨盘替换会
退化成「复制 + 删除」两步，中途崩溃照样留下半截。
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl


def _temp_for(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".tmp")


def write_bytes(path: Path, payload: bytes) -> None:
    """原子写入字节。父目录不存在时自动创建。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _temp_for(path)
    temp.write_bytes(payload)
    os.replace(temp, path)


def write_text(path: Path, content: str) -> None:
    """原子写入文本（UTF-8）。"""
    write_bytes(path, content.encode("utf-8"))


def write_parquet(frame: pl.DataFrame, path: Path) -> None:
    """原子写入 parquet。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _temp_for(path)
    frame.write_parquet(temp)
    os.replace(temp, path)


def write_csv(frame: pl.DataFrame, path: Path) -> None:
    """原子写入 CSV。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _temp_for(path)
    frame.write_csv(temp)
    os.replace(temp, path)
