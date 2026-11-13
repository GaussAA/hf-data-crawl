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
import time
from pathlib import Path
from typing import Literal

import polars as pl

#: `os.replace` 遇到瞬态占用时的重试节奏（秒）。见 `replace` 的说明。
REPLACE_BACKOFF: tuple[float, ...] = (0.02, 0.06, 0.18)


def _temp_for(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".tmp")


def replace(temp: Path, target: Path) -> None:
    """`os.replace` 的重试版：只对**瞬态**占用重试，持续性占用如实失败。

    为什么需要：Windows 不允许替换「正被另一个句柄打开」的文件，而 Defender 的
    实时扫描与索引服务恰好在文件刚写完时短暂持有它。2026-11-08 实测：`reconcile`
    跑到第 10 分钟替换 `reconcile-checkpoint.json` 时被 WinError 5 拒绝，整轮
    30 分钟作废（游标尚可续跑，但那一轮的对账结论全丢）。同一文件此后手动替换
    立即成功——说明占用是毫秒级的，不是锁。

    只捕 `PermissionError`：其余失败（磁盘满、路径不存在）重试没有意义，应当
    立刻报出真因。三次退避合计约 0.26 秒，能覆盖扫描器的瞬时窗口，又不会让
    真正的错误迟到近一秒才浮现。
    """
    attempts = len(REPLACE_BACKOFF) + 1
    for attempt in range(attempts):
        try:
            os.replace(temp, target)
            return
        except PermissionError:
            if attempt == attempts - 1:
                raise
            time.sleep(REPLACE_BACKOFF[attempt])


def write_bytes(path: Path, payload: bytes) -> None:
    """原子写入字节。父目录不存在时自动创建。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _temp_for(path)
    temp.write_bytes(payload)
    replace(temp, path)


def write_text(path: Path, content: str) -> None:
    """原子写入文本（UTF-8）。"""
    write_bytes(path, content.encode("utf-8"))


def write_parquet(
    frame: pl.DataFrame,
    path: Path,
    *,
    compression: Literal["zstd", "lz4", "snappy", "gzip", "brotli", "uncompressed"] = "zstd",
    compression_level: int | None = None,
) -> None:
    """原子写入 parquet,默认 zstd 压缩。

    Args:
        frame: 要写入的 DataFrame
        path: 目标路径
        compression: 压缩格式,可选:
            - "zstd" (默认): 平衡压缩率(~5x)与速度,推荐用于归档
            - "lz4": 极快但压缩率低(~2x),适合临时文件
            - "snappy": 快但压缩率低(~2x),兼容性好
            - "gzip": 慢但压缩率高(~6x),兼容性最好
            - "uncompressed": 无压缩,仅用于测试
        compression_level: 压缩级别,None 表示默认:
            - zstd: 1-22 (默认 3)
            - gzip: 1-9 (默认 6)
            - 其他格式忽略此参数
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _temp_for(path)
    frame.write_parquet(temp, compression=compression, compression_level=compression_level)
    replace(temp, path)


def write_csv(frame: pl.DataFrame, path: Path) -> None:
    """原子写入 CSV。"""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = _temp_for(path)
    frame.write_csv(temp)
    replace(temp, path)
