"""数据目录的单写者锁。

为什么需要
----------
原子写只防「半截文件」，防不了「丢更新」：`current.parquet` 是整体读、改、写，
两个写命令交错时后写的一方会把先写的更新整个盖掉，而且不报任何错。
计划任务的 `multipleInstances=IgnoreNew` 只防同一个任务重入，管不到
collect、reconcile、手动 build 之间的互斥。

做法
----
对 `<data>/.lock` 的第 0 字节加操作系统级独占锁，非阻塞：拿不到就立刻失败并
说明谁占着。进程退出（包括被杀、断电重启）时系统自动释放，所以不存在
「残留锁文件导致永远起不来」。占用者信息写在第 0 字节之后，供诊断读取；
被锁的只有第 0 字节，读信息不受影响。
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
from collections.abc import Iterator
from datetime import datetime
from pathlib import Path
from typing import IO

LOCK_NAME = ".lock"

if sys.platform == "win32":
    import msvcrt

    def _try_lock(handle: IO[bytes]) -> bool:
        handle.seek(0)
        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError:
            return False
        return True

    def _unlock(handle: IO[bytes]) -> None:
        handle.seek(0)
        with contextlib.suppress(OSError):
            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)

else:
    import fcntl

    def _try_lock(handle: IO[bytes]) -> bool:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return False
        return True

    def _unlock(handle: IO[bytes]) -> None:
        with contextlib.suppress(OSError):
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


class LockBusy(RuntimeError):
    """另一个写命令正持有数据目录。"""


def describe_holder(root: Path) -> str:
    """读出当前占用者的说明；读不到就如实说不知道。"""
    try:
        with (root / LOCK_NAME).open("rb") as handle:
            handle.seek(1)
            info = json.loads(handle.read().decode("utf-8"))
        return f"{info['command']}（pid {info['pid']}，{info['since']} 起）"
    except (OSError, ValueError, KeyError):
        return "未知（占用者信息不可读）"


@contextlib.contextmanager
def exclusive(root: Path, command: str) -> Iterator[None]:
    """持有数据目录的独占写锁；拿不到抛 `LockBusy`。"""
    root.mkdir(parents=True, exist_ok=True)
    handle = (root / LOCK_NAME).open("a+b")
    try:
        # 第 0 字节必须存在，作为被锁的那一格；占用者信息写在它后面。
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"\0")
            handle.flush()
        if not _try_lock(handle):
            raise LockBusy(f"数据目录正被另一个写命令占用：{describe_holder(root)}")
        info = json.dumps(
            {"command": command, "pid": os.getpid(), "since": datetime.now().isoformat()},
            ensure_ascii=False,
        )
        handle.truncate(1)
        handle.write(info.encode("utf-8"))
        handle.flush()
        try:
            yield
        finally:
            handle.truncate(1)
            _unlock(handle)
    finally:
        handle.close()
