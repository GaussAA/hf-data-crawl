"""数据目录的单写者锁。

为什么需要
----------
原子写只防「半截文件」，防不了「丢更新」：`current.parquet` 是整体读、改、写，
两个写命令交错时后写的一方会把先写的更新整个盖掉，而且不报任何错。
计划任务的 `multipleInstances=IgnoreNew` 只防同一个任务重入，管不到
collect、reconcile、手动 build 之间的互斥。

做法
----
对 `<data>/.lock` 的第 0 字节加操作系统级独占锁：拿不到时，默认立刻失败并说明
谁占着；给了 `wait_seconds` 就先等一会儿再试。进程退出（包括被杀、断电重启）
时系统自动释放，所以不存在「残留锁文件导致永远起不来」。占用者信息写在第 0
字节之后，供诊断读取；被锁的只有第 0 字节，读信息不受影响。

为什么要有等待
--------------
立刻失败对人工命令是对的——报错比干等更有用。但对计划任务太脆：两个任务
时间重叠一次，就白丢一轮。2026-11-07 实测：collect 03:10 撞上仍在跑的
fetch-sizes，整轮采集作废，那天少了一个观测点。
"""

from __future__ import annotations

import contextlib
import json
import os
import sys
import time
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


#: 等锁时的轮询间隔（秒）。太短空耗 CPU，太长让计划任务白等。
LOCK_POLL_SECONDS = 0.5


def _open_lock_file(root: Path) -> IO[bytes] | None:
    """打开锁文件；拿不到写权限返回 None。

    Windows 上「别人持锁」并不必然阻止打开写句柄（临时目录里跨进程实测三种模式
    都能开），但 2026-11-13 实测在本项目的 data 目录上确实发生了：一个卡住的对账
    持锁期间，`rb` 正常而 `r+b`/`a+b` 一律 PermissionError（60/60）。此时第二个
    进程连锁文件都打不开，于是既等不到锁、也拿不到 `LockBusy`——计划任务的 900
    秒等待与 doctor 的降级统统失效，命令直接以异常栈收场。

    打不开的原因究竟是共享模式冲突还是别的持有者，臣无法从不成功的 open 里分辨；
    但处置只有一种：当作「被占用」，交给统一的等待与报错逻辑。真正的目录权限或
    磁盘故障仍会在 `mkdir`、写入或 `describe_holder` 处暴露，不会被这里永久吞掉。
    """
    try:
        return (root / LOCK_NAME).open("a+b")
    except PermissionError:
        return None


@contextlib.contextmanager
def exclusive(root: Path, command: str, wait_seconds: float = 0.0) -> Iterator[None]:
    """持有数据目录的独占写锁；拿不到抛 `LockBusy`。

    `wait_seconds` 为 0（默认）时立刻失败，适合人工命令；计划任务给正数，
    撞上别的写命令时先等，等到就接着干。
    """
    root.mkdir(parents=True, exist_ok=True)
    deadline = time.monotonic() + max(0.0, wait_seconds)
    handle = _open_lock_file(root)
    while True:
        if handle is not None:
            # 第 0 字节必须存在，作为被锁的那一格；占用者信息写在它后面。
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            if _try_lock(handle):
                break
            handle.close()
            handle = None
        if time.monotonic() >= deadline:
            raise LockBusy(f"数据目录正被另一个写命令占用：{describe_holder(root)}")
        time.sleep(min(LOCK_POLL_SECONDS, max(0.0, deadline - time.monotonic())))
        handle = _open_lock_file(root)

    try:
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
