"""断点续爬的落盘与内存实现。

checkpoint 只记一件事：下一页该从哪个游标开始。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Protocol

from ...platform.atomic import write_text


class Checkpoint(Protocol):
    """扫描进度的最小契约。"""

    @property
    def cursor(self) -> str | None: ...

    def save(self, cursor: str | None) -> None: ...


class MemoryCheckpoint:
    """进程内 checkpoint，供测试与一次性扫描使用。"""

    def __init__(self) -> None:
        self._cursor: str | None = None

    @property
    def cursor(self) -> str | None:
        return self._cursor

    def save(self, cursor: str | None) -> None:
        self._cursor = cursor


class FileCheckpoint:
    """文件 checkpoint，原子写：先写临时文件再整体替换，避免半行 JSON。"""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)
        self._cursor = self._read()

    @property
    def cursor(self) -> str | None:
        return self._cursor

    def save(self, cursor: str | None) -> None:
        write_text(self._path, json.dumps({"cursor": cursor}, ensure_ascii=False))
        self._cursor = cursor

    def _read(self) -> str | None:
        if not self._path.exists():
            return None
        try:
            payload = json.loads(self._path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            # 游标可重建（重跑一轮即可），不值得为此中断采集。
            return None
        cursor = payload.get("cursor")
        return cursor if isinstance(cursor, str) else None
