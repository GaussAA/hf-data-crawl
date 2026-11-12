"""断点续爬落盘的行为测试：原子写、重读、损坏容错。"""

from __future__ import annotations

import json
from pathlib import Path

from hf_data_crawl.acquire.hf.checkpoint import FileCheckpoint


def test_保存后可重读到同一游标(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.json"
    checkpoint = FileCheckpoint(path)
    assert checkpoint.cursor is None

    checkpoint.save("abc==")
    assert FileCheckpoint(path).cursor == "abc=="


def test_写入后不残留临时文件(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.json"

    FileCheckpoint(path).save("c1")

    assert sorted(p.name for p in tmp_path.iterdir()) == ["checkpoint.json"]


def test_游标文件损坏时视为无游标而非抛错(tmp_path: Path) -> None:
    """游标可重建，不值得为它中断整轮采集。"""
    path = tmp_path / "checkpoint.json"
    path.write_text("{半截 json", encoding="utf-8")

    assert FileCheckpoint(path).cursor is None


def test_终页游标为空时落盘为null(tmp_path: Path) -> None:
    path = tmp_path / "checkpoint.json"

    FileCheckpoint(path).save(None)

    assert json.loads(path.read_text(encoding="utf-8")) == {"cursor": None}
