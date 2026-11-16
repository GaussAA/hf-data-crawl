"""命令异常要留下现场：台账只记结论，日志里必须有堆栈。

`_execute` 的 catch-all 是架构选择（任何异常都要留一条 failed 台账），
但不能因此把 TypeError 之类的编程错误压成一行类型名，让排障无从下手。
"""

from __future__ import annotations

import contextlib
import json
from collections.abc import Iterator
from datetime import date
from pathlib import Path

import pytest

from hf_data_crawl.ops import cli
from hf_data_crawl.ops.cli import _execute
from hf_data_crawl.platform.lock import LockBusy
from hf_data_crawl.platform.runs import Outcome
from hf_data_crawl.platform.store import DatasetStore


def test_未捕获异常打印完整堆栈且不污染台账(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """只打印类型名时，编程错误看起来和「网络断了」一样，且定位不到行号。"""
    store = DatasetStore(tmp_path / "data")

    def boom() -> Outcome:
        raise TypeError("故意的编程错误")

    code = _execute(store, "collect", boom)

    assert code == 1
    captured = capsys.readouterr()
    assert "Traceback" in captured.err
    assert "TypeError: 故意的编程错误" in captured.err
    runs = list((store.root / "runs").glob("*-collect.json"))
    assert len(runs) == 1
    body = json.loads(runs[0].read_text(encoding="utf-8"))
    assert body["status"] == "failed"
    assert "Traceback" not in json.dumps(body, ensure_ascii=False)


def test_重放失败记下带日期的告警但不抛出(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """一个坏日期不该阻塞其余写命令，但「静默跳过一天」不能没有现场。"""
    from hf_data_crawl.pipeline import recovery

    def broken(*args: object, **kwargs: object) -> None:
        raise RuntimeError("Raw 坏了")

    monkeypatch.setattr(recovery, "rebuild_from_raw", broken)
    store = DatasetStore(tmp_path / "data")
    store.raw_dir.mkdir(parents=True, exist_ok=True)
    store.state_dir.mkdir(parents=True, exist_ok=True)

    recovery._rederive(store, date(2026, 11, 7))

    out = capsys.readouterr().out
    assert "2026-11-07" in out
    assert "RuntimeError" in out
    assert "保留标记" in out


def test_启动恢复必须持有数据锁(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """恢复会写 current / lineage / 派生分区，不能是全项目唯一绕过锁的写路径。"""
    calls: list[str] = []
    original = cli.exclusive

    @contextlib.contextmanager
    def spy(root: Path, command: str, wait_seconds: float = 0.0) -> Iterator[None]:
        calls.append(command)
        with original(root, command, wait_seconds):
            yield

    monkeypatch.setattr(cli, "exclusive", spy)
    monkeypatch.setattr(cli, "check_and_recover", lambda _store: [])

    cli.main(["--data-dir", str(tmp_path / "data"), "prune"])

    assert "recovery" in calls


def test_恢复锁被占用时跳过而不无痕崩溃(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """这段在 `_execute` 之外，LockBusy 不自己兜住就是没有台账、没有堆栈的崩溃。"""
    original = cli.exclusive
    probed: list[bool] = []

    @contextlib.contextmanager
    def busy(root: Path, command: str, wait_seconds: float = 0.0) -> Iterator[None]:
        if command == "recovery":
            raise LockBusy("另一个写命令正持有数据锁")
        with original(root, command, wait_seconds):
            yield

    def should_not_run(_store: DatasetStore) -> list[date]:
        probed.append(True)
        return []

    monkeypatch.setattr(cli, "exclusive", busy)
    monkeypatch.setattr(cli, "check_and_recover", should_not_run)

    cli.main(["--data-dir", str(tmp_path / "data"), "prune"])

    assert "跳过启动恢复" in capsys.readouterr().out
    assert probed == [], "拿不到锁就不该在锁外做恢复"


def test_锁被占只报一句不打堆栈(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """锁被占用是预期状态：人要读「谁占着」，不是一段 traceback。

    原先它与真正的崩溃走同一条 catch-all：既打堆栈，又把同一条原因打印两遍
    （「失败：…」+「未完成：…」），两处措辞还不一样。
    """
    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)

    def busy() -> Outcome:
        raise LockBusy("数据目录正被另一个写命令占用：reconcile（pid 42）")

    code = _execute(store, "build", busy)

    output = capsys.readouterr()
    assert code == 1
    assert "reconcile（pid 42）" in output.out
    assert "Traceback" not in output.err
    assert output.out.count("未完成") == 1, "同一条原因不该打印两遍"


def test_空库导出给出可照做的失败而不是堆栈(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """空库是预期状态：要读到「先跑 collect」，而不是 GoldNotReady 的堆栈。"""
    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)

    code = cli.main(["--data-dir", str(store.root), "export-wide"])

    output = capsys.readouterr()
    assert code == 1
    assert "collect" in output.out
    assert "Traceback" not in output.err
