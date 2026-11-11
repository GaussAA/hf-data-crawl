"""数据目录单写者锁：互斥、可诊断、进程死亡后自动释放，且命令层面真的用上了它。"""

from __future__ import annotations

import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from hf_data_crawl.ops.cli import main
from hf_data_crawl.platform.lock import LOCK_NAME, LockBusy, describe_holder, exclusive
from hf_data_crawl.platform.runs import read_runs
from hf_data_crawl.platform.store import DatasetStore


def test_同一目录第二把锁拿不到并说明占用者(tmp_path: Path) -> None:
    with exclusive(tmp_path, "collect"):
        with pytest.raises(LockBusy, match="collect"):
            with exclusive(tmp_path, "build"):
                pass


def test_释放后可以再次获取(tmp_path: Path) -> None:
    with exclusive(tmp_path, "collect"):
        pass

    with exclusive(tmp_path, "build"):
        assert "build" in describe_holder(tmp_path)


def test_异常退出也会释放(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError), exclusive(tmp_path, "collect"):
        raise RuntimeError("boom")

    with exclusive(tmp_path, "build"):
        pass


def test_不同目录的锁互不影响(tmp_path: Path) -> None:
    with exclusive(tmp_path / "a", "collect"), exclusive(tmp_path / "b", "collect"):
        pass


def test_另一个进程持有时本进程拿不到且进程结束后释放(tmp_path: Path) -> None:
    """跨进程才是锁的真实用途；进程被杀后系统自动释放，不会留下死锁文件。"""
    script = textwrap.dedent(
        """
        import sys, time
        from pathlib import Path
        from hf_data_crawl.platform.lock import exclusive
        with exclusive(Path(sys.argv[1]), "reconcile"):
            print("held", flush=True)
            time.sleep(60)
        """
    )
    holder = subprocess.Popen(
        [sys.executable, "-c", script, str(tmp_path)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert holder.stdout is not None
        assert holder.stdout.readline().strip() == "held"

        with pytest.raises(LockBusy, match="reconcile"):
            with exclusive(tmp_path, "build"):
                pass
    finally:
        holder.kill()
        holder.wait()

    # 被强杀的持有者没机会清理，锁文件还在，但锁会被系统收回。
    # Windows 在进程终止后约 10ms 才回收文件锁（实测 5 次均为 11~13ms），
    # 所以这里在有限时间内重试，而不是 wait() 返回就立刻要求拿到。
    assert (tmp_path / LOCK_NAME).exists()
    deadline = time.monotonic() + 5
    while True:
        try:
            with exclusive(tmp_path, "build"):
                break
        except LockBusy:
            if time.monotonic() > deadline:
                raise
            time.sleep(0.02)


def test_写命令拿不到锁时失败并留下运行记录(tmp_path: Path) -> None:
    root = tmp_path / "data"
    with exclusive(root, "reconcile"):
        code = main(["--data-dir", str(root), "build"])

    assert code == 1
    runs = read_runs(DatasetStore(root))
    assert [item["status"] for item in runs] == ["failed"]
    assert "LockBusy" in runs[0]["reason"]
    assert "reconcile" in runs[0]["reason"]


def test_只读命令不受锁影响(tmp_path: Path) -> None:
    root = tmp_path / "data"
    with exclusive(root, "reconcile"):
        # query 在没有数据时返回 1 并提示先采集，但不能因为锁而失败。
        code = main(["--data-dir", str(root), "query"])

    assert code == 1
    assert read_runs(DatasetStore(root)) == []


def test_docs在锁被占用时等不到就失败(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """docs 需要一致快照，撞上写命令时应先等；等不到再明确失败，不生成半新半旧的文件。"""
    from hf_data_crawl.ops import cli

    monkeypatch.setattr(cli, "SNAPSHOT_LOCK_WAIT", 0.0)
    root = tmp_path / "data"
    target = tmp_path / "DATA.md"

    with exclusive(root, "collect"):
        code = main(["--data-dir", str(root), "docs", "--out", str(target)])

    assert code == 1
    assert "docs 失败" in capsys.readouterr().out
    assert not target.exists()


def test_report在锁被占用时留下失败记录(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """report 同样要读一致快照；锁失败也照常写 runs，便于事后看到原因。"""
    from hf_data_crawl.ops import cli

    monkeypatch.setattr(cli, "SNAPSHOT_LOCK_WAIT", 0.0)
    root = tmp_path / "data"

    with exclusive(root, "collect"):
        code = main(["--data-dir", str(root), "report"])

    assert code == 1
    runs = read_runs(DatasetStore(root))
    assert [item["status"] for item in runs] == ["failed"]
    assert "LockBusy" in runs[0]["reason"]


# ---------------------------------------------------------------- 计划任务的等待


def test_不等待时只试一次就失败(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """人工命令：报错比干等更有用。"""
    from hf_data_crawl.platform import lock as lock_mod

    calls = {"n": 0}

    def always_busy(_handle: object) -> bool:
        calls["n"] += 1
        return False

    monkeypatch.setattr(lock_mod, "_try_lock", always_busy)

    with pytest.raises(lock_mod.LockBusy):
        with lock_mod.exclusive(tmp_path, "manual"):
            pass

    assert calls["n"] == 1


def test_给等待时会重试直到拿到(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """计划任务：撞上别的写命令时先等，而不是白丢一轮。

    2026-11-07 实测：collect 03:10 撞上仍在跑的 fetch-sizes，整轮采集作废。
    """
    from hf_data_crawl.platform import lock as lock_mod

    calls = {"n": 0}

    def busy_twice(_handle: object) -> bool:
        calls["n"] += 1
        return calls["n"] >= 3

    monkeypatch.setattr(lock_mod, "_try_lock", busy_twice)
    monkeypatch.setattr(lock_mod.time, "sleep", lambda _seconds: None)

    with lock_mod.exclusive(tmp_path, "scheduled", wait_seconds=60):
        pass

    assert calls["n"] == 3, "应当一直重试到拿到为止"


def test_等待超时后仍然失败(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """等到超时还没释放，必须如实失败，不能无限等下去。"""
    from hf_data_crawl.platform import lock as lock_mod

    monkeypatch.setattr(lock_mod, "_try_lock", lambda _handle: False)
    monkeypatch.setattr(lock_mod.time, "sleep", lambda _seconds: None)
    ticks = iter([0.0, 1.0, 100.0])
    monkeypatch.setattr(lock_mod.time, "monotonic", lambda: next(ticks, 100.0))

    with pytest.raises(lock_mod.LockBusy):
        with lock_mod.exclusive(tmp_path, "scheduled", wait_seconds=10):
            pass
