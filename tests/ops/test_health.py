"""体检流水：每次 doctor 一行，让「哪天开始不对劲」有据可查。"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from hf_data_crawl.ops import health
from hf_data_crawl.ops.doctor import Finding, diagnose
from hf_data_crawl.ops.health import HEALTH_NAME
from hf_data_crawl.platform.store import DatasetStore

NOW = datetime(2026, 11, 10, 12, 0, 0)


def entries(*problems: str, code: str = "raw_gaps") -> list[Finding]:
    """造一组结论：先若干「有问题」，最后一条正常项。

    必须带 `code`：`streak` 现在按检查项标识数，不按文案子串。测试若还按文案造、
    按文案数，就等于在测一条已经不存在的实现——它会一直绿，而真实现早已换掉。
    """
    return [
        *(Finding(True, text, code=code) for text in problems),
        Finding(False, "某项正常", code="storage"),
    ]


def test_写入一条后能读回(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")

    health.record(store, entries("Raw 断日：2026-11-06"), NOW)

    history = health.read(store)
    assert len(history) == 1
    assert history[0].problems == ("Raw 断日：2026-11-06",)
    assert history[0].total == 2


def test_全部正常时不记问题(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")

    entry = health.record(store, entries(), NOW)

    assert entry.problems == ()
    assert health.read(store)[0].problems == ()


def test_按时间升序累积(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")

    health.record(store, entries("甲"), NOW)
    health.record(store, entries("乙"), NOW + timedelta(hours=9))

    assert [item.problems for item in health.read(store)] == [("甲",), ("乙",)]


def test_超过上限只留最近的(tmp_path: Path) -> None:
    """上限的意义与 runs/ 相同：防止目录随计划任务无限增长。"""
    store = DatasetStore(tmp_path / "data")
    total = health.HEALTH_KEEP + 5
    for index in range(total):
        health.record(store, entries(f"第 {index} 次"), NOW)

    history = health.read(store)
    assert len(history) == health.HEALTH_KEEP
    assert history[-1].problems == (f"第 {total - 1} 次",)


def test_坏行跳过不拖垮读取(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    health.record(store, entries("真问题"), NOW)
    target = store.state_dir / HEALTH_NAME
    target.write_text(
        '{"at": "2026-11-10T12:00:00", "total": 1, "problems": ["真问题"]}\n'
        "{ 坏行\n"
        '{"at": "不是时间"}\n',
        encoding="utf-8",
    )

    history = health.read(store)

    assert len(history) == 1 and history[0].problems == ("真问题",)


def test_没有流水时返回空(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")

    assert health.read(store) == []
    assert health.streak(store, "raw_gaps") == 0


def test_连续次数只数最近的一段(tmp_path: Path) -> None:
    """「断日连续几天了」决定要不要动手，中间有一天没断就该归零。

    按检查项标识数，不按文案：中间那天文案里没有「断日」三个字也该归零，
    而文案换成别的说法也不该让计数断掉。
    """
    store = DatasetStore(tmp_path / "data")
    for offset, texts in enumerate(
        [("Raw 断日：11-05",), (), ("Raw 断日：11-07",), ("Raw 断日：11-08",)]
    ):
        health.record(store, entries(*texts), NOW + timedelta(days=offset))

    assert health.streak(store, "raw_gaps") == 2


def test_降级体检不清零连续天数(tmp_path: Path) -> None:
    """降级轮不写任何 code——它既不证明红，也不证明恢复。

    原先 `streak` 遇到缺 code 直接 break，于是撞一次写锁就把「已连续 3 天」变成 0：
    `sync_alert` 的「（已连续 N 天）」后缀消失，读起来像刚坏，而那一天根本没读成。
    """
    store = DatasetStore(tmp_path / "data")
    for offset in range(3):
        health.record(store, entries("Raw 断日"), NOW + timedelta(days=offset))
    health.record(store, [], NOW + timedelta(days=3), snapshot=False)

    assert health.streak(store, "raw_gaps") == 3


def test_连续次数不受文案改动影响(tmp_path: Path) -> None:
    """同一检查项换了说法，仍该算作同一件事。

    这是按 `code` 计数的全部理由：原先用子串匹配，文案里混进日期、或把
    「Raw 断日」改成「Raw 缺日」，计数就静默归零——归零还不报错，只让人以为
    「今天突然好了」。
    """
    store = DatasetStore(tmp_path / "data")
    health.record(store, entries("Raw 断日：2026-11-05"), NOW)
    health.record(store, entries("Raw 缺日（措辞已改）：2026-11-06"), NOW + timedelta(days=1))

    assert health.streak(store, "raw_gaps") == 2


def test_流水本身进体检而不算问题(tmp_path: Path) -> None:
    """写流水不该改变任何体检结论——它记的是结论，不是输入。"""
    store = DatasetStore(tmp_path / "data")
    before = [item.text for item in diagnose(store, NOW)]

    health.record(store, diagnose(store, NOW), NOW)

    assert [item.text for item in diagnose(store, NOW)] == before


def test_记录快照标记且旧记录读作一致(tmp_path: Path) -> None:
    """非快照体检要留痕；旧流水没有该字段，不能因此判成非快照。"""
    store = DatasetStore(tmp_path / "data")

    health.record(store, entries(), NOW, snapshot=False)
    assert health.read(store)[0].snapshot is False

    target = store.state_dir / HEALTH_NAME
    target.write_text(
        '{"at": "2026-11-09T12:00:00", "total": 1, "problems": []}\n', encoding="utf-8"
    )
    assert health.read(store)[0].snapshot is True


def test_记录的是问题原文而不是序号(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    health.record(store, entries("层间勾稽：3 个数据集缺属性行"), NOW)

    assert health.read(store)[0].problems[0].startswith("层间勾稽")


def test_并发写流水不丢记录(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """两个 doctor 各读同一份旧历史、各写一行，后写者会静默抹掉先写者。"""
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    original_write = health.write_text

    def slow_write(path: Path, content: str) -> None:
        time.sleep(0.05)  # 放大「读过旧历史 → 写回」之间的窗口
        original_write(path, content)

    monkeypatch.setattr(health, "write_text", slow_write)
    store = DatasetStore(tmp_path / "data")
    barrier = threading.Barrier(2)

    def writer(text: str) -> None:
        barrier.wait(timeout=5)
        health.record(store, entries(text), NOW)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(writer, text) for text in ("甲", "乙")]
        for future in futures:
            future.result()

    problems = {item.problems[0] for item in health.read(store) if item.problems}
    assert problems == {"甲", "乙"}


def test_流水小锁不与主数据锁相争(tmp_path: Path) -> None:
    """降级的 doctor 拿不到主锁也照常留痕；若误用主锁，这里会直接撞锁。"""
    from hf_data_crawl.platform.lock import exclusive

    store = DatasetStore(tmp_path / "data")

    with exclusive(store.root, "collect"):
        health.record(store, entries("甲"), NOW)

    assert len(health.read(store)) == 1


def test_连续天数按自然日不按体检次数(tmp_path: Path) -> None:
    """同一天跑三次体检，只算一天。

    流水是一行一次 `doctor`，而 `doctor` 一天可能跑好几次：改完代码验一次、
    排障再跑一次。按次数数会把「红了一天」说成「红了三天」，而那个数是用来
    催人动手的，虚高比不报更坏。
    """
    store = DatasetStore(tmp_path / "data")
    for hours in (0, 2, 5):
        health.record(store, entries("Raw 断日"), NOW + timedelta(hours=hours))

    assert health.streak(store, "raw_gaps") == 1
