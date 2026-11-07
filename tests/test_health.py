"""体检流水：每次 doctor 一行，让「哪天开始不对劲」有据可查。"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from hf_data_crawl import health
from hf_data_crawl.doctor import Finding, diagnose
from hf_data_crawl.health import HEALTH_NAME, HealthRecord
from hf_data_crawl.store import DatasetStore

NOW = datetime(2026, 11, 10, 12, 0, 0)


def entries(*problems: str) -> list[Finding]:
    """造一组结论：先若干「有问题」，最后一条正常项。"""
    return [
        *(Finding(True, text) for text in problems),
        Finding(False, "某项正常"),
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

    assert entry.healthy
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
    assert health.streak(store, "断日") == 0


def test_连续次数只数最近的一段(tmp_path: Path) -> None:
    """「断日连续几天了」决定要不要动手，中间有一天没断就该归零。"""
    store = DatasetStore(tmp_path / "data")
    for offset, texts in enumerate(
        [("Raw 断日：11-05",), (), ("Raw 断日：11-07",), ("Raw 断日：11-08",)]
    ):
        health.record(store, entries(*texts), NOW + timedelta(days=offset))

    assert health.streak(store, "断日") == 2


def test_流水本身进体检而不算问题(tmp_path: Path) -> None:
    """写流水不该改变任何体检结论——它记的是结论，不是输入。"""
    store = DatasetStore(tmp_path / "data")
    before = [item.text for item in diagnose(store, NOW)]

    health.record(store, diagnose(store, NOW), NOW)

    assert [item.text for item in diagnose(store, NOW)] == before


def test_记录的是问题原文而不是序号(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    health.record(store, entries("层间勾稽：3 个数据集缺属性行"), NOW)

    assert health.read(store)[0].problems[0].startswith("层间勾稽")
    assert isinstance(health.read(store)[0], HealthRecord)
