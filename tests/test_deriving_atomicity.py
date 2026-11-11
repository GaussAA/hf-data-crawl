"""派生原子性保护测试。

测试派生中标记文件机制是否正确：
- 标记文件创建和删除
- 超时检测
- 自动恢复
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from hf_data_crawl import raw
from hf_data_crawl.deriving import (
    STALE_THRESHOLD_SECONDS,
    DerivingFlag,
    check_and_recover,
    clear_deriving,
    flag_path,
    mark_deriving,
)
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.raw import CARD_LABEL


def test_标记文件创建(tmp_path: Path) -> None:
    """测试标记文件是否正确创建。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    mark_deriving(store, snapshot_date)

    path = flag_path(store, snapshot_date)
    assert path.exists()

    # 验证内容
    content = json.loads(path.read_text(encoding="utf-8"))
    assert content["snapshot_date"] == "2026-11-07"
    assert "started_at" in content

    # 验证时间戳格式
    started_at = datetime.fromisoformat(content["started_at"])
    assert started_at.tzinfo is not None  # 应该是 UTC


def test_标记文件删除(tmp_path: Path) -> None:
    """测试标记文件是否正确删除。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    mark_deriving(store, snapshot_date)
    path = flag_path(store, snapshot_date)
    assert path.exists()

    clear_deriving(store, snapshot_date)
    assert not path.exists()


def test_删除不存在的标记不报错(tmp_path: Path) -> None:
    """删除不存在的标记文件不应报错。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    # 直接删除，不应崩溃
    clear_deriving(store, snapshot_date)


def test_无标记时不触发恢复(tmp_path: Path) -> None:
    """没有标记文件时，不应触发恢复。"""
    store = DatasetStore(tmp_path)

    recovered = check_and_recover(store)
    assert recovered == []


def test_新标记不触发恢复(tmp_path: Path) -> None:
    """刚创建的标记（未超时）不应触发恢复。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    mark_deriving(store, snapshot_date)

    recovered = check_and_recover(store)
    assert recovered == []

    # 标记文件应仍然存在
    assert flag_path(store, snapshot_date).exists()


def test_超时标记触发恢复(tmp_path: Path) -> None:
    """超过阈值的标记应触发恢复。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    # 创建派生层文件（模拟派生到一半）
    store.state_dir.mkdir(parents=True, exist_ok=True)
    bronze_path = store.path("bronze", snapshot_date)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.write_text("fake bronze", encoding="utf-8")

    silver_path = store.path("silver", snapshot_date)
    silver_path.parent.mkdir(parents=True, exist_ok=True)
    silver_path.write_text("fake silver", encoding="utf-8")

    obs_path = store.path("observations", snapshot_date)
    obs_path.parent.mkdir(parents=True, exist_ok=True)
    obs_path.write_text("fake obs", encoding="utf-8")

    # 创建超时的标记（时间设为很久以前）
    path = flag_path(store, snapshot_date)
    old_time = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)
    flag = DerivingFlag(
        snapshot_date=snapshot_date,
        started_at=old_time.isoformat(timespec="seconds"),
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "snapshot_date": flag.snapshot_date.isoformat(),
                "started_at": flag.started_at,
            }
        ),
        encoding="utf-8",
    )

    # 触发恢复
    recovered = check_and_recover(store)

    assert recovered == [snapshot_date]

    # 标记文件应被删除
    assert not path.exists()

    # 派生层文件应被清理
    assert not bronze_path.exists()
    assert not silver_path.exists()
    assert not obs_path.exists()


def test_恢复不影响其他日期(tmp_path: Path) -> None:
    """恢复某一天不应影响其他日期的派生层。"""
    store = DatasetStore(tmp_path)
    date1 = date(2026, 11, 6)
    date2 = date(2026, 11, 7)

    # 两天都有派生层
    for dt in [date1, date2]:
        bronze_path = store.path("bronze", dt)
        bronze_path.parent.mkdir(parents=True, exist_ok=True)
        bronze_path.write_text(f"bronze {dt}", encoding="utf-8")

    # 只有 date2 有超时标记
    path = flag_path(store, date2)
    old_time = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "snapshot_date": date2.isoformat(),
                "started_at": old_time.isoformat(timespec="seconds"),
            }
        ),
        encoding="utf-8",
    )

    recovered = check_and_recover(store)

    assert recovered == [date2]

    # date2 的派生层被清理
    assert not store.path("bronze", date2).exists()

    # date1 的派生层保留
    assert store.path("bronze", date1).exists()


def test_多个超时标记全部恢复(tmp_path: Path) -> None:
    """多个超时标记应全部触发恢复。"""
    store = DatasetStore(tmp_path)
    dates = [date(2026, 11, 5), date(2026, 11, 6), date(2026, 11, 7)]

    # 创建多个超时标记
    old_time = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)
    for dt in dates:
        path = flag_path(store, dt)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "snapshot_date": dt.isoformat(),
                    "started_at": old_time.isoformat(timespec="seconds"),
                }
            ),
            encoding="utf-8",
        )

    recovered = check_and_recover(store)

    assert set(recovered) == set(dates)

    # 所有标记文件被删除
    for dt in dates:
        assert not flag_path(store, dt).exists()


def test_损坏的标记文件被删除(tmp_path: Path) -> None:
    """损坏的标记文件应被删除，不应崩溃。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    # 创建损坏的标记文件
    path = flag_path(store, snapshot_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("not json", encoding="utf-8")

    recovered = check_and_recover(store)

    # 不应崩溃，损坏的标记被删除
    assert recovered == []
    assert not path.exists()


def test_时间戳格式错误的标记被删除(tmp_path: Path) -> None:
    """时间戳格式错误的标记应被删除。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    path = flag_path(store, snapshot_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "snapshot_date": snapshot_date.isoformat(),
                "started_at": "not-a-timestamp",
            }
        ),
        encoding="utf-8",
    )

    recovered = check_and_recover(store)

    assert recovered == []
    assert not path.exists()


def test_只清理派生层不清理current(tmp_path: Path) -> None:
    """恢复时只清理派生层分区，不清理 current（它是全局的）。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    # 创建 current 文件
    current_path = store.path("current")
    current_path.parent.mkdir(parents=True, exist_ok=True)
    current_path.write_text("fake current", encoding="utf-8")

    # 创建派生层
    bronze_path = store.path("bronze", snapshot_date)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.write_text("fake bronze", encoding="utf-8")

    # 创建超时标记
    path = flag_path(store, snapshot_date)
    old_time = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "snapshot_date": snapshot_date.isoformat(),
                "started_at": old_time.isoformat(timespec="seconds"),
            }
        ),
        encoding="utf-8",
    )

    check_and_recover(store)

    # bronze 被清理
    assert not bronze_path.exists()

    # current 保留
    assert current_path.exists()


def test_有Raw时清理后重新派生(tmp_path: Path, monkeypatch) -> None:
    """清理过期派生层后，若该日 Raw 存在，应真正调用 derive_day 重建。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    bronze_path = store.path("bronze", snapshot_date)
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    bronze_path.write_text("fake bronze", encoding="utf-8")

    path = flag_path(store, snapshot_date)
    old_time = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "snapshot_date": snapshot_date.isoformat(),
                "started_at": old_time.isoformat(timespec="seconds"),
            }
        ),
        encoding="utf-8",
    )

    rederived: list[date] = []
    monkeypatch.setattr("hf_data_crawl.deriving.has_raw", lambda *_: True)
    monkeypatch.setattr(
        "hf_data_crawl.deriving._rederive", lambda _store, day: rederived.append(day)
    )

    recovered = check_and_recover(store)

    assert recovered == [snapshot_date]
    assert not bronze_path.exists()
    assert rederived == [snapshot_date]


def test_只有卡片批次时按可重放处理(tmp_path: Path, monkeypatch) -> None:
    """仅 card 批次的日子也算可重放：补位会进 current 与 observations，跳过反而制造不一致。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)
    raw.write(
        store,
        snapshot_date=snapshot_date,
        page_index=1,
        records=[{"id": "acme/ds-1"}],
        meta={},
        label=CARD_LABEL,
    )

    path = flag_path(store, snapshot_date)
    path.parent.mkdir(parents=True, exist_ok=True)
    old_time = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)
    path.write_text(
        json.dumps(
            {
                "snapshot_date": snapshot_date.isoformat(),
                "started_at": old_time.isoformat(timespec="seconds"),
            }
        ),
        encoding="utf-8",
    )

    rederived: list[date] = []
    monkeypatch.setattr(
        "hf_data_crawl.deriving._rederive", lambda _store, day: rederived.append(day)
    )

    recovered = check_and_recover(store)

    assert recovered == [snapshot_date]
    assert rederived == [snapshot_date]


def test_阈值边界(tmp_path: Path) -> None:
    """测试阈值边界：刚好达到阈值应触发，未达到不触发。"""
    store = DatasetStore(tmp_path)

    # 刚好达到阈值 + 1 秒
    snapshot_date1 = date(2026, 11, 7)
    path1 = flag_path(store, snapshot_date1)
    time1 = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 1)
    path1.parent.mkdir(parents=True, exist_ok=True)
    path1.write_text(
        json.dumps(
            {
                "snapshot_date": snapshot_date1.isoformat(),
                "started_at": time1.isoformat(timespec="seconds"),
            }
        ),
        encoding="utf-8",
    )

    # 刚好未达到阈值 - 1 秒
    snapshot_date2 = date(2026, 11, 8)
    path2 = flag_path(store, snapshot_date2)
    time2 = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS - 1)
    path2.parent.mkdir(parents=True, exist_ok=True)
    path2.write_text(
        json.dumps(
            {
                "snapshot_date": snapshot_date2.isoformat(),
                "started_at": time2.isoformat(timespec="seconds"),
            }
        ),
        encoding="utf-8",
    )

    recovered = check_and_recover(store)

    # 只有超过阈值的被恢复
    assert recovered == [snapshot_date1]
    assert not path1.exists()
    assert path2.exists()


def test_state目录不存在不报错(tmp_path: Path) -> None:
    """state 目录不存在时不应报错。"""
    store = DatasetStore(tmp_path)

    # state 目录不存在
    assert not store.state_dir.exists()

    # 不应崩溃
    recovered = check_and_recover(store)
    assert recovered == []
