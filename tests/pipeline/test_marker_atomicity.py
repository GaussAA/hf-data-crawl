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

import pytest

from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.marker import (
    STALE_THRESHOLD_SECONDS,
    clear_marker,
    load_marker,
    marker_path,
    write_marker,
)
from hf_data_crawl.pipeline.raw import CARD_LABEL
from hf_data_crawl.pipeline.recovery import check_and_recover
from hf_data_crawl.platform.store import DatasetStore


def test_标记文件创建(tmp_path: Path) -> None:
    """测试标记文件是否正确创建。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    write_marker(store, snapshot_date)

    path = marker_path(store, snapshot_date)
    assert path.exists()

    # 验证内容
    content = json.loads(path.read_text(encoding="utf-8"))
    assert (content["since"], content["until"]) == ("2026-11-07", "2026-11-07")
    assert "started_at" in content

    # 验证时间戳格式
    started_at = datetime.fromisoformat(content["started_at"])
    assert started_at.tzinfo is not None  # 应该是 UTC


def test_标记文件删除(tmp_path: Path) -> None:
    """测试标记文件是否正确删除。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    write_marker(store, snapshot_date)
    path = marker_path(store, snapshot_date)
    assert path.exists()

    clear_marker(store, snapshot_date)
    assert not path.exists()


def test_删除不存在的标记不报错(tmp_path: Path) -> None:
    """删除不存在的标记文件不应报错。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    # 直接删除，不应崩溃
    clear_marker(store, snapshot_date)


def test_无标记时不触发恢复(tmp_path: Path) -> None:
    """没有标记文件时，不应触发恢复。"""
    store = DatasetStore(tmp_path)

    recovered = check_and_recover(store)
    assert recovered == []


def test_新标记不触发恢复(tmp_path: Path) -> None:
    """刚创建的标记（未超时）不应触发恢复。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    write_marker(store, snapshot_date)

    recovered = check_and_recover(store)
    assert recovered == []

    # 标记文件应仍然存在
    assert marker_path(store, snapshot_date).exists()


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
    path = marker_path(store, snapshot_date)
    old_time = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)
    path.parent.mkdir(parents=True, exist_ok=True)
    write_marker(store, snapshot_date, started_at=old_time.isoformat(timespec="seconds"))

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
    path = marker_path(store, date2)
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
        path = marker_path(store, dt)
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
        assert not marker_path(store, dt).exists()


def test_损坏的标记文件被删除(tmp_path: Path) -> None:
    """损坏的标记文件应被删除，不应崩溃。"""
    store = DatasetStore(tmp_path)
    snapshot_date = date(2026, 11, 7)

    # 创建损坏的标记文件
    path = marker_path(store, snapshot_date)
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

    path = marker_path(store, snapshot_date)
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
    path = marker_path(store, snapshot_date)
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

    path = marker_path(store, snapshot_date)
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
    monkeypatch.setattr("hf_data_crawl.pipeline.marker.has_raw", lambda *_: True)

    def ok_rederive(_store: DatasetStore, day: date) -> bool:
        rederived.append(day)
        return True

    # `_rederive` 现在返回 bool：False 表示重放失败、标记必须留下。
    monkeypatch.setattr("hf_data_crawl.pipeline.recovery._rederive", ok_rederive)

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

    path = marker_path(store, snapshot_date)
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

    def ok_rederive(_store: DatasetStore, day: date) -> bool:
        rederived.append(day)
        return True

    # `_rederive` 现在返回 bool：False 表示重放失败、标记必须留下。
    monkeypatch.setattr("hf_data_crawl.pipeline.recovery._rederive", ok_rederive)

    recovered = check_and_recover(store)

    assert recovered == [snapshot_date]
    assert rederived == [snapshot_date]


def test_阈值边界(tmp_path: Path) -> None:
    """测试阈值边界：刚好达到阈值应触发，未达到不触发。"""
    store = DatasetStore(tmp_path)

    # 刚好达到阈值 + 1 秒
    snapshot_date1 = date(2026, 11, 7)
    path1 = marker_path(store, snapshot_date1)
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
    path2 = marker_path(store, snapshot_date2)
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


def stale_marker(store: DatasetStore, day: date, *, naive: bool = False) -> tuple[Path, str]:
    """写一个已超时的标记，返回 (路径, 原始 started_at)。"""
    old = datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)
    if naive:
        old = old.replace(tzinfo=None)
    started_at = old.isoformat(timespec="seconds")
    path = marker_path(store, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"snapshot_date": day.isoformat(), "started_at": started_at}),
        encoding="utf-8",
    )
    return path, started_at


def test_无时区的标记时间戳按UTC处理而不崩溃(tmp_path: Path) -> None:
    """历史或人工写入的 marker 可能没有 Z；aware 减 naive 会抛 TypeError。

    这段检查跑在 `_execute` 之外，抛出去就是所有写命令启动即崩、没有 failed
    run、没有堆栈，故必须在这里兜住。
    """
    store = DatasetStore(tmp_path)
    day = date(2026, 11, 7)
    path, _ = stale_marker(store, day, naive=True)

    recovered = check_and_recover(store)

    assert recovered == [day]
    assert not path.exists()


def test_重放失败时标记保留且原时间戳不变(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """失败可能发生在首个 derive_day 之前，那时没有新标记续命。"""
    store = DatasetStore(tmp_path)
    day = date(2026, 11, 7)
    monkeypatch.setattr("hf_data_crawl.pipeline.marker.has_raw", lambda *_: True)
    monkeypatch.setattr("hf_data_crawl.pipeline.recovery._rederive", lambda *_: False)
    path, started_at = stale_marker(store, day)

    recovered = check_and_recover(store)

    assert recovered == []
    assert path.exists(), "标记先删会让该日派生层已清却再无人重放"
    assert json.loads(path.read_text(encoding="utf-8"))["started_at"] == started_at


def test_重放抛异常时留下标记供下次立即重试(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """真实 `_rederive` 吞异常返回 False；恢复链要据此保住标记。"""
    store = DatasetStore(tmp_path)
    day = date(2026, 11, 7)
    monkeypatch.setattr("hf_data_crawl.pipeline.marker.has_raw", lambda *_: True)

    def broken(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("Raw 坏了")

    monkeypatch.setattr("hf_data_crawl.pipeline.recovery.rebuild_from_raw", broken)
    path, _ = stale_marker(store, day)

    recovered = check_and_recover(store)

    assert recovered == []
    assert path.exists()
    assert "保留标记" in capsys.readouterr().out


# ---------------------------------------------------------------- 范围标记


def test_旧的逐日标记文件仍能读(tmp_path: Path) -> None:
    """升级成区间格式后，磁盘上已有的 `snapshot_date` 标记不能被当成损坏扔掉。

    生产机上那份文件是上一版写的：当成损坏删掉，就等于「这天的派生残留永远没人清」。
    """
    store = DatasetStore(tmp_path / "data")
    day = date(2026, 11, 7)
    path = marker_path(store, day)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"snapshot_date": day.isoformat(), "started_at": "2026-11-07T00:00:00+00:00"}),
        encoding="utf-8",
    )

    entry = load_marker(path)

    assert entry is not None
    assert (entry.since, entry.until) == (day, day)


def test_范围标记吞掉它覆盖的逐日标记(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """同一次崩溃会留下范围标记 + 在飞那天的逐日标记。

    范围先处理、逐日那个跟着删：否则同一段后缀会被重放两遍，而第二遍的起点还更晚。
    """
    store = DatasetStore(tmp_path / "data")
    store.state_dir.mkdir(parents=True, exist_ok=True)
    old = (datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)).isoformat(
        timespec="seconds"
    )
    day1, day2 = date(2026, 11, 1), date(2026, 11, 2)
    write_marker(store, day1, until=day2, started_at=old)
    write_marker(store, day2, started_at=old)
    replayed: list[date] = []
    monkeypatch.setattr("hf_data_crawl.pipeline.marker.has_raw", lambda *_: True)
    monkeypatch.setattr(
        "hf_data_crawl.pipeline.recovery.rebuild_from_raw",
        lambda _store, since: replayed.append(since),
    )

    recovered = check_and_recover(store)

    assert replayed == [day1], "整段只重放一次，起点是最早那天"
    assert recovered == [day1, day2]
    assert list(store.state_dir.glob("marker-*.marker")) == []


def test_范围里没有_Raw_的日子只清残留(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """冷区压掉的日子没有 Raw 可重放：清掉半成品分区，但不把整段重放拒掉。"""
    store = DatasetStore(tmp_path / "data")
    store.state_dir.mkdir(parents=True, exist_ok=True)
    day1, day2 = date(2026, 11, 1), date(2026, 11, 2)
    stale = (datetime.now(UTC) - timedelta(seconds=STALE_THRESHOLD_SECONDS + 60)).isoformat(
        timespec="seconds"
    )
    write_marker(store, day1, until=day2, started_at=stale)
    cleared: list[date] = []
    monkeypatch.setattr("hf_data_crawl.pipeline.marker.has_raw", lambda _store, day: day == day2)
    monkeypatch.setattr(
        "hf_data_crawl.pipeline.marker.clear_day", lambda _store, day: cleared.append(day)
    )
    monkeypatch.setattr(
        "hf_data_crawl.pipeline.recovery.rebuild_from_raw", lambda _store, since: None
    )

    recovered = check_and_recover(store)

    assert cleared == [day1, day2]
    assert recovered == [day1, day2]
