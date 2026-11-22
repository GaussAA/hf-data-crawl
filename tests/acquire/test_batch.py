"""批量补采骨架的行为测试：心跳、进度、边跑边落库。

三条都是「长任务在半夜跑」时才显形的问题，靠人工守夜发现的成本太高，所以钉在这里。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.acquire import batch

SCHEMA: dict[str, Any] = {"id": pl.String, "value": pl.Int64}


@dataclass(frozen=True)
class FakeResult:
    status: str
    row: dict[str, Any]


def ok(dataset_id: str, value: int = 1) -> FakeResult:
    return FakeResult("ok", {"id": dataset_id, "value": value})


def test_一条都没完成时发心跳(monkeypatch: pytest.MonkeyPatch) -> None:
    """「跑得慢」与「卡住了」必须能从日志上分开。

    2026-11-16 实测：一次 `fetch-sizes` 卡住 1 小时 35 分，日志一个字节没多写，
    与「老老实实跑得慢」看不出任何分别。心跳就是那一声「我还活着，但没进展」。
    """
    monkeypatch.setattr(batch, "STALL_HEARTBEAT_SECONDS", 0.05)
    beats: list[tuple[int, int]] = []

    def slow(dataset_id: str) -> FakeResult:
        time.sleep(0.25)
        return ok(dataset_id)

    results, _, _ = batch.collect_rows(
        pending=["a", "b"],
        fetch=slow,
        schema=SCHEMA,
        workers=1,
        heartbeat=lambda done, total: beats.append((done, total)),
    )

    assert len(results) == 2
    assert beats, "这么久没一条完成，心跳却没响"
    # 心跳报告的是「此刻已经完成几条」，所以 (0, 2) 与 (1, 2) 都算对；
    # 但「一条都没成」的那次必须有——那正是最需要一声呼救的时刻。
    assert (0, 2) in beats
    assert all(0 <= done <= 2 and total == 2 for done, total in beats)


def test_顺利跑完时不需要心跳() -> None:
    """正常速度下不响，否则心跳就成了噪声，真出问题那行反而被淹掉。"""
    beats: list[tuple[int, int]] = []

    results, frame, _ = batch.collect_rows(
        pending=[f"acme/ds-{index}" for index in range(20)],
        fetch=ok,
        schema=SCHEMA,
        workers=4,
        heartbeat=lambda done, total: beats.append((done, total)),
    )

    assert len(results) == 20
    assert frame.height == 20
    assert beats == []


def test_终态必发进度且空任务不发() -> None:
    """停在 96% 比没有进度条更糟；而零任务回调会让调用方除零。"""
    seen: list[tuple[int, int]] = []

    batch.collect_rows(
        pending=[f"acme/ds-{index}" for index in range(7)],
        fetch=ok,
        schema=SCHEMA,
        workers=2,
        progress=lambda done, total: seen.append((done, total)),
    )

    # 7 条不足 100，只有终态那一次；但必须有。
    assert seen[-1] == (7, 7)

    empty: list[tuple[int, int]] = []
    batch.collect_rows(
        pending=[], fetch=ok, schema=SCHEMA, progress=lambda d, t: empty.append((d, t))
    )
    assert empty == []


def test_边跑边落库每批一次() -> None:
    """十万条要跑一小时，只在末尾写一次等于把一小时押在最后一个字节上。"""
    batches: list[int] = []

    batch.collect_rows(
        pending=[f"acme/ds-{index}" for index in range(10)],
        fetch=ok,
        schema=SCHEMA,
        workers=2,
        flush=lambda frame: batches.append(frame.height),
        flush_every=3,
    )

    assert batches == [3, 3, 3], f"每满 flush_every 条落一次；尾巴留在返回值里：{batches}"


# ---------------------------------------------------------------- 到点收工


def test_到点后不再投新任务并报告原因() -> None:
    """收工只停投放：已发出去的跑完、照常返回，`stopped` 说明这一轮没跑完。

    刷新链用它把 files 卡在次日 00:20，好给 00:30 的 collect 让出同一个 api 桶。
    若这里改成「砍进程」，在飞的那批大库（往往最慢也最值钱）就白跑了。
    """
    now = [0.0]

    def fetch(dataset_id: str) -> FakeResult:
        now[0] += 0.5
        return ok(dataset_id)

    results, _, stopped = batch.collect_rows(
        pending=[f"ds-{index}" for index in range(100)],
        fetch=fetch,
        schema=SCHEMA,
        workers=1,
        deadline=2.0,
        clock=lambda: now[0],
    )

    assert stopped == "deadline"
    assert 0 < len(results) < 100, "到点后不该把 100 条全跑完"
    assert len({item.row["id"] for item in results}) == len(results), "不该重复抓同一个 id"


def test_没到点时不报stopped() -> None:
    """跑完了却记一条「到点收工」，摘要里会凭空多一个没跑完的假象。"""
    results, _, stopped = batch.collect_rows(
        pending=[f"ds-{index}" for index in range(5)],
        fetch=ok,
        schema=SCHEMA,
        workers=2,
        deadline=9_999_999_999.0,
        clock=lambda: 0.0,
    )

    assert stopped is None
    assert len(results) == 5


def test_到点也把已抓的落盘() -> None:
    """到点不等于丢数据：盘上必须留下已完成的那些。"""
    now = [0.0]
    written: list[int] = []

    def fetch(dataset_id: str) -> FakeResult:
        now[0] += 0.5
        return ok(dataset_id)

    _, frame, stopped = batch.collect_rows(
        pending=[f"ds-{index}" for index in range(100)],
        fetch=fetch,
        schema=SCHEMA,
        workers=1,
        flush=lambda part: written.append(part.height),
        flush_every=1,
        deadline=2.0,
        clock=lambda: now[0],
    )

    assert stopped == "deadline"
    assert written, "到点前完成的行必须已写出去"
    assert sum(written) == frame.height
