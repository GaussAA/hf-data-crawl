"""配额节流的契约：不超发、不空转、窗口滑过后立刻恢复。"""

from __future__ import annotations

import pytest

from hf_data_crawl.acquire.hf.pace import MIN_SLEEP_SECONDS, Pacer


class FakeClock:
    """手动推进的时钟。节流的测试若用真时间，跑一次要几分钟。"""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def pacer(clock: FakeClock, *, quota: int = 3, window: float = 10.0) -> Pacer:
    return Pacer(quota=quota, window=window, clock=clock, sleep=clock.sleep)


def test_配额内一次都不睡() -> None:
    """配额以内的放行必须是零成本：每次请求都睡一下，等于把并发废掉。"""
    clock = FakeClock()
    gate = pacer(clock, quota=3)

    for _ in range(3):
        gate.wait()

    assert clock.slept == []


def test_超出配额后睡到最早一次滑出窗口() -> None:
    clock = FakeClock()
    gate = pacer(clock, quota=2, window=10.0)
    gate.wait()
    clock.now = 4.0
    gate.wait()

    gate.wait()  # 第三次：第一个名额要到 t=10 才滑出

    assert clock.slept == [pytest.approx(6.0)], "应只等 10 - 4 = 6 秒"


def test_窗口滑过后立即恢复放行() -> None:
    clock = FakeClock()
    gate = pacer(clock, quota=2, window=10.0)
    gate.wait()
    gate.wait()
    gate.wait()

    clock.now = 25.0  # 远晚于窗口
    gate.wait()

    assert len(clock.slept) == 1, "第三个之后不该再有等待"


def test_等待下限防止忙等() -> None:
    """算出的等待可能恰好为 0（第一次请求刚好滑出窗口那一刻），那时必须给下限。"""
    clock = FakeClock()
    gate = pacer(clock, quota=1, window=5.0)
    gate.wait()

    gate.wait()

    assert clock.slept and clock.slept[0] >= MIN_SLEEP_SECONDS


def test_配额非法时直接拒绝() -> None:
    """quota=0 会让 wait 永远睡下去——静默挂死，不如在构造时炸掉。"""
    clock = FakeClock()

    with pytest.raises(ValueError):
        Pacer(quota=0, window=10.0, clock=clock, sleep=clock.sleep)
    with pytest.raises(ValueError):
        Pacer(quota=1, window=0.0, clock=clock, sleep=clock.sleep)
