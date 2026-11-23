"""配额节流的契约：不超发、不空转、窗口滑过后立刻恢复。"""

from __future__ import annotations

from datetime import datetime, time, timedelta

import pytest

from hf_data_crawl.acquire.hf.pace import (
    BUCKET_API,
    BUCKET_SITE,
    BUCKETS,
    MIN_SLEEP_SECONDS,
    Pacer,
    Quota,
    RateLimiter,
    pacer_for,
    shared,
)


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


# ---------------------------------------------------------------- 按桶共享


def test_桶配额按实测口径() -> None:
    """这两个数是量出来的，不是估的：改它之前先再量一次响应头。"""
    assert BUCKETS[BUCKET_API] == Quota(1000, 300.0)
    assert BUCKETS[BUCKET_SITE] == Quota(3000, 300.0)


def test_同桶取到同一个闸() -> None:
    """各建一份就等于各领一份额度，合起来是 N 倍超发。"""
    assert shared(BUCKET_API) is shared(BUCKET_API)
    assert shared(BUCKET_SITE) is shared(BUCKET_SITE)


def test_不同桶各自记账() -> None:
    """readme 打满 resolvers 的 10/s 不该挤占 files 的 3.33/s——两本账互不相干。"""
    assert shared(BUCKET_API) is not shared(BUCKET_SITE)


def test_未知桶名直接炸() -> None:
    """拼错的桶名若默默放行，等于这一路没有节流，且查不出来。"""
    with pytest.raises(KeyError):
        shared("apis")


# ---------------------------------------------------------------- 匀速器


def test_限速器按间隔放行() -> None:
    """无配额头可读的端点只能靠匀速自我约束（`Pacer` 允许突发，这里不行）。"""
    now = [0.0]
    sleeps: list[float] = []
    limiter = RateLimiter(10, clock=lambda: now[0], sleep=sleeps.append)

    limiter.acquire()
    now[0] = 0.05
    limiter.acquire()

    assert sleeps == [pytest.approx(0.05)]


def test_限速为零时不等待() -> None:
    sleeps: list[float] = []
    limiter = RateLimiter(0, clock=lambda: 0.0, sleep=sleeps.append)

    limiter.acquire()
    limiter.acquire()

    assert sleeps == []
    assert not limiter.enabled


# ---------------------------------------------------------------- 让路窗口


class FakeWorld:
    """同时推进单调钟与墙钟的假时间。

    让路看墙钟（几点钟），配额看单调钟（过了多少秒），`wait` 里两套都在用，
    所以一次 sleep 必须把两个都推走；只推一个会让循环原地打转。
    """

    def __init__(self, wall_iso: str) -> None:
        self.wall = datetime.fromisoformat(wall_iso)
        self.mono = 0.0
        self.slept: list[float] = []

    def clock(self) -> float:
        return self.mono

    def wall_clock(self) -> datetime:
        return self.wall

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.mono += seconds
        self.wall += timedelta(seconds=seconds)


def quiet_pacer(world: FakeWorld, windows: tuple[tuple[time, time], ...]) -> Pacer:
    return Pacer(
        quota=10,
        window=10.0,
        clock=world.clock,
        sleep=world.sleep,
        quiet=windows,
        wall=world.wall_clock,
    )


def test_让路窗口内先睡到窗口结束() -> None:
    """配额按账号记账：另一台机器的每日链在跑时，这里一个请求都不该发。"""
    world = FakeWorld("2026-11-24T00:30:00")
    gate = quiet_pacer(world, ((time(0, 20), time(2, 0)),))

    gate.wait()

    assert world.slept == [pytest.approx(90 * 60)], "00:30 起应睡到 02:00"
    assert world.wall.time() == time(2, 0)


def test_窗口外不额外睡() -> None:
    world = FakeWorld("2026-11-24T03:00:00")
    gate = quiet_pacer(world, ((time(0, 20), time(2, 0)),))

    gate.wait()

    assert world.slept == []


def test_窗口结束那一刻就该放行() -> None:
    """`[起, 止)` 是左闭右开：02:00 整已经不在窗口里，再睡一轮等于白等一分钟。"""
    world = FakeWorld("2026-11-24T02:00:00")
    gate = quiet_pacer(world, ((time(0, 20), time(2, 0)),))

    gate.wait()

    assert world.slept == []


def test_跨零点的窗口也照睡() -> None:
    """`23:50-01:40`：止早于起就是跨零点，终点要算到明天，不能算成 22 小时前。"""
    world = FakeWorld("2026-11-23T23:55:00")
    gate = quiet_pacer(world, ((time(23, 50), time(1, 40)),))

    gate.wait()

    assert world.slept == [pytest.approx(105 * 60)]
    assert world.wall.day == 24


def test_跨零点窗口过了午夜仍在其中() -> None:
    world = FakeWorld("2026-11-24T00:30:00")
    gate = quiet_pacer(world, ((time(23, 50), time(1, 40)),))

    gate.wait()

    assert world.slept == [pytest.approx(70 * 60)]


def test_没配窗口时取共享闸() -> None:
    """不让路就不该多建一个闸：多建等于多领一份额度。"""
    assert pacer_for(BUCKET_API) is shared(BUCKET_API)


def test_带让路的闸不进共享表() -> None:
    """共享闸是「不让路」的那一个；把让路挂上去会把同进程里别的命令一起停住。"""
    gate = pacer_for(BUCKET_API, quiet=((time(0, 20), time(2, 0)),))

    assert gate is not shared(BUCKET_API)
    assert pacer_for(BUCKET_API) is shared(BUCKET_API)
