"""按上游配额给请求节流。

为什么不靠「撞 429 再退避」
--------------------------
上游是固定窗口配额：响应头 `RateLimit-Policy: "fixed window";"api";q=1000;w=300`
（2026-11-22 带令牌实测），即 300 秒内最多 1000 次。撞到 429 之后 `retry-after`
给的是**距窗口重置的秒数**，照它睡会把一个 worker 停住几十到几百秒。实测：
`fetch-files` 12 并发跑时长尾段吞吐从 3.4 掉到 2.76 个/秒，而配额是 3.33——
**退避机制本身吃掉了约 17% 的产能**，还不算那些睡过去又重试一次的请求。

配额是可知的，就不该靠撞墙去发现它。这里按滑动窗口匀速放行：请求不再被 429
打断，吞吐贴着配额走，也能一眼看出「瓶颈在配额，不在并发」。

它不能替代重试
--------------
429 仍可能来自别处的并发（同一令牌跑着另一个命令），所以 429 的退避要保留。
Pacer 只负责把「自己这一侧」的速率压到配额以内。
"""

from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from typing import Protocol


class RateGate(Protocol):
    """请求前的闸。收成协议是为了让调用方与测试都不必依赖 `Pacer` 的具体实现。"""

    def wait(self) -> None: ...


#: 带令牌的固定窗口配额（2026-11-22 实测响应头 q=1000;w=300）。
QUOTA_REQUESTS = 1000
QUOTA_WINDOW_SECONDS = 300.0

#: 睡眠下限：算出的等待若为 0，`wait` 会退化成忙等。
MIN_SLEEP_SECONDS = 0.01


class Pacer:
    """滑动窗口限速：任意 `window` 秒内最多放行 `quota` 次。

    以「最近 quota 次请求的时刻」为判据，而不是固定窗口计数器——后者在窗口边界
    会一次放行两倍配额（前一窗口末尾撞上后一窗口开头）。
    """

    def __init__(
        self,
        *,
        quota: int = QUOTA_REQUESTS,
        window: float = QUOTA_WINDOW_SECONDS,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if quota < 1:
            raise ValueError("quota 至少为 1")
        if window <= 0:
            raise ValueError("window 必须为正数")
        self._quota = quota
        self._window = window
        self._clock = clock
        self._sleep = sleep
        self._stamps: deque[float] = deque()
        self._lock = threading.Lock()

    def wait(self) -> None:
        """占一个名额：未超配额立即返回，超了则睡到最早那次请求滑出窗口。"""
        while True:
            with self._lock:
                now = self._clock()
                while self._stamps and now - self._stamps[0] >= self._window:
                    self._stamps.popleft()
                if len(self._stamps) < self._quota:
                    self._stamps.append(now)
                    return
                delay = self._window - (now - self._stamps[0])
            # 睡眠放在锁外：否则一个 worker 等窗口会把其余 worker 一起堵住，
            # 而那正是「并发看起来很高、吞吐却上不去」的成因。
            self._sleep(max(delay, MIN_SLEEP_SECONDS))
