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
from dataclasses import dataclass
from typing import Protocol


class RateGate(Protocol):
    """请求前的闸。收成协议是为了让调用方与测试都不必依赖 `Pacer` 的具体实现。"""

    def wait(self) -> None: ...


#: 带令牌的固定窗口配额（2026-11-22 实测响应头 q=1000;w=300）。
QUOTA_REQUESTS = 1000
QUOTA_WINDOW_SECONDS = 300.0

#: 站点域的解析器配额（同日实测 `GET /datasets/<id>/raw/main/README.md` 的
#: `RateLimit-Policy: "fixed window";"resolvers";q=3000;w=300`）。
SITE_QUOTA_REQUESTS = 3000
SITE_QUOTA_WINDOW_SECONDS = 300.0

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


class RateLimiter:
    """按固定间隔放行的匀速器：多线程共享，串行地给每个请求「买票」。

    与 `Pacer` 的分工：`Pacer` 用于**上游给了配额响应头**的端点——它允许在窗口
    内突发放行，贴着配额跑最快；`RateLimiter` 用于**没有响应头可读**的端点
    （`datasets-server` 的 size / rows），那时只能靠保守的匀速自我约束，宁可慢
    一点也别把对方打毛。

    为什么不用「撞 429 再退避」：配额窗口以分钟计，一次撞线后十几个线程会各自
    睡满 `retry-after`（实测约 58 秒），醒来仍贴着上限跑，于是反复撞线——实测
    吞吐会掉到 0.2 req/s。

    `clock` / `sleep` 可注入，便于测试「间隔真的被拉开」。
    """

    def __init__(
        self,
        per_second: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._interval = (1.0 / per_second) if per_second and per_second > 0 else 0.0
        self._clock = clock
        self._sleep = sleep
        self._lock = threading.Lock()
        self._next = 0.0

    @property
    def enabled(self) -> bool:
        return self._interval > 0

    def acquire(self) -> None:
        """取一个放行名额；必要时睡到下一个名额的时间点。"""
        if not self.enabled:
            return
        with self._lock:
            now = self._clock()
            delay = max(0.0, self._next - now)
            self._next = max(now, self._next) + self._interval
        if delay > 0:
            self._sleep(delay)


@dataclass(frozen=True)
class Quota:
    """一个桶的配额：`window` 秒内最多 `requests` 次。"""

    requests: int
    window: float


#: 上游不是一本账，是**按桶分账**的。2026-11-22 带令牌实测三处响应头：
#:
#: | 端点 | 桶 | 配额 |
#: |---|---|---|
#: | `huggingface.co/api/*`（清单/详情） | `api` | 1000 / 300s = 3.33/s |
#: | `huggingface.co/<type>/<id>/raw/*`（README） | `resolvers` | 3000 / 300s = 10/s |
#: | `datasets-server.huggingface.co/*`（size / rows） | 无 `RateLimit-*` 头 | — |
#:
#: 前两处**互不挤占**：readme 跑满 10/s 不会吃掉 files 的 3.33/s。反过来也意味着
#: 把同一个闸套到两个桶上，等于把其中一个桶的产能砍掉两三倍。所以闸必须按桶取。
API_QUOTA = Quota(QUOTA_REQUESTS, QUOTA_WINDOW_SECONDS)

#: README 走站点域（`/raw/main/README.md`），归 `resolvers` 桶。
#:
#: 这一行是推翻旧注释来的：parsers 里原先写「5000 次 / 5 分钟 ≈ 16.7 req/s」并
#: 据此把默认值定 15，实测却是 3000 / 300s = 10/s——**默认值超配额 50%**，靠撞
#: 429 退避兜着跑，`--workers` 的 help 里那句「12 稳定在 19 req/s」正是撞墙前的
#: 瞬时读数，不是可持续吞吐。配额既然测得出来，就按它压平。
SITE_QUOTA = Quota(SITE_QUOTA_REQUESTS, SITE_QUOTA_WINDOW_SECONDS)

BUCKET_API = "api"
BUCKET_SITE = "resolvers"

BUCKETS: dict[str, Quota] = {BUCKET_API: API_QUOTA, BUCKET_SITE: SITE_QUOTA}

_SHARED: dict[str, Pacer] = {}
_SHARED_LOCK = threading.Lock()


def shared(bucket: str) -> Pacer:
    """取进程内该桶**唯一**的闸。

    为什么要共享：配额是按「桶 + 令牌」在服务端记账的，客户端各建一个 Pacer
    就等于各持一份额度，两个命令接连跑时合计会超发一倍。files、readme 现在分属
    两个桶互不影响，但将来往同一桶里加第三个端点时，这里必须是同一个对象。

    未知桶名直接抛 `KeyError`：宁可启动就炸，也别默默套上一个错误的配额——
    那正是「15 req/s 打 10 req/s 的桶」这种错误能活这么久的缘故。
    """
    quota = BUCKETS[bucket]
    with _SHARED_LOCK:
        gate = _SHARED.get(bucket)
        if gate is None:
            gate = Pacer(quota=quota.requests, window=quota.window)
            _SHARED[bucket] = gate
        return gate
