"""一次 GET 之后怎么读：终态、可重试、退避。

为什么要有这个模块
------------------
四张单库富化表（size / readme / files / preview）各写一份「状态码怎么分」与退避：
两份 `_delay_for` 逐字相同（连 docstring 一起），`retry-after` 的大小写兜底另有三份，
`_backoff` 四份。改一处、其余三处立刻漂移——不报错、不变红，只是某些库多烧几次请求
或者少重试一次。

**同一个 500 在不同端点含义相反，这不是 bug**：`/size` 用 500/501 表示「这个数据集
不支持」，其余端点的 500 是瞬时故障。故分区由端点自己声明（`StatusPolicy`），
本模块只提供「怎么读」与「怎么等」。

分区互斥是硬约束：同一个状态码不能既声明为可重试、又声明为终态——那两件事只能选一件，
写错时 `__post_init__` 直接抛，而不是让「先判断谁」这种顺序偶然决定行为。
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

#: 指数退避的单次上限（秒）。四张表共用：上游的 `retry-after` 可能等于整个限流窗口，
#: 照单全睡会让一次调用累积半小时以上。**只用于单库抓取**；扫描路径的上限另有其值，
#: 见 `hf.api.MAX_RATE_LIMIT_SLEEP`（那里睡满窗口反而更快）。
BACKOFF_CAP: Final = 30.0

#: `Retry-After` 可接受的上限（秒）。比退避上限宽松：它是上游明说的，听完比自算准，
#: 但仍要防脏值（实测见过整窗口 300 秒）。
RETRY_AFTER_CAP: Final = 60.0


@dataclass(frozen=True)
class StatusPolicy:
    """一张端点自己的状态码分区：终态（带名字）与可重试。

    `permanent` 是「状态码 → 终态名」：名字就是落库的 `status` 取值（`unavailable`
    / `absent` / `denied`），由端点决定。同一状态码在不同端点可以是不同的名字，
    甚至一边终态、一边可重试（500 在两处的分歧见模块 docstring）。
    """

    retryable: frozenset[int]
    permanent: Mapping[int, str]

    def __post_init__(self) -> None:
        overlap = sorted(self.retryable & set(self.permanent))
        if overlap:
            raise ValueError(f"状态码 {overlap} 同时被声明为可重试与终态，只能选一件")

    def terminal(self, status: int) -> str | None:
        """终态名；不是终态返回 None（调用方随后再看 `retry`）。"""
        return self.permanent.get(status)

    def retry(self, status: int) -> bool:
        return status in self.retryable


def header_value(headers: Mapping[str, str], name: str) -> str | None:
    """大小写不敏感取头。

    真实接口经 HTTP/2 会把头名规范成小写；按大写字面量查找会静默取空，
    进而导致分页在首页就「正常终止」、退避回退成固定值——这类失败不报错，只会
    丢数据或多烧请求。原先这份兜底在 `hf.api` 与两条 `_delay_for` 里各写一遍。
    """
    target = name.lower()
    for key, value in headers.items():
        if key.lower() == target:
            return value
    return None


def retry_after_seconds(
    headers: Mapping[str, str], *, cap: float = RETRY_AFTER_CAP
) -> float | None:
    """`Retry-After` 的秒数；没给、不是数字、非有限、负数一律 None。

    夹具里出现过 `"soon"`、`inf`、`-3` 三种脏值，全部回退指数退避而不是照睡：
    照 `inf` 睡等于挂死，照负数睡等于没睡（却没有退避增长）。
    """
    raw = header_value(headers, "retry-after")
    if raw is None:
        return None
    try:
        seconds = float(raw)
    except ValueError:
        return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return min(seconds, cap)


def exponential_seconds(attempt: int, *, base: float, cap: float = BACKOFF_CAP) -> float:
    """第 `attempt` 次重试前睡多久（0 起算）：`base × 2^attempt`，封顶 `cap`。

    `cap` 可调是因为「睡多久算合理」取决于有没有别的活等着：单库抓取 30 秒就到顶，
    而探测路径攥着写锁，与扫描同用一个更宽松的上限（见 `hf.api.MAX_RETRY_SLEEP`）。
    """
    return min(base * (2**attempt), cap)


def delay_seconds(
    status: int,
    headers: Mapping[str, str],
    attempt: int,
    *,
    base: float,
) -> float:
    """退避时长：429 优先听上游的 `Retry-After`，没给或给脏值才用指数退避。

    为什么只对 429 听：其余状态码上游不给这个头；即便给了也不是「等一会就好」的
    意思（501 等一年仍是 501）。4xx/5xx 是不是终态由 `StatusPolicy` 判，这里只算时长。
    """
    if status == 429:
        seconds = retry_after_seconds(headers)
        if seconds is not None:
            return seconds
    return exponential_seconds(attempt, base=base)
