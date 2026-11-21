"""一次 GET 之后怎么读：分区互斥、退避上限、脏 `Retry-After`。

这一层的价值全在「四份合成一份」：哪张表的分区变了，这里的一组断言会打红，
而不是等到某个库多烧几百次请求、或者把可救的库钉成永久缺失才发现。
"""

from __future__ import annotations

import pytest

from hf_data_crawl.acquire.hf.retry import (
    BACKOFF_CAP,
    RETRY_AFTER_CAP,
    StatusPolicy,
    delay_seconds,
    exponential_seconds,
    header_value,
    retry_after_seconds,
)


def test_同一状态码不能既终态又可重试() -> None:
    """只能选一件。写错时直接抛，而不是让「先后判断顺序」偶然决定行为。"""
    with pytest.raises(ValueError):
        StatusPolicy(retryable=frozenset({500}), permanent={500: "unavailable"})


def test_同一个500在两个端点分区相反() -> None:
    """`/size` 用 500 表示「不支持」，其余端点的 500 是瞬时故障。

    这是上游事实，不是疏忽：把它统一成一种含义，要么让 1,269 个不支持的库
    各空转 5 次重试（约 3.6 小时），要么把可救的库判成永久缺失。
    """
    sizes = StatusPolicy(
        retryable=frozenset({429, 502, 503, 504}),
        permanent={401: "unavailable", 404: "unavailable", 500: "unavailable"},
    )
    readme = StatusPolicy(
        retryable=frozenset({429, 500, 502, 503, 504}),
        permanent={401: "denied", 404: "absent"},
    )

    assert sizes.terminal(500) == "unavailable"
    assert not sizes.retry(500)
    assert readme.retry(500)
    assert readme.terminal(500) is None


def test_头名大小写不敏感() -> None:
    """HTTP/2 会把头名规范成小写；按大写字面量查找会静默取空。"""
    assert header_value({"retry-after": "12"}, "Retry-After") == "12"
    assert header_value({"Retry-After": "12"}, "retry-after") == "12"
    assert header_value({}, "retry-after") is None


def test_429优先听上游的retry_after() -> None:
    assert delay_seconds(429, {"retry-after": "17"}, 0, base=2.0) == 17.0


@pytest.mark.parametrize("raw", ["soon", "inf", "-3", "nan", ""])
def test_脏retry_after回退指数退避(raw: str) -> None:
    """三种脏值都实测出现过：照 `inf` 睡等于挂死，照负数睡等于没睡。"""
    assert retry_after_seconds({"retry-after": raw}) is None
    assert delay_seconds(429, {"retry-after": raw}, 1, base=2.0) == 4.0


def test_retry_after夹在上限内() -> None:
    """上游可能给整个窗口（实测 300 秒），照单全睡会让一次调用累积半小时。"""
    assert retry_after_seconds({"retry-after": "300"}) == RETRY_AFTER_CAP


def test_非429不看retry_after() -> None:
    """其余状态码上的这个头不是「等一会就好」的意思；时长只按退避算。"""
    assert delay_seconds(503, {"retry-after": "17"}, 2, base=2.0) == 8.0


def test_指数退避封顶() -> None:
    assert exponential_seconds(0, base=2.0) == 2.0
    assert exponential_seconds(3, base=2.0) == 16.0
    assert exponential_seconds(10, base=2.0) == BACKOFF_CAP
