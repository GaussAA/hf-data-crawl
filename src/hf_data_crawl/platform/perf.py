"""性能监控工具：识别慢操作。

使用方法:
    from hf_data_crawl.platform.perf import timed

    @timed
    def slow_function():
        ...

或者手动计时:
    with timed_block("操作名称"):
        ...
"""

from __future__ import annotations

import time
from collections.abc import Callable
from contextlib import contextmanager
from functools import wraps
from typing import Any, TypeVar

F = TypeVar("F", bound=Callable[..., Any])

#: 只记录超过此阈值的操作（秒）
SLOW_THRESHOLD = 1.0


def timed(func: F) -> F:  # noqa: UP047
    """装饰器：记录超过阈值的慢操作。

    示例:
        @timed
        def run_collect(...):
            ...
    """

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        if elapsed > SLOW_THRESHOLD:
            print(f"⚠️  {func.__name__} 耗时 {elapsed:.2f}s")
        return result

    return wrapper  # type: ignore[return-value]


@contextmanager
def timed_block(name: str):
    """上下文管理器：记录代码块的执行时间。

    示例:
        with timed_block("处理 1000 个文件"):
            for file in files:
                process(file)
    """
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        if elapsed > SLOW_THRESHOLD:
            print(f"⚠️  {name} 耗时 {elapsed:.2f}s")
