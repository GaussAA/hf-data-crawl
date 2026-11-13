"""Acquire 切片公开接口：数据采集与对账。

提供:
- 传输抽象 (Transport, 仅供测试注入)
- API 常量 (MAX_LIMIT)
- 探测限额 (PROBE_DAILY_LIMIT)
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    # Transport 只在类型检查时导入,避免运行时加载 httpx
    from .hf.api import Transport
else:
    Transport = None  # type: ignore[misc,assignment]

from .hf.api import MAX_LIMIT
from .reconcile import PROBE_DAILY_LIMIT

__all__ = [
    "Transport",
    "MAX_LIMIT",
    "PROBE_DAILY_LIMIT",
]
