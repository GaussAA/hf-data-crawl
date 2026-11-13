"""Acquire 切片公开接口：数据采集与对账。

对外只有两个常量：单页上限 `MAX_LIMIT`、每轮探测上限 `PROBE_DAILY_LIMIT`。

`Transport` 是**类型**，不是运行期对象，只在 `TYPE_CHECKING` 下导入。曾经写成
`else: Transport = None` 并列入 `__all__`，于是 `from ..acquire import Transport`
真的拿到 `None`——类型检查一路绿灯，等到真拿它做判断或构造时才炸，且错误指向
调用处而非定义处。看不见的对象不如不导出。
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from .hf.api import MAX_LIMIT
from .reconcile import PROBE_DAILY_LIMIT

if TYPE_CHECKING:
    from .hf.api import Transport as Transport

__all__ = ["MAX_LIMIT", "PROBE_DAILY_LIMIT"]
