"""状态词表：Status 枚举与 INACTIVE 集合。

被 state、reconcile、rebuild 共用，所以放在最底层的 platform：上层各取所需，
不必为拿一个枚举而互相依赖。探测结论到状态的映射在 `reconcile`，状态留存协议
在 `rebuild`，各归用它的模块。
"""

from __future__ import annotations

from enum import StrEnum


class Status(StrEnum):
    """current.status 的取值。"""

    ACTIVE = "active"
    DISAPPEARED = "disappeared"
    RESTRICTED = "restricted"
    RENAMED = "renamed"


#: 所有「非在架」状态。StrEnum 与同值字符串相等且同哈希，故可直接与读出的字符串比较。
INACTIVE: frozenset[Status] = frozenset(status for status in Status if status is not Status.ACTIVE)
