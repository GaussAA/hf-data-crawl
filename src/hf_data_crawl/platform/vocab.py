"""状态词表：Status 枚举与 INACTIVE 集合。

从 status.py 拆出来是因为这两个被多处引用（store、state、reconcile），
而 retain/restore 只在 rebuild 里用。拆开后 platform 不再依赖 pipeline。
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
