"""交付档案里的表头：指纹与差异。

这两个函数原先住在 `backfill.py`，只因「表头」这个概念最早出现在回填。可档案层
也要用它比「这次的输入表和上次一样吗」——于是档案层不得不 import 回填模块，只为了
一个纯函数。搬到独立模块后，谁都能用，谁都不必认识回填。
"""

from __future__ import annotations

import hashlib
from typing import Any


def header_digest(columns: tuple[str, ...]) -> str:
    """表头指纹：列名与顺序。加一列、删一列、换个次序，指纹都会变。"""
    return hashlib.sha256("\n".join(columns).encode("utf-8")).hexdigest()


def header_change(old: tuple[str, ...], new: tuple[str, ...]) -> dict[str, Any]:
    """两份表头的差异：加列、减列、共同列是否挪了位。

    不试图给出「最小移动序列」：人要的是「哪几列不一样」，不是「怎么挪最省」，
    后者算得再准也没人按它搬。
    """
    old_set, new_set = set(old), set(new)
    common_old = [name for name in old if name in new_set]
    common_new = [name for name in new if name in old_set]
    return {
        "added": [name for name in new if name not in old_set],
        "removed": [name for name in old if name not in new_set],
        "reordered": common_old != common_new,
    }
