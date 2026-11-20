"""测试公共的 pytest 夹具。

**为什么每个用例都要「假装工作区干净」**：交付闸门会查 `src/` 有没有未提交改动
（DECISIONS #110）。测试本来就跑在开发者改了一半的工作区里——不假装干净，改一行代码
就有几十个用例变红，而红的不是被测行为。

**只替换 `dirty_paths()`，不替换 `worktree_state()`**：后者还负责组装「查没查」
「干净不干净」「哪几处」三段结论，整套用例里没有一处测它——档案例子会变成
「夹具常量等于夹具常量」。脏场景由 `test_delivery.py` 的两处显式用例覆盖。
"""

from __future__ import annotations

import pytest

from hf_data_crawl.delivery import method


@pytest.fixture(autouse=True)
def clean_worktree(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(method, "dirty_paths", lambda: [])
