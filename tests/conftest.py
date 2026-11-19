"""测试公共的 pytest 夹具。

**为什么每个用例都要「假装工作区干净」**：交付闸门会查 `src/` 有没有未提交改动
（DECISIONS #110）。测试本来就跑在开发者改了一半的工作区里——不假装干净，改一行代码
就有几十个用例变红，而红的不是被测行为。想测闸门本身的用例，自己把状态改成脏
（见 `test_delivery.py::test_脏代码不许交付`），别指望这里开的口子。
"""

from __future__ import annotations

import pytest

from hf_data_crawl.delivery import method


@pytest.fixture(autouse=True)
def clean_worktree(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        method,
        "worktree_state",
        lambda: {"checked": True, "clean": True, "paths": [], "note": ""},
    )
