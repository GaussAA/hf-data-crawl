"""测试公共的 pytest 夹具。

**为什么每个用例都要「假装工作区干净」**：交付闸门会查 `src/` 有没有未提交改动
（DECISIONS #110）。测试本来就跑在开发者改了一半的工作区里——不假装干净，改一行代码
就有几十个用例变红，而红的不是被测行为。

**只替换 `dirty_paths()`，不替换 `worktree_state()`**：后者还负责组装「查没查」
「干净不干净」「哪几处」三段结论，整套用例里没有一处测它——档案例子会变成
「夹具常量等于夹具常量」。脏场景由 `test_delivery.py` 的两处显式用例覆盖。
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from hf_data_crawl.delivery import method


@pytest.fixture(autouse=True)
def clean_worktree(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(method, "dirty_paths", lambda: [])


@pytest.fixture(autouse=True)
def isolate_env() -> Iterator[None]:
    """每个用例都从同一份 `os.environ` 出发，跑完原样还原。

    `platform.env.load_env_file()` 是**直接写 `os.environ`** 的（它要把值传给子进程），
    而若干用例会经命令层把**开发者本机的 `.env`** 载进来。那份文件不入库、每台机器
    一份：本机的 `HF_FILL_ENABLED=0` 曾漏到后面的用例里，让「兜底段该不该跑」这类
    断言单独跑全绿、全量跑必红。

    这类泄漏不报错、只是换个顺序就变脸，比真 bug 更难查。夹具放在最外层统一兜住，
    比在几十个用例里各写一遍 `monkeypatch.delenv` 可靠。
    """
    saved = dict(os.environ)
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)
