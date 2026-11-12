"""探测结论到状态的映射：`unknown` 不在表内，表示「先别动」。"""

from __future__ import annotations

from datetime import date

from hf_data_crawl.acquire.reconcile import STATUS_BY_VERDICT
from hf_data_crawl.platform.vocab import Status

DAY = date(2026, 11, 1)


def test_探测结论映射到状态() -> None:
    assert STATUS_BY_VERDICT == {
        "deleted": Status.DISAPPEARED,
        "restricted": Status.RESTRICTED,
        "renamed": Status.RENAMED,
        "present": Status.ACTIVE,
    }


def test_unknown结论不在映射内表示先别动() -> None:
    assert "unknown" not in STATUS_BY_VERDICT
