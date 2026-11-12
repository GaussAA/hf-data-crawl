"""状态词表：恰好四值，非在架集合不含 active。"""

from __future__ import annotations

from datetime import date

from hf_data_crawl.platform.vocab import INACTIVE, Status

DAY = date(2026, 11, 1)


def test_词表恰好四值() -> None:
    assert {status.value for status in Status} == {
        "active",
        "disappeared",
        "restricted",
        "renamed",
    }


def test_非在架集合不含active() -> None:
    assert Status.ACTIVE not in INACTIVE
    assert INACTIVE == {Status.DISAPPEARED, Status.RESTRICTED, Status.RENAMED}
