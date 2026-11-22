"""`--until` 的解析契约：顺延、拒绝、封顶。

这三件事都跟时钟有关，而时钟出错的形状最难看出来——收工点算错一小时，长跑就会
跨过次日的链起点，与 `collect` 抢同一个 `api` 桶，两边一起 429。
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from hf_data_crawl.platform.deadline import (
    MAX_SPAN,
    DeadlineError,
    deadline_epoch,
    resolve_deadline,
)

#: 固定的「现在」：晚上 23:00，正好卡在「今天还没过、明天还早」的位置。
NOW = datetime(2026, 11, 22, 23, 0)


def test_还没到的钟点取今天() -> None:
    assert resolve_deadline("23:50", now=NOW) == datetime(2026, 11, 22, 23, 50)


def test_已经过的钟点顺延到明天() -> None:
    """`--until 00:20` 在 23:00 传进来，说的是明天 00:20，不是今天凌晨那个。"""
    assert resolve_deadline("00:20", now=NOW) == datetime(2026, 11, 23, 0, 20)


def test_正好等于当前时刻也顺延并被封顶() -> None:
    """相等取明天——「现在就到点」没有意义，那种调用只该按 Ctrl+C。

    而「明天同一刻」正好是 24 小时，会撞上 23 小时的封顶，落到 22:00。
    两道规则在这里同时生效，缺一条就会贴到次日链起跑线上。
    """
    assert resolve_deadline("23:00", now=NOW) == NOW + MAX_SPAN


def test_不补零的钟点也认() -> None:
    """手敲命令行时写 `9:05` 很自然，不该因此报错。"""
    assert resolve_deadline("9:05", now=NOW) == datetime(2026, 11, 23, 9, 5)


@pytest.mark.parametrize("spec", ["25:00", "12:60", "12:5x", "abc", ""])
def test_非法取值直接拒绝(spec: str) -> None:
    """静默落回「不限时」是最坏的结果：长跑会一直跑下去，而这条参数就是为了让它停。"""
    with pytest.raises(DeadlineError):
        resolve_deadline(spec, now=NOW)


def test_ISO时刻原样采用() -> None:
    assert resolve_deadline("2026-11-23T02:30", now=NOW) == datetime(2026, 11, 23, 2, 30)


def test_跨度封顶() -> None:
    """超过 23 小时的死线一律压回来。

    兜的是「时钟错乱」和「手滑写了明天同一时刻」：正常 `HH:MM` 最长 24 小时，
    恰好能贴到次日链起跑线上；封顶之后最多只到 23 小时。
    """
    target = datetime(2026, 11, 30, 12, 0)

    assert resolve_deadline(target.isoformat(), now=NOW) == NOW + MAX_SPAN


def test_封顶不影响正常取值() -> None:
    """封顶不该把「明天 00:20」这类正常调用也压掉。"""
    resolved = resolve_deadline("00:20", now=NOW)

    assert resolved < NOW + MAX_SPAN


def test_epoch口径与解析一致() -> None:
    """`collect_rows` 判到点用的是 `time.time()`，两边必须是同一个钟。"""
    resolved = resolve_deadline("00:20", now=NOW)

    assert deadline_epoch("00:20", now=NOW) == pytest.approx(resolved.timestamp())
    assert deadline_epoch("00:20", now=NOW) - NOW.timestamp() == pytest.approx(
        timedelta(hours=1, minutes=20).total_seconds()
    )
