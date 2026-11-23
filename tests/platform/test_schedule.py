"""班次配置的契约：起跑点、收工点、是否跑兜底段。

这三条以前是散在两处的硬编码（计划任务的 `00:30`、代码里的 `FILL_UNTIL = "00:20"`），
改一处另一处不动就会悄悄错位。测试盯的就是这个。
"""

from __future__ import annotations

from datetime import time

import pytest

from hf_data_crawl.platform.schedule import daily_start, fill_enabled, fill_until


def test_缺省起跑点是凌晨半点() -> None:
    """没有配置时保持历史行为，部署脚本不必先写 .env 才能跑。"""
    assert daily_start(env={}) == time(0, 30)


def test_起跑点可配置() -> None:
    assert daily_start(env={"HF_DAILY_START": "04:00"}) == time(4, 0)


def test_起跑点容许不补零() -> None:
    assert daily_start(env={"HF_DAILY_START": "9:05"}) == time(9, 5)


@pytest.mark.parametrize("value", ["25:00", "12:60", "凌晨四点", "04-00"])
def test_起跑点写错就报错(value: str) -> None:
    """不回落默认值：静默回到 00:30 等于「以为错开了、其实没有」。"""
    with pytest.raises(ValueError):
        daily_start(env={"HF_DAILY_START": value})


def test_收工点比起点早十分钟() -> None:
    assert fill_until(env={"HF_DAILY_START": "04:00"}) == "03:50"


def test_收工点跨零点() -> None:
    """00:05 减十分钟是前一天 23:55；用 `time` 直接算会因小时为负炸掉。"""
    assert fill_until(env={"HF_DAILY_START": "00:05"}) == "23:55"


def test_缺省配置下收工点仍是00_20() -> None:
    """与 #177 前的行为对齐：起跑 00:30、收工 00:20。"""
    assert fill_until(env={}) == "00:20"


def test_收工点可调间隔() -> None:
    from datetime import timedelta

    assert fill_until(env={"HF_DAILY_START": "04:00"}, lead=timedelta(minutes=25)) == "03:35"


def test_兜底段缺省是开的() -> None:
    """单机部署时它就是该跑的，默认关掉会让 files 永远不动。"""
    assert fill_enabled(env={}) is True


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "on"])
def test_开关的真值写法(value: str) -> None:
    assert fill_enabled(env={"HF_FILL_ENABLED": value}) is True


@pytest.mark.parametrize("value", ["0", "false", "False", "no", "off"])
def test_开关的假值写法(value: str) -> None:
    """`bool("0")` 是 `True`，所以这里绝不能按 Python 的真值规则判。"""
    assert fill_enabled(env={"HF_FILL_ENABLED": value}) is False


def test_开关写错就报错() -> None:
    with pytest.raises(ValueError):
        fill_enabled(env={"HF_FILL_ENABLED": "maybe"})
