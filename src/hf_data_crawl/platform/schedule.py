"""班次配置：每日链几点起跑、兜底段在哪些机器上跑。

为什么要有这一份
----------------
同一套代码会同时部署在多台机器上（一台夜里关机、一台常开），上游配额与「谁该干
重活」都得按机器区分。把起跑时刻放在环境变量里，每台机器各写一份 `.env`，代码
保持一份——这比在两台机器上各自改一行硬编码更不容易漂。

**起跑点与兜底段收工点必须同源**。它们曾经是一对独立的硬编码（`setup-schedule.ps1`
的 `00:30` 与 `refresh.py` 的 `FILL_UNTIL = "00:20"`），后果是改了一处、另一处仍按
旧值算：把任务挪到 04:00，兜底段还是「次日 00:20 收工」，只能跑 20 小时；挪到
22:00，它会在 00:20 收工，只跑两小时。现在收工点由起点减 `FILL_LEAD` 算出来。
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from datetime import datetime, time, timedelta

from .deadline import parse_clock

#: 每日链的默认起跑时刻。与历史行为一致，缺省不动。
DEFAULT_DAILY_START = time(0, 30)

#: 兜底段比次日起点提前多久收工。
#:
#: 十分钟是给收尾合并留的：它要拿短锁把几千个分片并进 `enrich/files.parquet`，
#: 并完之后 `collect` 才好在同一个 `api` 桶上起跑——`fetch-files` 与
#: `collect --full-scan` 打的是同一个桶（1000 次 / 300 秒），重叠就是两倍超发，
#: 双方一起 429、各自退避几十秒（2026-11-22 实测过这个形状）。
DEFAULT_FILL_LEAD = timedelta(minutes=10)

ENV_DAILY_START = "HF_DAILY_START"
ENV_FILL_ENABLED = "HF_FILL_ENABLED"

#: 关掉兜底段时认的写法。不用 `bool(value)`：`bool("0")` 是 `True`，那个坑太深。
_FALSE_WORDS = frozenset({"0", "false", "no", "off", ""})
_TRUE_WORDS = frozenset({"1", "true", "yes", "on"})


def _env(env: Mapping[str, str] | None) -> Mapping[str, str]:
    return os.environ if env is None else env


def daily_start(*, env: Mapping[str, str] | None = None) -> time:
    """每日链的起跑时刻。

    写错就抛 `ValueError`，**不回落默认值**：一个拼错的时间会让整条链悄悄回到
    00:30，而「以为错开了、其实没有」正是这套配置要解决的事。
    """
    text = _env(env).get(ENV_DAILY_START, "").strip()
    if not text:
        return DEFAULT_DAILY_START
    try:
        return parse_clock(text)
    except ValueError as error:
        raise ValueError(f"{ENV_DAILY_START} 需为 HH:MM（当前 {text!r}）：{error}") from error


def fill_until(*, env: Mapping[str, str] | None = None, lead: timedelta | None = None) -> str:
    """兜底段的收工钟点，`HH:MM`，供 `--until` 用。

    返回的是**钟点**而不是日期：`resolve_deadline` 把它理解成「今天这个点，过了取
    明天」，于是 04:00 起跑时传 03:50，自然落到次日 03:50——正是想要的那一天。

    跨零点要靠 `timedelta` 取模，不能直接减：00:05 减十分钟是前一天 23:55，
    用 `time` 的算术会因为「小时为负」直接炸掉。
    """
    start = daily_start(env=env)
    moment = datetime.combine(datetime(2000, 1, 1).date(), start) - (lead or DEFAULT_FILL_LEAD)
    return moment.strftime("%H:%M")


def fill_enabled(*, env: Mapping[str, str] | None = None) -> bool:
    """本机是否跑兜底段（files + preview 的全量补抓）。

    缺省**开**：单机部署时它就是该跑的。多机部署里只留一台，其余在 `.env` 写
    `HF_FILL_ENABLED=0`——兜底段会吃掉链尾到次日起点之间的全部空窗，两台一起跑
    只是把同一份配额劈成两半。

    认 `0/false/no/off` 与 `1/true/yes/on`，大小写不敏感；别的一律报错。
    """
    text = _env(env).get(ENV_FILL_ENABLED, "").strip().lower()
    if not text:
        return True
    if text in _FALSE_WORDS:
        return False
    if text in _TRUE_WORDS:
        return True
    raise ValueError(f"{ENV_FILL_ENABLED} 只认 0/1、true/false、yes/no、on/off（当前 {text!r}）")
