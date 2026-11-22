"""抓取的收工时刻：把 `--until 00:20` 解析成一个绝对的墙钟时刻。

为什么是绝对时刻，而不是「再跑 N 小时」
--------------------------------------
这条参数的用主是刷新链。链每天跑完的时刻并不固定——`fetch-readme --changed-all`
遇上大批变更就是十几个小时，`fetch-sizes` 补全量是三小时。按相对时长算，收工点
会跟着漂，迟早漂过次日 00:30 的链起点，于是长跑与 `collect` 又撞在同一个 `api`
桶上。用绝对时刻，收工点钉死，漂移只发生在起点那一侧（跑得越晚，窗口越短，
这正是想要的）。

为什么还要有跨度上限
--------------------
`HH:MM` 的语义是「今天这个点，过了就取明天」，最长 24 小时。若恰好在 00:10 传
`--until 00:00`，字面意思是次日 00:00——已经贴到链起跑线上了。上限取 23 小时，
就是为了让这种取值也被压回安全区。
"""

from __future__ import annotations

import re
from datetime import datetime, time, timedelta

#: 最长跨度。超过它就不可能是「今晚收工」，要么传错了，要么时钟错乱。
#: 兜这一手是因为它的失败方式很难看：跑到次日 00:30 与链抢配额，双方一起 429。
MAX_SPAN = timedelta(hours=23)

#: 只给时分、不给日期。允许 `9:05` 这种不补零的写法——手敲命令行时很自然。
_TIME_ONLY = re.compile(r"^(\d{1,2}):(\d{2})$")


class DeadlineError(ValueError):
    """`--until` 的取值既不是 `HH:MM`，也不是 ISO 时刻。"""


def resolve_deadline(spec: str, *, now: datetime | None = None) -> datetime:
    """把 `--until` 的取值解析成绝对时刻。

    - `HH:MM`：今天该时刻；若今天已经过去（或正好相等），取**明天**。
      相等也取明天，是因为「现在就到点」没有意义，那种调用只该用 Ctrl+C。
    - ISO 时刻（`2026-11-23T00:20`）：原样采用，带时区的先折算成本地墙钟。
    - 结果距 `now` 超过 `MAX_SPAN` 时夹到 `now + MAX_SPAN`。

    `now` 可注入，便于测试「过了点就顺延到次日」这类跟时钟走的判断。
    """
    moment = now or datetime.now()
    text = spec.strip()
    match = _TIME_ONLY.match(text)
    if match:
        hour, minute = int(match.group(1)), int(match.group(2))
        if not (0 <= hour < 24 and 0 <= minute < 60):
            raise DeadlineError(f"--until 的时刻不合法：{spec!r}")
        target = datetime.combine(moment.date(), time(hour, minute))
        if target <= moment:
            target += timedelta(days=1)
    else:
        try:
            target = datetime.fromisoformat(text)
        except ValueError as error:
            raise DeadlineError(
                f"--until 需为 HH:MM 或 ISO 时刻（如 00:20 / 2026-11-23T00:20）：{spec!r}"
            ) from error
        if target.tzinfo is not None:
            target = target.astimezone().replace(tzinfo=None)
    return min(target, moment + MAX_SPAN)


def deadline_epoch(spec: str, *, now: datetime | None = None) -> float:
    """同上，但直接给出 epoch 秒——`collect_rows` 判到点用的是 `time.time()` 口径。"""
    return resolve_deadline(spec, now=now).timestamp()
