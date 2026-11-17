"""体检历史：每次 `doctor` 落一行，让「哪天开始不对劲」可查。

为什么需要
----------
体检结论原本只有两个去处：终端（看完即散）与 `ALERT.txt`（覆盖式，只存最新一次）。
于是「断日是从哪天开始的」「这个告警连续多少天了」永远答不上来——而这两问恰恰
决定要不要动手。`runs/` 不能承担：它记的是「命令成没成功」，而 `doctor` 退出码 1
是「有告警」而非「命令失败」，混进去会污染「近 7 天无失败运行」这条检查。

所以单独一份流水：一行一次，只存时间、问题条数与问题原文。体积按次计（十几条
文本），保留 `HEALTH_KEEP` 次后按时间修剪——上限存在的意义与 `runs/` 相同：
防止目录随计划任务无限增长。

只记问题不记全量结论
--------------------
正常项每天都在变（小时数、文件数、行龄），存下来只会把「哪天开始坏」淹在噪音里。
要复看某天的完整结论，取同日 `logs/` 的 `时间戳-doctor.log`——两份同键，一份是
流水、一份是现场，与 `runs/`+`logs/` 的分工同一个道理。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from typing import TYPE_CHECKING

from ..platform.atomic import write_text
from ..platform.lock import exclusive

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..platform.store import DatasetStore
    from .finding import Finding

HEALTH_NAME = "health-history.jsonl"
#: 保留最近多少次体检。每日两次（09:00 + 登录后）约合 250 天。
HEALTH_KEEP = 500

#: 「读—修剪—整份重写」的互斥锁，与主数据锁分开。两个 doctor 并发时各读到
#: 同一份历史、各写一行，后写者会静默抹掉先写者；而降级的 doctor 本就拿不到
#: 主锁却仍要留痕（`snapshot=False` 也进流水），故不能用主锁来保护这一步。
WRITE_LOCK_NAME = ".doctor-write.lock"
WRITE_LOCK_WAIT = 2.0


@dataclass(frozen=True)
class HealthRecord:
    """一次体检的结论摘要。"""

    at: datetime
    total: int
    problems: tuple[str, ...]
    #: 与 `problems` 一一对应的检查项标识。文案随时会改，按它计数才不会断代；
    #: 2026-11-15 之前的旧记录没有这一项，读作空——那些轮次不参与连续计数。
    codes: tuple[str, ...] = ()
    #: 这次体检是否在一份一致快照上跑；旧记录没有该字段，读作 True。
    snapshot: bool = True

    @property
    def healthy(self) -> bool:
        return not self.problems


def read(store: DatasetStore) -> list[HealthRecord]:
    """读全部历史，按时间升序。坏行跳过——体检流水坏掉不该拖垮体检本身。"""
    target = store.state_dir / HEALTH_NAME
    if not target.exists():
        return []
    records: list[HealthRecord] = []
    try:
        text = target.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return []
    for line in text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
            records.append(
                HealthRecord(
                    at=datetime.fromisoformat(str(payload["at"])),
                    total=int(payload.get("total", 0)),
                    problems=tuple(str(item) for item in payload.get("problems", [])),
                    codes=tuple(str(item) for item in payload.get("codes", [])),
                    snapshot=bool(payload.get("snapshot", True)),
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return records


def record(
    store: DatasetStore,
    findings: Iterable[Finding],
    now: datetime,
    *,
    snapshot: bool = True,
) -> HealthRecord:
    """追加一次体检结论，并按 `HEALTH_KEEP` 修剪。

    整份重写而非追加：文件只有几十 KB，而「读—修剪—原子替换」能保证任何时刻
    读到的都是一份完整可解析的流水；`open(..., "a")` 在断电时会留下半行，
    下一次读取要为此容错到永远。读—修剪—写三段必须整体互斥（见
    `WRITE_LOCK_NAME`），否则原子替换只保证文件完整、不保证不丢更新。
    """
    items = list(findings)
    # 降级项不进流水：这次读数可能跨时点，它「是不是问题」还没定。记进去会让
    # 「连续红了几轮」虚高，而这正是用来判断「要不要现在动手」的那个数。
    live = [item for item in items if item.counts]
    entry = HealthRecord(
        at=now,
        total=len(items),
        problems=tuple(item.text for item in live),
        codes=tuple(item.code for item in live),
        snapshot=snapshot,
    )

    with exclusive(store.root, "health-write", WRITE_LOCK_WAIT, lock_name=WRITE_LOCK_NAME):
        history = [*read(store), entry][-HEALTH_KEEP:]
        write_text(
            store.state_dir / HEALTH_NAME,
            "\n".join(
                json.dumps(
                    {
                        "at": item.at.isoformat(timespec="seconds"),
                        "total": item.total,
                        "problems": list(item.problems),
                        "codes": list(item.codes),
                        "snapshot": item.snapshot,
                    },
                    ensure_ascii=False,
                )
                for item in history
            )
            + "\n",
        )
    return entry


def streak(store: DatasetStore, code: str) -> int:
    """某个检查项连续几**天**是红的（从最近一次往回数）。

    按 `code` 数，不按文案子串。原先的写法是 `any(keyword in text for text in
    item.problems)`——文案里混进日期或数字就匹配不上，而它断得**不报错**，只是
    从此永远返回 0。按检查项标识计数，文案怎么改都不影响。

    按**自然日**去重，不按体检次数：流水是一行一次 `doctor`，而 `doctor` 一天可能
    跑好几次（改完代码验一次、排障再跑一次）。按次数数会得出「连续 6 轮」，读起来
    像红了六天——2026-11-15 当天就跑了六次，差点把这句话写进告警。
    """
    days = 0
    last_day: date | None = None
    for item in reversed(read(store)):
        if not item.snapshot:
            # 降级轮（没拿到写锁）不写任何 code——它既不证明红、也不证明恢复。
            # 直接往下走会把「撞了一次写锁」当成「告警消失了」，连续天数当场归零，
            # `sync_alert` 的「已连续 N 天」后缀随之消失，看着像刚坏。
            continue
        if code not in item.codes:
            break
        day = item.at.date()
        if day != last_day:
            days += 1
            last_day = day
    return days


def red_days(store: DatasetStore, code: str, now: datetime) -> int:
    """把「本次」也算进去的连续天数。

    `streak` 数的是流水里已有的记录；本次体检还没写进流水，所以：

    - 今天已经红过一次 → `streak` 已含今天，不重复加；
    - 今天是第一次红 → 本次是新的一天，加一。

    分开算而不是让 `streak` 猜，是因为「今天到底算不算新的一天」取决于本次这条
    结论还没落盘这个事实——那是调用方知道的，不是流水能推出来的。
    """
    today = now.date()
    already = any(record.at.date() == today and code in record.codes for record in read(store))
    return streak(store, code) + (0 if already else 1)
