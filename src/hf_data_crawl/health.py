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
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .atomic import write_text

if TYPE_CHECKING:
    from collections.abc import Iterable

    from .doctor import Finding
    from .store import DatasetStore

HEALTH_NAME = "health-history.jsonl"
#: 保留最近多少次体检。每日两次（09:00 + 登录后）约合 250 天。
HEALTH_KEEP = 500


@dataclass(frozen=True)
class HealthRecord:
    """一次体检的结论摘要。"""

    at: datetime
    total: int
    problems: tuple[str, ...]

    @property
    def healthy(self) -> bool:
        return not self.problems


def path_of(store: DatasetStore) -> Path:
    return store.state_dir / HEALTH_NAME


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
                )
            )
        except (KeyError, TypeError, ValueError):
            continue
    return records


def record(store: DatasetStore, findings: Iterable[Finding], now: datetime) -> HealthRecord:
    """追加一次体检结论，并按 `HEALTH_KEEP` 修剪。

    整份重写而非追加：文件只有几十 KB，而「读—修剪—原子替换」能保证任何时刻
    读到的都是一份完整可解析的流水；`open(..., "a")` 在断电时会留下半行，
    下一次读取要为此容错到永远。
    """
    items = list(findings)
    problems = tuple(item.text for item in items if item.problem)
    entry = HealthRecord(at=now, total=len(items), problems=problems)

    history = [*read(store), entry][-HEALTH_KEEP:]
    write_text(
        store.state_dir / HEALTH_NAME,
        "\n".join(
            json.dumps(
                {
                    "at": item.at.isoformat(timespec="seconds"),
                    "total": item.total,
                    "problems": list(item.problems),
                },
                ensure_ascii=False,
            )
            for item in history
        )
        + "\n",
    )
    return entry


def streak(store: DatasetStore, keyword: str) -> int:
    """某个关键词连续出现了几次（从最近一次往回数）。

    典型用法是回答「断日连续几天了」：关键词在最近几行里连续命中即为连续，
    中间有一行没命中就断。给不出结论时返回 0，而不是猜。
    """
    count = 0
    for item in reversed(read(store)):
        if any(keyword in text for text in item.problems):
            count += 1
        else:
            break
    return count
