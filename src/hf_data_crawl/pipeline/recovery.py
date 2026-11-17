"""派生崩溃恢复：发现超时的派生标记，清掉残留并从该日起后缀重放。

标记的写与清是 `marker` 里的底层原语，`derive` / `rebuild` 都要用；恢复要调
`rebuild` 做重放，所以恢复不能放进 `marker`，否则 `derive → marker → rebuild →
derive` 成环。依赖只许一个方向：`recovery → rebuild → derive → marker`，
由 `tests/test_architecture.py` 守护。

原理、窗口与不覆盖的场景见 `marker` 模块文档串。
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING

from . import marker
from .rebuild import rebuild_from_raw

if TYPE_CHECKING:
    from ..platform.store import DatasetStore


def _elapsed_since(started_at: str, now: datetime) -> float | None:
    """解析标记时间戳，返回已过秒数；格式坏一律当损坏返回 None。

    历史或人工写入的 `started_at` 可能没有时区，而 `now` 是 aware UTC：两者
    直接相减会抛 `TypeError`。这段检查跑在 `_execute` 之外，抛出去就是「所有
    写命令启动即崩、没有 failed run、没有堆栈」，故必须在这里兜住。naive 一律
    按 UTC 解释——`write_marker` 写的正是 UTC。
    """
    try:
        started = datetime.fromisoformat(started_at)
    except (TypeError, ValueError):
        return None
    if started.tzinfo is None:
        started = started.replace(tzinfo=UTC)
    return (now - started).total_seconds()


def _span(entry: marker.Marker) -> list[date]:
    """标记覆盖的每一天（闭区间）。"""
    return [
        entry.since + timedelta(days=offset)
        for offset in range((entry.until - entry.since).days + 1)
    ]


def check_and_recover(store: DatasetStore) -> list[date]:
    """检查所有派生标记，恢复未完成的派生。返回**确实恢复成功**的日期列表。

    标记是闭区间：逐日派生写 `[D, D]`，`rebuild` 写整段。范围标记先处理——它覆盖的
    逐日标记属于同一次崩溃，逐个再恢复一遍只是把同一段后缀重放 N 次。
    """
    if not store.state_dir.exists():
        return []

    recovered: list[date] = []
    covered: list[marker.Marker] = []
    now = datetime.now(UTC)

    entries: list[tuple[Path, marker.Marker]] = []
    for path in store.state_dir.glob("marker-*.marker"):
        entry = marker.load_marker(path)
        if entry is None:
            # 损坏的标记文件，直接删除
            path.unlink(missing_ok=True)
            continue
        entries.append((path, entry))

    # 起点早的先处理：范围标记的起点是它覆盖的所有逐日标记里最早的。
    entries.sort(key=lambda item: (item[1].since, -item[1].until.toordinal()))

    for path, entry in entries:
        if any(span.since <= entry.since and entry.until <= span.until for span in covered):
            path.unlink(missing_ok=True)
            continue
        elapsed = _elapsed_since(entry.started_at, now)
        if elapsed is None:
            path.unlink(missing_ok=True)
            continue
        if elapsed <= marker.STALE_THRESHOLD_SECONDS:
            continue

        # 超过阈值：先把这一段残留的派生层清掉，再视 Raw 是否还在决定是否重派生。
        # 清在这里而不是只交给 `rebuild_from_raw`，是因为重放可能死在清空之前
        # （retain / ReplayCarry.load 抛错），那时半成品分区还留着。
        days = _span(entry)
        for day in days:
            marker.clear_day(store, day)
        pending = [day for day in days if marker.has_raw(store, day)]

        ok = True
        if pending:
            ok = _rederive(store, entry.since)
        if ok:
            path.unlink(missing_ok=True)
            covered.append(entry)
            # 报「这一段处理过了」，而不是只报重放过的日子：无 Raw 的那几天虽然只是
            # 清残留，也属于本次恢复的成果，调用方那句「无 Raw 的仅清理」靠它才成立。
            recovered.extend(_span(entry))
        else:
            # 失败可能发生在首个 `derive_day` 之前（retain / clear_derived /
            # ReplayCarry.load），那时并没有新标记续命。补写并保留原 started_at：
            # 标记不能先删，否则该日派生层已清却再无人重放，而 lineage 仍报一致。
            marker.write_marker(store, entry.since, until=entry.until, started_at=entry.started_at)

    return recovered


def _rederive(store: DatasetStore, since: date) -> bool:
    """清理后真正重派生：从 `since` 起**后缀重放**，而不是只重算这一天。返回成功与否。

    只重算一天会把 `current` 覆盖成那一天的值：`state._rebuild_current` 用当日帧
    替换同名 id 的全部标量列与 `last_seen_at`。若崩溃那天不是最新 Raw 日，
    「自愈」的结果是当前态整体回退——比不修更坏。后缀重放（`rebuild_from_raw` 收到
    `since` 后自己算的那段）保证最后一天是最新日，`current` 收敛到正确值。

    失败时记下现场但不抛异常，一个坏日期不阻塞其余写命令；返回值让
    `check_and_recover` 把标记留下，下一次写命令立即重试。
    """
    try:
        rebuild_from_raw(store, since=since)
    except Exception as error:
        # 不抛是架构选择，但「静默跳过一天」不能没有现场：打印带日期与异常
        # 类型的一行，由 scripts/_common.ps1 收进 logs/，排障时能看出哪天没修好。
        print(f"[恢复] 自 {since} 起重放失败，保留标记待下次重试：{type(error).__name__}: {error}")
        return False
    return True
