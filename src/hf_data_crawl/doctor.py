"""数据目录体检：采集与对账是否按时、Raw 有没有断日、有没有残留的临时文件。

为什么需要
----------
计划任务失败或被错过时不会有任何痕迹：笔记本关机错过 03:10，任务不补跑，
`runs/` 里也不会多出一条失败记录——因为进程根本没起来。「没有记录」本身就是
故障信号，只有拿当前时间去比最近一次成功运行的时间才看得出来。

只读：不改任何数据，唯一的写入是 `ALERT.txt`（有问题时写、恢复后删），
让人不必特意运行命令也能在目录里看到告警。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from .lock import LOCK_NAME, LockBusy, describe_holder, exclusive
from .rebuild import discover_raw_dates
from .runs import read_runs
from .store import DatasetStore

ALERT_NAME = "ALERT.txt"

#: 日增量按天跑，留一天半的余量；对账按周跑，留两天余量。
COLLECT_MAX_AGE = timedelta(hours=36)
RECONCILE_MAX_AGE = timedelta(days=9)
#: 回看多少天的 Raw 是否断日。
GAP_WINDOW_DAYS = 14
#: 临时文件超过这个年纪才算残留；正在写的文件只会存在几秒。
TMP_STALE_AFTER = timedelta(hours=1)
#: 回看多久内的失败运行。
FAILURE_WINDOW = timedelta(days=7)

#: 算「完成」的停止原因。有界试跑（max_pages）与 dry_run 不是完成。
_COLLECT_DONE = frozenset({"end", "watermark"})
_RECONCILE_DONE = frozenset({"end"})


@dataclass(frozen=True)
class Finding:
    """一条体检结论。`problem` 为真才会让退出码非零、并写入告警文件。"""

    problem: bool
    text: str


def _last_success(runs: list[dict], command: str, done_reasons: frozenset[str]) -> datetime | None:
    latest: datetime | None = None
    for item in runs:
        if item.get("command") != command or item.get("status") != "success":
            continue
        payload = item.get("payload") or {}
        if payload.get("stopped_reason") not in done_reasons:
            continue
        try:
            finished = datetime.fromisoformat(str(item.get("finished_at")))
        except ValueError:
            continue
        if latest is None or finished > latest:
            latest = finished
    return latest


def _freshness(
    runs: list[dict],
    now: datetime,
    command: str,
    done_reasons: frozenset[str],
    max_age: timedelta,
    label: str,
) -> Finding:
    last = _last_success(runs, command, done_reasons)
    if last is None:
        return Finding(True, f"{label}：从未成功完成过")
    age = now - last
    hours = age.total_seconds() / 3600
    if age > max_age:
        return Finding(True, f"{label}：已 {hours:.0f} 小时没有成功完成（上次 {last:%m-%d %H:%M}）")
    return Finding(False, f"{label}：正常（{hours:.0f} 小时前，{last:%m-%d %H:%M}）")


def _raw_gaps(store: DatasetStore, today: date) -> Finding:
    dates = set(discover_raw_dates(store))
    if not dates:
        return Finding(True, "Raw：没有任何分区")
    start = max(min(dates), today - timedelta(days=GAP_WINDOW_DAYS - 1))
    missing = [
        start + timedelta(days=offset)
        for offset in range((today - start).days)
        if start + timedelta(days=offset) not in dates
    ]
    if missing:
        shown = "、".join(day.isoformat() for day in missing[:5])
        more = f" 等 {len(missing)} 天" if len(missing) > 5 else ""
        return Finding(True, f"Raw 断日：{shown}{more}没有采集（那天的观测点已丢失）")
    return Finding(False, f"Raw：近 {GAP_WINDOW_DAYS} 天没有断日")


def _stale_temp_files(store: DatasetStore, now: datetime) -> Finding:
    stale = [
        path
        for path in store.root.rglob("*.tmp")
        if now - datetime.fromtimestamp(path.stat().st_mtime) > TMP_STALE_AFTER
    ]
    if stale:
        return Finding(True, f"残留临时文件 {len(stale)} 个（写入中途被打断），例：{stale[0]}")
    return Finding(False, "没有残留的临时文件")


def _failure_reason(item: dict) -> str:
    """失败原因。旧版本只把它写在 `payload.error`，`reason` 为空，两处都要看。"""
    payload = item.get("payload")
    fallback = payload.get("error") if isinstance(payload, dict) else None
    return str(item.get("reason") or fallback or "未记录原因")


def _recent_failures(runs: list[dict], now: datetime) -> Finding:
    """近 7 天里**尚未被后来的成功运行覆盖**的失败。

    同一命令失败后又成功了（例如对账断网后重跑），问题已经解决，再报只会让
    告警文件长期有效而失去信号价值。
    """
    last_success: dict[str, str] = {}
    for item in runs:
        if item.get("status") == "success":
            last_success[str(item.get("command"))] = str(item.get("started_at") or "")

    failed = []
    for item in runs:
        if item.get("status") != "failed":
            continue
        try:
            started = datetime.fromisoformat(str(item.get("started_at")))
        except ValueError:
            continue
        if now - started > FAILURE_WINDOW:
            continue
        if last_success.get(str(item.get("command")), "") > str(item.get("started_at")):
            continue
        failed.append(item)
    if failed:
        last = failed[-1]
        return Finding(
            True,
            f"近 7 天有 {len(failed)} 次未恢复的失败运行，最近一次："
            f"{last.get('command')} {last.get('started_at')} {_failure_reason(last)}",
        )
    return Finding(False, "近 7 天没有未恢复的失败运行")


def _lock_state(store: DatasetStore) -> Finding:
    if not (store.root / LOCK_NAME).exists():
        return Finding(False, "写锁：空闲")
    try:
        with exclusive(store.root, "doctor"):
            pass
    except LockBusy:
        return Finding(False, f"写锁：占用中（{describe_holder(store.root)}）")
    return Finding(False, "写锁：空闲")


def diagnose(store: DatasetStore, now: datetime | None = None) -> list[Finding]:
    now = now or datetime.now()
    runs = read_runs(store)
    return [
        _freshness(runs, now, "collect", _COLLECT_DONE, COLLECT_MAX_AGE, "日增量采集"),
        _freshness(runs, now, "reconcile", _RECONCILE_DONE, RECONCILE_MAX_AGE, "全量对账"),
        _raw_gaps(store, now.date()),
        _recent_failures(runs, now),
        _stale_temp_files(store, now),
        _lock_state(store),
    ]


def sync_alert(store: DatasetStore, findings: list[Finding], now: datetime) -> None:
    """有问题就写 `ALERT.txt`，没有就删掉，让这个文件的存在本身就是信号。"""
    path = store.root / ALERT_NAME
    problems = [item.text for item in findings if item.problem]
    if not problems:
        path.unlink(missing_ok=True)
        return
    lines = [f"hf-data-crawl 体检发现 {len(problems)} 个问题（{now:%Y-%m-%d %H:%M}）：", ""]
    lines += [f"- {text}" for text in problems]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
