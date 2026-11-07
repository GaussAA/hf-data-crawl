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

import polars as pl

from . import lineage, raw, retention
from .lock import LOCK_NAME, LockBusy, describe_holder, exclusive
from .rebuild import discover_raw_dates
from .retention import (
    ARCHIVE_AFTER_DAYS,
    BACKUP_PRUNE_AFTER_DAYS,
    REPLAY_WINDOW_DAYS,
)
from .runs import COLLECT_COMPLETE_REASONS, read_runs
from .store import DatasetStore
from .tables import table

ALERT_NAME = "ALERT.txt"

#: 日增量按天跑，留一天半的余量；对账已改为每日，同口径留一天半——
#: 再松就会把「昨天那轮没跑成」当成正常，而这正是底库变旧的开始。
COLLECT_MAX_AGE = timedelta(hours=36)
RECONCILE_MAX_AGE = timedelta(hours=36)
#: 回看多少天的 Raw 是否断日。
GAP_WINDOW_DAYS = 14
#: 临时文件超过这个年纪才算残留；正在写的文件只会存在几秒。
TMP_STALE_AFTER = timedelta(hours=1)
#: Raw 是唯一不可重算、也是唯一会无限增长的一层。归档只压同一天内的重复，
#: 跨天全量快照仍会累积；超过阈值说明归档没跟上或 backup 没清。
RAW_MAX_BYTES = 5 * 1024**3
RAW_MAX_FILES = 20_000
#: 回看多久内的失败运行。
FAILURE_WINDOW = timedelta(days=7)
#: 底库行龄的告警线（按 P95）。全量对账已改为每日，理想值 0-1 天；
#: 留到 7 天是兜底——超过它说明每日对账已连续多天没跑成，底库在拿旧值充数。
STALENESS_ALERT_DAYS = 7

#: 有界试跑（max_pages）与 dry_run 不是完成；collect 的完成态与 cli 共用一份枚举。
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


def _catalog_staleness(store: DatasetStore, today: date) -> Finding:
    """底库有多旧：按 `last_seen_at` 算行龄。

    为什么不能只看「全量对账几天前跑成功过」：那条只说明任务跑没跑，不说明库里
    每行是哪天的值。日增量按 `lastModified` 走，实测一天只覆盖约 6%——一个两年
    没更新的库永远不会进入增量范围，它的 downloads 却每天都在变。长尾全靠全量
    对账刷新；对账连着几天没成时，运行记录仍可能是「成功」，而底库已经在拿几天
    前的值充数。这里直接量行龄，不猜。
    """
    frame = store.read_current()
    if frame.height == 0:
        return Finding(True, "底库新鲜度：current 为空")

    ages = (
        frame.select(
            (pl.lit(today) - pl.col("last_seen_at").dt.date()).dt.total_days().alias("age")
        )
        .get_column("age")
        .drop_nulls()
    )
    if ages.len() == 0:
        return Finding(True, "底库新鲜度：current 没有 last_seen_at，无法判断")

    p50 = int(ages.quantile(0.5, interpolation="nearest") or 0)
    p95 = int(ages.quantile(0.95, interpolation="nearest") or 0)
    # max() 的类型标注是宽联合（date/timedelta 都可能），先收窄再取整。
    oldest_value = ages.max()
    oldest = int(oldest_value) if isinstance(oldest_value, int | float) else 0
    text = f"底库新鲜度：中位 {p50} 天 · P95 {p95} 天 · 最旧 {oldest} 天"
    if p95 > STALENESS_ALERT_DAYS:
        return Finding(
            True,
            f"{text}（P95 超过 {STALENESS_ALERT_DAYS} 天：全量对账已多日未跑成，底库在拿旧值充数）",
        )
    return Finding(False, text)


def _raw_gaps(store: DatasetStore, today: date) -> Finding:
    # 冷区分区已被 compact 移出 data/raw，但那天确实采过：
    # 部分压缩后残留的早分区会把窗口拉长，已压缩的日子不登记就被误判成断日。
    dates = set(discover_raw_dates(store)) | retention.read_compressed_dates(store)
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


def _derive_state(store: DatasetStore) -> Finding:
    """派生层有没有跟上 Raw。

    这是全项目唯一直接回答「派生表是不是旧值」的检查。此前只能靠人回忆
    「上次 build 是什么时候」，而 Raw 可以被 `archive` 重分片、被补采追加、
    被手工修数据——重写之后派生层不会自己更新，也没有任何文件时间能证明
    它落后（派生文件常常比 Raw 更晚，因为别的日子刚跑过）。

    判据取 `lineage` 的戳记：Raw 指纹变了而戳记没变，就是落后。没有戳记
    按「无凭证」报——它与落后在执行上是同一件事：都得跑一次派生才有据可查。
    """
    statuses = lineage.compare(store, discover_raw_dates(store))
    if not statuses:
        return Finding(False, "派生凭证：没有可重放的 Raw 日期")

    stale = [item.day for item in statuses if item.state == lineage.STALE]
    unverified = [item.day for item in statuses if item.state == lineage.UNVERIFIED]
    ok = len(statuses) - len(stale) - len(unverified)

    if stale:
        shown = "、".join(day.isoformat() for day in stale[:5])
        more = f" 等 {len(stale)} 天" if len(stale) > 5 else ""
        return Finding(
            True,
            f"派生滞后：{shown}{more} 的 Raw 已被重写，派生层仍是旧值；"
            "跑 hf-data-crawl build --stale",
        )
    if unverified:
        shown = "、".join(day.isoformat() for day in unverified[:5])
        more = f" 等 {len(unverified)} 天" if len(unverified) > 5 else ""
        return Finding(
            True,
            f"派生无凭证：{shown}{more} 没有派生戳记，无法证明跟着 Raw 走；"
            "跑 hf-data-crawl build --stale 建立基线",
        )
    return Finding(False, f"派生凭证：{ok} 天均与 Raw 一致")


def _ids(store: DatasetStore, name: str) -> set[str] | None:
    """一张分区表里的全部 id；表不存在返回空集，读取失败返回 None（跳过该检查）。

    只扫 id 一列：`silver` 两百多万行、`observations` 同量级，读全列会让体检
    从秒级掉到十秒级，而这里只需要知道「某个 id 在不在」。
    """
    spec = table(name)
    if not any(store.root.glob(spec.glob())):
        return set()
    pattern = f"{store.root.as_posix()}/{spec.glob()}"
    try:
        frame = pl.scan_parquet(pattern).select("id").unique().collect()
    except (OSError, pl.exceptions.PolarsError):
        return None
    return set(frame.get_column("id").to_list())


def _layer_reconcile(store: DatasetStore) -> Finding:
    """层间勾稽：同一批数据集在 current / observations / silver 里是否对得上。

    这三张表回答的是同一件事的不同面——当前态、指标史、属性史。任一侧多出
    或少了 id，都不会在查询时报错：少的那侧在 `datasets` 宽视图里表现为
    整片 null（左连接以 current 为基底），多的那侧则永远查不出来。

    最后一条比的是日期：当前态的最晚 `last_seen_at` 落后于最新 Raw 日，
    说明有 Raw 采进来了却没有任何一行进入当前态——典型成因是派生没跑，
    或跑的是别的日期。
    """
    current = store.read_current()
    if current.height == 0:
        return Finding(True, "层间勾稽：current 为空，无法比对")

    current_ids = set(current["id"].to_list())
    problems: list[str] = []
    notes: list[str] = []

    observation_ids = _ids(store, "observations")
    if observation_ids is None:
        notes.append("观测流水读取失败，已跳过")
    else:
        missing = current_ids - observation_ids
        orphan = observation_ids - current_ids
        if missing:
            problems.append(f"{len(missing)} 个数据集在 observations 里没有观测来源")
        if orphan:
            problems.append(f"{len(orphan)} 个数据集有观测却不在 current 里")
        notes.append(f"观测覆盖 {len(observation_ids)} 个 id")

    silver_ids = _ids(store, "silver")
    if silver_ids is not None:
        no_attribute = current_ids - silver_ids
        if no_attribute:
            problems.append(f"{len(no_attribute)} 个数据集在 silver 里没有属性行（宽表这些行为空）")
        notes.append(f"属性覆盖 {len(silver_ids)} 个 id")

    latest_raw = max(discover_raw_dates(store), default=None)
    newest_seen = current["last_seen_at"].max()
    if isinstance(newest_seen, date) and latest_raw is not None and newest_seen < latest_raw:
        problems.append(
            f"当前态最新只到 {newest_seen}，而 Raw 已到 {latest_raw}（采了却没进 current）"
        )

    if problems:
        return Finding(True, "层间勾稽：" + "；".join(problems))
    detail = "，".join(notes)
    return Finding(False, f"层间勾稽：current {len(current_ids)} 个 id 与各层一致（{detail}）")


def _raw_growth(store: DatasetStore) -> Finding:
    """盯住 Raw 规模：compact 压跨天重复，热区仍会逐日增长，靠阈值兜底。"""
    files = (
        [path for path in store.raw_dir.rglob("*.json.gz") if path.is_file()]
        if store.raw_dir.exists()
        else []
    )
    total = sum(path.stat().st_size for path in files)
    if len(files) > RAW_MAX_FILES or total > RAW_MAX_BYTES:
        return Finding(
            True,
            f"Raw 规模超阈值：{len(files)} 个文件 / {total / 1024**3:.1f} GB"
            f"（阈值 {RAW_MAX_FILES} 个 / {RAW_MAX_BYTES / 1024**3:.0f} GB）；"
            "检查 compact 是否在跑、backup 是否清理",
        )
    return Finding(False, f"Raw 规模：{len(files)} 个文件 / {total / 1024**2:.0f} MB（阈值内）")


def _cold_backlog(store: DatasetStore, today: date) -> Finding:
    """超过重放窗口仍留在 data/raw 的分区：compact 没跟上，冷区未压缩。"""
    cutoff = today - timedelta(days=REPLAY_WINDOW_DAYS)
    pending = [day for day in discover_raw_dates(store) if day < cutoff]
    if pending:
        shown = "、".join(day.isoformat() for day in pending[:5])
        more = f" 等 {len(pending)} 天" if len(pending) > 5 else ""
        return Finding(
            True,
            f"冷区未压缩：{shown}{more} 超过 {REPLAY_WINDOW_DAYS} 天仍在 data/raw"
            "（默认 build 不再重放它们，却仍占主目录）；请跑 hf-data-crawl compact",
        )
    return Finding(False, f"Raw 冷区：{REPLAY_WINDOW_DAYS} 天前的分区均已压缩或不在主目录")


def _archive_backlog(store: DatasetStore, today: date) -> Finding:
    """超过保留窗口仍是原始分块：写入序未冻结、同天重复未压缩。"""
    if not store.raw_dir.exists():
        return Finding(False, "Raw 归档：无分区")
    cutoff = today - timedelta(days=ARCHIVE_AFTER_DAYS)
    pending: list[date] = []
    for path in sorted(store.raw_dir.glob("dt=*")):
        if not path.is_dir():
            continue
        try:
            day = date.fromisoformat(path.name.removeprefix("dt="))
        except ValueError:
            continue
        if day >= cutoff:
            continue
        files = list(path.glob("*.json.gz"))
        if files and not all(raw.is_archive(item) for item in files):
            pending.append(day)
    if pending:
        shown = "、".join(day.isoformat() for day in pending[:5])
        more = f" 等 {len(pending)} 天" if len(pending) > 5 else ""
        return Finding(
            True,
            f"Raw 归档积压：{shown}{more} 超过 {ARCHIVE_AFTER_DAYS} 天仍是原始分块"
            "（写入序未冻结进 groups、同天重复未压缩）；请跑 hf-data-crawl archive",
        )
    return Finding(False, f"Raw 归档：{ARCHIVE_AFTER_DAYS} 天前的分区均已归档")


def _backup_backlog(store: DatasetStore, now: datetime) -> Finding:
    """archive 只移不删；backup 长期不清理，磁盘就没有真正释放。"""
    backup_root = store.root / "backup" / "raw"
    if not backup_root.exists():
        return Finding(False, "Raw 备份：无待清理分区")
    limit = timedelta(days=BACKUP_PRUNE_AFTER_DAYS)
    stale = [
        path
        for path in backup_root.glob("dt=*")
        if path.is_dir() and now - datetime.fromtimestamp(path.stat().st_mtime) > limit
    ]
    if stale:
        shown = "、".join(path.name.removeprefix("dt=") for path in stale[:5])
        return Finding(
            True,
            f"backup/raw 有 {len(stale)} 个分区超过 {BACKUP_PRUNE_AFTER_DAYS} 天未清理（{shown}）；"
            "归档不删原文件，确认派生无误后按 RECOVERY.md 人工清理",
        )
    return Finding(False, f"Raw 备份：无超过 {BACKUP_PRUNE_AFTER_DAYS} 天的待清理分区")


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
        _freshness(runs, now, "collect", COLLECT_COMPLETE_REASONS, COLLECT_MAX_AGE, "日增量采集"),
        _freshness(runs, now, "reconcile", _RECONCILE_DONE, RECONCILE_MAX_AGE, "全量对账"),
        _catalog_staleness(store, now.date()),
        _layer_reconcile(store),
        _raw_gaps(store, now.date()),
        _derive_state(store),
        _raw_growth(store),
        _cold_backlog(store, now.date()),
        _archive_backlog(store, now.date()),
        _backup_backlog(store, now),
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
