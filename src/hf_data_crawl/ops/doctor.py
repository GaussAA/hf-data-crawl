"""数据目录体检：采集与对账是否按时、Raw 有没有断日、有没有残留的临时文件。

为什么需要
----------
计划任务失败或被错过时不会有任何痕迹：笔记本关机错过 03:10，任务不补跑，
`runs/` 里也不会多出一条失败记录——因为进程根本没起来。「没有记录」本身就是
故障信号，只有拿当前时间去比最近一次成功运行的时间才看得出来。

只读：不改任何表。写入只有两处，都在命令层——`ALERT.txt`（有问题时写、恢复后删，
让人不必特意运行命令也能在目录里看到告警）与体检流水 `state/health-history.jsonl`
（`cli._doctor` 每次追加一行，回答「哪天开始不对劲」）。
"""

from __future__ import annotations

import hashlib
import statistics
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from ..acquire.readme import stale_readme_ids
from ..acquire.reconcile import PROBE_DEGRADE_AFTER
from ..acquire.sizes import stale_ids
from ..delivery.delivery import (
    deliveries_of,
    index_problem,
    method,
    pruned_facts,
    read_manifest,
)
from ..delivery.fields import (
    COLUMN_SOURCES,
    COLUMN_TYPES,
    DEFAULT_COLUMNS,
    contract_errors,
    type_family,
)
from ..delivery.requirement import (
    HISTORY_DIR,
    SPEC_NAME,
    SpecError,
    request_dirs,
    version_conflict,
)
from ..delivery.requirement import load as load_spec
from ..delivery.wide import export_path
from ..pipeline import lineage, raw, state
from ..pipeline.rebuild import discover_raw_dates, snapshot_health, snapshot_path
from ..platform.atomic import write_text
from ..platform.lock import read_holder
from ..platform.retention import (
    ARCHIVE_AFTER_DAYS,
    BACKUP_KEEP_DAYS,
    REPLAY_WINDOW_DAYS,
)
from ..platform.runs import COLLECT_COMPLETE_REASONS, read_runs
from ..platform.store import DatasetStore
from ..platform.tables import table
from ..query.gold import share_text
from .finding import Finding
from .health import red_days

ALERT_NAME = "ALERT.txt"

#: 日增量按天跑，留一天半的余量；对账已改为每日，同口径留一天半——
#: 再松就会把「昨天那轮没跑成」当成正常，而这正是底库变旧的开始。
COLLECT_MAX_AGE = timedelta(hours=36)
#: 全量对账的时限。日报里 `reconcile` 带 `--max-pages 600`（整站约 1127 页），
#: 两天才走完一轮；按 36 小时判会每天亮一次「对账过期」的假红。
RECONCILE_MAX_AGE = timedelta(hours=48)
#: 回看多少天的 Raw 是否断日。
GAP_WINDOW_DAYS = 14
#: Raw 某日的分块数低于近期中位的这个比例，就认为那天没采全。实测 11-11..11-18
#: 稳定在 1,155–1,171（波动 1.4%），90% 的线抓得住「只跑了一半」，又不会被
#: 个别块的加减触发。
RAW_VOLUME_FLOOR = 0.9
#: 算中位与判异常的回看天数。取 7：一周足够盖住工作日的常规波动。
RAW_VOLUME_WINDOW_DAYS = 7
#: 临时文件超过这个年纪才算残留；正在写的文件只会存在几秒。
TMP_STALE_AFTER = timedelta(hours=1)
#: Raw 是唯一不可重算、也是唯一会无限增长的一层。归档只压同一天内的重复，
#: 跨天全量快照仍会累积；超过阈值说明归档没跟上或 backup 没清。
RAW_MAX_BYTES = 5 * 1024**3
RAW_MAX_FILES = 20_000
#: 单轮对账的探测失败占比超过此值即告警。取 20%：偶发限流不该天天喊，
#: 成片失败必须说话——那些库的状态会静默停在旧值。
PROBE_FAIL_SHARE = 0.2
#: 回看多久内的失败运行。
FAILURE_WINDOW = timedelta(days=7)
#: 底库行龄的告警线（按 P95）。全量对账已改为每日，理想值 0-1 天；
#: 留到 7 天是兜底——超过它说明每日对账已连续多天没跑成，底库在拿旧值充数。
STALENESS_ALERT_DAYS = 7

#: 富化表允许的最长不更新天数。规模表按周补、卡片按 enrich 节奏，超过这个天数
#: 说明那条链路已经停了，而不是「它本来就慢」。
ARTIFACT_MAX_AGE_DAYS = 7

#: 最近一轮全站对账该覆盖全库。低于此值即判不合格：那说明这一轮扫描没扫完
#: （被截断、失败或没跑），而不是个别库慢半拍。基准取「最近一次成功对账的
#: 快照日」，不取日历上的今天——链条 03:10 才跑，零点到那时之间必须不算陈旧。
FIELD_SWEEP_SHARE = 0.90

#: 富化表的年龄红线：默认 7 天。`files` 是周日任务、且只补下载量前 1000 名的首屏，
#: 给到 10 天——漏跑一次不至于立刻喊，连丢一周才说明这条链路真停了。
TABLE_MAX_AGE_DAYS: dict[str, int] = {"files": 10}

#: 「活跃库」的判定窗口：日增量按 lastModified 扫到水位线，最近一天基本覆盖；
#: 留 3 天是给漏跑与补跑留余量，而不把长尾算进卡片凭证的落后比例。
ACTIVE_DAYS = 3
#: 活跃库里卡片对不上当前 sha 的告警比例。判据不是「必须有变化」——变化本来就是
#: 常态，卡片抓在 commit 之后、下一轮增量之前，短暂不一致是正常的；超过这个比例
#: 说明整趟没跑，而不是个别库在窗口边缘。
CARD_BEHIND_ALERT_SHARE = 20.0

#: README 全文（`readme` / `readme_fields`）的刷新时限。它不在任何一条链里：全量
#: 那趟 21 小时、抽字段 6 分钟，都只能人工择机跑。定时任务按周刷新，这里给两个
#: 周期——连着两周没动才算「没人管」，漏跑一次不至于喊。
README_MAX_AGE_DAYS = 14

#: 有界试跑（max_pages）与 dry_run 不是完成；collect 的完成态与 cli 共用一份枚举。
_RECONCILE_DONE = frozenset({"end"})


@dataclass(frozen=True)
class RunContext:
    """一次体检看到的世界：读一次，各处共用。

    21 项检查原先各收自己要的那几样——`(store, today)`、`(runs, now)`、或者只有
    `(requests_root)`——于是两个再简单不过的问题都答不上来：「这次体检读了哪些表」
    要翻五个函数体，「陈旧按哪一刻算的」要在 `diagnose` 的 18 行里逐行核对。

    读数只做一次。`current` 112 万行，读一次 0.08 秒，重复四次也才 0.3 秒——
    所以这不是性能优化，是让「体检看了什么」有唯一答案。真正的收益在读代码的人：
    一个值里看全，而不是跟着 21 个签名各猜一遍。

    这不是「把参数打包」，而是**把共享输入收成一处**：谁需要什么、以哪个时刻为准，
    从此写在这个类型上，而不是散在 21 个签名里。新增一项检查只写判定，取数不必
    重来一遍。
    """

    store: DatasetStore
    #: 判定用的时刻。时序类判据（几小时没跑、几天没动）一律取它。
    now: datetime
    runs: list[dict[str, Any]]
    #: 底库全表。为空时各检查自己判「无法比对」，这里不做统一裁决。
    current: pl.DataFrame
    current_ids: frozenset[str]
    #: 需求目录在数据目录之外，由调用方显式传入；没传就如实说没查。
    requests_root: Path | None = None

    @property
    def today(self) -> date:
        """日历日。**只给**「某张表最老停在哪天」这类判据用。

        「新鲜不新鲜」不能用它：链条每天 03:10 才跑，零点到那时之间拿今天当尺子
        必然全红（2026-11-15 01:40 实测踩过这个坑）。那种判据一律以「最近一次
        成功对账的快照日」为基准，见 `_last_sweep_date`。
        """
        return self.now.date()


def _context(store: DatasetStore, now: datetime, requests_root: Path | None) -> RunContext:
    """把一次体检要用的东西读齐。所有读表都在这里发生，检查函数不再自己读。"""
    current = state.read_current(store)
    return RunContext(
        store=store,
        now=now,
        runs=read_runs(store),
        current=current,
        current_ids=frozenset(current["id"].to_list()) if current.height else frozenset(),
        requests_root=requests_root,
    )


def _run_time(value: object) -> datetime | None:
    """把 runs/ 里的时间戳统一成 naive UTC；坏值、空值返回 None。

    `runs.write_run` 用 `datetime.now()` 写本地无时区时间，而外部注入或未来
    版本可能带 `Z` / `+00:00`。混用两种形态做减法会抛 TypeError；更隐蔽的是
    拿 ISO 字符串做字典序比较——精度或时区写法一变，「谁在谁之后」就反了。
    """
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC).replace(tzinfo=None)
    return parsed


def _last_success(
    runs: list[dict[str, Any]], command: str, done_reasons: frozenset[str]
) -> datetime | None:
    latest: datetime | None = None
    for item in runs:
        if item.get("command") != command or item.get("status") != "success":
            continue
        payload = item.get("payload") or {}
        if payload.get("stopped_reason") not in done_reasons:
            continue
        finished = _run_time(item.get("finished_at"))
        if finished is None:
            continue
        if latest is None or finished > latest:
            latest = finished
    return latest


def _freshness(
    runs: list[dict[str, Any]],
    now: datetime,
    command: str,
    done_reasons: frozenset[str],
    max_age: timedelta,
    label: str,
) -> Finding:
    last = _last_success(runs, command, done_reasons)
    if last is None:
        return Finding(True, f"{label}：从未成功完成过")
    age = (_run_time(now) or now) - last
    hours = age.total_seconds() / 3600
    if age > max_age:
        return Finding(True, f"{label}：已 {hours:.0f} 小时没有成功完成（上次 {last:%m-%d %H:%M}）")
    return Finding(False, f"{label}：正常（{hours:.0f} 小时前，{last:%m-%d %H:%M}）")


def _catalog_staleness(ctx: RunContext) -> Finding:
    """底库有多旧：按 `last_seen_at` 算行龄。

    为什么不能只看「全量对账几天前跑成功过」：那条只说明任务跑没跑，不说明库里
    每行是哪天的值。日增量按 `lastModified` 走，实测一天只覆盖约 6%——一个两年
    没更新的库永远不会进入增量范围，它的 downloads 却每天都在变。长尾全靠全量
    对账刷新；对账连着几天没成时，运行记录仍可能是「成功」，而底库已经在拿几天
    前的值充数。这里直接量行龄，不猜。
    """
    today = ctx.today
    frame = ctx.current
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


#: 已确认且决定不补的 Raw 断日 → 理由。
#:
#: 写进这张表等于「承认这个洞，别再天天喊」：体检仍会把它印出来，但不判红、不进
#: ALERT.txt。加日期前先想清楚——这张表一旦变成「所有告警的坟墓」，告警就废了。
#: 判据是「补不回来」或「补的成本与失真不可接受」，不是「不想修」。
KNOWN_RAW_GAPS: dict[date, str] = {
    date(2026, 11, 6): "当天未开机、未采集；补造观测点等于伪造数据，故不补"
}


def _raw_gaps(ctx: RunContext) -> Finding:
    store, today = ctx.store, ctx.today
    # 冷区分区已被 compact 移出 data/raw，但那天确实采过：
    # 部分压缩后残留的早分区会把窗口拉长，已压缩的日子不登记就被误判成断日。
    dates = {day.snapshot_date for day in raw.observed_days(store)}
    if not dates:
        return Finding(True, "Raw：没有任何分区")
    start = max(min(dates), today - timedelta(days=GAP_WINDOW_DAYS - 1))
    missing = [
        start + timedelta(days=offset)
        for offset in range((today - start).days)
        if start + timedelta(days=offset) not in dates
    ]
    known = [day for day in missing if day in KNOWN_RAW_GAPS]
    unknown = [day for day in missing if day not in KNOWN_RAW_GAPS]
    if unknown:
        shown = "、".join(day.isoformat() for day in unknown[:5])
        more = f" 等 {len(unknown)} 天" if len(unknown) > 5 else ""
        return Finding(True, f"Raw 断日：{shown}{more}没有采集（那天的观测点已丢失）")
    if known:
        # 已知缺口只印不判：天天红只会训练人忽略告警，而这条已经补不回来了。
        shown = "、".join(day.isoformat() for day in known)
        reason = KNOWN_RAW_GAPS[known[0]]
        return Finding(False, f"Raw 断日：{shown} 为已知缺口，{reason}")
    return Finding(False, f"Raw：近 {GAP_WINDOW_DAYS} 天没有断日")


def _raw_volume(ctx: RunContext) -> Finding:
    """某一天的 Raw 分块数是否明显少于近期常态。

    与 `_raw_gaps` 互补：那条只抓「整天没采」，抓不到「采了一半就断了」——目录
    在、分块在，断日判据看不见。这里补的正是这一格。

    为什么不用观测行数判：`observations` 只追加指标真变过的行，行数少往往说明
    上游计数没动（11-14 只有 2,188 行，而当日 Raw 完整、1,160 块）。拿它当采集
    完整度会天天误报，且误报的恰恰是「上游很平静」这种好日子。

    分块数只由采集自己决定：一页一块、每块约一千条，跑到一半断掉，块数就少一半。
    压缩过的日期已并档（数不出块数），今天可能还在采，两者都不判。
    """
    store, today = ctx.store, ctx.today
    counts = [
        (day.snapshot_date, len(raw.batch_files(store, day.snapshot_date)))
        for day in raw.observed_days(store)
        if not day.compacted and day.snapshot_date < today
    ]
    if len(counts) < 4:
        return Finding(False, f"Raw 分量：可比的完整日只有 {len(counts)} 天，不足以比较")
    median = statistics.median(n for _, n in counts)
    if median <= 0:
        return Finding(False, "Raw 分量：近期分块数为零，无法比较")
    floor = median * RAW_VOLUME_FLOOR
    window = [
        (day, n) for day, n in counts if day >= today - timedelta(days=RAW_VOLUME_WINDOW_DAYS)
    ]
    short = [(day, n) for day, n in window if n < floor]
    if short:
        shown = "、".join(f"{day.isoformat()}（{n} 块）" for day, n in short[:3])
        more = f" 等 {len(short)} 天" if len(short) > 3 else ""
        return Finding(
            True,
            f"Raw 分量：{shown}{more} 明显少于近期中位 {median:.0f} 块；"
            "那天可能只采了一部分，核对该日的 collect 与补抓是否中途退出",
        )
    return Finding(False, f"Raw 分量：近 {len(window)} 天分块数与中位 {median:.0f} 块相当")


def _derive_state(ctx: RunContext) -> Finding:
    """派生层有没有跟上 Raw。

    这是全项目唯一直接回答「派生表是不是旧值」的检查。此前只能靠人回忆
    「上次 build 是什么时候」，而 Raw 可以被 `archive` 重分片、被补采追加、
    被手工修数据——重写之后派生层不会自己更新，也没有任何文件时间能证明
    它落后（派生文件常常比 Raw 更晚，因为别的日子刚跑过）。

    判据取 `lineage` 的戳记：Raw 指纹变了而戳记没变，就是落后。没有戳记
    按「无凭证」报——它与落后在执行上是同一件事：都得跑一次派生才有据可查。
    """
    store = ctx.store
    statuses = lineage.compare(store, [day.snapshot_date for day in raw.replayable_days(store)])
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


def _layer_reconcile(ctx: RunContext) -> Finding:
    """层间勾稽：同一批数据集在 current / observations / silver 里是否对得上。

    这三张表回答的是同一件事的不同面——当前态、指标史、属性史。任一侧多出
    或少了 id，都不会在查询时报错：少的那侧在 `datasets` 宽视图里表现为
    整片 null（左连接以 current 为基底），多的那侧则永远查不出来。

    最后一条比的是日期：当前态的最晚 `last_seen_at` 落后于最新 Raw 日，
    说明有 Raw 采进来了却没有任何一行进入当前态——典型成因是派生没跑，
    或跑的是别的日期。
    """
    store, current = ctx.store, ctx.current
    if current.height == 0:
        return Finding(True, "层间勾稽：current 为空，无法比对")

    current_ids = set(ctx.current_ids)
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
    # 说清比的是什么。这条只比 **id 集合**：同名 id 在两层的 13 个标量列是否一致，
    # 它不看。原先写「与各层一致」会让人以为值也对过，而唯一的值比对在
    # `tests/query/test_gold.py` 的一个 4 行夹具里——那种保证不该由体检来暗示。
    return Finding(False, f"层间勾稽：current {len(current_ids)} 个 id 集合与各层一致（{detail}）")


def _scan_frame(pattern: Path | str, columns: list[str]) -> pl.DataFrame | None:
    """只取需要的列（parquet 列裁剪），读失败返回 None。

    富化表里 `carddata` 78 万行、`size` 114 万行，而这里通常只要 id 与日期。
    裁列省的是几十毫秒（2026-11-15 实测：读全列 0.10 / 0.18 秒），不是分钟级——
    先前这里写着「读全列会让体检多花十几秒」，那是没实测的估计，已按实测改正。
    读失败不抛：体检不能因为一份产物坏掉而整体失败。

    **什么时候该用 `store.read` 而不是这里**：`store.read` 会按表契约补齐缺列，
    后加的列（如 `size.status`、`carddata.source_sha`）在旧分区里可能不存在，
    直接 `scan_parquet(...).select([...])` 会抛错——这里的「读失败返回 None」
    就会把「表是旧的」误报成「表不在了」。所以两套读法不是纪律不统一，是两种
    用途：**列一定存在**（id / snapshot_date）用这里快，**需要对齐 schema**
    用 `store.read`。C-1 审视时曾把这条记成摩擦，是误判，此处写明免得再被重提。
    """
    try:
        return pl.scan_parquet(pattern).select(columns).collect()
    except (OSError, pl.exceptions.PolarsError):
        return None


def _covered(frame: pl.DataFrame, current_ids: set[str]) -> int:
    """表里有多少行真的落在底库内（交集口径，唯一算法）。

    分子取交集而不是表内 id 总数：富化表留着已消失的库（卡片曾跟 `lastModified`
    抓过、后来下架），直接数表内 id 会把它们也算成覆盖，覆盖率虚高。

    2026-11-15 体检里「产物新鲜度」与「字段新鲜度」对同一份 `carddata` 报出
    68% 与 68.3%：前者交集、后者只数 `n_unique`。数字不同不是精度差异，
    是同一件事算了两遍、其中一遍算错。两处现都走这里，分母统一用
    `len(current_ids)`——以后再有人加一处覆盖统计，也不会再长出第三个数字。
    """
    if frame.height == 0:
        return 0
    return len(set(frame["id"].to_list()) & current_ids)


def _share_text(ratio: float) -> str:
    """覆盖率的统一写法，全体检只此一种；实现见 `query.gold.share_text`。

    留这个别名不直接改名，是因为体检里四条检查项与它们的测试都按这个名字找；
    真正的格式规则（10% 以上取整、以下一位、不足 0.1% 写 `<0.1%`）只有一处。
    """
    return share_text(ratio)


def _artifact_freshness(ctx: RunContext) -> Finding:
    """富化表与导出物有没有跟上底库。

    为什么单独查：这些文件**都不跟着派生自动更新**。导出物只在跑 `export-wide`
    时刷新——2026-11-08 实测：对账在 04:11 把底库刷到 1,123,235 行，而导出仍是
    前夜 02:33 的 1,123,090 行，差 145 行且没有任何提示；`carddata` 只在 `enrich`
    或 `build` 时重算；`size` / `files` 来自外部接口，重建时刻意保留。它们旧了
    不报错，查询照常返回，只是拿的是旧快照。

    判据分两档，因为两类东西的「应该全」不同：

    - **导出物必须与底库逐 id 一致**。它是给人做分析的宽表，缺一行就可能得出
      相反的结论，所以差集非空即告警。
    - **富化表只报覆盖与最新日期，不因覆盖不全告警**。长尾本来就没补过（按下载量
      取头部是设计），但「整张表超过 `ARTIFACT_MAX_AGE_DAYS` 没动过」是真停了。
    """
    store, today = ctx.store, ctx.today
    current = ctx.current
    if current.height == 0:
        return Finding(True, "产物新鲜度：current 为空，无法比对")
    current_ids = set(ctx.current_ids)
    problems: list[str] = []
    notes: list[str] = []

    export = export_path(store)
    if not export.exists():
        problems.append("宽表导出不存在，跑 export-wide")
    else:
        exported_frame = _scan_frame(export, ["id"])
        if exported_frame is None:
            problems.append("宽表导出读取失败，跑 export-wide 重建")
        else:
            exported = set(exported_frame["id"].to_list())
            missing = len(current_ids - exported)
            surplus = len(exported - current_ids)
            if missing or surplus:
                problems.append(f"宽表导出落后：缺 {missing} 行、多 {surplus} 行（跑 export-wide）")
            else:
                notes.append(f"导出与底库一致（{len(exported)} 行）")

            # 列集勾稽：这份文件是「默认交付字段」的物化，列被手改或仍是旧版
            # 72 列时，查询交付按新契约投列、文件却按旧口径，两者就漂移了。
            schema = pl.scan_parquet(export.as_posix()).collect_schema()
            columns = tuple(schema.names())
            # 列名对了还不够：类型族变了（`tags` 从 VARCHAR 改回 LIST 那种）说明
            # 这份导出是按旧口径写的，同一个列名在两边给出不同的值语义。
            wrong_types = [
                (name, type_family(schema[name]), COLUMN_TYPES[name])
                for name in DEFAULT_COLUMNS
                if name in schema
                and COLUMN_TYPES.get(name) not in (None, type_family(schema[name]))
                and type_family(schema[name]) != "unknown"
            ]
            if columns != DEFAULT_COLUMNS:
                problems.append(
                    f"宽表导出列集与默认交付契约不一致（{len(columns)} vs "
                    f"{len(DEFAULT_COLUMNS)} 列），跑 export-wide 重建"
                )
            elif wrong_types:
                problems.append(
                    f"宽表导出列类型与默认交付契约不一致（列名、实际族、契约族）："
                    f"{wrong_types}，跑 export-wide 重建"
                )
            else:
                notes.append(f"导出列集与默认交付契约一致（{len(columns)} 列）")
        issues = contract_errors()
        if issues:
            problems.append(f"默认交付字段契约不自洽：{'；'.join(issues)}")

    for name, label in (("carddata", "卡片"), ("size", "规模"), ("files", "文件清单")):
        spec = table(name)
        if not any(store.root.glob(spec.glob())):
            notes.append(f"{label}未生成")
            continue
        frame = _scan_frame(f"{store.root.as_posix()}/{spec.glob()}", ["id", "snapshot_date"])
        if frame is None or frame.height == 0:
            notes.append(f"{label}读取失败")
            continue
        covered = _covered(frame, current_ids)
        share_note = _share_text(covered / len(current_ids))
        newest = frame["snapshot_date"].max()
        # `Series.max()` 的类型标注是宽联合（date / bytes / …），先收窄再用。
        if not isinstance(newest, date):
            notes.append(f"{label}覆盖 {share_note}（日期缺失）")
            continue
        notes.append(f"{label}覆盖 {share_note}（最新 {newest}）")
        limit_days = TABLE_MAX_AGE_DAYS.get(name, ARTIFACT_MAX_AGE_DAYS)
        age_days = (today - newest).days
        if age_days > limit_days:
            problems.append(
                f"{label}表已 {age_days} 天没更新（最新 {newest}），"
                f"检查 enrich / fetch-sizes / fetch-files 是否还在跑"
            )

    if problems:
        return Finding(True, "产物新鲜度：" + "；".join(problems))
    return Finding(False, "产物新鲜度：" + "；".join(notes))


def _contract_coverage(ctx: RunContext) -> Finding:
    """默认交付面有没有「整列为空」的列。

    行数、列名、类型族都对，仍可能整列没有值：`size` 有数据而 `server_*` 全空、
    `carddata` 有行而 `pretty_name` 全空——拿导出物做分析的人会以为数据坏了，
    而体检此前全绿。2026-11-04 `private` 整列静默变空是同一个形态，发生在 bronze；
    这项把它挪到交付面上再守一次。

    判据是**整列为空**，不是覆盖率阈值：低覆盖是上游分布（`arxiv_ids` 3.7%、
    `languages` 17%），拿阈值判会天天红，红了就没人看。整列空时再按来源分两种：
    来源表本身没有数据（`size` / `carddata`，计划内还没抓）只报提示；有数据却
    全空才告警——那说明这一列没接上，而不是「上游没有」。

    空列表算空：`languages` 是空列表而不是 NULL 时，对使用者与 NULL 同样不可用。
    """
    store, current = ctx.store, ctx.current
    if current.height == 0:
        return Finding(True, "交付面取值：current 为空，无法判定")
    export = export_path(store)
    if not export.exists():
        # 缺文件由「产物新鲜度」报，这里不重复。
        return Finding(False, "交付面取值：宽表导出不存在，跳过（见产物新鲜度）")
    try:
        schema = pl.scan_parquet(export.as_posix()).collect_schema()
    except (OSError, pl.exceptions.PolarsError):
        return Finding(False, "交付面取值：宽表导出读取失败，跳过（见产物新鲜度）")
    # 列集不符契约时也由「产物新鲜度」报；这里若硬按 35 列去 select，一份少了列的
    # 旧导出会让整个体检抛错——体检不能因为一份产物坏掉而整体失败。
    missing = [column for column in DEFAULT_COLUMNS if column not in schema]
    if missing:
        return Finding(False, f"交付面取值：导出物缺 {len(missing)} 列，跳过（见产物新鲜度）")

    expressions: list[pl.Expr] = [pl.len().alias("__rows")]
    for column in DEFAULT_COLUMNS:
        empty = pl.col(column).is_null()
        if COLUMN_TYPES.get(column) == "list":
            empty = empty | (pl.col(column).list.len() == 0)
        expressions.append(empty.sum().alias(column))
    counted = pl.scan_parquet(export.as_posix()).select(expressions).collect().row(0, named=True)
    rows = int(counted.pop("__rows"))
    if rows == 0:
        return Finding(False, "交付面取值：宽表导出为空，跳过（见产物新鲜度）")

    empty_columns = [column for column in DEFAULT_COLUMNS if counted[column] == rows]
    if not empty_columns:
        worst = max(DEFAULT_COLUMNS, key=lambda column: counted[column])
        # 报**有值的行数**而不是空值百分比：`pretty_name` 常常只有个位数行有值，
        # 取整后会印成「100% 为空」，与同一句里的「35 列都有值」自相矛盾
        # ——2026-11-18 实测就这么印过一次。
        filled = rows - int(counted[worst])
        # 契约列覆盖率不足 1%：按「空值率 > 99% 就该删」这条规则，它们是**下一次
        # 契约重审的待办**，不是数据坏了。所以这里只列名字与实测行数，不给结论。
        thin = [column for column in DEFAULT_COLUMNS if (rows - int(counted[column])) / rows < 0.01]
        if thin:
            detail = "、".join(f"{column}（{rows - int(counted[column]):,} 行）" for column in thin)
            return Finding(
                False,
                f"交付面取值：{len(DEFAULT_COLUMNS)} 列都有值，但 {len(thin)} 列覆盖率不足 1%："
                f"{detail}——按契约规则该在下一次重审时决定去留",
            )
        return Finding(
            False,
            f"交付面取值：{len(DEFAULT_COLUMNS)} 列都有值"
            f"（最空的是 {worst}，{filled:,} / {rows:,} 行有值）",
        )

    blocked: list[str] = []
    pending: dict[str, list[str]] = {}
    for column in empty_columns:
        source = COLUMN_SOURCES.get(column)
        if source is None:
            blocked.append(column)
        elif store.read(source).height == 0:
            pending.setdefault(source, []).append(column)
        else:
            blocked.append(f"{column}（{source} 有数据却全空）")

    pending_text = "；".join(
        f"{source} 未采集，{'、'.join(columns)} 整列为空" for source, columns in pending.items()
    )
    if blocked:
        text = "交付面取值：整列为空 —— " + "、".join(blocked)
        if pending_text:
            text += f"；另有 {pending_text}"
        return Finding(True, text)
    return Finding(False, f"交付面取值：{pending_text}；交付前跑对应采集命令，或注明这几列不提供")


def _last_sweep_date(runs: list[dict[str, Any]]) -> date | None:
    """最近一次**完整**全站对账的快照日；没跑完或没跑过则 None。

    快照日是「那轮本该覆盖到的那一天」，比 `finished_at` 稳：续跑、跨零点、
    补跑都不会让它漂。取不到就退回完成时刻的日期。

    必须同时看 `stopped_reason`：`reconcile --max-pages N` 的退出码是 0、状态是
    success，但它只扫了前 N 页。只认 `status` 会让半轮的 snapshot_date 顶替真正的
    全量轮，成为 `_field_freshness` 的基准——覆盖率随之失真（假红「没扫完」，或把
    此前的全量轮挡在基准之外）。同一判据在 `_freshness` 里已用 `_RECONCILE_DONE`，
    这里原先漏了。
    """
    for item in reversed(runs):
        if item.get("command") != "reconcile" or item.get("status") != "success":
            continue
        payload = item.get("payload") or {}
        if payload.get("stopped_reason") not in _RECONCILE_DONE:
            continue
        raw = payload.get("snapshot_date")
        if raw:
            try:
                return date.fromisoformat(str(raw))
            except ValueError:
                pass
        finished = item.get("finished_at")
        if finished:
            try:
                return datetime.fromisoformat(str(finished)).date()
            except ValueError:
                pass
    return None


def _field_freshness(ctx: RunContext) -> Finding:
    """按字段类别报「新不新」，而不是笼统说「数据新鲜」。

    三类字段的刷新机制不同，揉成一个指标必然误导：

    - **列表字段**（标识、描述、下载量、标签…）：`reconcile` 每轮按 `createdAt`
      把全站翻完并落 raw，故每个周期内全库都该被「见过」一次。判据是「最近一轮
      成功对账覆盖了多少行」——**不能拿日历上的「今天」当尺子**：链条每天 03:10
      才跑，零点到那时之间查一次，全库都还是昨天的日期，会天天误报全红（这正是
      2026-11-15 01:40 实测踩到的）。以最近一次成功对账为基准，既没有这段空窗，
      也能照样逮住「扫到一半就收工」。
    - **卡片字段**：跟着 `lastModified` 变化集抓，覆盖率天然受上游限制——
      2026-11-14 抽 30 个「无卡」库直问 HF，30 个上游同样没有 `cardData`，
      故 68% 是上游分布，不是我们的缺口。真正该盯的是「有卡库的凭证是否
      与当前 sha 一致」，那由 `_card_provenance` 负责，此处只报覆盖。
    - **规模字段**：`fetch-sizes` 的跳过判据只能表达「问过没」，表达不了
      「问过之后它变了没有」。`--changed` 按 `lastModified > snapshot_date`
      重取，这里报还剩多少没跟上、最老的一批停在哪天。**「有行」与「有值」必须
      分开报**：`unavailable`（上游没做 parquet 转换）也占一行，但它的行数、
      字节全是空——2026-11-18 实测两者差 25 个百分点（99.2% vs 73.8%），
      只报前者会让读的人以为行数基本齐全，而实际四分之一是空的。
    """
    store, runs = ctx.store, ctx.runs
    current = ctx.current
    if current.height == 0:
        return Finding(True, "字段新鲜度：current 为空，无法判定")

    sweep = _last_sweep_date(runs)
    if sweep is None:
        return Finding(True, "字段新鲜度：还没有成功的全量对账，无法判定列表字段")
    seen = current.filter(pl.col("last_seen_at") >= sweep).height
    share = seen / current.height

    current_ids = set(ctx.current_ids)
    total = len(current_ids)

    cards = store.read("carddata")
    covered = _covered(cards, current_ids)
    sizes = store.read("size")
    covered_sizes = _covered(sizes, current_ids)
    # 「有行」≠「有值」：`unavailable` 行也占一行，但 num_rows 与字节全是空。
    # 两者相差 25 个百分点（2026-11-18：99.2% vs 73.8%），只报前者等于虚报。
    covered_rows = _covered(
        sizes.filter((pl.col("status") == "ok") & pl.col("num_rows").is_not_null()),
        current_ids,
    )
    # 直接调 `fetch-sizes --changed` 用的那个函数，不在这里复刻一份：复制品与原件
    # 分居两个切片，改了一边不会报错、不会变红，只会静默漂移。原先这里有一份逐字
    # 抄来的 join——「同一判据三处实现」的中间那一份，正是最容易被改坏的那一份。
    stale = stale_ids(current, sizes).height
    newest_size: date | None = None
    if sizes.height:
        newest_size = sizes.select(pl.col("snapshot_date").max()).item()

    text = (
        f"字段新鲜度：列表字段最近一轮（{sweep}）覆盖 {share:.1%}（{seen}/{current.height}）"
        f" · 卡片覆盖 {_share_text(covered / total)}（上游卡片本就不全）"
        f" · 规模 有行 {_share_text(covered_sizes / total)}"
        f" / 有值 {_share_text(covered_rows / total)}（未做 parquet 转换的不计）"
        f"、最新 {newest_size or '无'}"
        f" · 规模待重取 {stale} 个"
    )
    if share < FIELD_SWEEP_SHARE:
        return Finding(
            True,
            text + f"；最近一轮覆盖不足 {FIELD_SWEEP_SHARE:.0%}，那轮全站扫描没扫完",
        )
    return Finding(False, text)


def _card_provenance(ctx: RunContext) -> Finding:
    """卡片落后于仓库多少：拿 `carddata.source_sha` 对 `current.sha`。

    为什么要有这一项：卡片是「仓库里 README 的渲染结果」，仓库一变它就旧了。
    此前没有任何列记「这张卡抓自哪一版」，于是「卡片陈旧」只能靠感觉——`snapshot_date`
    只说抓取时间，不说内容对应哪一版，一个 2025 年抓的卡和昨天抓的卡看起来一样新鲜。

    判据只看**活跃库**（最近 `ACTIVE_DAYS` 天被观测到）：长尾几百万年不动的库，
    卡片本来就该保持原样，把它们算进来会让比例永远难看。活跃库是日增量真正扫过的
    那批，也正是 `collect --cards` 该覆盖的那批；它们对不上，说明那趟没跑。

    历史行 `source_sha` 为空（加列之前抓的），单独计数、不计入落后比例：空值是
    「没记账」，不是「记错了」，混在一起会把补记和漏抓说成同一件事。
    """
    store, today = ctx.store, ctx.today
    pattern = f"{store.root.as_posix()}/{table('carddata').glob()}"
    # 旧表没有 source_sha 列（加列之前抓的），直接按两列 select 会整表读不出来，
    # 那会被误读成「没有卡片」。先看列名，缺了就补一列空值——空值是「没记账」。
    try:
        names = pl.scan_parquet(pattern).collect_schema().names()
    except (OSError, pl.exceptions.PolarsError):
        return Finding(False, "卡片凭证：尚无卡片表，无从对照")
    columns = ["id", "source_sha"] if "source_sha" in names else ["id"]
    card_frame = _scan_frame(pattern, columns)
    if card_frame is None or card_frame.height == 0:
        return Finding(False, "卡片凭证：尚无卡片表，无从对照")
    if "source_sha" not in card_frame.columns:
        card_frame = card_frame.with_columns(pl.lit(None, dtype=pl.Utf8).alias("source_sha"))

    # 从上下文里取，不重读：同一份 current 在一次体检里只该读一遍。
    current = ctx.current.select("id", "sha", "last_seen_at")
    if current.height == 0:
        return Finding(True, "卡片凭证：current 读取失败，无法对照")

    active = current.filter(
        (pl.lit(today) - pl.col("last_seen_at").dt.date()).dt.total_days() <= ACTIVE_DAYS
    )
    joined = card_frame.join(active, on="id", how="inner", suffix="_now")
    if joined.height == 0:
        return Finding(False, "卡片凭证：最近没有活跃库，无从对照")

    recorded = joined.filter(pl.col("source_sha").is_not_null() & (pl.col("source_sha") != ""))
    blank = joined.height - recorded.height
    if recorded.height == 0:
        return Finding(
            True,
            f"卡片凭证：活跃库 {joined.height} 个全无 source_sha（均为加列之前抓的），"
            "跑一次 enrich 或 collect --cards 即可补记",
        )

    behind = recorded.filter(pl.col("source_sha") != pl.col("sha")).height
    share = behind / recorded.height * 100
    tail = f"，另有 {blank} 个无凭证" if blank else ""
    if share > CARD_BEHIND_ALERT_SHARE:
        return Finding(
            True,
            f"卡片凭证：活跃库中 {behind}/{recorded.height}（{share:.0f}%）的卡片落后于"
            f"仓库当前 sha{tail}；检查 collect --cards 是否在跑",
        )
    return Finding(
        False,
        f"卡片凭证：活跃库中 {recorded.height - behind}/{recorded.height}"
        f"（{(1 - behind / recorded.height) * 100:.0f}%）与当前 sha 一致{tail}",
    )


def _readme_fields_behind(store: DatasetStore, readme: pl.DataFrame) -> int:
    """侧栏字段落后多少：`readme_fields.source_sha` 与正文当前的 `source_sha` 比。

    正文换了新版而字段没重抽，是**安静的错误**——查询照样返回 URL、BibTeX，
    只是那些值属于上一版正文。保鲜任务把两步串起来跑，就为堵这一处。
    """
    path = store.path("readme_fields")
    if not path.exists():
        return 0
    extracted = pl.read_parquet(path, columns=["id", "source_sha"]).rename(
        {"source_sha": "extracted_sha"}
    )
    ok_rows = readme.filter(pl.col("status") == "ok").select("id", "source_sha")
    if ok_rows.height == 0:
        return 0
    joined = ok_rows.join(extracted, on="id", how="left")
    return joined.filter(
        pl.col("extracted_sha").is_null() | (pl.col("extracted_sha") != pl.col("source_sha"))
    ).height


def _readme_freshness(ctx: RunContext) -> Finding:
    """README 全文有没有人管：抽过侧栏字段的那份资产会悄悄变旧。

    为什么单列一项：`readme` 与 `readme_fields` 是两次人工触发的产物（全量约 21 小时、
    抽字段约 6 分钟），**不属于任何一条链**。不盯它，就会出现「体检全 ok，而 2.8 GB
    正文已经三周没动」——库里没有任何一处读数会变红，因为没有一处看它。

    判据取「最近一次刷新时刻」，两处取较大者：

    - 表里 `snapshot_date` 的最大值——真写过数据；
    - 最近一次成功的 `fetch-readme` 运行——`--changed` 若找不到陈旧会提前返回、**不写行**，
      只查数据会把它误报成「没刷新」。

    只查数据会误报，只查运行记录会漏报（跑成功但一条没抓），故两者取大。

    另外报三个计数：`陈旧`（commit 已变的库，该重取的量）、`无记录`（新库还没抓过）、
    `侧栏落后`（正文已换版而字段没重抽）。它们只在两次刷新之间自然增长，不单独判红
    ——判红看的是「多久没刷新」这一件事；`侧栏落后` 长期不降，则说明保鲜任务只跑了
    第一步（`fetch-readme`）而漏了第二步（`parse-readme`）。

    **没有 readme 表不算故障**：它与 `carddata` 同为富化产物，日常链不生成它，
    全新安装或刻意不抓都该是绿的。缺表时只报「未抓过」，把红留给「抓过、但没人管了」。
    """
    store = ctx.store
    pattern = f"{store.root.as_posix()}/{table('readme').glob()}"
    try:
        names = pl.scan_parquet(pattern).collect_schema().names()
    except (OSError, pl.exceptions.PolarsError):
        return Finding(False, "README 全文：尚无 readme 表（未抓过，不判新旧）")
    if "snapshot_date" not in names:
        return Finding(True, "README 全文：表里没有 snapshot_date，无法判断新旧")

    frame = _scan_frame(pattern, ["id", "source_sha", "status", "snapshot_date"])
    if frame is None or frame.height == 0:
        return Finding(False, "README 全文：表为空（未抓过，不判新旧）")

    last_run: datetime | None = None
    for item in ctx.runs:
        if item.get("command") != "fetch-readme" or item.get("status") != "success":
            continue
        finished = _run_time(item.get("finished_at"))
        if finished is not None and (last_run is None or finished > last_run):
            last_run = finished

    written = frame["snapshot_date"].max()
    marks: list[datetime] = []
    if isinstance(written, datetime):
        marks.append(written)
    elif isinstance(written, date):
        marks.append(datetime.combine(written, time.min))
    if last_run is not None:
        marks.append(last_run)
    if not marks:
        return Finding(True, "README 全文：既无写入日期也无成功记录，无法判断新旧")

    refreshed = max(marks)
    age_days = (ctx.now - refreshed).days
    stale = stale_readme_ids(ctx.current, frame).height
    missing = len(ctx.current_ids - set(frame["id"].to_list()))
    behind = _readme_fields_behind(store, frame)
    counts = (
        f"陈旧 {stale:,} · 无记录 {missing:,} · 侧栏落后 {behind:,} · 最近刷新 {refreshed:%Y-%m-%d}"
    )

    if age_days > README_MAX_AGE_DAYS:
        return Finding(
            True,
            f"README 全文：已 {age_days} 天没有刷新（{counts}）；"
            "跑 fetch-readme --changed-all，或确认 hf-refresh 的 readme 步骤有没有跑成",
        )
    return Finding(False, f"README 全文：正常（{age_days} 天前刷新，{counts}）")


def _raw_growth(ctx: RunContext) -> Finding:
    """盯住 Raw 规模：compact 压跨天重复，热区仍会逐日增长，靠阈值兜底。"""
    store = ctx.store
    files = (
        [
            path
            for suffix in raw.SUFFIXES
            for path in store.raw_dir.rglob(f"*{suffix}")
            if path.is_file()
        ]
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


def _cold_backlog(ctx: RunContext) -> Finding:
    """超过重放窗口仍留在 data/raw 的分区：compact 没跟上，冷区未压缩。"""
    store, today = ctx.store, ctx.today
    cutoff = today - timedelta(days=REPLAY_WINDOW_DAYS)
    pending = [
        day.snapshot_date for day in raw.replayable_days(store) if day.snapshot_date < cutoff
    ]
    if pending:
        shown = "、".join(day.isoformat() for day in pending[:5])
        more = f" 等 {len(pending)} 天" if len(pending) > 5 else ""
        return Finding(
            True,
            f"冷区未压缩：{shown}{more} 超过 {REPLAY_WINDOW_DAYS} 天仍在 data/raw"
            "（默认 build 不再重放它们，却仍占主目录）；请跑 hf-data-crawl compact",
        )
    return Finding(False, f"Raw 冷区：{REPLAY_WINDOW_DAYS} 天前的分区均已压缩或不在主目录")


def _archive_backlog(ctx: RunContext) -> Finding:
    """超过保留窗口仍是原始分块：写入序未冻结、同天重复未压缩。"""
    store, today = ctx.store, ctx.today
    cutoff = today - timedelta(days=ARCHIVE_AFTER_DAYS)
    pending = [
        day.snapshot_date
        for day in raw.replayable_days(store)
        if day.snapshot_date < cutoff and any(not raw.is_archive(path) for path in day.files)
    ]
    if pending:
        shown = "、".join(day.isoformat() for day in pending[:5])
        more = f" 等 {len(pending)} 天" if len(pending) > 5 else ""
        return Finding(
            True,
            f"Raw 归档积压：{shown}{more} 超过 {ARCHIVE_AFTER_DAYS} 天仍是原始分块"
            "（写入序未冻结进 groups、同天重复未压缩）；请跑 hf-data-crawl archive",
        )
    return Finding(False, f"Raw 归档：{ARCHIVE_AFTER_DAYS} 天前的分区均已归档")


def _storage(ctx: RunContext) -> Finding:
    """存储总账：谁在长，一眼看见。

    只报数、不判断——阈值判断交给各自那一项（备份积压、冷区积压、归档积压）。
    把四层摊开列出来，是为了让「哪天发现磁盘吃紧」时不必再逐个 du：
    raw 是热区（7 天窗口）、cold 是去重后的基线（体量恒定）、backup 是待销毁的
    副本（应当有界）、reports 是诊断产物。

    后两句回答的是「会不会只增不减」：`backup` 的稳态由「保留天数 × 每天搬进多少」
    决定，`observations` 是唯一无界增长的一层（变化才追加，不能压缩、不能采样），
    按最近一天的速率年化。两句都是粗估，但比「等磁盘满了再看」强。
    """
    store = ctx.store
    from .prune import total_size

    def tree(path: Path) -> int:
        if not path.exists():
            return 0
        return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())

    raw_bytes = tree(store.root / "raw")
    cold_bytes = tree(store.root / "cold")
    backup_bytes = total_size(store)
    report_bytes = tree(store.root / "reports")
    whole = tree(store.root)
    other = max(0, whole - raw_bytes - cold_bytes - backup_bytes - report_bytes)

    raw_days = len([path for path in (store.root / "raw").glob("dt=*") if path.is_dir()])
    steady_backup = (raw_bytes / raw_days) * BACKUP_KEEP_DAYS if raw_days else 0
    yearly_obs = _latest_partition_bytes(store, "observations") * 365
    return Finding(
        False,
        f"存储：data {whole / 1e9:.2f} GB（raw {raw_bytes / 1e6:.0f} MB · "
        f"cold {cold_bytes / 1e6:.0f} MB · backup {backup_bytes / 1e6:.0f} MB · "
        f"reports {report_bytes / 1e6:.0f} MB · 其余 {other / 1e6:.0f} MB）；"
        f"按当前速率，backup 稳态约 {steady_backup / 1e9:.1f} GB"
        f"（{BACKUP_KEEP_DAYS} 天 × 每天搬入量）、观测流水每年约 {yearly_obs / 1e9:.1f} GB",
    )


def _latest_partition_bytes(store: DatasetStore, name: str) -> int:
    """某张分区表最新一个分区的字节数；没有分区返回 0。"""
    parts = sorted(store.root.glob(table(name).glob()))
    return parts[-1].stat().st_size if parts else 0


#: `.staging` 是抓取中间产物的落点：分片先写这里，收尾才并进正式表。
#: 一个目录里最新分片超过这么久没动，才算停在半路——全量抓取最长约 21 小时
#: （readme），2 天留足余量，不会把正在跑的批误报成残留。
STAGING_STALE_AFTER = timedelta(days=2)


def _staging_backlog(ctx: RunContext) -> Finding:
    """抓取中间产物有没有停在半路。

    `fetch-sizes` / `fetch-readme` 是「先写分片、收尾才并表」的：进程被杀、机器
    重启、任务被停，分片就留在 `data/.staging/<表>/` 里。它们不进任何表，也不在
    `*.tmp` 的扫描范围，于是「有半成品却没人知道」——与 `.tmp` 那条检查防的是
    同一种事，只是换了个目录。

    **只报不删**：分片是断点续跑的凭据，删掉等于让几十小时的抓取从头再来。
    判断「是否还在跑」看最新分片的 mtime，不看目录本身——空目录或只剩旧分片都
    说明这一批已经停了。
    """
    store, now = ctx.store, ctx.now
    staging = store.root / ".staging"
    if not staging.is_dir():
        return Finding(False, "抓取暂存：无残留")
    stalled: list[str] = []
    for child in sorted(staging.iterdir()):
        if not child.is_dir():
            continue
        files = [path for path in child.rglob("*.parquet") if path.is_file()]
        if not files:
            continue
        try:
            newest = max(datetime.fromtimestamp(path.stat().st_mtime) for path in files)
        except OSError:
            continue
        if now - newest > STAGING_STALE_AFTER:
            stalled.append(
                f"{child.name}（{len(files)} 个分片，最近写入 {(now - newest).days} 天前）"
            )
    if stalled:
        return Finding(
            True,
            f"抓取暂存：{len(stalled)} 处停在半路 —— "
            + "、".join(stalled)
            + "；分片是断点续跑的凭据，确认不再续跑再删 `data/.staging/` 下对应目录",
        )
    return Finding(False, "抓取暂存：无停滞的分片（有批次在跑时也走这条）")


def _backup_backlog(ctx: RunContext) -> Finding:
    """archive / compact 只移不删；销毁归 `prune`，这里只盯它有没有跟上。

    判龄与清理共用 `prune.plan()`：若这里按 mtime、那边按目录名，两边会各说各话
    ——体检报「已清理」而实际没删，或反之。同一条规则只能有一处实现。
    """
    store, now = ctx.store, ctx.now
    from .prune import plan as plan_prune

    targets = plan_prune(store, now=now)
    if not targets:
        return Finding(False, f"备份清理：无超过 {BACKUP_KEEP_DAYS} 天的待清理对象")

    shown = "、".join(item.path.name.removeprefix("dt=") for item in targets[:5])
    reclaimable = sum(item.bytes for item in targets) / 1e6
    kinds = "、".join(sorted({item.kind for item in targets}))
    return Finding(
        True,
        f"{kinds} 有 {len(targets)} 处超过 {BACKUP_KEEP_DAYS} 天未清理（{shown}），"
        f"可释放约 {reclaimable:.1f} MB；跑 hf-data-crawl prune",
    )


def _status_snapshot(ctx: RunContext) -> Finding:
    """状态留存快照：存在即异常，损坏会让非在架状态静默失去凭证。

    `doctor` 只读，不做自动修复（Q10 原议的 `--fix-snapshot`）：坏文件是重建
    被打断的唯一现场证据，删掉等于销毁证据，也修不回已经清空的 state。这里
    只把「静默回退」变成可见告警，并给出分情况的处置指引。
    """
    store = ctx.store
    health = snapshot_health(store)
    path = snapshot_path(store).as_posix()
    if health.kind == "absent":
        return Finding(False, "状态快照：无残留（重建写回后已清理）")
    if health.kind == "corrupt":
        return Finding(
            True,
            f"状态快照：{path} 读不出来（{health.error}）；retain 会静默回退读 current，"
            "若 state 已被清空则非在架状态永久丢失——先备份该文件，核对 current 与"
            "reports/reconcile-*.csv 的非在架记录后人工重建，别直接删",
        )
    invalid = f"，另有 {health.invalid_count} 个词表外的值会被丢弃" if health.invalid_count else ""
    return Finding(
        True,
        f"状态快照：{path} 残留 {health.valid_count} 个非在架状态{invalid}"
        "（重建写回成功后必须删除）；先备份并确认无 build 在跑——current 已空就跑"
        " hf-data-crawl build 完成写回，current 仍完整则人工比对后移走，别让旧快照盖掉新状态",
    )


def _latest_probe_payload(runs: list[dict[str, Any]]) -> dict[str, Any] | None:
    """最近一轮带探测计数的对账摘要；升级前的旧记录没有这些字段，不据以下结论。"""
    latest: dict[str, Any] | None = None
    for item in runs:
        if item.get("command") != "reconcile":
            continue
        payload = item.get("payload")
        if isinstance(payload, dict) and "probe_probed" in payload:
            latest = payload
    return latest


def _probe_quality(ctx: RunContext) -> Finding:
    """最近一轮对账的探测质量。

    探测失败落成 `unknown`，处置是「先别动」——不报错，只是状态原地不动。配额紧
    的时候，一批早已下架的库会长期挂着 `active`，而界面上看不出任何异常。这条把
    它变成可数的结论：探了多少、失败多少、还有多少顺延。
    """
    runs = ctx.runs
    latest = _latest_probe_payload(runs)
    if latest is None:
        return Finding(False, "探测复核：尚无带计数的对账记录（升级后首轮才有）")

    probed = int(latest.get("probe_probed") or 0)
    failed = int(latest.get("probe_failed") or 0)
    deferred = int(latest.get("probe_deferred") or 0)
    if probed == 0 and deferred == 0:
        return Finding(False, "探测复核：本轮没有待复核的库")

    tail = f"，顺延 {deferred:,} 个待下轮" if deferred else ""
    share = failed / probed if probed else 0.0
    if share > PROBE_FAIL_SHARE:
        return Finding(
            True,
            f"探测复核：本轮探 {probed:,} 个、失败 {failed:,}（{share:.0%}）{tail}；"
            "失败即「先别动」，这些库的状态会停在旧值——检查配额与重试退避",
        )
    return Finding(False, f"探测复核：本轮探 {probed:,} 个、失败 {failed:,}{tail}")


def _probe_degraded(ctx: RunContext) -> Finding:
    """连续多轮判不出来的库要单独点名，不能混在单轮失败占比里被稀释。

    单轮失败可能只是限流，下轮优先再探就会好；连续 `PROBE_DEGRADE_AFTER` 轮
    仍无结论，说明该库的状态已长期停在旧值，需要人去核对上游或配额。与
    `_probe_quality` 分开报，是为了让「成片失败」与「个别顽固」各自可见。
    """
    runs = ctx.runs
    latest = _latest_probe_payload(runs)
    if latest is None:
        return Finding(False, "探测顽固失败：尚无带计数的对账记录")
    count = int(latest.get("probe_degraded") or 0)
    if count == 0:
        return Finding(False, "探测顽固失败：无连续失败项")
    return Finding(
        True,
        f"探测顽固失败：{count} 个库连续 {PROBE_DEGRADE_AFTER} 轮拿不到结论，"
        "状态一直停在旧值；见 reports/reconcile-*.csv 的 probe_degraded 行，"
        "核对配额/令牌与该库上游状态",
    )


def _stale_temp_files(ctx: RunContext) -> Finding:
    store, now = ctx.store, ctx.now

    def stale(path: Path) -> bool:
        try:
            return now - datetime.fromtimestamp(path.stat().st_mtime) > TMP_STALE_AFTER
        except OSError:
            return False

    found = [path for path in store.root.rglob("*.tmp") if stale(path)]
    # 交付暂存目录用 `.tmp-` 前缀（先写目录、三件齐了再整体改名），中断时留在
    # `requests/*/deliveries/` 下。原先只扫 `data/`，那批残留永远不会被点名——
    # 而它正是「有数据没说明」的半成品，比 `data/` 下的 `.tmp` 更该被看见。
    if ctx.requests_root is not None and ctx.requests_root.is_dir():
        found += [
            path for path in ctx.requests_root.rglob(".tmp-*") if path.is_dir() and stale(path)
        ]
    if found:
        return Finding(True, f"残留临时文件 {len(found)} 个（写入中途被打断），例：{found[0]}")
    return Finding(False, "没有残留的临时文件")


def _failure_reason(item: dict[str, Any]) -> str:
    """失败原因。旧版本只把它写在 `payload.error`，`reason` 为空，两处都要看。"""
    payload = item.get("payload")
    fallback = payload.get("error") if isinstance(payload, dict) else None
    return str(item.get("reason") or fallback or "未记录原因")


def _recent_failures(ctx: RunContext) -> Finding:
    """近 7 天里**尚未被后来的成功运行覆盖**的失败。

    同一命令失败后又成功了（例如对账断网后重跑），问题已经解决，再报只会让
    告警文件长期有效而失去信号价值。
    """
    runs, now = ctx.runs, ctx.now
    reference = _run_time(now) or datetime.now()
    last_success: dict[str, datetime] = {}
    for item in runs:
        if item.get("status") != "success":
            continue
        stamp = _run_time(item.get("started_at"))
        if stamp is None:
            continue
        command = str(item.get("command"))
        if command not in last_success or stamp > last_success[command]:
            last_success[command] = stamp

    failed = []
    for item in runs:
        if item.get("status") != "failed":
            continue
        # 单次 `request run` 是人在键盘前主动发起的：被闸门拦下、被护栏拦下，他都当场
        # 看见了，且「今天没有新信息」也会走到这里。把它算进全局红点，等于让一次正常的
        # 拒绝报警七天——红点一旦变成噪音，真故障就没人看。
        # `request-run-due` 不在此列：那是没人盯着的定时路径，坏了必须报。
        if item.get("command") == "request-run":
            continue
        # `fetch-preview` 撞 429 是**设计内的主动停车**：`preview.py` 规定撞上就整轮
        # 中止（CloudFront 按 IP 限流，各线程各自退避只会把封禁拖长，实测吞吐掉到
        # 0.5 req/s）。下次重跑会接着抓，把「主动刹车」算成故障只会让红点变噪音。
        # 只认 reason 里的「被限流」——真的崩了照样报。
        if item.get("command") == "fetch-preview" and "被限流" in str(item.get("reason") or ""):
            continue
        started = _run_time(item.get("started_at"))
        if started is None:
            continue
        if reference - started > FAILURE_WINDOW:
            continue
        success = last_success.get(str(item.get("command")))
        if success is not None and success > started:
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


#: 写锁被占用多久才算异常。正常的一轮 `reconcile` 约 30 分钟，`fetch-sizes` 全量
#: 可达 3 小时——但那个命令只在并表时短暂持锁。这里要抓的是「命令卡死、锁不放」：
#: 2026-11-13 实测过一次卡住的对账持锁，当时只有在终端露一行。
LOCK_ALERT_AFTER_MINUTES = 120


def _observation_shrink(ctx: RunContext) -> Finding:
    """最近一次重建里有没有「某天的观测只写回不到一半」。

    重放会先清掉该日分区再写回，所以被删掉的历史观测不留任何痕迹——除非那一轮
    自己记下来。`build` 把每次触发的 (日期, 旧行数, 新行数) 写进 `runs/` 的
    `shrinks`；这里往回找最近一条带记录的，点名让人核对当天 Raw 是否缺批。
    """
    for entry in reversed(ctx.runs):
        payload = entry.get("payload") or {}
        shrinks = payload.get("shrinks")
        if not shrinks:
            continue
        shown = "、".join(str(row[0]) for row in shrinks[:3])
        return Finding(
            True,
            f"观测骤降：{entry.get('command')} 在 {entry.get('started_at')} 这一轮有 "
            f"{len(shrinks)} 天的观测只写回不到一半（{shown}）；"
            "多半是当天 Raw 缺批，核对后重跑派生",
        )
    return Finding(False, "观测：最近的重建没有出现行数骤降")


def _lock_state(ctx: RunContext) -> Finding:
    """写锁被占用本身不是问题，占用太久才是。

    这项只在拿不到主锁时才会被调用，原先两个分支都返回绿——于是「谁占着」永远
    进不了 `ALERT.txt`，一个挂死几小时的进程只能靠人恰好看见终端。判据取占用
    时长：正常的一轮对账约 30 分钟，超过 `LOCK_ALERT_AFTER_MINUTES` 说明它没有
    在正常推进。
    """
    holder = read_holder(ctx.store.root)
    if holder is None:
        return Finding(False, "写锁：空闲")
    held = ctx.now - holder.since
    minutes = int(max(0.0, held.total_seconds()) // 60)
    text = f"写锁：{holder.command}（pid {holder.pid}）已持有 {minutes} 分钟"
    if minutes < LOCK_ALERT_AFTER_MINUTES:
        return Finding(False, text)
    return Finding(
        True,
        text + f"，超过 {LOCK_ALERT_AFTER_MINUTES} 分钟仍未释放；"
        "该进程可能卡死——确认它在跑什么，必要时结束进程后重跑（被杀会由系统释放锁）",
    )


def _snapshot_dependent(finding: Finding) -> Finding:
    """把一条跨表检查的结论标记为「只有一致快照上才有意义」。"""
    return replace(finding, snapshot_dependent=True)


def _request_self_contained(ctx: RunContext) -> Finding:
    """已交付的需求，方法是否也留在了目录里。

    只判**交付过**的需求：还没交过的，方法正在写，天天报红只会训练人忽略告警；
    已交付却说不清做法，才是真问题——那份数据永远补不回来历。

    需求目录不在数据目录里，所以这项由调用方显式传进来；没传就如实说没查，
    不能一声不响地跳过（沉默会被当成「查过且通过」）。
    """
    requests_root = ctx.requests_root
    if requests_root is None:
        return Finding(False, "需求自包含：未指定需求目录，本次未检查")
    if not requests_root.is_dir():
        return Finding(False, f"需求自包含：{requests_root} 不存在，本次未检查")

    checked = 0
    bad: list[str] = []
    for request_dir in request_dirs(requests_root):
        if not deliveries_of(request_dir):
            continue
        checked += 1
        found = method.problems(request_dir)
        if found:
            extra = f" 等 {len(found)} 处" if len(found) > 1 else ""
            bad.append(f"{request_dir.name}：{found[0]}{extra}")

    if bad:
        return Finding(
            True,
            f"需求自包含：{len(bad)}/{checked} 份已交付需求的过程记录不合规 —— " + "；".join(bad),
        )
    return Finding(False, f"需求自包含：{checked} 份已交付需求的方法与脚本索引一致")


def _delivery_index(ctx: RunContext) -> Finding:
    """交付索引是否跟得上台账。

    索引是给人看「哪条算数、哪条是重钉」的。它一旦落后于台账，比没有更坏——读者会
    拿一份过期清单当现状，而清单长得完全正常。判据用 INDEX 里的机读标记，不按
    mtime：`git checkout` 会把时间全部刷新，按时间比会在每次 clone 后误报一遍。
    """
    requests_root = ctx.requests_root
    if requests_root is None:
        return Finding(False, "交付索引：未指定需求目录，本次未检查")
    if not requests_root.is_dir():
        return Finding(False, f"交付索引：{requests_root} 不存在，本次未检查")

    checked = 0
    bad: list[str] = []
    for request_dir in request_dirs(requests_root):
        if not deliveries_of(request_dir):
            continue
        checked += 1
        problem = index_problem(request_dir)
        if problem:
            bad.append(problem)

    if bad:
        return Finding(
            True,
            f"交付索引：{len(bad)}/{checked} 份需求的 INDEX.md 缺失或过期 —— "
            + "；".join(bad)
            + "。跑 `hf-data-crawl request index` 重写。",
        )
    return Finding(False, f"交付索引：{checked} 份需求的 INDEX.md 与台账一致")


def _delivery_anchors(ctx: RunContext) -> Finding:
    """交付档案钉住的两样东西还在不在：输入快照与历史版本。

    档案里写的 sha256 与 `history/vN.yaml` 是「这份数据基于什么」的自证链。链路断掉
    时**不会报错**，只会让档案显得比实际更可信：

    - 输入快照被就地改过 → 产物再也对不上那行 `fill.input.sha256`；
    - `history/vN.yaml` 被删 → `version_conflict` 从那一刻起静默失效，
      「版本号不能复用」这条规则对那一版不再成立。

    两样都按档案里记的值对账，不按「现在的 spec 应该是什么」——后者会随代码演进
    漂移，历史交付没有义务重算成今天的口径。
    """
    requests_root = ctx.requests_root
    if requests_root is None:
        return Finding(False, "交付锚点：未指定需求目录，本次未检查")
    if not requests_root.is_dir():
        return Finding(False, f"交付锚点：{requests_root} 不存在，本次未检查")

    checked = 0
    bad: list[str] = []
    for request_dir in request_dirs(requests_root):
        directories = deliveries_of(request_dir)
        if not directories:
            continue
        checked += 1
        for directory in directories:
            body = read_manifest(directory)
            if body is None:
                bad.append(f"{request_dir.name}/{directory.name}：manifest 读不出来")
                continue
            version = body.get("spec_version")
            history = request_dir / HISTORY_DIR / f"v{version}.yaml"
            if isinstance(version, int) and not history.is_file():
                bad.append(
                    f"{request_dir.name}/{directory.name}：v{version} 的历史快照不见了，"
                    "版本号闸门对这一版失效"
                )
            fill = body.get("fill") or {}
            source = fill.get("input") or {}
            relative, expected = source.get("path"), source.get("sha256")
            if not relative or not expected:
                continue
            snapshot = (request_dir / str(relative)).resolve()
            # 档案是引擎写的，但体检读的是磁盘：手改过的 manifest 不该让它去读目录外。
            if not snapshot.is_relative_to(request_dir.resolve()):
                bad.append(f"{request_dir.name}/{directory.name}：输入快照路径越出了需求目录")
                continue
            if not snapshot.is_file():
                bad.append(f"{request_dir.name}/{directory.name}：输入快照 {relative} 不见了")
            elif hashlib.sha256(snapshot.read_bytes()).hexdigest() != expected:
                bad.append(
                    f"{request_dir.name}/{directory.name}：输入快照 {relative} 与档案指纹不符"
                    "（原件被改过？它本该只读）"
                )

    if bad:
        return Finding(True, f"交付锚点：{len(bad)} 处自证链断了 —— " + "；".join(bad))
    return Finding(False, f"交付锚点：{checked} 份需求的输入快照与历史版本都与档案对得上")


def _delivery_products(ctx: RunContext) -> Finding:
    """产物留存：清理记录必须「账上有、盘上无」，台账不能与自己对不上。

    产物是**可重算的副本**，不是台账本身——没留存不是错（早期交付的产物早就
    手工删过，重算即可）。真正会骗人的是账实不符：

    - 清理记录里的 sha256 与档案不一致 → 说不清清掉的是哪一份；
    - 说清理了、文件却还在 → 磁盘没回收，或上次清理中断；
    - 当前有效那条被列进清理清单 → `INDEX` 指着的「当前有效」打不开。

    没留存的产物只记个数，报在通过的那一行里——它是状态，不是故障。
    """
    requests_root = ctx.requests_root
    if requests_root is None:
        return Finding(False, "产物留存：未指定需求目录，本次未检查")
    if not requests_root.is_dir():
        return Finding(False, f"产物留存：{requests_root} 不存在，本次未检查")

    checked = 0
    absent = 0
    bad: list[str] = []
    for request_dir in request_dirs(requests_root):
        directories = deliveries_of(request_dir)
        if not directories:
            continue
        checked += 1
        facts = pruned_facts(request_dir)
        current = directories[-1]
        if current.name in facts:
            bad.append(f"{request_dir.name}：当前有效的 {current.name} 产物被清理了")
        for directory in directories:
            body = read_manifest(directory) or {}
            files = (body.get("result") or {}).get("files") or []
            entry = facts.get(directory.name)
            recorded = {
                str(item.get("name")): item.get("sha256")
                for item in (entry or {}).get("files") or []
                if isinstance(item, dict)
            }
            for item in files:
                if not isinstance(item, dict):
                    continue
                name = str(item.get("name") or "")
                present = (directory / name).is_file()
                if entry is None:
                    if not present:
                        absent += 1
                    continue
                if name not in recorded:
                    bad.append(
                        f"{request_dir.name}/{directory.name}：{name} 没记进清理清单，"
                        "清理与档案对不上"
                    )
                    continue
                if recorded[name] != item.get("sha256"):
                    bad.append(
                        f"{request_dir.name}/{directory.name}：{name} 的清理记录与档案 sha256 不一致"
                    )
                elif present:
                    bad.append(
                        f"{request_dir.name}/{directory.name}：{name} 说清理了却还在"
                        "（上次清理中断？重跑 `request prune` 即可）"
                    )

    if bad:
        return Finding(True, f"产物留存：{len(bad)} 处账实不符 —— " + "；".join(bad[:5]))
    note = f"；{absent} 个产物未留存（可重算，不影响台账）" if absent else ""
    return Finding(False, f"产物留存：{checked} 份需求的清理记录与档案一致{note}")


def _spec_version_consistency(ctx: RunContext) -> Finding:
    """口径改了却没升版本：档案会指着一份「当时说过、现在已经不是」的定义。

    这类错不会报错，只会让人拿着错的前提核对三个月，所以单列一项盯着。
    只判已交付的：没交过的需求，`history/` 里还没有「用过的那一版」，无所谓复用。
    """
    requests_root = ctx.requests_root
    if requests_root is None:
        return Finding(False, "口径版本：未指定需求目录，本次未检查")
    if not requests_root.is_dir():
        return Finding(False, f"口径版本：{requests_root} 不存在，本次未检查")

    checked = 0
    bad: list[str] = []
    for request_dir in request_dirs(requests_root):
        if not deliveries_of(request_dir):
            continue
        checked += 1
        spec_path = request_dir / SPEC_NAME
        try:
            spec = load_spec(spec_path)
        except SpecError as error:
            bad.append(f"{spec_path.parent.name}：{error}")
            continue
        # 报短句：这一行会进 `ALERT.txt`，长解释留给 `request show` 与交付时的报错。
        if version_conflict(spec_path, spec.version):
            bad.append(f"{spec_path.parent.name}：v{spec.version} 的口径已被改动，版本号没跟着升")

    if bad:
        return Finding(
            True,
            f"口径版本：{len(bad)}/{checked} 份已交付需求的版本号与内容对不上 —— " + "；".join(bad),
        )
    return Finding(False, f"口径版本：{checked} 份已交付需求的版本号未被复用")


@dataclass(frozen=True)
class Check:
    """一条体检项：码、判定，以及「这次读数算不算数」。

    三样写在一起，因为它们描述的是同一件事——「这一项检查」。原先码与「要不要标
    快照依赖」散在 `diagnose` 的调用点上、各写一遍：漏包 `_snapshot_dependent` 一次，
    非快照模式下跨表读数就会照样参与退出码，读数可能跨时点却报成真问题。
    """

    code: str
    run: Callable[[RunContext], Finding]
    #: 跨表读数：非快照模式下降级为「本次不算数」，`problem` 保持原值。
    snapshot_dependent: bool = False


def _collect_freshness(ctx: RunContext) -> Finding:
    return _freshness(
        ctx.runs, ctx.now, "collect", COLLECT_COMPLETE_REASONS, COLLECT_MAX_AGE, "日增量采集"
    )


def _reconcile_freshness(ctx: RunContext) -> Finding:
    return _freshness(
        ctx.runs, ctx.now, "reconcile", _RECONCILE_DONE, RECONCILE_MAX_AGE, "全量对账"
    )


#: 体检项登记表。顺序就是输出顺序——人读的那份清单靠它，别按字母排。
#: 文档口径漂移：这些词属于**已弃用**的概念，出现在「当前口径」文档里就该被改掉。
#:
#: 只扫 README / AGENTS / CODING_STANDARDS / operations / agents / skill 这类「讲现在」
#: 的文档；GLOSSARY 是历史词表、ADR 是决策快照、PRD 与 ISSUES 是归档，它们的职责正是
#: 记录「当时是什么样」，扫它们只会逼人删掉历史。
_DEPRECATED_TERMS: dict[str, str] = {
    "hf-daily": "旧的日常链任务，2026-11-19 并入 hf-refresh",
    "hf-compact": "旧的压缩任务，已并入 refresh 的串行头",
    "hf-sizes": "旧的规模任务，已并入 refresh 的并行段",
    "hf-files": "旧的文件清单任务，已并入 refresh 的并行段",
    "hf-archive": "旧的归档任务，已并入 refresh 的串行尾",
    "hf-readme": "旧的 README 保鲜任务，已并入 refresh 的并行段",
    "daily.ps1": "旧链脚本，被 refresh.ps1 取代",
    "clean-readme": "回滚掉的物化命令",
    "readme_clean": "回滚掉的物化表",
    "31 列": "契约 v5 的列数，当前是 35",
}

#: 行内出现这些词说明是在讲历史或现状说明，不算漂移。
_HISTORY_MARKERS = (
    "已删除",
    "已并入",
    "已取代",
    "取代",
    "历史",
    "不再",
    "旧任务",
    "旧链",
    "旧的",
    "v2 时",
    "v4 时",
    "契约 v4",
    "契约 v5",
)


#: `.agents` 下的本机状态，不属于「讲现在」的仓库文档。
#:
#: `worktrees/` 是 git worktree 的完整副本，里面带着历史的 DECISIONS/ADR；扫它等于
#: 把本机开发副本当成项目文档，制造一条 CI 永远看不到的假红。`memory/` 同理。
_LOCAL_AGENT_DIRS = frozenset({"worktrees", "memory"})


def _agent_docs(root: Path) -> list[Path]:
    """`.agents/**/*.md`，跳过 worktree 副本与本机记忆。"""
    base = root / ".agents"
    return [
        path
        for path in sorted(base.rglob("*.md"))
        if not (_LOCAL_AGENT_DIRS & set(path.relative_to(base).parts))
    ]


def _doc_drift(ctx: RunContext) -> Finding:
    """「讲现在」的文档里，还有没有在讲已弃用的概念。

    每次重构都会留下这类残渣：代码改了，而描述这套代码的散文散在 README、
    operations、skill 里各写一份。`check_docs_links.py` 只能验链接与编号落地，
    验不了「这段话说得对不对」——2026-11-19 刷新链与契约 v6 之后，一次人工审计
    找出 16 处过时。这项把它变成可执行的检查。

    判定刻意**窄**：只认一份固定的弃用词表，不猜「这句话是否过时」。宁可漏，
    不可误报——体检红了没人看，比漏报更糟。行内带「已删除 / 已并入 / 历史」这类
    词的豁免：MONITORING 里「旧的六个任务已删除」是现状说明，不是漂移。

    `.agents/worktrees` 与 `.agents/memory` 是本机状态，不参与扫描：前者会把整份
    历史 DECISIONS/ADR 带进来（2026-11-20 实测本机 34 条假红，CI 全绿），后者是
    记忆文件，里面的旧路径本就不该按「当前口径」要求。
    """
    root = ctx.store.root.parent
    targets = [
        root / "README.md",
        root / "AGENTS.md",
        root / "CODING_STANDARDS.md",
        root / "docs" / "README.md",
        root / "docs" / "TODO.md",
        *sorted((root / "docs" / "operations").glob("*.md")),
        *sorted((root / "docs" / "agents").glob("*.md")),
        *_agent_docs(root),
    ]
    found: list[str] = []
    scanned = 0
    for path in targets:
        if not path.exists():
            continue
        scanned += 1
        try:
            lines = path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line_no, line in enumerate(lines, 1):
            if any(marker in line for marker in _HISTORY_MARKERS):
                continue
            for term, reason in _DEPRECATED_TERMS.items():
                if term in line:
                    found.append(f"{path.name}:{line_no} 提到 `{term}`（{reason}）")
    if found:
        detail = "；".join(found[:5]) + (f"；等 {len(found)} 处" if len(found) > 5 else "")
        return Finding(True, f"文档口径：{detail}")
    return Finding(False, f"文档口径：{scanned} 份「讲现在」的文档没有已弃用概念")


CHECKS: tuple[Check, ...] = (
    Check("collect_freshness", _collect_freshness),
    Check("reconcile_freshness", _reconcile_freshness),
    Check("catalog_staleness", _catalog_staleness),
    Check("layer_reconcile", _layer_reconcile, snapshot_dependent=True),
    Check("raw_gaps", _raw_gaps),
    Check("raw_volume", _raw_volume),
    Check("derive_state", _derive_state),
    Check("status_snapshot", _status_snapshot),
    Check("artifact_freshness", _artifact_freshness, snapshot_dependent=True),
    Check("contract_coverage", _contract_coverage, snapshot_dependent=True),
    Check("field_freshness", _field_freshness, snapshot_dependent=True),
    Check("card_provenance", _card_provenance, snapshot_dependent=True),
    Check("readme_freshness", _readme_freshness, snapshot_dependent=True),
    Check("raw_growth", _raw_growth),
    Check("cold_backlog", _cold_backlog),
    Check("archive_backlog", _archive_backlog),
    Check("storage", _storage),
    Check("staging_backlog", _staging_backlog),
    Check("backup_backlog", _backup_backlog),
    Check("probe_quality", _probe_quality),
    Check("probe_degraded", _probe_degraded),
    # 文档口径与代码漂移：只认一份固定的弃用词表，宁可漏不可误报。
    Check("doc_drift", _doc_drift),
    Check("recent_failures", _recent_failures),
    Check("observation_shrink", _observation_shrink),
    Check("temp_files", _stale_temp_files),
    Check("request_self_contained", _request_self_contained),
    Check("spec_version", _spec_version_consistency),
    Check("delivery_index", _delivery_index),
    Check("delivery_anchors", _delivery_anchors),
    Check("delivery_products", _delivery_products),
)

#: 只在非快照模式跑：快照模式下本次体检已持写锁，再去抢锁会自己跟自己冲突。
LOCK_STATE = Check("lock_state", _lock_state)


def diagnose(
    store: DatasetStore,
    now: datetime | None = None,
    *,
    snapshot: bool = True,
    requests_root: Path | None = None,
) -> list[Finding]:
    """跑全部体检项。

    `snapshot=False` 表示调用方没拿到写锁：跨表对比（current / observations /
    silver / 导出物 / 卡片）可能在写入中途读到瞬时缺行，这类结论标 `degraded`
    ——**保留原来的 `problem`**，只是不参与退出码与告警。消费者自己判
    `problem and not degraded`：「它是不是问题」与「这次读数算不算数」是两件事。

    快照模式下 **不跑 `_lock_state`**：本次体检已经持有写锁，它内部会再次抢锁，
    自己和自己冲突。锁被占用只可能发生在非快照模式，那里才需要报告占用者。
    """
    now = now or datetime.now()
    ctx = _context(store, now, requests_root)
    checks = CHECKS if snapshot else (*CHECKS, LOCK_STATE)
    findings: list[Finding] = []
    for check in checks:
        finding = check.run(ctx)
        if check.snapshot_dependent:
            finding = _snapshot_dependent(finding)
        findings.append(replace(finding, code=check.code))
    if snapshot:
        return findings
    return [
        replace(item, degraded=True, text=f"[非快照] {item.text}")
        if item.snapshot_dependent
        else item
        for item in findings
    ]


def sync_alert(
    store: DatasetStore, findings: list[Finding], now: datetime, *, snapshot: bool = True
) -> None:
    """有问题就写 `ALERT.txt`，没有就删掉，让这个文件的存在本身就是信号。

    非快照模式会在首行写明：这次读数可能跨时点，问题清单未必完整；写命令结束后
    应重跑一次。没有问题时仍删文件——锁占用本身不是数据告警。
    """
    path = store.root / ALERT_NAME
    problems = [item for item in findings if item.counts]
    if not problems:
        path.unlink(missing_ok=True)
        return
    lines: list[str] = []
    if not snapshot:
        lines.append("[非快照] 写锁被占用，跨表读数可能跨时点；请在写命令结束后重跑 doctor")
    lines += [f"hf-data-crawl 体检发现 {len(problems)} 个问题（{now:%Y-%m-%d %H:%M}）：", ""]
    for item in problems:
        # 连续天数按 `code` 数，且按自然日去重：`doctor` 一天可能跑好几次（改完验一次、
        # 排障再跑一次），按次数数会得出「连续 6 轮」——读起来像红了六天。首次红不标，
        # 免得每条都带个尾巴。
        days = red_days(store, item.code, now) if item.code else 0
        lines.append(f"- {item.text}" + (f"（已连续 {days} 天）" if days > 1 else ""))
    # 走原子写：两个 doctor 并发时 `path.write_text` 会先截断再写，读者可能
    # 看到半截告警。这里是全项目最后一个绕过 `platform.atomic` 的写点。
    write_text(path, "\n".join(lines) + "\n")
