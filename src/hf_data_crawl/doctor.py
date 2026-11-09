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

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl

from . import lineage, raw
from .fields import DEFAULT_COLUMNS, contract_errors
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

#: 富化表允许的最长不更新天数。规模表按周补、卡片按 enrich 节奏，超过这个天数
#: 说明那条链路已经停了，而不是「它本来就慢」。
ARTIFACT_MAX_AGE_DAYS = 7

#: 「活跃库」的判定窗口：日增量按 lastModified 扫到水位线，最近一天基本覆盖；
#: 留 3 天是给漏跑与补跑留余量，而不把长尾算进卡片凭证的落后比例。
ACTIVE_DAYS = 3
#: 活跃库里卡片对不上当前 sha 的告警比例。判据不是「必须有变化」——变化本来就是
#: 常态，卡片抓在 commit 之后、下一轮增量之前，短暂不一致是正常的；超过这个比例
#: 说明整趟没跑，而不是个别库在窗口边缘。
CARD_BEHIND_ALERT_SHARE = 20.0

#: 有界试跑（max_pages）与 dry_run 不是完成；collect 的完成态与 cli 共用一份枚举。
_RECONCILE_DONE = frozenset({"end"})


@dataclass(frozen=True)
class Finding:
    """一条体检结论。`problem` 为真才会让退出码非零、并写入告警文件。"""

    problem: bool
    text: str
    #: 只有在一份一致快照上才有意义；非快照模式下降级为提示，不参与退出码。
    snapshot_dependent: bool = False


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
    dates = {day.snapshot_date for day in raw.observed_days(store)}
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


def _scan_frame(pattern: Path | str, columns: list[str]) -> pl.DataFrame | None:
    """只取需要的列（parquet 列裁剪），读失败返回 None。

    富化表里 `carddata` 76 万行、`size` 112 万行，读全列会让体检多花十几秒；
    而这里只需要 id 与日期。读失败不抛：体检不能因为一份产物坏掉而整体失败。
    """
    try:
        return pl.scan_parquet(pattern).select(columns).collect()
    except (OSError, pl.exceptions.PolarsError):
        return None


def _artifact_freshness(store: DatasetStore, today: date) -> Finding:
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
    current = store.read_current()
    if current.height == 0:
        return Finding(True, "产物新鲜度：current 为空，无法比对")
    current_ids = set(current["id"].to_list())
    problems: list[str] = []
    notes: list[str] = []

    export = store.root / "exports" / "datasets.parquet"
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
            columns = tuple(pl.scan_parquet(export.as_posix()).collect_schema().names())
            if columns != DEFAULT_COLUMNS:
                problems.append(
                    f"宽表导出列集与默认交付契约不一致（{len(columns)} vs "
                    f"{len(DEFAULT_COLUMNS)} 列），跑 export-wide 重建"
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
        covered = len(set(frame["id"].to_list()) & current_ids)
        share = covered / len(current_ids) * 100
        # 覆盖率保留一位小数直到 10%：`files` 试点只有 1000 行，四舍五入成「0%」
        # 会读成「一行都没有」，而它确实有 1000 行、只是不足千分之一。
        share_text = f"{share:.1f}%" if share < 10 else f"{share:.0f}%"
        newest = frame["snapshot_date"].max()
        # `Series.max()` 的类型标注是宽联合（date / bytes / …），先收窄再用。
        if not isinstance(newest, date):
            notes.append(f"{label}覆盖 {share_text}（日期缺失）")
            continue
        notes.append(f"{label}覆盖 {share_text}（最新 {newest}）")
        age_days = (today - newest).days
        if age_days > ARTIFACT_MAX_AGE_DAYS:
            problems.append(
                f"{label}表已 {age_days} 天没更新（最新 {newest}），"
                f"检查 enrich / fetch-sizes / fetch-files 是否还在跑"
            )

    if problems:
        return Finding(True, "产物新鲜度：" + "；".join(problems))
    return Finding(False, "产物新鲜度：" + "；".join(notes))


def _card_provenance(store: DatasetStore, today: date) -> Finding:
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

    current = _scan_frame(store.path("current").as_posix(), ["id", "sha", "last_seen_at"])
    if current is None or current.height == 0:
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


def _archive_backlog(store: DatasetStore, today: date) -> Finding:
    """超过保留窗口仍是原始分块：写入序未冻结、同天重复未压缩。"""
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


def _storage(store: DatasetStore) -> Finding:
    """存储总账：谁在长，一眼看见。

    只报数、不判断——阈值判断交给各自那一项（备份积压、冷区积压、归档积压）。
    把四层摊开列出来，是为了让「哪天发现磁盘吃紧」时不必再逐个 du：
    raw 是热区（7 天窗口）、cold 是去重后的基线（体量恒定）、backup 是待销毁的
    副本（应当有界）、reports 是诊断产物。
    """
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
    return Finding(
        False,
        f"存储：data {whole / 1e9:.2f} GB（raw {raw_bytes / 1e6:.0f} MB · "
        f"cold {cold_bytes / 1e6:.0f} MB · backup {backup_bytes / 1e6:.0f} MB · "
        f"reports {report_bytes / 1e6:.0f} MB · 其余 {other / 1e6:.0f} MB）",
    )


def _backup_backlog(store: DatasetStore, now: datetime) -> Finding:
    """archive / compact 只移不删；销毁归 `prune`，这里只盯它有没有跟上。

    判龄与清理共用 `prune.plan()`：若这里按 mtime、那边按目录名，两边会各说各话
    ——体检报「已清理」而实际没删，或反之。同一条规则只能有一处实现。
    """
    from .prune import plan as plan_prune

    targets = plan_prune(store, now=now)
    if not targets:
        return Finding(False, f"备份清理：无超过 {BACKUP_PRUNE_AFTER_DAYS} 天的待清理对象")

    shown = "、".join(item.path.name.removeprefix("dt=") for item in targets[:5])
    reclaimable = sum(item.bytes for item in targets) / 1e6
    kinds = "、".join(sorted({item.kind for item in targets}))
    return Finding(
        True,
        f"{kinds} 有 {len(targets)} 处超过 {BACKUP_PRUNE_AFTER_DAYS} 天未清理（{shown}），"
        f"可释放约 {reclaimable:.1f} MB；跑 hf-data-crawl prune",
    )


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


def _snapshot_dependent(finding: Finding) -> Finding:
    """把一条跨表检查的结论标记为「只有一致快照上才有意义」。"""
    return Finding(finding.problem, finding.text, snapshot_dependent=True)


def diagnose(
    store: DatasetStore,
    now: datetime | None = None,
    *,
    snapshot: bool = True,
) -> list[Finding]:
    """跑全部体检项。

    `snapshot=False` 表示调用方没拿到写锁：跨表对比（current / observations /
    silver / 导出物 / 卡片）可能在写入中途读到瞬时缺行，这类结论降级为提示，
    不参与退出码与告警文件；文件系统与单表检查照常判定。

    快照模式下 **不跑 `_lock_state`**：本次体检已经持有写锁，它内部会再次抢锁，
    自己和自己冲突。锁被占用只可能发生在非快照模式，那里才需要报告占用者。
    """
    now = now or datetime.now()
    runs = read_runs(store)
    findings = [
        _freshness(runs, now, "collect", COLLECT_COMPLETE_REASONS, COLLECT_MAX_AGE, "日增量采集"),
        _freshness(runs, now, "reconcile", _RECONCILE_DONE, RECONCILE_MAX_AGE, "全量对账"),
        _catalog_staleness(store, now.date()),
        _snapshot_dependent(_layer_reconcile(store)),
        _raw_gaps(store, now.date()),
        _derive_state(store),
        _snapshot_dependent(_artifact_freshness(store, now.date())),
        _snapshot_dependent(_card_provenance(store, now.date())),
        _raw_growth(store),
        _cold_backlog(store, now.date()),
        _archive_backlog(store, now.date()),
        _storage(store),
        _backup_backlog(store, now),
        _recent_failures(runs, now),
        _stale_temp_files(store, now),
    ]
    if snapshot:
        return findings
    findings.append(_lock_state(store))
    return [
        Finding(False, f"[非快照] {item.text}", snapshot_dependent=True)
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
    problems = [item.text for item in findings if item.problem]
    if not problems:
        path.unlink(missing_ok=True)
        return
    lines: list[str] = []
    if not snapshot:
        lines.append("[非快照] 写锁被占用，跨表读数可能跨时点；请在写命令结束后重跑 doctor")
    lines += [f"hf-data-crawl 体检发现 {len(problems)} 个问题（{now:%Y-%m-%d %H:%M}）：", ""]
    lines += [f"- {text}" for text in problems]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
