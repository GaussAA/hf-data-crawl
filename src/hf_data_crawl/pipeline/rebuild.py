"""从 Raw 重建派生层：默认只重放热区，冷区以流水为权威。

语义变更（2026-11-07，大帅钦点）
--------------------------------
旧契约是「Raw 是唯一事实来源」：清空全部派生层，按天重放**所有**历史 Raw。
于是跨天全量快照必须永久留在 `data/raw`，每天 ~240 MB 线性堆积，且一次
build 要重放几百天。

新契约分三层，见 `retention` 模块：

- **冷区权威层**：`silver`（属性变更流水）、`observations`（指标变更流水）、
  `current`（当前态）的历史不再清空。它们本就是「变化才追加」的压缩表示，
  信息量与每天一份全量快照等价，却小得多。
- **热区**：默认只重放最近 `REPLAY_WINDOW_DAYS` 天的 Raw；冷区 Raw 由
  `compact` 跨天压成基线、移出主目录。
- **逃生舱**：`--full`（或 `window_days=None`）走旧的全量重放；权威层若已损坏
  （观测流水或当前态为空）会自动回退全量，避免局部重放静默丢掉冷区历史。

重放集合必须后缀闭合
--------------------
除了默认窗口，还支持「从某一天起修好」（`since`，供 `build --stale`、崩溃恢复与
补采后用）。选了 D 日，就要把 D 之后所有有 Raw 的日子一起重放。

原因是派生不是逐日独立的：`silver` 的「变了才追加」要拿「早于该日最近一行」当锚点，
`observations` 同理，而 `current` 是拿当日帧**覆盖**同名 id 的全量当前态。只重放一天
D 而不重放 D 之后的日，会得到「D 之后新增的属性变更被吃掉」与「current 的指标
回退到 D」——两者都不报错，只是数字变小。

**这条约束不进调用方的手**：`rebuild_from_raw` 收 `since`（一个日期），后缀由它自己
算。曾经它也收过一个现成的日期列表，三个调用方各自 `replay_suffix(...)` 一下再传进来
——规则只有一份，但「记得去调它」挂在三个人的记性上，而漏掉中间某一天的后果是静默的。

等价性由 `tests/pipeline/test_rebuild.py` 的三向契约钉死：局部重放 == 全量重放 == 逐日
正常跑。三者都读「早于该日的历史」，冷区不动，热区自然收敛到同一结果。
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import polars as pl

from ..platform.atomic import write_text
from ..platform.retention import REPLAY_WINDOW_DAYS
from ..platform.store import DatasetStore
from ..platform.tables import TABLES, Origin, TableSpec
from ..platform.vocab import Status
from . import lineage, marker, raw, state
from .derive import ReplayCarry, derive_day


@dataclass(frozen=True)
class RebuildSummary:
    dates: list[date]
    bronze_rows: int
    silver_rows: int
    observations: int
    current_rows: int
    #: window（默认）/ full（显式全量）/ recovery（权威层缺失自动回退）/
    #: target（指定日期重放，见 `build --stale`）。
    mode: str = "window"
    card_rows: int = 0
    config_rows: int = 0
    #: 已压入冷区、主目录没有 Raw 可重放的日期数。非零说明这次重建拿不回这些
    #: 日期的历史；`--full` 会在执行前就被拒，recovery 回退则由它进台账。
    cold_gaps: int = 0
    #: 观测被大幅覆盖的日期：(日期, 旧行数, 新行数)。非空说明某天只写回了不到
    #: 一半的历史观测——多半是当天 Raw 缺批。它必须进 `runs/` 台账：重放会先
    #: 清分区再写回，没有这条记录，删掉的历史不留任何痕迹。
    shrinks: list[tuple[date, int, int]] = field(default_factory=list)


def discover_raw_dates(store: DatasetStore) -> list[date]:
    """可从主目录重放的日期（含归档分片与 card 补位），按时间升序。"""
    return [day.snapshot_date for day in raw.replayable_days(store)]


def cold_gap_days(store: DatasetStore) -> list[date]:
    """采过、但 Raw 已被 `compact` 搬离主目录、因而无法重放的日期。

    `--full` 承诺「按全部 Raw 重放」，而可重放的只有主目录里那些分块：冷区
    日期的分块在 `data/backup/raw/`，基线又只保「每个 id 最后一条」，按日切不出
    历史。于是这些日期的 silver / observations 在被清空后**永远补不回来**。
    判据取 `RawDay.compacted and not replayable`：只认「确实压走了、主目录也
    确实再无分块」的日子，压缩后仍有残留文件（可重放）的不算。
    """
    return [
        day.snapshot_date
        for day in raw.observed_days(store)
        if day.compacted and not day.replayable
    ]


def _authority_intact(store: DatasetStore) -> bool:
    """冷区权威层是否还在。两者任一为空，局部重放都会丢历史。"""
    return store.read_all("observations").height > 0 and state.read_current(store).height > 0


def replay_suffix(store: DatasetStore, since: date) -> list[date]:
    """从 `since` 起（含）到最新 Raw 日的全部日期。

    只给「动手前想先报一句会重放几天」的调用方用。真正开跑时把 `since` 交给
    `rebuild_from_raw`，它按同一条规则自己算——别把这个列表再传回去，那正是
    接口曾经允许、后来收掉的那条路。
    """
    return _suffix(discover_raw_dates(store), since)


def _suffix(dates: list[date], since: date) -> list[date]:
    """`since` 之后（含）的所有 Raw 日。重放集合唯一的构造规则。"""
    return [day for day in dates if day >= since]


def divergent_since(store: DatasetStore) -> date | None:
    """最早一个「派生与 Raw 不符」的日期；全部对齐时返回 None。

    取最早的而不是最新的一天，是因为重放后缀只能从最早那处开始：先修最新的
    再回头修更早的，会让 `current` 再退一次。
    """
    days = lineage.divergent(store, discover_raw_dates(store))
    return min(days) if days else None


def _select_replay(
    store: DatasetStore,
    dates: list[date],
    *,
    window_days: int | None,
    since: date | None,
) -> tuple[str, list[date]] | None:
    """定出这次重放的日期集合与模式；无事可做时返回 None。"""
    if since is not None:
        picked = _suffix(dates, since)
        if not picked:
            return None
        if not _authority_intact(store):
            # 定点重放同样依赖冷区权威层：它俩没了，局部重放照样会抹掉历史。
            return "recovery", dates
        return "target", picked

    if window_days is None:
        return "full", dates
    if not _authority_intact(store):
        return "recovery", dates
    return "window", dates[-window_days:]


def _observation_rows(store: DatasetStore, snapshot: date) -> int:
    """某日观测分区的行数；分区不存在按 0。只读 parquet 元数据，不载入整表。"""
    path = store.path("observations", snapshot)
    if not path.exists():
        return 0
    return int(pl.scan_parquet(path).select(pl.len()).collect().item())


def rebuild_from_raw(
    store: DatasetStore,
    *,
    window_days: int | None = REPLAY_WINDOW_DAYS,
    since: date | None = None,
) -> RebuildSummary:
    """重建派生层；默认只重放热区，`window_days=None` 表示全量重放。

    `since` 给定时改成「从这一天起修好」：`since`（含）到最新 Raw 日整段重放，
    与 `window_days` 二选一——给了 `since` 就忽略窗口。后缀闭合由本函数自己保证，
    调用方只给一个日期，给不出「缺了中间几天」的集合。
    """
    if since is None and window_days is not None and window_days < 1:
        raise ValueError(f"window_days 必须 >= 1 或设为 None（全量），实得 {window_days}")
    dates = discover_raw_dates(store)
    if not dates:
        return RebuildSummary(
            dates=[], bronze_rows=0, silver_rows=0, observations=0, current_rows=0
        )

    selected = _select_replay(store, dates, window_days=window_days, since=since)
    if selected is None:
        return RebuildSummary(
            dates=[], bronze_rows=0, silver_rows=0, observations=0, current_rows=0
        )
    mode, replay = selected
    full = mode in {"full", "recovery"}
    # 清空全部派生层之前，先记下有多少日期已经无 Raw 可重放：`--full` 由
    # `run_build` 在入口拦下，recovery 是「权威层已坏」的抢救路径、不能拒，
    # 但必须把「救不回来多少」喊出来并落进台账。
    gaps = cold_gap_days(store) if full else []
    if gaps and mode == "recovery":
        _warn_cold_gaps(gaps)

    # 失效状态不是从 raw 派生的：它来自对账时的单库探测（404/401/307），
    # 而 state.update 会把本轮出现的每条记录都置为 active。清理前留存、
    # 重建后写回，协议见本模块末尾「状态留存协议」；GLOSSARY #15/#18/#26 记有实测事故。
    retained = retain(store)

    # 观测骤降的护栏要拿「这轮之前的行数」当尺子，而下一行就会把这些分区清掉，
    # 所以必须在这里先记下来。原先重放路径把 `before` 硬写成 0，护栏在最需要它
    # 的时候（一次性清 7 天再重写）结构性失效——2026-11-16 审计实锤。
    observation_before = {day: _observation_rows(store, day) for day in replay}

    # 清空之前先写下「本次会动哪些天」。逐日标记只覆盖得了「派生到一半」，
    # 覆盖不了「已清空、尚未开始」与「两天之间」——那两段原先谁都不认账，
    # 而 `build --stale` 还会说「已对齐」。范围就是上面算出来的重放集合。
    marker.write_marker(store, replay[0], until=replay[-1])

    clear_derived(store, hot_dates=None if full else set(replay))

    bronze_rows = 0
    silver_rows = 0
    shrinks: list[tuple[date, int, int]] = []
    # 日期升序重放，历史锚点从上一日顺推：否则每天都要重读全部 silver /
    # observations / current，代价随天数平方增长。
    carry = ReplayCarry.load(store, before=replay[0])
    for snapshot in replay:
        day = derive_day(
            store,
            snapshot,
            enrich=True,
            carry=carry,
            observation_before=observation_before[snapshot],
        )
        bronze_rows += day.bronze_rows
        silver_rows += day.silver_rows
        if day.observation_shrink is not None:
            shrinks.append((snapshot, *day.observation_shrink))

    restore(store, retained)

    marker.clear_marker(store, replay[0], until=replay[-1])

    return RebuildSummary(
        dates=list(replay),
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        observations=store.read_all("observations").height,
        current_rows=state.read_current(store).height,
        mode=mode,
        card_rows=store.read("carddata").height,
        config_rows=store.read("card_configs").height,
        cold_gaps=len(gaps),
        shrinks=shrinks,
    )


def _warn_cold_gaps(gaps: list[date]) -> None:
    """recovery 回退时点名救不回来的日期。不抛错——此刻能救多少算多少。"""
    shown = "、".join(day.isoformat() for day in gaps[:3])
    more = f" 等 {len(gaps)} 天" if len(gaps) > 3 else ""
    print(
        f"[!] 冷区 {len(gaps)} 天没有 Raw 可重放（{shown}{more}）：本次只能按主目录里"
        "仅存的 Raw 重建，这些日期的 silver / observations 历史无法恢复。"
        "核对 data/backup/raw 是否还留着原分块，必要时先搬回再重跑"
    )


def clear_derived(store: DatasetStore, *, hot_dates: set[date] | None = None) -> None:
    """清除可重算的产物。

    `hot_dates=None` 是全量模式：按 `Origin` 清掉所有 RAW/STATE 表（NETWORK
    不可重算，跳过），并作废对账会话文件——旧行为，供 `--full` 逃生舱使用。

    给了 `hot_dates` 是局部模式：只清热区日期在 `tables.DERIVED_DAY_TABLES`
    里的分区；冷区分区、单文件表（current / 卡片表 / 规模表）与 state 会话
    文件**一律保留**，它们已是权威层，清掉就重建不回来。
    """
    if hot_dates is None:
        for spec in TABLES.values():
            if spec.origin is Origin.NETWORK:
                continue
            _clear(store, spec)
        _clear_stale_state(store)
        return

    # 局部重放只清热区日期的分区，Bronze 也按日清：此前整表清除会把冷区
    # 日期的 Bronze 一并删掉，而这些日子不在本次重放范围内。
    for snapshot in hot_dates:
        marker.clear_day(store, snapshot)


def _clear_stale_state(store: DatasetStore) -> None:
    """全量模式下作废游标与对账会话：重建后它们不再对应任何有效基线。

    `lineage.json` 也落在 `state/*.json` 里，但它不是会话文件：冷区日期的 Raw 已
    移出主目录、不再参与重放，它们的派生凭证只存在这一份档案里。删掉等于把
    「哪天派生过、基于哪版 Raw」永久抹掉，且 `lineage.compare` 会因档案缺失
    静默跳过那些日子。故从通配结果里显式排除，名字取自 `lineage.path_of`。
    """
    keep = {lineage.path_of(store).name}
    stale_files = [
        *store.state_dir.glob("*.json"),
        *store.state_dir.glob("reconcile-*.csv"),
        *store.state_dir.glob("reconcile-baseline.parquet"),
    ]
    for stale in stale_files:
        if stale.name in keep:
            continue
        stale.unlink(missing_ok=True)


def _clear(store: DatasetStore, spec: TableSpec) -> None:
    """按表的形态清除：分区表删其父目录，单文件表删文件本身。"""
    if spec.partitioned:
        parent = store.root / spec.path.split("/dt={dt}", 1)[0]
        shutil.rmtree(parent, ignore_errors=True)
    else:
        (store.root / spec.path).unlink(missing_ok=True)


# ----------------------------------------------------------------------
# 状态留存协议
# ------------
# 失效状态不来自 Raw（来自单库探测），而 rebuild 会清空 state。所以：
#
# - `retain`：清理**之前**调用。快照存在，说明上次重建被打断，以快照为准；
#   否则读 current 里非 active 的行，非空就写快照。
# - `restore`：写回之后调用，并删除快照。
#
# **快照只表示「重建进行中」。** 成功后必须删除——留着的话，下一次重建会把它当成
# 「上次被打断」，把期间已恢复的库改回旧状态。
#
# 已接受的缝隙：写回完成、快照尚未删除时进程被杀，之后又有对账把某个库恢复，
# 下一次重建会被旧快照改回去。窗口只夹在「写回」与「删文件」之间，不做合并逻辑。


def snapshot_path(store: DatasetStore) -> Path:
    """放在 meta/ 而非 state/：它不属于任何登记表，重建清不到它。"""
    return store.root / "meta" / "failed-status.json"


def retain(store: DatasetStore) -> dict[str, str]:
    """清理前留存「非在架」状态，并保证快照已落盘。返回 id → 状态。"""
    snapshot = _read_snapshot(store)
    if snapshot is not None:
        return snapshot

    current = store.read("current")
    if current.height == 0:
        return {}
    stale = current.filter(pl.col("status") != Status.ACTIVE.value)
    retained = _valid(
        (str(dataset_id), str(value))
        for dataset_id, value in zip(stale["id"].to_list(), stale["status"].to_list(), strict=True)
    )
    if retained:
        write_text(snapshot_path(store), json.dumps(retained, ensure_ascii=False, sort_keys=True))
    return retained


def restore(store: DatasetStore, retained: dict[str, str]) -> None:
    """把留存的状态写回，然后删除快照。

    `mark_status` 只改 current 里**已有**的行。重建后 current 里没有的 id，状态
    与整行一起静默消失：`--full` 在冷区已压缩时就是这种局面（`run_build` 会拦，
    放行则明知故犯），recovery 回退也可能。快照随后就被删掉，这里是最后一次
    能看见它的机会，故把丢弃数喊出来——沉默的丢失比报错的丢失更难查。
    """
    if retained:
        present = set(state.read_current(store)["id"].to_list())
        dropped = sorted(dataset_id for dataset_id in retained if dataset_id not in present)
        state.mark_status(store, retained)
        if dropped:
            shown = "、".join(dropped[:3])
            more = f" 等 {len(dropped)} 个" if len(dropped) > 3 else ""
            print(
                f"[!] 状态写回丢了 {len(dropped)} 个 id（{shown}{more}）：它们已不在重建后的 "
                "current 里，主目录也没有其 Raw，整行随之消失。核对 data/backup/raw 与冷区清单"
            )
    snapshot_path(store).unlink(missing_ok=True)


@dataclass(frozen=True)
class SnapshotHealth:
    """状态留存快照的只读诊断结论。

    `kind`：`absent` 不存在（正常终态）、`valid` 可解析（但存在本身就是异常）、
    `corrupt` 读不出来或顶层不是 JSON 对象。`valid_count` / `invalid_count`
    只对 `valid` 有意义。
    """

    kind: str
    valid_count: int = 0
    invalid_count: int = 0
    error: str = ""


def _load_snapshot_file(store: DatasetStore) -> tuple[object, SnapshotHealth]:
    """读快照原文并顺带产出诊断结论；`retain` 与 doctor 共用同一份判据。

    分开「文件损坏」与「文件不存在」是这条诊断的全部价值：两者在 `retain`
    里都归一为 None，但一个是正常终态，另一个会让非在架状态静默失去凭证。
    """
    path = snapshot_path(store)
    if not path.exists():
        return None, SnapshotHealth(kind="absent")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as error:
        return None, SnapshotHealth(kind="corrupt", error=f"{type(error).__name__}: {error}")
    if not isinstance(payload, dict):
        return None, SnapshotHealth(kind="corrupt", error="顶层不是 JSON 对象")
    retained = _valid((str(key), str(value)) for key, value in payload.items())
    return payload, SnapshotHealth(
        kind="valid",
        valid_count=len(retained),
        invalid_count=len(payload) - len(retained),
    )


def snapshot_health(store: DatasetStore) -> SnapshotHealth:
    """只读诊断状态留存快照，供 `doctor` 把静默回退变成可见告警。

    快照存在本身即异常：它只表示「重建进行中」，成功写回后必须删除；留着会被
    下一次重建当成「上次被打断」，把期间已恢复的库改回旧状态。损坏更麻烦——
    `_read_snapshot` 返回 None，`retain` 静默回退读 current，而重建若已清空
    state，非在架状态就再无来源。这里刻意不做自动修复：坏文件是唯一的现场
    证据，删掉等于销毁证据，也修不回已经丢掉的状态。
    """
    return _load_snapshot_file(store)[1]


def _read_snapshot(store: DatasetStore) -> dict[str, str] | None:
    """读上次留下的快照；不存在或损坏返回 None（调用方回退读 current）。"""
    payload, health = _load_snapshot_file(store)
    if health.kind != "valid" or not isinstance(payload, dict):
        return None
    return _valid((str(key), str(value)) for key, value in payload.items())


def _valid(pairs: Iterable[tuple[str, str]]) -> dict[str, str]:
    """丢掉词表之外的值：写回时 `mark_status` 会对未知值报错，而此刻 state
    已被清空，中途报错等于把重建打断在最糟的位置。"""
    known = {status.value for status in Status}
    return {dataset_id: value for dataset_id, value in pairs if value in known}
