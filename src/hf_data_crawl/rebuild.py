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
除了默认窗口，还支持指定一组日期重放（`days`，供 `build --stale` 用）。这组日期
**必须后缀闭合**：选了 D 日，就要把 D 之后所有有 Raw 的日子一起重放。

原因是派生不是逐日独立的：`silver` 的「变了才追加」要拿「早于该日最近一行」当锚点，
`observations` 同理，而 `current` 是拿当日帧**覆盖**同名 id 的全量当前态。只重放一天
D 而不重放 D 之后的日，会得到「D 之后新增的属性变更被吃掉」与「current 的指标
回退到 D」——两者都不报错，只是数字变小。`replay_suffix` 就是这条约束的唯一实现，
调用方不要自己拼日期列表。

等价性由 `tests/test_rebuild.py` 的三向契约钉死：局部重放 == 全量重放 == 逐日
正常跑。三者都读「早于该日的历史」，冷区不动，热区自然收敛到同一结果。
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date

from . import deriving, lineage, raw
from .derive import derive_day
from .retention import REPLAY_WINDOW_DAYS
from .status import restore, retain
from .store import DatasetStore
from .tables import TABLES, Origin, TableSpec


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


def discover_raw_dates(store: DatasetStore) -> list[date]:
    """可从主目录重放的日期（含归档分片与 card 补位），按时间升序。"""
    return [day.snapshot_date for day in raw.replayable_days(store)]


def _authority_intact(store: DatasetStore) -> bool:
    """冷区权威层是否还在。两者任一为空，局部重放都会丢历史。"""
    return store.read_observations().height > 0 and store.read_current().height > 0


def replay_suffix(store: DatasetStore, since: date) -> list[date]:
    """从 `since` 起（含）到最新 Raw 日的全部日期——重放集合唯一合法的构造方式。

    只重放 `since` 那一天是不够的：`silver` 与 `observations` 要靠「早于该日的
    最近一行」判定变化，`current` 又拿当日帧覆盖同名 id，少放中间任何一天，
    后面的变更判定就会拿错锚点、当前态也会回退到那一日的值。
    """
    return [day for day in discover_raw_dates(store) if day >= since]


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
    days: list[date] | None,
) -> tuple[str, list[date]] | None:
    """定出这次重放的日期集合与模式；无事可做时返回 None。"""
    if days is not None:
        wanted = set(days)
        picked = [day for day in dates if day in wanted]
        if not picked:
            return None
        if not _authority_intact(store):
            # 指定日期同样依赖冷区权威层：它俩没了，局部重放照样会抹掉历史。
            return "recovery", dates
        return "target", picked

    if window_days is None:
        return "full", dates
    if not _authority_intact(store):
        return "recovery", dates
    return "window", dates[-window_days:]


def rebuild_from_raw(
    store: DatasetStore,
    *,
    window_days: int | None = REPLAY_WINDOW_DAYS,
    days: list[date] | None = None,
) -> RebuildSummary:
    """重建派生层；默认只重放热区，`window_days=None` 表示全量重放。

    `days` 给定时按这组日期重放，与 `window_days` 互斥（给了 `days` 就忽略窗口）。
    调用方必须用 `replay_suffix` 构造它——见模块 docstring 的后缀闭合约束。
    """
    if days is None and window_days is not None and window_days < 1:
        raise ValueError(f"window_days 必须 >= 1 或设为 None（全量），实得 {window_days}")
    dates = discover_raw_dates(store)
    if not dates:
        return RebuildSummary(
            dates=[], bronze_rows=0, silver_rows=0, observations=0, current_rows=0
        )

    selected = _select_replay(store, dates, window_days=window_days, days=days)
    if selected is None:
        return RebuildSummary(
            dates=[], bronze_rows=0, silver_rows=0, observations=0, current_rows=0
        )
    mode, replay = selected
    full = mode in {"full", "recovery"}

    # 失效状态不是从 raw 派生的：它来自对账时的单库探测（404/401/307），
    # 而 state.update 会把本轮出现的每条记录都置为 active。清理前留存、
    # 重建后写回，协议见 `status` 模块；GLOSSARY #15/#18/#26 记有实测事故。
    retained = retain(store)

    clear_derived(store, hot_dates=None if full else set(replay))

    bronze_rows = 0
    silver_rows = 0
    for snapshot in replay:
        day = derive_day(store, snapshot, enrich=True)
        bronze_rows += day.bronze_rows
        silver_rows += day.silver_rows

    restore(store, retained)

    return RebuildSummary(
        dates=list(replay),
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        observations=store.read_observations().height,
        current_rows=store.read_current().height,
        mode=mode,
        card_rows=store.read_enrich().height,
        config_rows=store.read_card_configs().height,
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
        deriving.clear_day(store, snapshot)


def _clear_stale_state(store: DatasetStore) -> None:
    """全量模式下作废游标与对账会话：重建后它们不再对应任何有效基线。"""
    stale_files = [
        *store.state_dir.glob("*.json"),
        *store.state_dir.glob("reconcile-*.csv"),
        *store.state_dir.glob("reconcile-baseline.parquet"),
    ]
    for stale in stale_files:
        stale.unlink(missing_ok=True)


def _clear(store: DatasetStore, spec: TableSpec) -> None:
    """按表的形态清除：分区表删其父目录，单文件表删文件本身。"""
    if spec.partitioned:
        parent = store.root / spec.path.split("/dt={dt}", 1)[0]
        shutil.rmtree(parent, ignore_errors=True)
    else:
        (store.root / spec.path).unlink(missing_ok=True)
