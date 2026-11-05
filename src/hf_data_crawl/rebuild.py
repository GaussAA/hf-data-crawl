"""从 Raw 重建全部派生层。

Raw 是唯一事实来源：派生规则变了、派生层坏了，都只需跑一次本模块，不联网。
重建会清空 bronze/silver/state——它们全可由 raw 重算，而 raw 不动。
"""

from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl

from .atomic import write_text
from .store import DatasetStore
from .tables import TABLES, Origin, TableSpec


@dataclass(frozen=True)
class RebuildSummary:
    dates: list[date]
    bronze_rows: int
    silver_rows: int
    observations: int
    current_rows: int
    card_rows: int = 0
    config_rows: int = 0


def discover_raw_dates(store: DatasetStore) -> list[date]:
    """扫描 raw/dt=YYYY-MM-DD 目录，返回按时间升序的日期。"""
    if not store.raw_dir.exists():
        return []
    dates: list[date] = []
    for path in store.raw_dir.iterdir():
        if not path.is_dir() or not path.name.startswith("dt="):
            continue
        try:
            dates.append(date.fromisoformat(path.name[3:]))
        except ValueError:
            # 不符合命名的目录不参与重建，跳过总比整轮失败强。
            continue
    return sorted(dates)


def rebuild_from_raw(store: DatasetStore) -> RebuildSummary:
    """按日期顺序重放全部派生层，结果与逐日跑 collect 一致。"""
    dates = discover_raw_dates(store)
    if not dates:
        return RebuildSummary(
            dates=[], bronze_rows=0, silver_rows=0, observations=0, current_rows=0
        )

    # 失效状态不是从 raw 派生的：它来自对账时的单库探测（404/401/307），
    # 而 update_state 会把本轮出现的每条记录都置为 active——直接重建等于把
    # 全部 restricted / renamed 判断清零。2026-11-05 实测踩到：463 条失效标记
    # 在一次 build 后归零，页面上「已失效」变成 0。
    # 故重建前留存，重建后写回。这是「Raw 是唯一事实来源」的一个例外，
    # 例外的边界写在 GLOSSARY.md 第 15 条。
    #
    # 留存分两步：先读（current 优先，为空则读上次的快照），再立刻把快照写盘。
    # 光读内存不够——rebuild 不是事务性的，清除 state 与写回状态之间若被打断
    # （实测：一次 build 被 SIGTERM 杀掉），那些判断就永久丢了。
    carried_status = _failed_status(store) or _read_status_snapshot(store)
    if carried_status:
        _write_status_snapshot(store, carried_status)

    # 清不清除，由登记表的 origin 决定，不再靠手工清单：
    # RAW / STATE 都能从 Raw 重算，NETWORK 删了就得重抓（size.parquet 两万条约 13 分钟）。
    # 2026-11-05 的「build 误删规模表」就是因为这张清单写在 rebuild 里，而
    # 「该表不从 raw 派生」这个事实写在 sizes.py，两边都不知道对方。
    for spec in TABLES.values():
        if spec.origin is Origin.NETWORK:
            continue
        _clear(store, spec)

    # state 目录下还散落着游标与会话文件，它们不属于任何表，重建后同样作废。
    for stale in list(store.state_dir.glob("*.json")) + list(
        store.state_dir.glob("reconcile-seen-*.csv")
    ):
        stale.unlink(missing_ok=True)

    bronze_rows = 0
    silver_rows = 0
    for snapshot in dates:
        bronze_rows += store.build_bronze(snapshot)
        silver_rows += store.build_silver(snapshot)
        store.build_enrich(snapshot)
        store.update_state(snapshot)

    if carried_status:
        store.mark_status(carried_status)

    return RebuildSummary(
        dates=dates,
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        observations=store.read_observations().height,
        current_rows=store.read_current().height,
        card_rows=store.read_enrich().height,
        config_rows=store.read_card_configs().height,
    )


def _clear(store: DatasetStore, spec: TableSpec) -> None:
    """按表的形态清除：分区表删其父目录，单文件表删文件本身。"""
    if spec.partitioned:
        parent = store.root / spec.path.split("/dt={dt}", 1)[0]
        shutil.rmtree(parent, ignore_errors=True)
    else:
        (store.root / spec.path).unlink(missing_ok=True)


def _failed_status(store: DatasetStore) -> dict[str, str]:
    """重建前留存「非在架」状态，供重建后写回。"""
    current = store.read_current()
    if current.height == 0:
        return {}
    stale = current.filter(pl.col("status") != "active")
    return dict(zip(stale["id"].to_list(), stale["status"].to_list()))


def _snapshot_path(store: DatasetStore) -> Path:
    # 放在 meta/ 而非 state/：它不属于任何登记表，重建清不到它。
    return store.root / "meta" / "failed-status.json"


def _read_status_snapshot(store: DatasetStore) -> dict[str, str]:
    """读上次留下的失效状态快照。

    只在 current 里读不到时使用——典型场景是上一次 rebuild 在「已清空 state、
    未写回状态」之间被打断。
    """
    path = _snapshot_path(store)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    return (
        {str(key): str(value) for key, value in payload.items()}
        if isinstance(payload, dict)
        else {}
    )


def _write_status_snapshot(store: DatasetStore, status: dict[str, str]) -> None:
    write_text(_snapshot_path(store), json.dumps(status, ensure_ascii=False, sort_keys=True))
