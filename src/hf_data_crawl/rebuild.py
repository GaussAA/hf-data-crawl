"""从 Raw 重建全部派生层。

Raw 是唯一事实来源：派生规则变了、派生层坏了，都只需跑一次本模块，不联网。
重建会清空 bronze/silver/state——它们全可由 raw 重算，而 raw 不动。
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date

from .store import DatasetStore


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
        return RebuildSummary(dates=[], bronze_rows=0, silver_rows=0, observations=0, current_rows=0)

    for derived in (store.bronze_dir, store.silver_dir, store.state_dir):
        shutil.rmtree(derived, ignore_errors=True)
    # enrich 下有两类产物，待遇不同：
    # - carddata / card_configs 可由 raw 里的 card-*.json.gz 重算，删了能重建；
    # - size.parquet 来自 datasets-server，raw 里没有对应数据，删了就白丢一轮抓取
    #   （两万条约 13 分钟）。故只删前者。
    for filename in ("carddata.parquet", "card_configs.parquet"):
        (store.enrich_dir / filename).unlink(missing_ok=True)

    bronze_rows = 0
    silver_rows = 0
    for snapshot in dates:
        bronze_rows += store.build_bronze(snapshot)
        silver_rows += store.build_silver(snapshot)
        store.build_enrich(snapshot)
        store.update_state(snapshot)

    return RebuildSummary(
        dates=dates,
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        observations=store.read_observations().height,
        current_rows=store.read_current().height,
        card_rows=store.read_enrich().height,
        config_rows=store.read_card_configs().height,
    )
