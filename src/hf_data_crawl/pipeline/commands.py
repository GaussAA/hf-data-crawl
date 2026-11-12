"""`pipeline` 切片的命令处理函数（原 ops/cli.py），解析与分派仍在 ops/cli。"""

from __future__ import annotations

import argparse

from ..platform.retention import REPLAY_WINDOW_DAYS
from ..platform.runs import Outcome
from ..platform.store import DatasetStore
from .rebuild import RebuildSummary, divergent_since, rebuild_from_raw, replay_suffix


def _build(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    if args.full and args.stale:
        print("失败：--full 与 --stale 互斥；全量重放不需要挑日期。")
        return Outcome({"error": "flags"}, code=1, reason="--full 与 --stale 不能同时给")

    if args.stale:
        since = divergent_since(store)
        if since is None:
            print("派生层与 Raw 已对齐（逐日戳记一致），无需重建。")
            return Outcome({"snapshot_date": None, "mode": "already-aligned", "days": 0})
        days = replay_suffix(store, since)
        # 说清楚为什么不是只修那一天：后缀闭合是正确性要求，不是保守做法。
        print(f"检出派生滞后：自 {since} 起重放 {len(days)} 天（含其后所有有 Raw 的日子）")
        summary = rebuild_from_raw(store, days=days)
    else:
        summary = rebuild_from_raw(store, window_days=None if args.full else REPLAY_WINDOW_DAYS)
    _print_build(summary)
    return Outcome(summary)


def _print_build(summary: RebuildSummary) -> None:
    if not summary.dates:
        print("raw 下没有任何分区，无可重建。")
        return

    mode = {
        "window": f"热区重放（近 {REPLAY_WINDOW_DAYS} 天，冷区权威层保留）",
        "full": "全量重放（显式 --full）",
        "recovery": "全量重放（冷区权威层缺失，自动回退）",
        "target": "定点重放（派生滞后的日期及其后，冷区权威层保留）",
    }[summary.mode]
    print(
        f"重建完成：{len(summary.dates)} 个日期"
        f"（{summary.dates[0]} ~ {summary.dates[-1]}），全程未联网 · {mode}"
    )
    print(
        f"  bronze {summary.bronze_rows} 行 · silver {summary.silver_rows} 行 · "
        f"观测 {summary.observations} 行 · current {summary.current_rows} 行"
    )
    if summary.card_rows or summary.config_rows:
        print(
            f"  卡片 {summary.card_rows} 行 · config 明细 {summary.config_rows} 行"
            f"（已按新规则从 raw 重算）"
        )
