"""`pipeline` 切片的命令处理函数，参数解析与分派在 ops/cli。"""

from __future__ import annotations

import argparse
from datetime import date

from ..platform.retention import REPLAY_WINDOW_DAYS
from ..platform.runs import Outcome
from ..platform.store import DatasetStore
from .rebuild import (
    RebuildSummary,
    cold_gap_days,
    divergent_since,
    rebuild_from_raw,
    replay_suffix,
)


def run_build(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    if args.full and args.stale:
        print("失败：--full 与 --stale 互斥；全量重放不需要挑日期。")
        return Outcome({"error": "flags"}, code=1, reason="--full 与 --stale 不能同时给")

    if args.full:
        gaps = cold_gap_days(store)
        if gaps and not getattr(args, "allow_cold_loss", False):
            print(
                f"失败：--full 只能重放主目录里的 Raw，但有 {_describe_gaps(gaps)}。\n"
                "这些日期的 silver / observations / current 会被清空且无法重放回来——不可逆。\n"
                "二选一：\n"
                "  ① 不需要重算冷区：改用 build（默认热区）或 build --stale；\n"
                "  ② 确实要全量重算：先把 data/backup/raw/dt=<日期>/ 搬回 data/raw/dt=<日期>/，\n"
                "     确认 doctor 无断日后再跑（见 docs/operations/RECOVERY.md）。\n"
                "若已确认接受这批历史丢失，加 --allow-cold-loss 显式放行。"
            )
            return Outcome(
                {
                    "error": "cold_gaps",
                    "cold_gaps": len(gaps),
                    "oldest": gaps[0].isoformat(),
                    "newest": gaps[-1].isoformat(),
                },
                code=1,
                reason=f"{len(gaps)} 天已压入冷区，--full 重放不到其历史",
            )
        if gaps:
            print(f"[!] 已按 --allow-cold-loss 放行：{_describe_gaps(gaps)}的历史将无法重放")

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


def _describe_gaps(gaps: list[date]) -> str:
    """把冷区缺口说成人能读的一句。中间可能不连续，故报最早/最晚而非区间。"""
    if len(gaps) == 1:
        return f"1 天已压入冷区（{gaps[0].isoformat()}）"
    return f"{len(gaps)} 天已压入冷区（最早 {gaps[0].isoformat()}，最晚 {gaps[-1].isoformat()}）"


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
    if summary.cold_gaps:
        # 全量重建才可能非零：这些日期的历史这次没被重放，别让人以为「已全量」。
        print(f"  [!] 冷区 {summary.cold_gaps} 天未参与重放（无 Raw 可读），其历史未恢复")
    if summary.card_rows or summary.config_rows:
        print(
            f"  卡片 {summary.card_rows} 行 · config 明细 {summary.config_rows} 行"
            f"（已按新规则从 raw 重算）"
        )
