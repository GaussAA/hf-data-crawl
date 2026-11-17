"""`pipeline` 切片的命令处理函数，参数解析与分派在 ops/cli。"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

import polars as pl

from ..platform.retention import REPLAY_WINDOW_DAYS
from ..platform.runs import Outcome
from ..platform.store import DatasetStore
from . import bronze as bronze_module
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


def run_bronze_view(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """按日期从 Raw 现算 Bronze，打印或导出，不写数据目录。

    为什么需要这条命令：Bronze 只保留最新一天（它可由 Raw 重建，历史快照留着只是
    占地方），历史那几天的分区早被 `prune` 删掉。此前想回看只能先 `build`——而
    `build` 会写正式目录并顺手 prune，等于为了看一眼而改数据。这条只读 Raw，
    也不进写命令的锁，跑的时候不影响正在跑的采集。

    「某天某个库长什么样」的答案在 Silver（属性变更流水）与 observations（指标流水）
    里也能拼出来，但那是推导结果；要看**原始标量**（含 `raw` 整行、`page_file`
    来源分块）只有这里。
    """
    day = date.fromisoformat(args.date) if args.date else date.today()
    frame = bronze_module.day_frame_from_raw(store, day)
    if frame.height == 0:
        print(f"{day}：Raw 里没有可派生的记录（扫描批次与补抓批次都没有）")
        return Outcome({"date": day.isoformat(), "rows": 0})

    if args.out:
        target = Path(args.out)
        target.parent.mkdir(parents=True, exist_ok=True)
        if target.suffix.lower() == ".csv":
            frame.write_csv(target)
        else:
            frame.write_parquet(target)
        print(f"{day}：{frame.height:,} 行 × {len(frame.columns)} 列 → {target}")
        return Outcome({"date": day.isoformat(), "rows": frame.height, "out": str(target)})

    print(f"{day}：{frame.height:,} 行 × {len(frame.columns)} 列（只读，来自 Raw）")
    if args.id:
        row = frame.filter(pl.col("id") == args.id)
        if row.height == 0:
            print(f"  这一天没有 id = {args.id} 的记录")
            return Outcome({"date": day.isoformat(), "rows": 0, "id": args.id})
        # 只挑能一眼判断的列，整行 21 列含 raw 字段，终端里会糊成一片。
        print(row.select("id", "last_modified", "downloads", "likes", "tags", "description"))
        print("  要整行（含 raw 原始 JSON）请加 --out 导出后看。")
        return Outcome({"date": day.isoformat(), "rows": row.height, "id": args.id})

    print(frame.head(args.limit).select("id", "last_modified", "downloads", "likes"))
    if frame.height > args.limit:
        print(f"  ……共 {frame.height:,} 行；用 --limit 多看几行，或 --out 导出整表。")
    return Outcome({"date": day.isoformat(), "rows": frame.height})


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
    if summary.shrinks:
        # 重放先清分区再写回：这条不喊，删掉的历史观测不留任何痕迹。
        shown = "、".join(
            f"{day}（{before}→{after}）" for day, before, after in summary.shrinks[:3]
        )
        print(
            f"  [!] {len(summary.shrinks)} 天的观测只写回不到一半，历史可能已丢失：{shown}；"
            "多半是当天 Raw 缺批，核对后重跑派生"
        )
    if summary.card_rows or summary.config_rows:
        print(
            f"  卡片 {summary.card_rows} 行 · config 明细 {summary.config_rows} 行"
            f"（已按新规则从 raw 重算）"
        )
