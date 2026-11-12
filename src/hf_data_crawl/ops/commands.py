"""`ops` 切片的命令处理函数，参数解析与分派在 ops/cli。"""

from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path

from ..pipeline.lineage import restamp
from ..platform.lock import LockBusy, exclusive
from ..platform.retention import BACKUP_PRUNE_AFTER_DAYS
from ..platform.runs import Outcome
from ..platform.store import DatasetStore
from .doctor import diagnose, sync_alert
from .health import record as record_health


def run_archive(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """归档符合条件的日期。某天验证失败就记为失败，但不影响已完成的其他日期。"""
    from datetime import timedelta

    from ..pipeline.rebuild import discover_raw_dates
    from .archive import ArchiveMismatch, archive_day

    if args.date:
        days = [date.fromisoformat(args.date)]
    else:
        cutoff = date.today() - timedelta(days=args.older_than_days)
        days = [day for day in discover_raw_dates(store) if day < cutoff]

    done: list[dict[str, object]] = []
    failures: list[str] = []
    for day in days:
        try:
            result = archive_day(store, day)
        except ArchiveMismatch as error:
            failures.append(str(error))
            print(f"  [!] {error}")
            continue
        if result is None:
            continue
        # 归档在移走原件之前校验过「前后 Bronze 逐行相等」，派生结果仍然有效，
        # 只是 Raw 分块换了一批文件名与 mtime。不刷新指纹，下轮体检会把这次
        # 等价改写报成「派生滞后」——假警报比漏报更能训练人忽略告警。
        restamp(store, day)
        done.append(
            {
                "date": day.isoformat(),
                "files": f"{result.source_files}→{result.shards}",
                "records": f"{result.records_before}→{result.records_after}",
                "bytes": f"{result.bytes_before}→{result.bytes_after}",
            }
        )
        print(
            f"{day} 归档：{result.source_files} 个文件→{result.shards} 个分片，"
            f"记录 {result.records_before}→{result.records_after}，"
            f"{result.bytes_before / 1e6:.1f}MB→{result.bytes_after / 1e6:.1f}MB，"
            f"原文件在 {result.backup_dir}"
        )
    if not days:
        print("没有符合条件的日期。")
    payload = {"archived": done, "failures": failures}
    if failures:
        return Outcome(payload, code=1, reason=f"{len(failures)} 个日期归档验证失败")
    return Outcome(payload)


def run_compact(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """压缩冷区日期。一次合并全部目标日期与已有基线，避免逐日重写基线。"""
    from datetime import timedelta

    from ..pipeline.rebuild import discover_raw_dates
    from .compact import CompactMismatch, compact

    if args.date:
        days = [date.fromisoformat(args.date)]
    else:
        cutoff = date.today() - timedelta(days=args.older_than_days)
        days = [day for day in discover_raw_dates(store) if day < cutoff]
    if not days:
        print("没有符合条件的冷区日期。")
        return Outcome({"compacted": [], "failures": []})

    try:
        result = compact(store, days)
    except CompactMismatch as error:
        print(f"  [!] {error}")
        return Outcome({"compacted": [], "failures": [str(error)]}, code=1, reason=str(error))
    if result is None:
        print("这些日期在 data/raw 里没有分块，无需压缩。")
        return Outcome({"compacted": [], "failures": []})

    print(
        f"冷区压缩完成：{len(result.dates)} 个日期 · {result.source_files} 个分块"
        f"→ {result.baselines} 个基线分片，记录 "
        f"{result.records_before}→{result.records_after}，"
        f"{result.bytes_before / 1e6:.1f}MB→{result.bytes_after / 1e6:.1f}MB"
    )
    print("原分块在 data/backup/raw/dt=<日期>/，未删除；回滚见 docs/operations/RECOVERY.md")
    return Outcome(
        {
            "dates": [day.isoformat() for day in result.dates],
            "files": f"{result.source_files}→{result.baselines}",
            "records": f"{result.records_before}→{result.records_after}",
            "bytes": f"{result.bytes_before}→{result.bytes_after}",
            "backup_files": result.backup_files,
        }
    )


def run_prune(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """清理备份。搬运与销毁分开：这里只销毁，且只碰 `backup/`。"""
    from .prune import apply, plan, total_size

    targets = plan(store)
    before = total_size(store)
    if not targets:
        print(f"备份没有需要清理的分区（阈值 {BACKUP_PRUNE_AFTER_DAYS} 天）：{before / 1e6:.1f} MB")
        return Outcome({"removed": [], "freed": 0, "backup_mb": round(before / 1e6, 1)})

    reclaimable = sum(item.bytes for item in targets)
    if args.dry_run:
        for item in targets:
            print(f"  将清理 {item.kind}/{item.path.name}  {item.bytes / 1e6:.1f} MB")
        print(
            f"预演：{len(targets)} 处可清理 · 释放 {reclaimable / 1e6:.1f} MB"
            f"（当前 backup {before / 1e6:.1f} MB，未动磁盘）"
        )
        return Outcome(
            {
                "removed": [],
                "freed": 0,
                "reclaimable": reclaimable,
                "dry_run": True,
            }
        )

    report = apply(store, targets)
    after = total_size(store)
    for item in report.removed:
        print(f"  已清理 {item.kind}/{item.path.name}  {item.bytes / 1e6:.1f} MB")
    print(
        f"清理完成：{report.count} 处 · 释放 {report.bytes_freed / 1e6:.1f} MB"
        f"（backup {before / 1e6:.1f} → {after / 1e6:.1f} MB）"
    )
    return Outcome(
        {
            "removed": [item.path.name for item in report.removed],
            "freed": report.bytes_freed,
            "backup_mb": round(after / 1e6, 1),
        }
    )


def _doctor(store: DatasetStore, *, snapshot: bool, requests_root: Path | None = None) -> int:
    """体检：有问题退出码为 1，写 ALERT.txt，并把结论追加进体检流水。

    流水（`state/health-history.jsonl`）与 `runs/` 分工不同：`runs/` 记「命令成没
    成功」，而体检退出码 1 是「有告警」——记进 runs 会让「近 7 天无失败运行」失真。
    没有流水时，「断日从哪天开始」这类问题只能靠人回忆。

    `snapshot=False` 时跨表检查已由 `diagnose` 降级为提示，退出码与 `ALERT.txt`
    只反映不依赖一致快照的问题。
    """
    now = datetime.now()
    if snapshot:
        print("快照：一致（已持有写锁）")
    else:
        print("快照：不可用（写锁被占用，未取得锁）；跨表读数可能跨时点，跨表检查已降级")
    findings = diagnose(store, now, snapshot=snapshot, requests_root=requests_root)
    sync_alert(store, findings, now, snapshot=snapshot)
    try:
        record_health(store, findings, now, snapshot=snapshot)
    except OSError as error:
        # 记不下流水不该让体检本身变成失败：结论仍按 findings 判定。
        print(f"[!] 体检历史写入失败：{error}")
    for item in findings:
        print(f"{'[!]' if item.problem else '[ok]'} {item.text}")
    return 1 if any(item.problem for item in findings) else 0


def run_doctor(store: DatasetStore, wait: float, *, requests_root: Path | None = None) -> int:
    """体检优先在一致快照上跑；等不到写锁就降级，不阻塞人工排障。

    与 report/docs 不同：那两个命令拿不到锁直接失败，因为产出的文件必须基于
    一致快照；体检的价值在于「现在就告诉我大致哪里不对」，标注非快照比拒绝
    执行更有用。
    """
    try:
        with exclusive(store.root, "doctor", wait):
            return _doctor(store, snapshot=True, requests_root=requests_root)
    except LockBusy:
        return _doctor(store, snapshot=False, requests_root=requests_root)


def run_docs(args: argparse.Namespace, store: DatasetStore) -> int:
    """生成 data 目录说明。纯本地动作，不联网。

    缺字段说明时退出码为 1：文档不完整等于没写，应当在生成那一刻就看见。
    """
    from .datadoc import missing_docs, write

    target = Path(args.out) if args.out else Path("docs") / "DATA.md"
    path = write(store.root, target)
    gaps = missing_docs()
    print(f"数据说明已生成：{path}")
    if gaps:
        preview = "、".join(f"{name}.{column}" for name, column in gaps[:6])
        print(f"  [!] {len(gaps)} 个字段缺说明：{preview}{' …' if len(gaps) > 6 else ''}")
        print("      补在 src/hf_data_crawl/ops/fielddocs.py 里")
        return 1
    return 0
