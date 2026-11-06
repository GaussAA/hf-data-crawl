"""Raw 归档：把一天里成百上千个小分块合并成少数几个分片，并去掉被覆盖的重复记录。

为什么
------
Raw 是唯一不可重算的层，也是唯一没有整理策略的层。全量对账一天就写下两千多个
小文件、228 万条记录，去重后只剩 112 万——重复约 2 倍，且每周一次。

不变量
------
**归档不改变任何一条记录的胜负。** 每个分片在 `groups` 里按段记下原分块的排序键与
来源文件名，`raw.load_batches` 据此切回多个 Batch，所以归档前后 `bronze.build`
的输出必须逐行相等。这一点在移走原文件**之前**就验证，不等则撤销、原文件原地不动。

去重只对 Bronze 消费的标签（part / top）做：同 id 只留排序最靠后的一条，正是
`unique(keep="last")` 本来就会留下的那条。`card` 标签只合并不去重——卡片表按
(id, config, split) 去重，记录级去重可能丢掉旧记录里独有的 config 行。

原文件不删除，移到 `backup/raw/dt=<日期>/`，确认无误后由人清理。

内存
----
合并阶段不把一天的记录全载入（未实测全载入的峰值）。分两遍：第一遍只收集每条记录的
id，定出每个 id 最后一次出现的位置；第二遍逐块重读，只写出胜出的记录，边读边落分片。
验证阶段则要把归档前后各算一遍 Bronze，峰值与 `bronze.build` 同量级的两倍。
"""

from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

from . import bronze, raw
from .atomic import write_bytes

if TYPE_CHECKING:
    from .store import DatasetStore

#: 一个分片约装这么多条记录。必须留足分片数，Bronze 重建才有并行度：
#: 5 万条一片时 11-04 只有 26 片，低于 `bronze.PARALLEL_MIN_FILES`，重建退回串行，
#: 在副本上实测 138s，而归档前并行是 56~62s。
SHARD_RECORDS = 10_000
#: 做记录级去重的标签。
DEDUPE_LABELS = frozenset({"part", "top"})
PARALLEL_MIN_FILES = 64
BACKUP_SUBDIR = ("backup", "raw")


class ArchiveMismatch(RuntimeError):
    """归档前后的 Bronze 不一致；已撤销新分片，原文件未动。"""


@dataclass(frozen=True)
class ArchiveResult:
    snapshot_date: date
    source_files: int
    shards: int
    records_before: int
    records_after: int
    bytes_before: int
    bytes_after: int
    backup_dir: Path | None


def backup_dir(store: DatasetStore, snapshot_date: date) -> Path:
    return store.root.joinpath(*BACKUP_SUBDIR, f"dt={snapshot_date.isoformat()}")


def _valid_id(record: Any) -> str | None:
    """与 `bronze.to_row` 同一口径：非空字符串才算有 id。"""
    if isinstance(record, dict):
        value = record.get("id")
        if isinstance(value, str) and value:
            return value
    return None


def _ids_of(path: str) -> list[tuple[tuple[Any, ...], str, list[str | None]]]:
    """第一遍：一个文件里每个 Batch 的 (排序键, 来源名, 各记录 id)。顶层函数供进程池调用。"""
    return [
        (batch.key, batch.file, [_valid_id(record) for record in batch.records])
        for batch in raw.load_batches(Path(path))
    ]


def _scan_ids(paths: list[Path]) -> list[tuple[tuple[Any, ...], str, list[str | None], Path, int]]:
    """第一遍的并行入口；返回按排序键升序的 (键, 来源名, ids, 文件, 文件内段号)。"""
    jobs = [str(path) for path in paths]
    results: list[list[tuple[tuple[Any, ...], str, list[str | None]]]]
    if len(jobs) < PARALLEL_MIN_FILES:
        results = [_ids_of(job) for job in jobs]
    else:
        workers = max(1, min(bronze.MAX_WORKERS, os.cpu_count() or 1))
        try:
            with ProcessPoolExecutor(max_workers=workers) as pool:
                results = list(pool.map(_ids_of, jobs, chunksize=16))
        except BrokenProcessPool:
            results = [_ids_of(job) for job in jobs]
    entries = [
        (key, file, ids, path, index)
        for path, groups in zip(paths, results, strict=True)
        for index, (key, file, ids) in enumerate(groups)
    ]
    entries.sort(key=lambda entry: entry[0])
    return entries


def _write_shard(
    directory: Path, label: str, index: int, groups: list[dict[str, Any]], records: list[Any]
) -> Path:
    payload = {
        "meta": {"label": label, "archived": True, "record_count": len(records)},
        "groups": groups,
        "records": records,
    }
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    digest = hashlib.sha256(body).hexdigest()[:8]
    path = directory / raw.archive_name(label, index, digest)
    if not path.exists():
        write_bytes(path, gzip.compress(body))
    return path


def _archive_label(directory: Path, label: str, paths: list[Path]) -> list[Path]:
    """把同一标签的若干文件合并成分片，返回新分片路径。不删任何东西。"""
    entries = _scan_ids(paths)
    dedupe = label in DEDUPE_LABELS

    last: dict[str, tuple[int, int]] = {}
    if dedupe:
        for position, (_key, _file, ids, _path, _index) in enumerate(entries):
            for offset, dataset_id in enumerate(ids):
                if dataset_id is not None:
                    last[dataset_id] = (position, offset)

    shards: list[Path] = []
    groups: list[dict[str, Any]] = []
    records: list[Any] = []
    cached_path: Path | None = None
    cached: list[raw.Batch] = []

    def flush() -> None:
        nonlocal groups, records
        if groups:
            shards.append(_write_shard(directory, label, len(shards) + 1, groups, records))
        groups, records = [], []

    for position, (key, file, ids, path, index) in enumerate(entries):
        if path != cached_path:
            cached, cached_path = raw.load_batches(path), path
        source = cached[index].records
        if dedupe:
            kept = []
            for offset, record in enumerate(source):
                dataset_id = ids[offset]
                # 无有效 id 的记录 Bronze 本就丢弃，原样保留，归档不替 Bronze 做取舍。
                if dataset_id is None or last[dataset_id] == (position, offset):
                    kept.append(record)
        else:
            kept = list(source)
        if not kept:
            continue
        groups.append({"key": list(key), "file": file, "count": len(kept)})
        records.extend(kept)
        if len(records) >= SHARD_RECORDS:
            flush()
    flush()
    return shards


def _card_view(paths: list[Path]) -> list[tuple[Any, ...]]:
    """不去重标签的逻辑内容：按排序键排好的 (键, 来源名, 记录)，忽略空段。"""
    batches = sorted(
        (batch for path in paths for batch in raw.load_batches(path)), key=lambda b: b.key
    )
    return [(tuple(b.key), b.file, b.records) for b in batches if b.records]


def archive_day(store: DatasetStore, snapshot_date: date) -> ArchiveResult | None:
    """归档某一天；该天没有 Raw 返回 None。验证失败抛 `ArchiveMismatch`，原文件不动。"""
    sources = raw.batch_files(store, snapshot_date)
    if not sources:
        return None
    if all(raw.is_archive(path) for path in sources):
        # 已经归档过：再合并只会换个分片名，没有收益。
        return None

    directory = raw.partition_dir(store, snapshot_date)
    by_label: dict[str, list[Path]] = {}
    for path in sources:
        by_label.setdefault(raw.label_of(path), []).append(path)

    created: list[Path] = []
    try:
        for label, paths in sorted(by_label.items()):
            created += _archive_label(directory, label, paths)

        old_frame = bronze.frame_from_files(sources, snapshot_date)
        new_frame = bronze.frame_from_files(created, snapshot_date)
        same_bronze = (old_frame is None and new_frame is None) or (
            old_frame is not None and new_frame is not None and old_frame.equals(new_frame)
        )
        del old_frame, new_frame
        cards_old = [p for p in sources if raw.label_of(p) not in DEDUPE_LABELS]
        cards_new = [p for p in created if raw.label_of(p) not in DEDUPE_LABELS]
        same_cards = _card_view(cards_old) == _card_view(cards_new)
        if not (same_bronze and same_cards):
            raise ArchiveMismatch(
                f"{snapshot_date} 归档前后不一致"
                f"（bronze 相同={same_bronze}，非去重标签相同={same_cards}）；已撤销，原文件未动"
            )
    except BaseException:
        for path in created:
            if path not in set(sources):
                path.unlink(missing_ok=True)
        raise

    keep = set(created)
    moved = [path for path in sources if path not in keep]
    target = backup_dir(store, snapshot_date)
    if moved:
        target.mkdir(parents=True, exist_ok=True)
        for path in moved:
            shutil.move(str(path), str(target / path.name))

    records_before = 0
    for path in moved:
        records_before += sum(len(b.records) for b in raw.load_batches(target / path.name))
    records_after = sum(len(b.records) for path in created for b in raw.load_batches(path))
    return ArchiveResult(
        snapshot_date=snapshot_date,
        source_files=len(sources),
        shards=len(created),
        records_before=records_before,
        records_after=records_after,
        bytes_before=sum((target / p.name).stat().st_size for p in moved),
        bytes_after=sum(p.stat().st_size for p in created),
        backup_dir=target if moved else None,
    )
