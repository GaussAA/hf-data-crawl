"""冷区 Raw 跨天压缩：多日全量快照压成一份基线，原分块只移入 backup、不删。

为什么
------
`archive` 只压**同一天内**被覆盖的重复；跨天的全量快照仍是一天一份，主目录
线性膨胀。而 `silver`（属性变更流水）与 `observations`（指标变更流水）本就是
「变化才追加」的压缩表示，冷区历史已由它们承载，默认 `build` 不再重放冷区
（见 `rebuild`）。因此冷区 Raw 只需留一份「每个 id 最后一次出现的原始响应」
供追溯，不必按天留全量。

不变量
------
**压缩不改变任何一条记录的胜负。** 每个基线分片在 `groups` 里按段记下原分块的
排序键、来源文件名与命名空间，`raw.load_batches` 据此切回多个 Batch。写完先
从新基线读回、与源分块的期望胜出者逐条比对，不等即撤销新分片、原文件不动。

原分块移入 `data/backup/raw/dt=<日期>/`，**不删除**：这是 `build --full`
全量重放的逃生舱，也是任何一步出错时的回滚依据。旧基线移到 `backup/cold/`。
"""

from __future__ import annotations

import gzip
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..pipeline import raw
from ..platform.atomic import write_bytes
from ..platform.retention import backup_raw_dir, cold_dir, manifest_path, read_compressed_dates

if TYPE_CHECKING:
    from ..platform.store import DatasetStore

#: 与 archive 分片同量级，保留并行度。
BASELINE_RECORDS = 10_000
_BASELINE_GLOB = "baseline-*.json.gz"


class CompactMismatch(RuntimeError):
    """压缩前后胜出者不一致，或备份目标已存在；已撤销新基线，原文件未动。"""


@dataclass(frozen=True)
class CompactResult:
    dates: tuple[date, ...]
    source_files: int
    baselines: int
    records_before: int
    records_after: int
    bytes_before: int
    bytes_after: int
    backup_files: int


def _valid_id(record: Any) -> str | None:
    """与 `bronze.to_row` / `archive._valid_id` 同一口径。"""
    if isinstance(record, dict):
        value = record.get("id")
        if isinstance(value, str) and value:
            return value
    return None


def _hash(record: Any) -> str:
    body = json.dumps(record, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return hashlib.sha256(body).hexdigest()[:16]


def _entries(paths: list[Path]) -> list[tuple[tuple[Any, ...], str, list[Any], Path]]:
    """按排序键展开所有分块；基线分片靠 groups 切回原来的分块。"""
    entries = [
        (batch.key, batch.file, batch.records, path)
        for path in paths
        for batch in raw.load_batches(path)
    ]
    entries.sort(key=lambda entry: entry[0])
    return entries


def _last_positions(
    entries: list[tuple[tuple[Any, ...], str, list[Any], Path]],
) -> dict[str, tuple[int, int]]:
    last: dict[str, tuple[int, int]] = {}
    for position, (_key, _file, records, _path) in enumerate(entries):
        for offset, record in enumerate(records):
            dataset_id = _valid_id(record)
            if dataset_id is not None:
                last[dataset_id] = (position, offset)
    return last


def _kept_records(records: list[Any], position: int, last: dict[str, tuple[int, int]]) -> list[Any]:
    """同一 id 只留最终胜出者；无有效 id 的记录 Bronze 本就丢弃，原样保留。"""
    kept: list[Any] = []
    for offset, record in enumerate(records):
        dataset_id = _valid_id(record)
        if dataset_id is None or last[dataset_id] == (position, offset):
            kept.append(record)
    return kept


def _write_baseline(
    directory: Path, index: int, groups: list[dict[str, Any]], records: list[Any]
) -> Path:
    payload: dict[str, Any] = {
        "meta": {"baseline": True, "record_count": len(records)},
        "groups": groups,
        "records": records,
    }
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    path = directory / f"baseline-{index:06d}-{hashlib.sha256(body).hexdigest()[:8]}.json.gz"
    if not path.exists():
        write_bytes(path, gzip.compress(body))
    return path


def _written(created: list[Path]) -> tuple[dict[str, str], int]:
    """从新基线读回实际胜出者与记录总数。"""
    winner: dict[str, str] = {}
    total = 0
    for path in created:
        for batch in raw.load_batches(path):
            for record in batch.records:
                dataset_id = _valid_id(record)
                if dataset_id is not None:
                    winner[dataset_id] = _hash(record)
            total += len(batch.records)
    return winner, total


def compact(store: DatasetStore, days: list[date]) -> CompactResult | None:
    """把若干冷区日期与已有基线合并成一份新基线；无源文件返回 None。"""
    if not days:
        return None
    sources: list[tuple[date, Path]] = []
    for day in sorted(set(days)):
        sources += [(day, path) for path in raw.batch_files(store, day)]
    if not sources:
        return None

    root = cold_dir(store)
    root.mkdir(parents=True, exist_ok=True)
    old_baselines = set(root.glob(_BASELINE_GLOB))

    # 移动之前把体积算好：移走后再 stat 会直接 FileNotFoundError。
    bytes_before = sum(path.stat().st_size for _day, path in sources)
    bytes_before += sum(path.stat().st_size for path in old_baselines)

    # 目标预检：中途发现「备份已存在」会留下半移动状态，必须一次判完。
    destinations = [(path, backup_raw_dir(store, day) / path.name) for day, path in sources]
    clashes = [str(target) for _path, target in destinations if target.exists()]
    if clashes:
        raise CompactMismatch(f"备份目标已存在，请人工核对后重试：{clashes[:3]}")

    paths = [path for _day, path in sources] + sorted(old_baselines)
    entries = _entries(paths)
    last = _last_positions(entries)
    expected_winner = {
        dataset_id: _hash(entries[position][2][offset])
        for dataset_id, (position, offset) in last.items()
    }
    expected_count = sum(
        len(_kept_records(records, position, last))
        for position, (_key, _file, records, _path) in enumerate(entries)
    )

    created: list[Path] = []
    try:
        groups: list[dict[str, Any]] = []
        records: list[Any] = []

        def flush() -> None:
            nonlocal groups, records
            if groups:
                created.append(_write_baseline(root, len(created) + 1, groups, records))
            groups, records = [], []

        for position, (key, file, chunk, _path) in enumerate(entries):
            kept = _kept_records(chunk, position, last)
            if not kept:
                continue
            groups.append(
                {
                    "key": list(key),
                    "file": file,
                    "count": len(kept),
                    "source": raw.source_of(Path(file)),
                }
            )
            records.extend(kept)
            if len(records) >= BASELINE_RECORDS:
                flush()
        flush()

        actual_winner, actual_count = _written(created)
        if actual_winner != expected_winner or actual_count != expected_count:
            raise CompactMismatch(
                "冷区基线校验失败：胜出者或记录数与源分块不一致；已撤销新分片，原文件未动"
            )
    except BaseException:
        for path in created:
            if path not in old_baselines:
                path.unlink(missing_ok=True)
        raise

    # 校验通过后才动原文件：旧基线进 backup/cold，源分块进 backup/raw/dt=。
    stale = old_baselines - set(created)
    backup_cold = store.root / "backup" / "cold"
    if stale:
        backup_cold.mkdir(parents=True, exist_ok=True)
        for path in sorted(stale):
            shutil.move(str(path), str(backup_cold / path.name))

    for day, path in sources:
        target = backup_raw_dir(store, day)
        target.mkdir(parents=True, exist_ok=True)
        shutil.move(str(path), str(target / path.name))

    # 空的 dt= 目录不该留在主目录：否则 doctor / build 会把「已压缩的冷区」
    # 又当成待重放的日期。只删空目录，有残留文件就原样保留待人处理。
    for day in sorted(set(days)):
        partition = raw.partition_dir(store, day)
        if partition.exists() and not any(partition.iterdir()):
            partition.rmdir()

    manifest_dates = read_compressed_dates(store) | set(days)
    payload = {
        "compressed_dates": sorted(day.isoformat() for day in manifest_dates),
        "baselines": [path.name for path in created],
        "updated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    write_bytes(
        manifest_path(store),
        json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"),
    )

    return CompactResult(
        dates=tuple(sorted(set(days))),
        source_files=len(sources),
        baselines=len(created),
        records_before=sum(len(entry[2]) for entry in entries),
        records_after=expected_count,
        bytes_before=bytes_before,
        bytes_after=sum(path.stat().st_size for path in created),
        backup_files=len(sources),
    )
