"""运行摘要落盘：每轮一条 JSON，便于连续两轮对比、失败留痕。

失败必须写、且要带原因——静默失败比跑得慢更糟。
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from .store import DatasetStore


def write_run(
    store: DatasetStore,
    command: str,
    started_at: datetime,
    status: str,
    payload: Any,
) -> Path:
    runs_dir = store.root / "runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    stamp = started_at.strftime("%Y%m%dT%H%M%S")
    path = runs_dir / f"{stamp}-{command}.json"
    # 同一秒内连跑两条会撞名，撞名就加序号——覆盖会导致上一轮摘要静默消失。
    suffix = 1
    while path.exists():
        suffix += 1
        path = runs_dir / f"{stamp}-{command}-{suffix}.json"

    body = {
        "command": command,
        "status": status,
        # 精度到微秒：同一秒内连跑两轮时，秒级时间戳无法区分先后，
        # 而「连续两轮对比」正是靠这个顺序成立的。
        "started_at": started_at.isoformat(timespec="microseconds"),
        "finished_at": datetime.now().isoformat(timespec="microseconds"),
        "payload": _jsonable(payload),
    }
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        # asdict 会递归展开，但 Path/date 仍需兜底转字符串
        return json.loads(json.dumps(dataclasses.asdict(value), default=str))
    return json.loads(json.dumps(value, default=str))


def read_runs(store: DatasetStore) -> list[dict[str, Any]]:
    """按时间升序读取全部运行摘要。

    排序依据是记录里的 `started_at`，**不是文件名**：同一秒内的第二轮会被写成
    `...-build-2.json`，而 `-` 的字典序排在 `.` 之前，按文件名排会把后一轮
    排到前一轮之前——「连续两轮对比」于是读到颠倒的一对。
    """
    runs_dir = store.root / "runs"
    if not runs_dir.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(runs_dir.glob("*.json")):
        try:
            records.append(json.loads(path.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError):
            # 单条损坏不影响其余摘要的读取
            continue
    records.sort(key=lambda item: (str(item.get("started_at") or ""), str(item.get("finished_at") or "")))
    return records
