"""来源指纹：这一份交付到底基于哪一版底库、哪一版输入。

单拎出来是因为它被三方共用——SQL 查询型交付、表格回填型交付、以及编排层。
放在 `delivery` 里会让 `backfill` 反过来依赖 `delivery`，而 `delivery` 又要按
kind 分派到 `backfill`，绕成一个环。环一旦成立，import 顺序就成了隐性契约：
谁先被导入决定谁能跑，而这种事只有在某个入口下才会炸。

输入快照的指纹与底库指纹分开记：底库天天变，输入表是受理时固化下来的那一份。
两个都写进 manifest，日后「为什么当时填出这个数」才有确切答案。
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

from .platform.store import DatasetStore


class SourceMissing(RuntimeError):
    """底库不存在：没有 `current` 就没有「基于哪一版」可言，任何交付都不成立。

    单独一个异常而不复用 `DeliveryBlocked`：后者住在 `delivery` 里，而 `delivery`
    要按 kind 分派到 `backfill`、`backfill` 又要算指纹，复用就会成环。编排层负责
    把它翻译成自己的失败类型，调用方看到的语义不变。
    """


@dataclass(frozen=True)
class SourceStamp:
    """底库指纹：这一份交付到底基于哪一版数据。"""

    current_md5: str
    current_rows: int
    newest_snapshot_date: date | None = None


def stamp(store: DatasetStore) -> SourceStamp:
    """算底库指纹。一次 run-due 只算一次，传给各个需求复用。"""
    path = store.path("current")
    if not path.exists():
        raise SourceMissing("底库不存在，先跑 `hf-data-crawl collect`")
    digest = hashlib.md5(path.read_bytes()).hexdigest()
    frame = store.read("current")
    newest = frame["last_seen_at"].max() if frame.height else None
    return SourceStamp(
        current_md5=digest,
        current_rows=frame.height,
        newest_snapshot_date=newest if isinstance(newest, date) else None,
    )


def file_stamp(path: Path) -> dict[str, Any]:
    """输入文件的指纹：字节数与 sha256。

    回填的输入表是需求目录里的快照，不再随外部文件变化。把它钉进 manifest，
    「这份交付基于哪一版清单」才和「基于哪一版底库」一样能自证。
    """
    payload = path.read_bytes()
    return {"bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}
