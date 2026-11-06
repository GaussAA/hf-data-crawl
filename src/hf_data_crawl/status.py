"""Dataset 的状态：词表、探测结论的映射，以及重建时的留存协议。

为什么需要它
------------
`active / disappeared / restricted / renamed` 四值原本是散在五个文件里的字符串，
`mark_status` 接受任意值；探测结论（deleted）与落库状态（disappeared）靠
reconcile 里一张私有映射对上；rebuild 为了不丢失败状态，自己读 current、
写快照、再回灌——「状态怎么留」的知识被摊在调用方。

这里把三件事收到一处：

1. **词表**：`Status`。写入时校验，未知值立刻报错，而不是静默落库。
2. **映射**：探测结论 → 状态。`present`（库还在、id 没变）映射到 `active`，
   所以历史上被误标成 restricted / renamed 的库，下一次对账探测时能自愈。
3. **留存协议**：`retain` / `restore`。

留存协议
--------
失效状态不来自 Raw（来自单库探测），而 rebuild 会清空 state。所以：

- `retain`：清理**之前**调用。快照存在，说明上次重建被打断，以快照为准；
  否则读 current 里非 active 的行，非空就写快照。
- `restore`：写回之后调用，并删除快照。

**快照只表示「重建进行中」。** 成功后必须删除——留着的话，下一次重建会把它当成
「上次被打断」，把期间已恢复的库改回旧状态。

已接受的缝隙：写回完成、快照尚未删除时进程被杀，之后又有对账把某个库恢复，
下一次重建会被旧快照改回去。窗口只夹在「写回」与「删文件」之间，不做合并逻辑。
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from enum import StrEnum
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl

from .atomic import write_text

if TYPE_CHECKING:
    from .store import DatasetStore


class Status(StrEnum):
    """current.status 的取值。"""

    ACTIVE = "active"
    DISAPPEARED = "disappeared"
    RESTRICTED = "restricted"
    RENAMED = "renamed"


#: 所有「非在架」状态。StrEnum 与同值字符串相等且同哈希，故可直接与读出的字符串比较。
INACTIVE: frozenset[Status] = frozenset(status for status in Status if status is not Status.ACTIVE)

#: 探测结论 → 落库状态。`unknown`（网络异常等）不在表内，表示「先别动」。
STATUS_BY_VERDICT: dict[str, Status] = {
    "deleted": Status.DISAPPEARED,
    "restricted": Status.RESTRICTED,
    "renamed": Status.RENAMED,
    "present": Status.ACTIVE,
}


def snapshot_path(store: DatasetStore) -> Path:
    """放在 meta/ 而非 state/：它不属于任何登记表，重建清不到它。"""
    return store.root / "meta" / "failed-status.json"


def retain(store: DatasetStore) -> dict[str, str]:
    """清理前留存「非在架」状态，并保证快照已落盘。返回 id → 状态。"""
    snapshot = _read_snapshot(store)
    if snapshot is not None:
        return snapshot

    current = store.read_current()
    if current.height == 0:
        return {}
    stale = current.filter(pl.col("status") != Status.ACTIVE.value)
    retained = _valid(
        (str(dataset_id), str(value))
        for dataset_id, value in zip(stale["id"].to_list(), stale["status"].to_list(), strict=True)
    )
    if retained:
        write_text(snapshot_path(store), json.dumps(retained, ensure_ascii=False, sort_keys=True))
    return retained


def restore(store: DatasetStore, retained: dict[str, str]) -> None:
    """把留存的状态写回，然后删除快照。"""
    if retained:
        store.mark_status(retained)
    snapshot_path(store).unlink(missing_ok=True)


def _read_snapshot(store: DatasetStore) -> dict[str, str] | None:
    """读上次留下的快照；不存在或损坏返回 None（调用方回退读 current）。"""
    path = snapshot_path(store)
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None
    if not isinstance(payload, dict):
        return None
    return _valid((str(key), str(value)) for key, value in payload.items())


def _valid(pairs: Iterable[tuple[str, str]]) -> dict[str, str]:
    """丢掉词表之外的值：写回时 `mark_status` 会对未知值报错，而此刻 state
    已被清空，中途报错等于把重建打断在最糟的位置。"""
    known = {status.value for status in Status}
    return {dataset_id: value for dataset_id, value in pairs if value in known}
