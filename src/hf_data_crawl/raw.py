"""Raw 层：原始响应落盘，以及按写入序读回。

Raw 是唯一不可重算的一层，其余各层都由它派生。这里只做两件事：写一页、读一天。
哪个标签对应哪种派生（`part` / `top` → Bronze，`card` → 卡片表）也在此约定，
写入方与读取方都引用这里的常量，而不是各写一份字面量。
"""

from __future__ import annotations

import gzip
import hashlib
import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

from .atomic import write_bytes

if TYPE_CHECKING:
    from .store import DatasetStore

#: 卡片批次的标签。`enrich` 写入、卡片表构建读取，靠这一个常量对齐。
CARD_LABEL = "card"


def batch_pattern(label: str | None = None) -> str:
    """某类批次的文件通配；不给标签则匹配当日全部批次。"""
    return f"{label}-*.json.gz" if label else "*.json.gz"


class Batch(NamedTuple):
    """一个已解压的 Raw 分块。"""

    #: 排序键，仅用于排序，不对外承诺结构。
    key: tuple[Any, ...]
    records: list[dict[str, Any]]
    file: str


def partition_dir(store: DatasetStore, snapshot_date: date) -> Path:
    return store.raw_dir / f"dt={snapshot_date.isoformat()}"


def write(
    store: DatasetStore,
    *,
    snapshot_date: date,
    page_index: int,
    records: list[dict[str, Any]],
    meta: dict[str, Any] | None = None,
    label: str = "part",
    written_at: str | None = None,
) -> Path:
    """落一页原始响应，返回文件路径。同内容重复写不新增文件。

    **排序的唯一依据是 `written_at`，它必须落在 payload 里。** 不能交给
    文件系统：mtime 在复制目录、解压归档、换机迁移后都会变，届时同一批
    Raw 会推出不同的 Bronze——而 `rebuild` 声称「Raw 是唯一事实来源」，
    两者不能同时为真。（2026-11-04 实测：字典序让旧批次压住新批次，
    头部两万名的 main_size 只剩 11,552 条。）

    `label` 只用于让人一眼看出是哪一批（part / top / card），**不参与排序**。
    同内容重写会复用已有文件，其 written_at 保持首次写入时刻——内容相同，
    谁胜出都不影响结果。
    """
    snapshot_meta: dict[str, Any] = {
        **(meta or {}),
        "snapshot_date": snapshot_date.isoformat(),
        "page_index": page_index,
        "record_count": len(records),
        "written_at": written_at or _utc_now(),
    }
    payload: dict[str, Any] = {"meta": snapshot_meta, "records": records}
    body = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    # 摘要**不含**写入时刻：否则同一页每跑一次就多一个文件，幂等性失效。
    # 排除后，「同内容重写复用已有文件」照旧成立，其 written_at 保持首次写入
    # 时刻——内容相同，谁胜出都不影响结果。历史文件本就没有该字段，故同一规则
    # 算出的摘要与从前一致，老文件仍能被复用。
    digest_source = json.dumps(
        {
            **payload,
            "meta": {key: value for key, value in snapshot_meta.items() if key != "written_at"},
        },
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    digest = hashlib.sha256(digest_source).hexdigest()[:8]

    target_dir = partition_dir(store, snapshot_date)
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{label}-{page_index:06d}-{digest}.json.gz"
    if path.exists():
        return path

    write_bytes(path, gzip.compress(body))
    return path


def batch_files(
    store: DatasetStore, snapshot_date: date, *, label: str | None = None
) -> list[Path]:
    """某天的分块文件，按文件名排序（仅为遍历稳定；写入序以 `Batch.key` 为准）。"""
    partition = partition_dir(store, snapshot_date)
    if not partition.exists():
        return []
    return sorted(partition.glob(batch_pattern(label)))


def is_archive(path: Path) -> bool:
    """归档分片的文件名形如 `part-archive-000001-<摘要>.json.gz`。"""
    return "-archive-" in path.name


def archive_name(label: str, index: int, digest: str) -> str:
    return f"{label}-archive-{index:06d}-{digest}.json.gz"


def label_of(path: Path) -> str:
    """文件名首段即标签（part / top / card）。"""
    return path.name.split("-", 1)[0]


def load_batches(path: Path) -> list[Batch]:
    """解压并解析一个 Raw 文件，还原成一个或多个带排序键的分块。

    独立成函数是为了能被进程池直接调用：解压加 `json.loads` 是 Bronze 重建里
    最重的一段纯 CPU 活，各文件互不依赖。

    普通分块一个文件对应一个 Batch。排序键取自 payload 的 `written_at`，文件名与
    文件属性都不作数。没有该字段的历史分块按文件名确定性定序（**不读 mtime**）：
    复制目录、解压归档、换机迁移都会改变 mtime，届时同一批 Raw 会推出不同 Bronze。
    这类分块应先跑 `archive --date ...`，把真实写入序冻结进归档分片的 `groups`；
    键首位的 0/1 保证新分块永远排在其后。

    归档分片（见 `archive` 模块）把多个原分块去重后并在一起，但**每条记录原来的排序键
    与来源文件名按段记在 `groups` 里**，这里按段切回多个 Batch——合并不改变任何
    一条记录的胜负。
    """
    payload = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
    records = payload.get("records", [])
    groups = payload.get("groups")
    if isinstance(groups, list):
        batches: list[Batch] = []
        start = 0
        for group in groups:
            count = int(group["count"])
            batches.append(
                Batch(tuple(group["key"]), records[start : start + count], group["file"])
            )
            start += count
        return batches

    stamp = (payload.get("meta") or {}).get("written_at")
    if isinstance(stamp, str) and stamp:
        key: tuple[Any, ...] = (1, stamp, path.name)
    else:
        # 确定性回退：文件名（页号 + 内容摘要）不随文件系统状态变化。
        # 未归档的历史分块应先跑 `archive --date`，把真实写入序冻结进 groups；
        # 在此之前按文件名定序是确定的，但可能与迁移前的 mtime 序不同，
        # doctor 会对「未归档且无 written_at」的历史分区告警。
        key = (0, "", path.name)
    return [Batch(key, records, path.name)]


def read_batches(
    store: DatasetStore, snapshot_date: date, *, label: str | None = None
) -> list[Batch]:
    """读出某天的分块并按写入时刻升序排列，同 id 以最后写入者为准。"""
    batches = [
        batch
        for path in batch_files(store, snapshot_date, label=label)
        for batch in load_batches(path)
    ]
    batches.sort(key=lambda batch: batch.key)
    return batches


def _utc_now() -> str:
    """带微秒的 UTC 时刻，作为 Raw 的写入序。

    微秒足够：每页之间至少隔着一次网络往返，不可能同微秒落两次盘。
    用 UTC 而非本地时间，避免跨时区迁移后序颠倒。
    """
    return datetime.now(UTC).isoformat(timespec="microseconds")
