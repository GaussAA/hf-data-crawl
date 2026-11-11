"""派生戳记：记住「某天的派生层是基于哪一版 Raw 算出来的」。

为什么需要
----------
Raw 可以被重写：`archive` 重新分片、`compact` 移走冷区、补采再落一块同名分块。
重写之后派生层不会自己更新——`derive_day` 只在 `collect` / `enrich` / `build` 里跑。
于是存在一种没有任何报错的坏状态：**Raw 已经是新的，bronze / silver / observations
却是旧的**，而且从文件时间上也看不出来（派生文件可能比 Raw 更晚，因为别的日子刚跑过）。

此前判断「派生是否最新」只能靠人回忆「上次 build 是什么时候」，或者干脆全量重放。
全量重放要几分钟到十几分钟，天天做不值当；不做又没有依据。缺的是一个**证据**。

戳记记什么
----------
每天一条，三个字段：

- `source`：该日 Raw 分块的指纹（文件名 + 字节数 + 修改时间，按名排序后 sha256）。
  取元数据而不取内容：一天上千个分块，读内容要解压全部，代价与重放本身相当，
  而「分块被重写」这件事在元数据上必然留下痕迹。
- `derived_at`：派生完成时刻，供人看「上次算是什么时候」。
- 三层的行数：不是判据，是快照。行数突变往往先于逻辑错误被发现。

判据只有一条：**Raw 指纹与戳记不一致，就说明派生落后了**。

不覆盖的场景
------------
Raw 被 `compact` 移出 `data/raw` 后无法重放，也就无从比对：那天的戳记保留原样，
不参与判定。这不是遗漏——冷区 Raw 已经不在主目录，重放本身就不可能，
报「陈旧」只会给出一个无法执行的建议。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from . import raw
from .platform.atomic import write_text

if TYPE_CHECKING:
    from .platform.store import DatasetStore

LINEAGE_NAME = "lineage.json"
#: 档案版本。字段增减时递增，读到旧版本按「无凭证」处理，宁可重派一次。
LINEAGE_VERSION = 1

#: `compare` 的三种结论。
OK = "ok"
STALE = "stale"
UNVERIFIED = "unverified"


@dataclass(frozen=True)
class DayStamp:
    """某一天的派生凭证。"""

    source: str
    derived_at: str
    bronze_rows: int
    silver_rows: int
    observations: int


@dataclass(frozen=True)
class DayStatus:
    """一天的比对结论。`current` 为空表示该日已不可重放（Raw 已移出主目录）。"""

    day: date
    state: str
    stamped: DayStamp | None
    current: str | None

    @property
    def replayable(self) -> bool:
        return self.current is not None


def path_of(store: DatasetStore) -> Path:
    return store.state_dir / LINEAGE_NAME


def _fingerprint(files: list[Path]) -> str | None:
    """一组 Raw 分块的指纹；一个文件都没有时返回 None。"""
    digest = hashlib.sha256()
    counted = 0
    for item in sorted(files, key=lambda p: p.name):
        try:
            stat = item.stat()
        except OSError:
            # 并发删除或权限异常：跳过这一个，指纹仍由其余分块决定。
            continue
        digest.update(item.name.encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(stat.st_mtime_ns).encode("ascii"))
        digest.update(b"\0")
        counted += 1
    return digest.hexdigest() if counted else None


def source_fingerprint(store: DatasetStore, snapshot_date: date) -> str | None:
    """某日 Raw 的指纹：进入 Bronze 的分块，加上只在补抓批次里出现的补位来源。

    为什么含 `card-*`：`bronze.day_frame` 会把补抓批次里独有的 id 补进当日帧，
    补位结果直接写进 `current` 与 `observations`。只盯 `part` / `top` 的话，
    补抓批次被重写而派生没重跑，同样会留下「今天的 current 用了前天的补抓」。

    为什么取 `RawDay.files` 而不是全部分块：这组正是派生的输入全集，口径与
    `RawDay.replayable` 共用一处，`raw.batch_files` 里若将来出现第三种标签，
    它会先在 `raw.py` 登记，再决定是否进入 `files`。
    """
    return _fingerprint(list(raw.describe_day(store, snapshot_date).files))


def read(store: DatasetStore) -> dict[date, DayStamp]:
    """读全部戳记。档案缺失、损坏或版本不符一律按「没有」处理。

    体检是只读诊断，不能因为一份坏档案自己变成故障点；而「没有凭证」的后果
    只是多重派一次，不是丢数据。
    """
    target = path_of(store)
    if not target.exists():
        return {}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(payload, dict) or payload.get("version") != LINEAGE_VERSION:
        return {}
    days = payload.get("days")
    if not isinstance(days, dict):
        return {}
    stamps: dict[date, DayStamp] = {}
    for key, item in days.items():
        try:
            stamps[date.fromisoformat(key)] = DayStamp(
                source=str(item["source"]),
                derived_at=str(item["derived_at"]),
                bronze_rows=int(item["bronze_rows"]),
                silver_rows=int(item["silver_rows"]),
                observations=int(item["observations"]),
            )
        except (KeyError, TypeError, ValueError):
            continue
    return stamps


def _write(store: DatasetStore, stamps: dict[date, DayStamp]) -> None:
    payload = {
        "version": LINEAGE_VERSION,
        "days": {
            day.isoformat(): {
                "source": stamp.source,
                "derived_at": stamp.derived_at,
                "bronze_rows": stamp.bronze_rows,
                "silver_rows": stamp.silver_rows,
                "observations": stamp.observations,
            }
            for day, stamp in sorted(stamps.items())
        },
    }
    write_text(path_of(store), json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def stamp(
    store: DatasetStore,
    snapshot_date: date,
    *,
    bronze_rows: int,
    silver_rows: int,
    observations: int,
) -> DayStamp | None:
    """派生产出后落一条戳记；该日已无 Raw 可指纹时不动档案，返回 None。

    「派生完成即写戳记」是这套机制唯一需要遵守的纪律：写入点在 `derive_day` 末尾，
    与 `clear_deriving` 相邻——标记文件回答「派生是不是跑到一半崩了」，戳记回答
    「跑完的那一次是照着哪一版 Raw 跑的」。
    """
    source = source_fingerprint(store, snapshot_date)
    if source is None:
        return None
    stamp_record = DayStamp(
        source=source,
        derived_at=datetime.now(UTC).isoformat(timespec="seconds"),
        bronze_rows=bronze_rows,
        silver_rows=silver_rows,
        observations=observations,
    )
    stamps = read(store)
    stamps[snapshot_date] = stamp_record
    _write(store, stamps)
    return stamp_record


def restamp(store: DatasetStore, snapshot_date: date) -> DayStamp | None:
    """Raw 被**等价改写**后刷新指纹，不重跑派生。

    目前只有一个调用方：`archive`。它把上千个小分块合并成少数分片，并在移走原件
    之前校验过「归档前后 Bronze 逐行相等」。派生结果因此仍然有效，只是指纹变了——
    不刷新的话，下一轮体检会把这次等价改写报成「派生滞后」。

    行数沿用旧戳记：这次的派生没重跑，行数就不该改；旧戳记不存在时返回 None，
    留给 `build --stale` 去真正派生一次。
    """
    previous = read(store).get(snapshot_date)
    if previous is None:
        return None
    source = source_fingerprint(store, snapshot_date)
    if source is None:
        return None
    stamps = read(store)
    refreshed = DayStamp(
        source=source,
        derived_at=previous.derived_at,
        bronze_rows=previous.bronze_rows,
        silver_rows=previous.silver_rows,
        observations=previous.observations,
    )
    stamps[snapshot_date] = refreshed
    _write(store, stamps)
    return refreshed


def compare(store: DatasetStore, dates: list[date]) -> list[DayStatus]:
    """逐日比对 Raw 指纹与戳记。Raw 已不在主目录的日子不出现在结果里。"""
    stamps = read(store)
    statuses: list[DayStatus] = []
    for day in dates:
        current = source_fingerprint(store, day)
        if current is None:
            continue
        stamped = stamps.get(day)
        if stamped is None:
            state = UNVERIFIED
        elif stamped.source != current:
            state = STALE
        else:
            state = OK
        statuses.append(DayStatus(day=day, state=state, stamped=stamped, current=current))
    return statuses


def divergent(store: DatasetStore, dates: list[date]) -> list[date]:
    """需要重派生才能重新对齐的日期：指纹不符（stale）与没有凭证（unverified）。

    把 unverified 也算进来，是因为「没有凭证」与「凭证过期」在执行层面是同一件事：
    都得跑一次派生才能有据可查。机制上线首轮会因此全量重放一次，之后只剩真正
    被改写过的日子。
    """
    return [item.day for item in compare(store, dates) if item.state != OK]
