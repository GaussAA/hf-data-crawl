"""Raw 层：原始响应落盘，以及按写入序读回。

Raw 是唯一不可重算的一层，其余各层都由它派生。这里只做两件事：写一页、读一天。
哪个标签对应哪种派生（`part` / `top` → Bronze，`card` → 卡片表）也在此约定，
写入方与读取方都引用这里的常量，而不是各写一份字面量。

文件名是「类别 + 命名空间 + 页码 + 内容摘要」：`part-lean_lastModified-000001-xxxx.json.zst`。
命名空间由 `ScanSpec` 用 mode + sort 派生——同一天里日增量、顶部刷新、全量对账、
卡片补齐各有独立页码空间，归档后仍能认出「这批是谁扫的」，不再共用
`part-000001` 这个模糊身份。旧格式（`part-000001-xxxx.json.gz`）与旧归档分片
一律按 `legacy` 兼容读取，不改写任何历史文件。

压缩从 gzip 换成 zstd 是在 2026-11-16 做的（见 `SUFFIX` 处实测数）。历史分块保持
`.json.gz` 原样不碰：文件名参与 `lineage` 指纹，重写一次就等于宣告那天派生作废。
"""

from __future__ import annotations

import gzip
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import orjson
import zstandard

from ..platform.atomic import write_bytes
from ..platform.retention import read_compressed_dates

if TYPE_CHECKING:
    from ..platform.store import DatasetStore

#: 卡片批次的标签。`enrich` 写入、卡片表构建读取，靠这一个常量对齐。
CARD_LABEL = "card"
#: 进入 Bronze 去重池的类别。归档按类别判定要不要做记录级去重，
#: 不再散落 `{"part", "top"}` 这类字面量。
BRONZE_LABELS = frozenset({"part", "top"})
#: 命名空间引入之前写入的历史分块（含旧归档分片）。
LEGACY_SOURCE = "legacy"

#: 新分块的后缀。压缩用 zstd 而不是 gzip，两个原因都可测：
#: 一页 1090 KB 的响应，gzip 默认 level 9 要压 61 ms、zstd level 3 只要 4.7 ms，
#: 而体积还小 5%（281 KB vs 295 KB）；解压一端 zstd 1.2 ms、gzip 4.6 ms。
#: 每天落 1000 多个分块，写这一端就差 60 秒上下。
SUFFIX = ".json.zst"
#: 历史分块的后缀（gzip）。**只读不改**：改后缀会让 `lineage` 的文件名指纹变掉，
#: 进而把已派生的日期全部判成「落后」。存量文件原样留着，指纹才不动。
LEGACY_SUFFIX = ".json.gz"
#: 读取时要认的全部后缀，新在前。
SUFFIXES = (SUFFIX, LEGACY_SUFFIX)

#: 压缩级别取 3：实测已与 level 1 相差不到 2 ms，体积却小 12%，是这条曲线上的拐点。
_LEVEL = 3
_COMPRESSOR = zstandard.ZstdCompressor(level=_LEVEL)
_DECOMPRESSOR = zstandard.ZstdDecompressor()


def pack(body: bytes) -> bytes:
    """把一页响应压成落盘字节。写入端**只此一处**，归档与冷区基线也调它。"""
    return _COMPRESSOR.compress(body)


def unpack(data: bytes, *, name: str) -> bytes:
    """按文件名后缀解压：新分块 zstd，历史分块 gzip。

    判据取文件名而不是试解压：试错会在「压缩数据恰好长得像另一种格式」时给出
    荒谬的错误，而文件名是我们自己写的，本来就知道答案。
    """
    if name.endswith(LEGACY_SUFFIX):
        return gzip.decompress(data)
    return _DECOMPRESSOR.decompress(data)


def source_slug(mode: str, sort: str) -> str:
    """把一轮扫描的 (mode, sort) 折成稳定、不含连字符的文件名段。

    连字符是文件名各段的分隔符，必须替换掉；其余字符保留，便于人眼认出来源。
    """
    return re.sub(r"[^A-Za-z0-9_]+", "_", f"{mode}_{sort}").strip("_")


def batch_patterns(label: str | None = None) -> list[str]:
    """某类批次的文件通配；不给标签则匹配当日全部批次。

    返回新旧两种后缀各一条——`Path.glob` 一次只吃一个模式，而一天里可能同时躺着
    历史 gz 分块与新 zst 分块（切换当天必然如此）。
    """
    head = f"{label}-*" if label else "*"
    return [f"{head}{suffix}" for suffix in SUFFIXES]


def strip_suffix(name: str) -> str:
    """去掉分块的压缩后缀（新 zst 或历史 gz），供按连字符解析文件名。"""
    for suffix in SUFFIXES:
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name


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
    source: str | None = None,
    written_at: str | None = None,
) -> Path:
    """落一页原始响应，返回文件路径。同内容重复写不新增文件。

    **排序的唯一依据是 `written_at`，它必须落在 payload 里。** 不能交给
    文件系统：mtime 在复制目录、解压归档、换机迁移后都会变，届时同一批
    Raw 会推出不同的 Bronze——而 `rebuild` 声称「Raw 是唯一事实来源」，
    两者不能同时为真。（2026-11-04 实测：字典序让旧批次压住新批次，
    头部两万名的 main_size 只剩 11,552 条。）

    `label`（类别）与 `source`（来源命名空间）共同决定文件身份，**都不参与排序**。
    同内容重写会复用已有文件，其 written_at 保持首次写入时刻——内容相同，
    谁胜出都不影响结果。不传 source 的直接写入沿用旧文件名格式，保持幂等与兼容。
    """
    snapshot_meta: dict[str, Any] = {
        **(meta or {}),
        "snapshot_date": snapshot_date.isoformat(),
        "page_index": page_index,
        "record_count": len(records),
        "written_at": written_at or _utc_now(),
    }
    # meta 自包含来源，归档后即使文件被改名也能追出批次；setdefault 让调用方
    # 传入的 meta 优先（ScanRun 已带 source），不覆盖既有值。
    snapshot_meta.setdefault("source", source or LEGACY_SOURCE)
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

    # 连字符是文件名段分隔符；来源里混进它会让 source_of 解析失败、静默退化成
    # legacy。宁可在写入前显式拒绝，也不要落下一个认不出来源的文件。
    if source is not None and not re.fullmatch(r"[A-Za-z0-9_]+", source):
        raise ValueError(f"source 只能含字母、数字与下划线（连字符是文件名分隔符）：{source!r}")

    target_dir = partition_dir(store, snapshot_date)
    target_dir.mkdir(parents=True, exist_ok=True)
    source_part = f"{source}-" if source else ""
    path = target_dir / f"{label}-{source_part}{page_index:06d}-{digest}{SUFFIX}"
    if path.exists():
        return path

    write_bytes(path, pack(body))
    return path


def batch_files(
    store: DatasetStore, snapshot_date: date, *, label: str | None = None
) -> list[Path]:
    """某天的分块文件，按文件名排序（仅为遍历稳定；写入序以 `Batch.key` 为准）。"""
    partition = partition_dir(store, snapshot_date)
    if not partition.exists():
        return []
    return sorted(path for pattern in batch_patterns(label) for path in partition.glob(pattern))


def bronze_files(store: DatasetStore, snapshot_date: date) -> list[Path]:
    """进入 Bronze 的分块：只取 `BRONZE_LABELS`，卡片批次不参与。

    `card-*` 是 enrich 为卡片表写的批次，内容虽同为数据集记录，但它的存在
    与否取决于补抓节奏，不属于「当天的标量快照」。混进来会让 Bronze 的行数
    随卡片补抓膨胀，也会让卡片表之外的派生层看到一批来源不同的记录。
    """
    return sorted(
        path
        for label in sorted(BRONZE_LABELS)
        for path in batch_files(store, snapshot_date, label=label)
    )


def supplement_files(store: DatasetStore, snapshot_date: date) -> list[Path]:
    """补充分块：`card-*`，enrich 为卡片表补抓的批次。

    它们默认不进 Bronze（见 `bronze_files`），但**只在它里面出现过的 id** 仍是当天
    观察到的数据集。完全忽略会让「current 里有、raw 重放不出来」——2026-11-08 全量
    重放实测丢了 27 行。故 Bronze 仅在主批次未见该 id 时用它补一条，不参与去重优先级。
    """
    return batch_files(store, snapshot_date, label=CARD_LABEL)


def is_archive(path: Path) -> bool:
    """归档分片的文件名含 `-archive-`（新旧命名空间两种格式都成立）。"""
    return "-archive-" in path.name


def archive_name(label: str, index: int, digest: str, source: str = LEGACY_SOURCE) -> str:
    """分片名保留命名空间；legacy 沿用旧格式，历史分片无需改名。"""
    head = f"{label}-" if source == LEGACY_SOURCE else f"{label}-{source}-"
    return f"{head}archive-{index:06d}-{digest}{SUFFIX}"


def label_of(path: Path) -> str:
    """文件名首段即类别（part / top / card）；命名空间在第二段，不影响它。"""
    return path.name.split("-", 1)[0]


def source_of(path: Path) -> str:
    """文件名里的来源命名空间；旧格式与旧归档分片返回 `legacy`。

    四种形态：

    - `part-000001-xxxx`                        → legacy
    - `part-lean_lastModified-000001-xxxx`     → lean_lastModified
    - `part-archive-000001-xxxx`               → legacy
    - `part-lean_lastModified-archive-000001-…`→ lean_lastModified
    """
    parts = strip_suffix(path.name).split("-")
    if len(parts) >= 3 and parts[1] == "archive":
        return LEGACY_SOURCE
    if len(parts) >= 4 and (parts[2] == "archive" or parts[2].isdigit()):
        return parts[1]
    return LEGACY_SOURCE


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
    # 解析用 orjson：它直接吃 bytes（省一次 decode），实测一页 3.5 ms，标准库 8.6 ms。
    # **只换读，不换写**：写入的 `json.dumps` 参与内容摘要，换成别的实现会改变字节，
    # 同内容重写就不再复用旧文件——幂等性与指纹都指着那份字节。
    payload = orjson.loads(unpack(path.read_bytes(), name=path.name))
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


def _utc_now() -> str:
    """带微秒的 UTC 时刻，作为 Raw 的写入序。

    微秒足够：每页之间至少隔着一次网络往返，不可能同微秒落两次盘。
    用 UTC 而非本地时间，避免跨时区迁移后序颠倒。
    """
    return datetime.now(UTC).isoformat(timespec="microseconds")


@dataclass(frozen=True)
class RawDay:
    """某一天在 Raw 主目录里的形态。

    此前「哪天有 Raw、哪天可重放」有三套答案：`rebuild.discover_raw_dates`
    只看目录里有没有任意分块，`marker.has_raw` 只认 `part` / `top`，
    `doctor._raw_gaps` 又自行并上冷区清单。恢复、重建、体检对同一天可能给出
    不同结论。这个模型把它们收成一处。

    判据按文件形态而不是目录名：空 `dt=` 目录只是壳（compact 移走原分块后
    会留下），「这天采过」必须有分块。
    """

    snapshot_date: date
    #: part/top 分块，含归档分片（`bronze_files` 的 glob 已覆盖 `*-archive-*`）。
    bronze: tuple[Path, ...]
    #: card 补充分块，只在 Bronze 主批次未见该 id 时补位。
    supplements: tuple[Path, ...]
    #: 已由 compact 压进冷区基线，主目录已无该日分块。
    compacted: bool = False

    @property
    def files(self) -> tuple[Path, ...]:
        """派生的输入全集：与 `lineage.source_fingerprint` 当前口径一致。"""
        return (*self.bronze, *self.supplements)

    @property
    def replayable(self) -> bool:
        """能从主目录的 Raw 重放：有 part/top 或有 card 补位。

        与 `lineage.DayStatus.replayable`（`current` 指纹非空）是同一判断的两层
        表达：`RawDay.replayable == (lineage.source_fingerprint(...) is not None)`。
        """
        return bool(self.bronze or self.supplements)

    @property
    def observed(self) -> bool:
        """这一天确实采过：可重放，或已压缩进冷区基线。"""
        return self.replayable or self.compacted

    @property
    def archived(self) -> bool:
        """是否存在归档分片（用于体检与测试断言）。"""
        return any(is_archive(path) for path in self.files)


def describe_day(store: DatasetStore, snapshot_date: date) -> RawDay:
    """读出某一天的形态；不判断它是否存在于主目录。"""
    return _describe(store, snapshot_date, read_compressed_dates(store))


def observed_days(store: DatasetStore) -> list[RawDay]:
    """全部「采过」的日期：主目录有分块的日期 ∪ 冷区清单，按日期升序。

    只认至少有一个分块文件的目录，空 `dt=` 目录不算——沿用
    `rebuild.discover_raw_dates` 的既有判据。
    """
    compressed = read_compressed_dates(store)
    days: dict[date, RawDay] = {}
    if store.raw_dir.exists():
        for path in sorted(store.raw_dir.glob("dt=*")):
            if not path.is_dir():
                continue
            try:
                snapshot_date = date.fromisoformat(path.name.removeprefix("dt="))
            except ValueError:
                # 不符合命名的目录不参与判定，跳过总比整轮失败强。
                continue
            if not [item for pattern in batch_patterns() for item in path.glob(pattern)]:
                continue
            days[snapshot_date] = _describe(store, snapshot_date, compressed)
    for snapshot_date in compressed:
        days.setdefault(
            snapshot_date,
            RawDay(snapshot_date=snapshot_date, bronze=(), supplements=(), compacted=True),
        )
    return [days[key] for key in sorted(days) if days[key].observed]


def replayable_days(store: DatasetStore) -> list[RawDay]:
    """全部可从主目录重放的日期，按日期升序。"""
    return [day for day in observed_days(store) if day.replayable]


def _describe(store: DatasetStore, snapshot_date: date, compressed: set[date]) -> RawDay:
    """构造 `RawDay`；调用方传入已读的冷区日期集合，避免逐日重读清单。"""
    return RawDay(
        snapshot_date=snapshot_date,
        bronze=tuple(bronze_files(store, snapshot_date)),
        supplements=tuple(supplement_files(store, snapshot_date)),
        compacted=snapshot_date in compressed,
    )
