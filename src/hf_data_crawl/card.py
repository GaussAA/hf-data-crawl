"""cardData 的抽取规则。

HF 卡片结构不统一：license 可能是字符串或列表，arxiv 同理，字段时有时无，
`configs` 与 `dataset_info` 甚至可能只出现其中一个。抽取一律容错，缺字段留空，
不因一张卡片畸形而中断整批。

产出两张表：

- **宽表**（`carddata.parquet`）：一库一行，放标签与汇总指标，便于 join 榜单。
- **明细表**（`card_configs.parquet`）：一行一个 (config, split)，放行数、字节、
  列结构。这是整条管线里唯一能回答「这个数据集有多少行、多少列、什么类型」的
  来源——`dataset_info` 在头部两万条里覆盖 22.4%，其余全靠它。
"""

from __future__ import annotations

import os
from collections.abc import Callable, Sequence
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any

import polars as pl

from . import raw
from .coerce import as_int, as_json, as_list, as_text
from .tables import CARD_CONFIGS_SCHEMA, ENRICH_SCHEMA, url_for

if TYPE_CHECKING:
    from .store import DatasetStore

#: 每个进程任务处理多少卡片文件。11-07 的 1,350 个分块按 64 个一组 ≈ 21 个任务。
CHUNK_FILES = 64
#: 分块数达到此值才起进程池；Windows 上起进程要秒级，小批量并行不如串行。
PARALLEL_MIN_FILES = 64
#: 卡片解析进程数。父进程还要收齐结果建表，进程数不宜过高以免峰值内存翻倍。
MAX_WORKERS = min(8, os.cpu_count() or 1)

#: 一个分块产出的 (排序键, 宽表, 明细表)。按分块而不是整体回传，父进程才能
#: 按与 `read_batches` 相同的键序归并，保证「同 id 以最后写入者为准」不变。
_CardPart = tuple[tuple[Any, ...], pl.DataFrame, pl.DataFrame]

#: 卡片里可直接取出的标量/列表字段：卡片键 → 宽表列。
_CARD_LIST_FIELDS: dict[str, str] = {
    "tags": "card_tags",
    "task_categories": "card_task_categories",
    "language": "card_languages",
    "size_categories": "card_size_categories",
    "annotations_creators": "card_annotations_creators",
    "language_creators": "card_language_creators",
    "source_datasets": "card_source_datasets",
}


def build(store: DatasetStore, snapshot_date: date) -> tuple[int, int]:
    """把该日期的 card 批次抽成两张表，按 id 覆盖旧值。

    返回 (宽表行数, 明细行数)。一次扫描产出两张表：卡片体量小但字段多，
    重扫两次的代价（重复解压 25 个分块）没有必要。
    """
    files = raw.batch_files(store, snapshot_date, label=raw.CARD_LABEL)
    if not files:
        return store.read("carddata").height, store.read("card_configs").height

    cards, configs = extract_files(files, snapshot_date)
    return (
        store.upsert("carddata", cards),
        store.upsert(
            "card_configs",
            configs,
            dedupe=["id", "config_name", "split"],
        ),
    )


def extract_files(
    files: Sequence[Path], snapshot_date: date, *, parallel: bool = True
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """把卡片文件抽成 (宽表, 明细表)。

    抽行与建表都下沉到子进程：11-07 有 1,350 个文件 / 135 万条记录，串行解压、
    抽行、再一次性 `pl.DataFrame(dicts)` 约 250 秒；而把原始记录字典回传父进程
    反而比串行更慢（实测 103s → 116s，pickle 吃掉了解压收益），所以子进程直接
    构造 DataFrame，只回传 Arrow 序列化后的表。

    每个分块带回自己的排序键，父进程按既有键序归并后再落表，因此与串行
    「`read_batches` 排序 → 逐条抽取」逐行等价（见 tests/test_card.py 的等价测试）。
    """
    stamp = snapshot_date.isoformat()
    jobs = [(tuple(str(path) for path in chunk), stamp) for chunk in _chunks(files)]
    parts: list[list[_CardPart]] = []
    if parallel and len(files) >= PARALLEL_MIN_FILES:
        try:
            with ProcessPoolExecutor(max_workers=MAX_WORKERS) as pool:
                parts = list(pool.map(_extract_chunk, jobs))
        except BrokenProcessPool:
            # 并行只是加速，抽取是纯函数：池坏了就重算，不能让构建整体失败。
            parts = [_extract_chunk(job) for job in jobs]
    else:
        parts = [_extract_chunk(job) for job in jobs]

    flat = [part for result in parts for part in result]
    flat.sort(key=lambda part: part[0])
    return (
        _concat([part[1] for part in flat], ENRICH_SCHEMA),
        _concat([part[2] for part in flat], CARD_CONFIGS_SCHEMA),
    )


def _chunks(files: Sequence[Path]) -> list[Sequence[Path]]:
    return [files[start : start + CHUNK_FILES] for start in range(0, len(files), CHUNK_FILES)]


def _extract_chunk(job: tuple[tuple[str, ...], str]) -> list[_CardPart]:
    """子进程入口：解析一批文件，每个分块产出一对 DataFrame。

    必须是模块级函数才能被进程池 pickle。入参用字符串而不是 `date` / `Path`，
    Windows 的 spawn 模式对这两者的序列化更挑剔。
    """
    paths, stamp = job
    snapshot_date = date.fromisoformat(stamp)
    parts: list[_CardPart] = []
    for path in paths:
        for batch in raw.load_batches(Path(path)):
            card_rows: list[dict[str, Any]] = []
            config_rows: list[dict[str, Any]] = []
            for record in batch.records:
                row = extract_card_row(record, snapshot_date)
                if row is not None:
                    card_rows.append(row)
                config_rows.extend(extract_config_rows(record, snapshot_date))
            parts.append(
                (
                    batch.key,
                    pl.DataFrame(card_rows, schema=ENRICH_SCHEMA),
                    pl.DataFrame(config_rows, schema=CARD_CONFIGS_SCHEMA),
                )
            )
    return parts


def _concat(frames: list[pl.DataFrame], schema: Any) -> pl.DataFrame:
    if not frames:
        return pl.DataFrame(schema=schema)
    return pl.concat(frames, how="vertical_relaxed")


def extract_card_row(record: dict[str, Any], snapshot_date: Any) -> dict[str, Any] | None:
    """从一条完整记录里抽出卡片宽表行；无卡片则返回 None。"""
    card = record.get("cardData")
    if not isinstance(card, dict) or not card:
        return None

    totals = _dataset_totals(card)
    row: dict[str, Any] = {
        "id": record["id"],
        "url": url_for(record["id"]),
        "snapshot_date": snapshot_date,
        # 卡片内容属于哪个仓库版本。取值来自同一条记录（列表接口必带 sha），
        # 不额外请求，也不从 lastModified 推断——sha 才是 commit 的身份。
        "source_sha": as_text(record.get("sha")),
        "pretty_name": as_text(card.get("pretty_name")),
        "paperswithcode_id": as_text(
            record.get("paperswithcode_id") or card.get("paperswithcode_id")
        ),
        "arxiv": as_text(card.get("arxiv") or record.get("arxiv")),
        "license_name": as_text(card.get("license")),
        "license_link": as_text(card.get("license_link")),
        "citation": as_text(card.get("citation") or record.get("citation")),
        "card_description": as_text(card.get("description")),
        "card_multilinguality": as_text(card.get("multilinguality")),
        "card_homepage": as_text(card.get("homepage")),
        "card_config_count": totals["config_count"],
        "card_num_rows": totals["num_rows"],
        "card_dataset_bytes": totals["dataset_bytes"],
        "card_download_bytes": totals["download_bytes"],
    }
    for card_key, column in _CARD_LIST_FIELDS.items():
        row[column] = as_list(card.get(card_key))
    return row


def extract_config_rows(record: dict[str, Any], snapshot_date: Any) -> list[dict[str, Any]]:
    """把卡片拆成 (config, split) 粒度的明细行；无卡片则返回空表。"""
    card = record.get("cardData")
    if not isinstance(card, dict) or not card:
        return []

    dataset_id = record["id"]
    files = _data_file_index(card)
    rows: list[dict[str, Any]] = []
    covered: set[str] = set()

    infos = card.get("dataset_info")
    if isinstance(infos, list):
        for info in infos:
            if not isinstance(info, dict):
                continue
            config_name = as_text(info.get("config_name")) or "default"
            covered.add(config_name)
            features = as_json(info.get("features"))
            dataset_size = as_int(info.get("dataset_size"))
            download_size = as_int(info.get("download_size"))
            splits = info.get("splits")
            emitted = False
            if isinstance(splits, list):
                for split in splits:
                    if not isinstance(split, dict):
                        continue
                    split_name = as_text(split.get("name"))
                    rows.append(
                        _config_row(
                            dataset_id,
                            snapshot_date,
                            config_name,
                            split_name,
                            num_bytes=as_int(split.get("num_bytes")),
                            num_examples=as_int(split.get("num_examples")),
                            dataset_size=dataset_size,
                            download_size=download_size,
                            features=features,
                            data_files=as_json(files.get((config_name, split_name))),
                        )
                    )
                    emitted = True
            if not emitted:
                # 有 config 无 splits：仍要保留一行，否则该 config 会整个消失。
                rows.append(
                    _config_row(
                        dataset_id,
                        snapshot_date,
                        config_name,
                        None,
                        num_bytes=None,
                        num_examples=None,
                        dataset_size=dataset_size,
                        download_size=download_size,
                        features=features,
                        data_files=as_json(_all_paths(files, config_name)),
                    )
                )

    # dataset_info 里没有、但 configs 里声明过的 config（如元数据子集）补骨架行。
    for config_name in _config_names(card):
        if config_name in covered:
            continue
        rows.append(
            _config_row(
                dataset_id,
                snapshot_date,
                config_name,
                None,
                num_bytes=None,
                num_examples=None,
                dataset_size=None,
                download_size=None,
                features=None,
                data_files=as_json(_all_paths(files, config_name)),
            )
        )

    return rows


def _config_row(
    dataset_id: str,
    snapshot_date: Any,
    config_name: str,
    split: str | None,
    *,
    num_bytes: int | None,
    num_examples: int | None,
    dataset_size: int | None,
    download_size: int | None,
    features: str | None,
    data_files: str | None,
) -> dict[str, Any]:
    return {
        "id": dataset_id,
        "url": url_for(dataset_id),
        "snapshot_date": snapshot_date,
        "config_name": config_name,
        "split": split,
        "num_bytes": num_bytes,
        "num_examples": num_examples,
        "dataset_size": dataset_size,
        "download_size": download_size,
        "features": features,
        "data_files": data_files,
    }


def _dataset_totals(card: dict[str, Any]) -> dict[str, int | None]:
    """跨 config 汇总行数与字节；全无取值时返回 None 而非 0。

    0 与「未知」必须分开：`card_num_rows = 0` 会被误读成空数据集。
    """
    infos = card.get("dataset_info")
    if not isinstance(infos, list) or not infos:
        return {
            "config_count": _config_count(card),
            "num_rows": None,
            "dataset_bytes": None,
            "download_bytes": None,
        }

    num_rows: int | None = None
    dataset_bytes: int | None = None
    download_bytes: int | None = None
    for info in infos:
        if not isinstance(info, dict):
            continue
        dataset_bytes = _add(dataset_bytes, as_int(info.get("dataset_size")))
        download_bytes = _add(download_bytes, as_int(info.get("download_size")))
        splits = info.get("splits")
        if not isinstance(splits, list):
            continue
        for split in splits:
            if isinstance(split, dict):
                num_rows = _add(num_rows, as_int(split.get("num_examples")))

    return {
        "config_count": _config_count(card, infos),
        "num_rows": num_rows,
        "dataset_bytes": dataset_bytes,
        "download_bytes": download_bytes,
    }


def _config_count(card: dict[str, Any], infos: list[Any] | None = None) -> int | None:
    """config 数量取两侧并集：dataset_info 与 configs 各有缺漏。"""
    names = set(_config_names(card))
    for info in infos or []:
        if isinstance(info, dict):
            name = as_text(info.get("config_name"))
            if name:
                names.add(name)
    return len(names) or None


def _config_names(card: dict[str, Any]) -> list[str]:
    configs = card.get("configs")
    if not isinstance(configs, list):
        return []
    names: list[str] = []
    for config in configs:
        if isinstance(config, dict):
            name = as_text(config.get("config_name")) or "default"
            if name not in names:
                names.append(name)
    return names


def _data_file_index(card: dict[str, Any]) -> dict[tuple[str, str | None], list[str]]:
    """(config, split) → 文件路径列表。"""
    index: dict[tuple[str, str | None], list[str]] = {}
    configs = card.get("configs")
    if not isinstance(configs, list):
        return index
    for config in configs:
        if not isinstance(config, dict):
            continue
        name = as_text(config.get("config_name")) or "default"
        files = config.get("data_files")
        if not isinstance(files, list):
            continue
        for entry in files:
            if not isinstance(entry, dict):
                continue
            path = as_text(entry.get("path"))
            if not path:
                continue
            index.setdefault((name, as_text(entry.get("split"))), []).append(path)
    return index


def _all_paths(
    files: dict[tuple[str, str | None], list[str]], config_name: str
) -> list[str] | None:
    collected: list[str] = []
    for (name, _split), paths in files.items():
        if name == config_name:
            collected.extend(paths)
    return collected or None


def _add(total: int | None, value: int | None) -> int | None:
    if value is None:
        return total
    return value if total is None else total + value


#: 一次按需补抓的上限。为什么要有：交付时若命中几千个缺卡片的库，逐条问接口会
#: 把一次交付拖成几十分钟，而收益只落在少数列上。宁可少补，也不让交付不可预期。
ON_DEMAND_LIMIT = 200


@dataclass(frozen=True)
class BackfillSummary:
    requested: int
    fetched: int
    with_card: int
    rows: int


def backfill(
    store: DatasetStore,
    ids: Sequence[str],
    snapshot_date: date,
    *,
    fetch: Callable[[str], dict[str, Any] | None],
    limit: int = ON_DEMAND_LIMIT,
) -> BackfillSummary:
    """按需补卡片：只问给定的那几十个库，取不到就放过。

    与 `build()` 的关系：`build()` 从当日 `card-*` 批次重算整张表，适合「一次扫
    一大批」；本函数直接从接口取单条记录，**不改 raw**，只把抽出的行并入现有表。
    两条路的抽取规则共用 `extract_card_row` / `extract_config_rows`，不会分叉。

    `source_sha` 仍取记录里的 `sha`：日后判断「这张卡片对应哪个仓库版本」靠它，
    与批量扫描写入的行完全同构，读的时候不必区分来源。
    """
    picked = list(dict.fromkeys(ids))[:limit]
    if not picked:
        return BackfillSummary(requested=0, fetched=0, with_card=0, rows=0)

    rows: list[dict[str, Any]] = []
    configs: list[dict[str, Any]] = []
    fetched = 0
    for dataset_id in picked:
        record = fetch(dataset_id)
        if record is None:
            continue
        fetched += 1
        row = extract_card_row(record, snapshot_date)
        if row is not None:
            rows.append(row)
        configs.extend(extract_config_rows(record, snapshot_date))

    if rows:
        store.upsert("carddata", pl.DataFrame(rows, schema=ENRICH_SCHEMA))
    if configs:
        store.upsert(
            "card_configs",
            pl.DataFrame(configs, schema=CARD_CONFIGS_SCHEMA),
            dedupe=["id", "config_name", "split"],
        )
    return BackfillSummary(
        requested=len(picked),
        fetched=fetched,
        with_card=len(rows),
        rows=len(rows),
    )
