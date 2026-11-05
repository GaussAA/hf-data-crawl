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

from typing import Any

from .coerce import as_int, as_json, as_list, as_text

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


def has_card(record: dict[str, Any]) -> bool:
    card = record.get("cardData")
    return isinstance(card, dict) and bool(card)


def extract_card_row(record: dict[str, Any], snapshot_date: Any) -> dict[str, Any] | None:
    """从一条完整记录里抽出卡片宽表行；无卡片则返回 None。"""
    card = record.get("cardData")
    if not isinstance(card, dict) or not card:
        return None

    totals = _dataset_totals(card)
    row: dict[str, Any] = {
        "id": record["id"],
        "snapshot_date": snapshot_date,
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
