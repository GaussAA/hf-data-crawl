"""Raw 命名空间的契约测试（2026-11-07）。

背景：同一天里日增量、顶部刷新、全量对账、卡片补齐四种扫描都写 `part` / `top`
且页码各自从 1 起，文件名 `part-000001-xxxx` 认不出「这批是谁扫的」；归档后
`groups` 又只剩文件名，来源彻底丢失。更险的是不同 `expand` 档位共享同一去重池，
后写的窄记录会覆盖完整记录、让整列静默变空。

此处把四件事钉成规范：来源由 ScanSpec 派生并写进文件名/meta；旧格式一律兼容；
写入 Raw 的扫描不得比 Bronze 基座更窄；命名空间**不参与排序**（胜负仍由 written_at
决定，改文件名不得改变任何一条记录的胜负）。
"""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl.acquire.enrich import ENRICH_EXPAND
from hf_data_crawl.acquire.hf.api import IDENTITY_EXPAND, Page
from hf_data_crawl.acquire.hf.scan import ScanRun, ScanSpec
from hf_data_crawl.ops import archive
from hf_data_crawl.pipeline import bronze, raw
from hf_data_crawl.platform.store import DatasetStore

DAY = date(2026, 11, 7)


class OnePageApi:
    """只产出一页，供 ScanRun 的落盘测试使用。"""

    def scan(self, **_kwargs: Any) -> Any:
        yield Page(records=[{"id": "acme/ds", "downloads": 7}], next_cursor=None)


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


def payload(path: Path) -> dict[str, Any]:
    return json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))


def spec(**overrides: Any) -> ScanSpec:
    base: dict[str, Any] = {
        "sort": "lastModified",
        "expand": IDENTITY_EXPAND,
        "mode": "lean",
    }
    base.update(overrides)
    return ScanSpec(**base)


# ---------------------------------------------------------------- 派生与身份


def test_来源slug由mode与sort派生且不含连字符() -> None:
    assert raw.source_slug("top-refresh", "downloads") == "top_refresh_downloads"
    assert raw.source_slug("lean", "lastModified") == "lean_lastModified"
    assert raw.source_slug("reconcile", "createdAt") == "reconcile_createdAt"
    assert "-" not in raw.source_slug("dry-run", "lastModified")


def test_ScanSpec的来源与文件名一致(store: DatasetStore) -> None:
    spec_ = spec(mode="top-refresh", sort="downloads", label="top")

    ScanRun(OnePageApi(), store).run(DAY, spec_)
    files = list(raw.batch_files(store, DAY))

    assert len(files) == 1
    assert files[0].name.startswith(f"top-{spec_.source}-000001-")
    assert raw.label_of(files[0]) == "top"
    assert raw.source_of(files[0]) == spec_.source
    assert payload(files[0])["meta"]["source"] == spec_.source
    assert payload(files[0])["meta"]["mode"] == "top-refresh"


def test_同一天不同来源同页各有独立文件(store: DatasetStore) -> None:
    """此前四种扫描共用 `part-000001`，只能靠解压 meta 分辨谁是谁。"""
    for index, source in enumerate(
        ("lean_lastModified", "reconcile_createdAt", "enrich_downloads"), 1
    ):
        raw.write(
            store,
            snapshot_date=DAY,
            page_index=1,
            records=[{"id": "acme/ds"}],
            source=source,
            written_at=f"2026-11-07T00:00:0{index}.000000+00:00",
        )

    files = raw.batch_files(store, DAY)

    assert len(files) == 3
    assert {raw.source_of(path) for path in files} == {
        "lean_lastModified",
        "reconcile_createdAt",
        "enrich_downloads",
    }
    assert all(path.name.split("-")[-2] == "000001" for path in files)
    assert all(raw.label_of(path) == "part" for path in files)


# ---------------------------------------------------------------- 兼容与常量


def test_四种文件名形态都能解析() -> None:
    cases = {
        "part-000001-deadbeef.json.gz": ("part", "legacy", False),
        "part-lean_lastModified-000001-deadbeef.json.gz": (
            "part",
            "lean_lastModified",
            False,
        ),
        "part-archive-000001-deadbeef.json.gz": ("part", "legacy", True),
        "part-lean_lastModified-archive-000001-deadbeef.json.gz": (
            "part",
            "lean_lastModified",
            True,
        ),
        "card-enrich_downloads-archive-000001-deadbeef.json.gz": (
            "card",
            "enrich_downloads",
            True,
        ),
    }

    for name, (label, source, archived) in cases.items():
        path = Path(name)
        assert raw.label_of(path) == label, name
        assert raw.source_of(path) == source, name
        assert raw.is_archive(path) is archived, name


def test_legacy分片名保持旧格式() -> None:
    assert raw.archive_name("part", 3, "deadbeef") == "part-archive-000003-deadbeef.json.gz"
    assert (
        raw.archive_name("part", 3, "deadbeef", "lean_lastModified")
        == "part-lean_lastModified-archive-000003-deadbeef.json.gz"
    )


def test_去重类别只有一份真相源() -> None:
    assert archive.DEDUPE_LABELS is raw.BRONZE_LABELS
    assert raw.CARD_LABEL not in raw.BRONZE_LABELS


# ---------------------------------------------------------------- 档位不变式


def test_比基座更窄的扫描在构造期被拒() -> None:
    with pytest.raises(ValueError, match="ScanSpec.expand 缺少 Bronze 基座字段"):
        spec(expand=("downloads",))

    with pytest.raises(ValueError, match="private"):
        spec(expand=tuple(field for field in IDENTITY_EXPAND if field != "private"))


def test_含分隔符的来源在写入前被拒(store: DatasetStore) -> None:
    """静默生成解析不出的文件名，比直接报错更难查。"""
    with pytest.raises(ValueError, match="source 只能含字母、数字与下划线"):
        raw.write(
            store,
            snapshot_date=DAY,
            page_index=1,
            records=[{"id": "acme/ds"}],
            source="top-refresh",
        )

    assert not raw.batch_files(store, DAY)


def test_合规档位一律放行() -> None:
    assert spec().source == "lean_lastModified"
    assert spec(expand=ENRICH_EXPAND, mode="enrich", sort="downloads").source == (
        "enrich_downloads"
    )


# ---------------------------------------------------------------- 胜负与归档


def test_命名空间不参与排序胜负仍由写入时刻决定(store: DatasetStore) -> None:
    """来源字母序与写入序相反：若有人把 source 编进排序键，此例会立刻红。"""
    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[{"id": "acme/ds", "downloads": 111}],
        source="zzz_last",
        written_at="2026-11-07T00:00:01.000000+00:00",
    )
    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[{"id": "acme/ds", "downloads": 222}],
        source="aaa_first",
        written_at="2026-11-07T00:00:02.000000+00:00",
    )

    bronze.build(store, DAY)

    assert store.read("bronze", DAY).row(0, named=True)["downloads"] == 222


def test_归档分片按来源分流并保留来源(store: DatasetStore) -> None:
    for index, source in enumerate(("lean_lastModified", "top_refresh_downloads"), 1):
        raw.write(
            store,
            snapshot_date=DAY,
            page_index=1,
            records=[{"id": "acme/ds", "downloads": index * 10}],
            source=source,
            written_at=f"2026-11-07T00:00:0{index}.000000+00:00",
        )
    before = bronze.build(store, DAY)

    result = archive.archive_day(store, DAY)

    assert result is not None
    shards = raw.batch_files(store, DAY)
    assert len(shards) == 2
    assert {raw.source_of(path) for path in shards} == {
        "lean_lastModified",
        "top_refresh_downloads",
    }
    for path in shards:
        body = payload(path)
        assert body["meta"]["source"] == raw.source_of(path)
        assert all(group["source"] == raw.source_of(path) for group in body["groups"])
    assert bronze.build(store, DAY) == before
