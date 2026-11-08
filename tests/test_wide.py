"""宽表导出的契约测试。

重点不是「能不能写出一个 parquet」，而是三条边界：

1. **口径**——列集合必须等于 `gold.datasets` 视图，不能另拼一份 join；
2. **无副作用**——导出前后所有状态表逐字节不变，它确实只是投影；
3. **原子性**——校验不过时撤销临时文件，绝不拿半截文件顶替上一版。

后两条各有一个「把校验弄坏」的用例，用来确认测试本身能变红：若有人图省事
删掉校验，这里会立刻暴露，而不是等线上拿到错口径的宽表才发现。
"""

from __future__ import annotations

import hashlib
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl import wide as wide_module
from hf_data_crawl.cli import WRITE_COMMANDS, main
from hf_data_crawl.derive import derive_day
from hf_data_crawl.fields import DEFAULT_COLUMNS
from hf_data_crawl.gold import GoldCatalog, wide_column_names
from hf_data_crawl.runs import read_runs
from hf_data_crawl.store import DatasetStore
from hf_data_crawl.wide import WideExportMismatch, export_wide

DAY = date(2026, 11, 1)


def record(index: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": index * 10,
        "downloadsAllTime": index * 100,
        "likes": index,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["license:mit", "task_categories:text-classification", "language:en"],
    }


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    store = DatasetStore(tmp_path / "data")
    store.write_raw(
        snapshot_date=DAY,
        page_index=1,
        records=[record(index) for index in range(1, 4)],
        meta={},
    )
    derive_day(store, DAY)
    return store


def _fingerprint(root: Path) -> dict[str, str]:
    """数据目录里所有登记表 parquet 的内容指纹，排除导出产物本身。"""
    exports = root / "exports"
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(root.rglob("*.parquet"))
        if exports not in path.parents
    }


def _temp_for(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".tmp")


def test_导出列集合与默认交付契约一致(store: DatasetStore) -> None:
    """导出物是给人用的裁剪投影；视图本身仍保留全列供内部查询。"""
    summary = export_wide(store)
    frame = pl.read_parquet(summary.path)

    assert frame.columns == list(DEFAULT_COLUMNS)
    assert summary.columns == len(DEFAULT_COLUMNS)
    assert summary.rows == store.read_current().height

    catalog = GoldCatalog(store)
    try:
        view = [
            row[0]
            for row in catalog.connection.execute("DESCRIBE SELECT * FROM datasets").fetchall()
        ]
    finally:
        catalog.close()
    assert view == list(wide_column_names())
    assert len(view) > len(frame.columns)


def test_默认产物落在_exports_目录(store: DatasetStore) -> None:
    assert export_wide(store).path == store.root / "exports" / "datasets.parquet"


def test_导出不改任何状态表(store: DatasetStore) -> None:
    before = _fingerprint(store.root)
    assert before, "夹具应当已造出登记表文件"

    export_wide(store)

    assert _fingerprint(store.root) == before


def test_重复导出结果一致且不留临时文件(store: DatasetStore) -> None:
    first = export_wide(store)
    first_frame = pl.read_parquet(first.path)

    second = export_wide(store)

    assert (second.rows, second.columns) == (first.rows, first.columns)
    assert pl.read_parquet(second.path).equals(first_frame)
    assert not _temp_for(second.path).exists()


def test_目标越出数据目录被拒且不创建任何文件(store: DatasetStore, tmp_path: Path) -> None:
    outside = tmp_path / "outside" / "wide.parquet"

    with pytest.raises(ValueError, match="必须在数据目录内"):
        export_wide(store, outside)

    assert not outside.exists()
    assert not (store.root / "exports").exists()


def test_静态清单与视图不一致时拒绝导出(
    store: DatasetStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """反向验证一：把静态清单改短，必须红——证明视图与清单没有各说各话。"""
    expected = wide_column_names()
    monkeypatch.setattr(wide_module, "wide_column_names", lambda: expected[:-1])

    with pytest.raises(WideExportMismatch, match="视图实际列"):
        export_wide(store)

    assert not (store.root / "exports" / "datasets.parquet").exists()


def test_契约列在视图里找不到时拒绝导出(
    store: DatasetStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """反向验证三：契约拼错列名，不能等 COPY 之后才发现文件少一列。"""
    monkeypatch.setattr(wide_module, "DEFAULT_COLUMNS", (*DEFAULT_COLUMNS, "不存在的列"))

    with pytest.raises(WideExportMismatch, match="视图里找不到"):
        export_wide(store)

    assert not (store.root / "exports" / "datasets.parquet").exists()


def test_行数不符时撤销临时文件且不覆盖旧产物(
    store: DatasetStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """反向验证二：COPY 已落盘才发现行数不对，旧产物必须原封不动。"""
    good = export_wide(store)
    before = good.path.read_bytes()

    monkeypatch.setattr(wide_module, "_current_rows", lambda _store: 999)

    with pytest.raises(WideExportMismatch, match="导出行数"):
        export_wide(store)

    assert good.path.read_bytes() == before
    assert not _temp_for(good.path).exists()


def test_命令行导出成功并留下运行摘要(
    store: DatasetStore, capsys: pytest.CaptureFixture[str]
) -> None:
    code = main(["--data-dir", str(store.root), "export-wide"])

    assert code == 0
    assert f"{len(DEFAULT_COLUMNS)} 列" in capsys.readouterr().out
    assert (store.root / "exports" / "datasets.parquet").exists()
    last = read_runs(store)[-1]
    assert last["command"] == "export-wide"
    assert last["status"] == "success"


def test_无采集数据时命令失败且留痕(tmp_path: Path) -> None:
    root = tmp_path / "empty-data"
    root.mkdir()

    code = main(["--data-dir", str(root), "export-wide"])

    assert code == 1
    assert not (root / "exports" / "datasets.parquet").exists()
    assert read_runs(DatasetStore(root))[-1]["status"] == "failed"


def test_导出命令属于写命令() -> None:
    """与 `collect` 并发会读到新旧混合的快照，必须持锁。"""
    assert "export-wide" in WRITE_COMMANDS
