"""交付比对的测试。

守三件事：**按主键对齐**（不是按行号，否则删一行全表皆变）、**列集差异要报出来**
（不静默取交集）、**回填的键要归一**（链接与 `作者/名称` 是同一个库）。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import openpyxl
import polars as pl
import pytest

from hf_data_crawl.delivery.compare import CompareError, compare_deliveries


def make_delivery(root: Path, name: str, kind: str, files: list[str], **extra: Any) -> Path:
    directory = root / name
    directory.mkdir(parents=True, exist_ok=True)
    body: dict[str, Any] = {
        "kind": kind,
        "result": {"rows": 0, "columns": [], "files": [{"name": item} for item in files]},
    }
    body.update(extra)
    (directory / "manifest.json").write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")
    return directory


def test_查询型按id对齐并报改值格数(tmp_path: Path) -> None:
    left = make_delivery(tmp_path, "A", "query", ["data.parquet"])
    pl.DataFrame({"id": ["a", "b"], "downloads": [1, 2], "tags": [["x"], ["y"]]}).write_parquet(
        left / "data.parquet"
    )
    right = make_delivery(tmp_path, "B", "query", ["data.parquet"])
    pl.DataFrame({"id": ["a", "c"], "downloads": [5, 3], "tags": [["x"], ["z"]]}).write_parquet(
        right / "data.parquet"
    )

    result = compare_deliveries(left, right)

    assert result.added == ("c",)
    assert result.removed == ("b",)
    assert result.common_rows == 1
    downloads = next(item for item in result.columns if item.name == "downloads")
    # 只有 a 是共同行，且它变了；b 被删、c 新增都不算改值格。
    assert downloads.changed == 1
    assert downloads.samples == (("a", "1", "5"),)
    tags = next(item for item in result.columns if item.name == "tags")
    assert tags.changed == 0


def test_列集不同要单独报出来(tmp_path: Path) -> None:
    left = make_delivery(tmp_path, "A", "query", ["data.parquet"])
    pl.DataFrame({"id": ["a"], "downloads": [1]}).write_parquet(left / "data.parquet")
    right = make_delivery(tmp_path, "B", "query", ["data.parquet"])
    pl.DataFrame({"id": ["a"], "downloads": [1], "likes": [9]}).write_parquet(
        right / "data.parquet"
    )

    result = compare_deliveries(left, right)

    assert result.right_only_columns == ("likes",)
    assert result.left_only_columns == ()
    assert result.changed_cells == 0


def write_book(path: Path, rows: list[tuple[str, str, str]]) -> Path:
    book = openpyxl.Workbook()
    sheet = book.active
    assert sheet is not None
    sheet.title = "Sheet1"
    sheet.cell(row=1, column=1, value="基础信息")
    for column, name in enumerate(["URL", "下载量", "训练阶段"], start=1):
        sheet.cell(row=2, column=column, value=name)
    for offset, (url, downloads, stage) in enumerate(rows):
        sheet.cell(row=3 + offset, column=1, value=url)
        sheet.cell(row=3 + offset, column=2, value=downloads)
        sheet.cell(row=3 + offset, column=3, value=stage)
    book.save(path)
    return path


def backfill_body() -> dict[str, Any]:
    return {
        "fill": {
            "source": {"key_column": "URL", "header_row": 2, "sheet": "Sheet1"},
            "output": "已补全.xlsx",
        }
    }


def test_回填型按标识列对齐且链接归一(tmp_path: Path) -> None:
    """产物里的键是完整链接，比对标的是 `作者/名称`；不归一，同一个库会被当成两个。"""
    left = make_delivery(tmp_path, "A", "backfill", ["已补全.xlsx"], **backfill_body())
    write_book(
        left / "已补全.xlsx",
        [("https://huggingface.co/datasets/acme/ds-1", "10", ""), ("acme/ds-2", "20", "")],
    )
    right = make_delivery(tmp_path, "B", "backfill", ["已补全.xlsx"], **backfill_body())
    write_book(
        right / "已补全.xlsx",
        [("acme/ds-1", "10", "后训练"), ("acme/ds-2", "20", "")],
    )

    result = compare_deliveries(left, right)

    assert result.added == () and result.removed == ()
    stage = next(item for item in result.columns if item.name == "训练阶段")
    assert stage.changed == 1
    assert stage.samples[0][0] == "acme/ds-1"
    assert stage.samples[0][2] == "后训练"


def test_形态不同直接拒绝(tmp_path: Path) -> None:
    left = make_delivery(tmp_path, "A", "query", ["data.parquet"])
    right = make_delivery(tmp_path, "B", "backfill", ["已补全.xlsx"], **backfill_body())

    with pytest.raises(CompareError, match="形态不同"):
        compare_deliveries(left, right)


def test_缺产物要说清楚而不是空比(tmp_path: Path) -> None:
    left = make_delivery(tmp_path, "A", "query", ["data.parquet"])
    right = make_delivery(tmp_path, "B", "query", ["data.parquet"])

    with pytest.raises(CompareError, match="没有 data.parquet"):
        compare_deliveries(left, right)


def run_cli(tmp_path: Path, *argv: str) -> int:
    from hf_data_crawl.ops.cli import main

    return main(
        ["--data-dir", str(tmp_path / "data"), "--requests-dir", str(tmp_path / "req"), *argv]
    )


def seed(tmp_path: Path, *names: str) -> Path:
    folder = tmp_path / "req" / "REQ-0001-query-高热度"
    (folder / "deliveries").mkdir(parents=True)
    (folder / "spec.yaml").write_text("id: REQ-0001\n", encoding="utf-8")
    for name in names:
        (folder / "deliveries" / name).mkdir()
    return folder


def test_compare默认比最近两次(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    folder = seed(tmp_path, "A", "B")
    left = make_delivery(folder / "deliveries", "A", "query", ["data.parquet"])
    pl.DataFrame({"id": ["a"], "downloads": [1]}).write_parquet(left / "data.parquet")
    right = make_delivery(folder / "deliveries", "B", "query", ["data.parquet"])
    pl.DataFrame({"id": ["a"], "downloads": [5]}).write_parquet(right / "data.parquet")

    assert run_cli(tmp_path, "request", "compare", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "交付对比" in out and "A → B" in out
    assert "downloads" in out and "1 → 5" in out


def test_compare只有一次交付时说清没有可比对象(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    folder = seed(tmp_path, "A")
    make_delivery(folder / "deliveries", "A", "query", ["data.parquet"])

    assert run_cli(tmp_path, "request", "compare", "REQ-0001") == 1
    assert "没有可比对象" in capsys.readouterr().out


def test_空值与空列表不算改动但一侧有值才算(tmp_path: Path) -> None:
    """真机上踩过：23 行 × 30 列凭空冒出 79 格「改值」，全是空值/空列表的存储差异。

    空列表与空值在存储上是两回事、在含义上是同一件事（都没值）。不统一，差异表
    就会满屏「（空）→（空）」，比不比都看不出东西。
    """
    left = make_delivery(tmp_path, "A", "query", ["data.parquet"])
    pl.DataFrame({"id": ["a", "b"], "tags": [[], None], "likes": [None, None]}).write_parquet(
        left / "data.parquet"
    )
    right = make_delivery(tmp_path, "B", "query", ["data.parquet"])
    pl.DataFrame({"id": ["a", "b"], "tags": [None, None], "likes": [None, 5]}).write_parquet(
        right / "data.parquet"
    )

    result = compare_deliveries(left, right)

    tags = next(item for item in result.columns if item.name == "tags")
    likes = next(item for item in result.columns if item.name == "likes")
    assert tags.changed == 0
    assert likes.changed == 1
    assert likes.samples == (("b", "（空）", "5"),)
