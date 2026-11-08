"""request 子命令的行为测试。

验的是三件事：**建出来的东西一定合法**（建完立即按真规矩回读一遍）、
**一个坏 spec 不挡住其余需求**、**坏输入不留半成品**。
"""

from __future__ import annotations

from pathlib import Path

import openpyxl
import pytest
import yaml

from hf_data_crawl.cli import main
from hf_data_crawl.requirement import SPEC_NAME, Kind, load


def run(tmp_path: Path, *argv: str) -> int:
    return main(
        ["--data-dir", str(tmp_path / "data"), "--requests-dir", str(tmp_path / "req"), *argv]
    )


def make(tmp_path: Path, request_id: str = "REQ-0001") -> Path:
    code = run(
        tmp_path,
        "request",
        "new",
        request_id,
        "--title",
        "高热度数据集清单",
        "--requester",
        "张三",
        "--purpose",
        "选型参考",
    )
    assert code == 0
    return tmp_path / "req" / request_id / SPEC_NAME


def test_新建后文件落盘且可读(tmp_path: Path) -> None:
    path = make(tmp_path)

    assert path.exists()
    spec = load(path)
    assert spec.id == "REQ-0001"
    assert spec.requester == "张三"


def test_重复需求号被拒且不覆盖原文件(tmp_path: Path) -> None:
    path = make(tmp_path)
    original = path.read_bytes()

    code = run(tmp_path, "request", "new", "REQ-0001", "--title", "改个名", "--requester", "李四")

    assert code == 1
    assert path.read_bytes() == original


def test_需求号不合法时不留半成品(tmp_path: Path) -> None:
    """建错就删掉：留一个读不出来的 spec，比没有更麻烦。"""
    code = run(tmp_path, "request", "new", "REQ-1", "--title", "x", "--requester", "y")

    assert code == 1
    assert not (tmp_path / "req" / "REQ-1").exists()


def test_空目录给出下一步提示(tmp_path: Path) -> None:
    assert run(tmp_path, "request", "list") == 0
    assert not (tmp_path / "req").exists()


def test_列出已建需求(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)
    capsys.readouterr()

    assert run(tmp_path, "request", "list") == 0

    printed = capsys.readouterr().out
    assert "REQ-0001" in printed
    assert "高热度数据集清单" in printed
    assert "从未交付" in printed


def test_坏spec不挡住其余需求(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)
    broken = tmp_path / "req" / "REQ-0002" / SPEC_NAME
    broken.parent.mkdir(parents=True)
    broken.write_text(yaml.safe_dump({"id": "REQ-0002"}), encoding="utf-8")
    capsys.readouterr()

    assert run(tmp_path, "request", "list") == 0

    printed = capsys.readouterr().out
    assert "[!] REQ-0002" in printed
    assert "REQ-0001" in printed


def test_show打印口径与编译后的SQL(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    printed = capsys.readouterr().out
    assert "落成列 downloads" in printed
    assert '"downloads" > 0' in printed
    assert "ORDER BY" in printed
    assert "尚未快照" in printed


def test_show不存在的需求返回非零(tmp_path: Path) -> None:
    assert run(tmp_path, "request", "show", "REQ-9999") == 1


# ---------------------------------------------------------------- 单一入口


def make_book(path: Path) -> Path:
    openpyxl.Workbook().save(path)
    return path


def test_fill命令已退役(tmp_path: Path) -> None:
    """回填不是另一套命令，它只是 `request` 的一种 kind。"""
    with pytest.raises(SystemExit) as caught:
        run(tmp_path, "fill")

    assert caught.value.code == 2


def test_新建回填需求走统一入口(tmp_path: Path) -> None:
    book = make_book(tmp_path / "in.xlsx")
    code = run(
        tmp_path,
        "request",
        "new",
        "REQ-0002",
        "--title",
        "清单补全",
        "--requester",
        "大帅",
        "--kind",
        "backfill",
        "--source",
        str(book),
    )

    assert code == 0
    spec = load(tmp_path / "req" / "REQ-0002" / SPEC_NAME)
    assert spec.kind is Kind.BACKFILL
    assert spec.fill is not None
    assert spec.fill.source.path == book.resolve()


def test_回填需求缺输入表被拒且不留半成品(tmp_path: Path) -> None:
    code = run(
        tmp_path,
        "request",
        "new",
        "REQ-0002",
        "--title",
        "清单补全",
        "--requester",
        "大帅",
        "--kind",
        "backfill",
    )

    assert code == 1
    assert not (tmp_path / "req" / "REQ-0002").exists()


def test_列表同时显示两种形态(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)
    book = make_book(tmp_path / "in.xlsx")
    assert (
        run(
            tmp_path,
            "request",
            "new",
            "REQ-0002",
            "--title",
            "清单补全",
            "--requester",
            "大帅",
            "--kind",
            "backfill",
            "--source",
            str(book),
        )
        == 0
    )
    capsys.readouterr()

    assert run(tmp_path, "request", "list") == 0

    printed = capsys.readouterr().out
    assert "query" in printed
    assert "backfill" in printed
    assert "REQ-0001" in printed
    assert "REQ-0002" in printed
