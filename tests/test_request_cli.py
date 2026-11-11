"""request 子命令的行为测试。

验的是三件事：**建出来的东西一定合法**（建完立即按真规矩回读一遍）、
**一个坏 spec 不挡住其余需求**、**坏输入不留半成品**。
"""

from __future__ import annotations

from pathlib import Path

import openpyxl
import pytest
import yaml

from hf_data_crawl.delivery.requirement import SPEC_NAME, Kind, load, resolve_dir
from hf_data_crawl.ops.cli import main


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
    # 目录名带语义后缀，按编号解析才找得到。
    return resolve_dir(tmp_path / "req", request_id) / SPEC_NAME


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
    root = tmp_path / "req"
    assert not (root / "REQ-1").exists()
    if root.is_dir():
        assert list(root.glob("REQ-1*")) == []


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
    folder = resolve_dir(tmp_path / "req", "REQ-0002")
    spec = load(folder / SPEC_NAME)
    assert spec.kind is Kind.BACKFILL
    assert spec.fill is not None
    assert spec.fill.source.relative == f"input/{book.name}"
    snapshot = folder / "input" / book.name
    assert snapshot.read_bytes() == book.read_bytes()
    assert spec.fill.source.path == snapshot.resolve()
    assert "backfill" in folder.name and "清单补全" in folder.name


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
    root = tmp_path / "req"
    assert not (root / "REQ-0002").exists()
    if root.is_dir():
        assert list(root.glob("REQ-0002*")) == []


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


def test_取材后外部文件改动不影响本需求(tmp_path: Path) -> None:
    """取完即断：外部原件被删被改，需求目录仍是能独立复现的封闭单元。"""
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
    folder = resolve_dir(tmp_path / "req", "REQ-0002")
    snapshot = folder / "input" / book.name
    before = snapshot.read_bytes()

    book.write_bytes(b"changed by someone else")

    assert snapshot.read_bytes() == before
    assert load(folder / SPEC_NAME).fill is not None


def test_新建即带过程记录骨架且标明未填(tmp_path: Path) -> None:
    """新建就把 method/ 的坑位占好：想不起来记「怎么做的」，是这类记录最常见的死法。"""
    path = make(tmp_path)

    doc = path.parent / "method" / "METHOD.md"
    index = path.parent / "method" / "method.json"
    assert doc.is_file() and index.is_file()
    assert "<!-- 待填：" in doc.read_text(encoding="utf-8")


def test_method命令只判不写且以退出码表达(tmp_path: Path) -> None:
    path = make(tmp_path)
    assert run(tmp_path, "request", "method", "REQ-0001", "--check") == 1

    doc = path.parent / "method" / "METHOD.md"
    doc.write_text(
        doc.read_text(encoding="utf-8").replace("<!-- 待填：", "<!-- 已填："), encoding="utf-8"
    )

    assert run(tmp_path, "request", "method", "REQ-0001", "--check") == 0


def test_show打印过程记录状态(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path)

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "过程记录：" in out
    assert "不合规" in out
    assert "口径指纹：" in out


def make_header_book(path: Path, headers: list[str], *, sheet_name: str = "Sheet1") -> Path:
    """造一张「第 1 行分组、第 2 行列名」的表，与回填骨架的 header_row: 2 对齐。"""
    book = openpyxl.Workbook()
    sheet = book.active
    assert sheet is not None
    sheet.title = sheet_name
    sheet.cell(row=1, column=1, value="基础信息")
    for index, name in enumerate(headers, start=1):
        sheet.cell(row=2, column=index, value=name)
    sheet.cell(row=3, column=headers.index("URL") + 1, value="https://huggingface.co/datasets/a/b")
    book.save(path)
    return path


def test_收新输入按版本归档并报字段差异(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """换表不是覆盖：旧快照要留着，否则上一份档案里的 sha256 就对不上磁盘了。"""
    old = make_header_book(tmp_path / "旧表.xlsx", ["URL", "数据集名称", "下载量", "备注"])
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
            str(old),
        )
        == 0
    )
    folder = resolve_dir(tmp_path / "req", "REQ-0002")
    before = (folder / SPEC_NAME).read_text(encoding="utf-8")

    new = make_header_book(tmp_path / "新表.xlsx", ["URL", "数据集名称", "下载量", "新增列"])
    assert run(tmp_path, "request", "input", "REQ-0002", "--add", str(new)) == 0

    out = capsys.readouterr().out
    assert (folder / "input" / "v2" / "新表.xlsx").is_file()
    assert (folder / "input" / "旧表.xlsx").is_file()  # 旧快照不动
    assert "新增 1 列" in out and "新增列" in out
    assert "少了 1 列" in out and "备注" in out
    # 换输入等于换口径：命令只搬文件报差异，不替人改 spec。
    assert (folder / SPEC_NAME).read_text(encoding="utf-8") == before


def test_新输入缺标识列时拒收且不留目录(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    old = make_header_book(tmp_path / "旧表.xlsx", ["URL", "数据集名称"])
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
            str(old),
        )
        == 0
    )
    folder = resolve_dir(tmp_path / "req", "REQ-0002")

    broken = tmp_path / "缺列.xlsx"
    book = openpyxl.Workbook()
    sheet = book.active
    assert sheet is not None
    sheet.cell(row=2, column=1, value="链接")
    book.save(broken)

    assert run(tmp_path, "request", "input", "REQ-0002", "--add", str(broken)) == 1
    assert "标识列" in capsys.readouterr().out
    assert not (folder / "input" / "v2").exists()


def write_versions(tmp_path: Path, *bodies: dict[str, object]) -> None:
    """造一份带 n 版快照的需求；最后一版同时作为当前 spec。"""
    folder = tmp_path / "req" / "REQ-0001-query-高热度"
    (folder / "history").mkdir(parents=True, exist_ok=True)
    for index, body in enumerate(bodies, start=1):
        text = yaml.safe_dump(body, allow_unicode=True)
        (folder / "history" / f"v{index}.yaml").write_text(text, encoding="utf-8")
        (folder / "spec.yaml").write_text(text, encoding="utf-8")


def version_body(version: int, **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {
        "id": "REQ-0001",
        "title": "高热度",
        "requester": "大帅",
        "version": version,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": 1000}],
    }
    body.update(overrides)
    return body


def test_diff默认比最近两版(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_versions(
        tmp_path,
        version_body(1),
        version_body(2, criteria=[{"metric": "下载量", "op": ">", "value": 800}]),
    )

    assert run(tmp_path, "request", "diff", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "口径 v1 → v2" in out
    assert "criteria[下载量].value" in out
    assert "1000" in out and "800" in out


def test_diff可以点名两版(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_versions(tmp_path, version_body(1), version_body(2), version_body(3))

    assert run(tmp_path, "request", "diff", "REQ-0001", "v1", "v3") == 0

    assert "口径 v1 → v3" in capsys.readouterr().out


def test_diff只有一版时说清没有可比对象(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_versions(tmp_path, version_body(1))

    assert run(tmp_path, "request", "diff", "REQ-0001") == 1
    assert "没有可比对象" in capsys.readouterr().out


def test_diff只改标题时明说口径未变(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    write_versions(tmp_path, version_body(1), version_body(2, title="高热度（改名）"))

    assert run(tmp_path, "request", "diff", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "口径未变" in out
    assert "标题" in out and "改名" in out
