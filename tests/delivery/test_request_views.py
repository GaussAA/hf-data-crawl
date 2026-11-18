"""`request show` 与 `request usage` 的分支测试。

这两条是「读视图」，不写数据，却最容易漏测：它们的分支几乎全是文案。但文案
承载的是判断——「已关闭」「这一版没交过」「改定义会影响谁」——读错一句就会让
人拿着错的前提去干活。所以按分支钉住，而不是只看有没有打印。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import openpyxl
import pytest
import yaml

from hf_data_crawl.delivery.requirement import SPEC_NAME, resolve_dir
from hf_data_crawl.ops.cli import main


def run(tmp_path: Path, *argv: str) -> int:
    return main(
        ["--data-dir", str(tmp_path / "data"), "--requests-dir", str(tmp_path / "req"), *argv]
    )


def make(
    tmp_path: Path,
    request_id: str = "REQ-0001",
    *extra: str,
    purpose: str = "选型参考",
) -> Path:
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
        purpose,
        *extra,
    )
    assert code == 0
    return resolve_dir(tmp_path / "req", request_id) / SPEC_NAME


def rewrite(path: Path, mutate: Any) -> None:
    body = yaml.safe_load(path.read_text(encoding="utf-8"))
    mutate(body)
    path.write_text(
        yaml.safe_dump(body, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )


def test_show_标出已关闭与样例身份(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """关闭与样例是两件独立的事实，各占一行；只看标题看不出这单已经停了。"""
    make(tmp_path, "REQ-0100", "--sample")
    assert run(tmp_path, "request", "close", "REQ-0100", "--reason", "需求方撤回") == 0
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0100") == 0

    out = capsys.readouterr().out
    assert "状态：已关闭" in out
    assert "需求方撤回" in out
    assert "样例需求" in out


def test_show_回填型展示输入表而不是SQL(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """回填没有 criteria / SQL，它的「怎么算」全在 fill 段——展示错了等于没展示。"""
    book = tmp_path / "in.xlsx"
    openpyxl.Workbook().save(book)
    make(tmp_path, "REQ-0002", "--kind", "backfill", "--source", str(book))
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0002") == 0

    out = capsys.readouterr().out
    assert "输入快照：input/in.xlsx" in out
    assert "表头第 2 行" in out
    assert "编译后的 SQL" not in out


def test_show_就地改口径未升版时预警(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """同一版本号塞进两份口径，档案会声称它「就是当时那一版」——必须当场说破。"""
    path = make(tmp_path)
    history = path.parent / "history"
    history.mkdir()
    (history / "v1.yaml").write_bytes(path.read_bytes())
    text = path.read_text(encoding="utf-8").replace(
        "    value: 0\n  - metric: 点赞数",
        "    value: 5\n  - metric: 点赞数",
    )
    path.write_text(text, encoding="utf-8")
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "[!]" in out
    assert "下一次交付会被拦下" in out


def test_show_用途为空时不打印空行(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    make(tmp_path, purpose="")
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    assert "用途：" not in capsys.readouterr().out


def test_usage_坏spec只跳过不中断反查(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """一份坏 spec 不该让整张影响面清单作废——那正是最需要它的时候。"""
    make(tmp_path)
    broken = tmp_path / "req" / "REQ-9000-broken"
    broken.mkdir(parents=True)
    (broken / SPEC_NAME).write_text("id: 这不是一份合法 spec\n", encoding="utf-8")
    capsys.readouterr()

    code = run(tmp_path, "request", "usage", "下载量")

    out = capsys.readouterr().out
    assert "[!]" in out and "REQ-9000-broken" in out
    assert "REQ-0001" in out
    assert code == 0


def test_usage_排序键也算引用(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """按词条排序也是依赖：改定义会让行序变，而「两次交付逐字节可比」正靠行序。"""
    path = make(tmp_path)
    rewrite(
        path, lambda body: body.__setitem__("order_by", [{"metric": "下载量", "direction": "desc"}])
    )
    capsys.readouterr()

    assert run(tmp_path, "request", "usage", "下载量") == 0

    assert "排序：下载量 降序" in capsys.readouterr().out


def test_usage_输出列也算引用(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    path = make(tmp_path)
    rewrite(path, lambda body: body["output"].__setitem__("columns", ["id", "点赞数"]))
    capsys.readouterr()

    assert run(tmp_path, "request", "usage", "点赞数") == 0

    assert "输出列：点赞数" in capsys.readouterr().out


def test_usage_只有样例引用时明说可以放心改(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """真实需求为零时结论必须是「可改」；样例单独报数量，别让它挡住改动。"""
    make(tmp_path, "REQ-0100", "--sample")
    capsys.readouterr()

    code = run(tmp_path, "request", "usage", "下载量")

    out = capsys.readouterr().out
    assert "没有真实需求用到它" in out
    assert "另有 1 份样例需求" in out
    assert code == 1


def test_show_外发记录只列最近三条(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """台账可能很长，终端只留最近几笔；完整记录在 HANDOFF.jsonl 里。"""
    path = make(tmp_path)
    entries = [
        {"at": f"2026-11-{day:02d}T10:00:00+08:00", "delivery": f"d{day}", "to": f"乙方{day}"}
        for day in range(1, 6)
    ]
    (path.parent / "HANDOFF.jsonl").write_text(
        "\n".join(json.dumps(item, ensure_ascii=False) for item in entries) + "\n",
        encoding="utf-8",
    )
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "乙方5" in out
    assert "乙方1" not in out


def test_usage_回填按列名反查(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """回填型没有 criteria，它的依赖全在 fill.columns 的 field 上——别只查查询型。"""
    book = tmp_path / "in.xlsx"
    openpyxl.Workbook().save(book)
    make(tmp_path, "REQ-0002", "--kind", "backfill", "--source", str(book))
    capsys.readouterr()

    code = run(tmp_path, "request", "usage", "id")

    out = capsys.readouterr().out
    assert "列「数据集名称」← field id" in out
    assert code == 0


def test_usage_回填模板列也算引用(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """模板里的 `{id}` 同样是依赖：底库改列名，这些格子会拼出半截链接。"""
    book = tmp_path / "in.xlsx"
    openpyxl.Workbook().save(book)
    path = make(tmp_path, "REQ-0002", "--kind", "backfill", "--source", str(book))
    rewrite(
        path,
        lambda body: body["fill"].__setitem__(
            "columns", {"链接": {"template": "https://huggingface.co/{id}"}}
        ),
    )
    capsys.readouterr()

    assert run(tmp_path, "request", "usage", "id") == 0

    out = capsys.readouterr().out
    assert "模板" in out and "huggingface.co/{id}" in out


def test_show_手写SQL时不再列条件(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """mode=sql 时 criteria 不参与编译；列出来会让人以为条件真的生效了。"""

    path = make(tmp_path)

    def to_sql(body: dict[str, Any]) -> None:
        body["mode"] = "sql"
        body["sql"] = "SELECT id, downloads FROM datasets"
        body.pop("criteria", None)

    rewrite(path, to_sql)
    capsys.readouterr()

    assert run(tmp_path, "request", "show", "REQ-0001") == 0

    out = capsys.readouterr().out
    assert "编译后的 SQL" in out
    assert "1. 下载量" not in out
