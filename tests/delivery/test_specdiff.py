"""口径比对的行为测试。

守两件事：**对齐方式不制造噪声**（插一条条件不该让后面每条都显示「变了」）、
**只比决定产物的部分**（改标题不算口径变更，但要单独说一句）。
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import openpyxl
import pytest
import yaml

from hf_data_crawl.delivery import specdiff
from hf_data_crawl.delivery.requirement import Spec, SpecError, load


def make(tmp_path: Path, name: str, **overrides: Any) -> Spec:
    """造一份可被 diff 的 spec。同名目录复用，便于「改一处再比」。"""
    folder = tmp_path / name
    folder.mkdir(parents=True, exist_ok=True)
    body: dict[str, Any] = {
        "id": "REQ-0001",
        "title": "高热度",
        "requester": "大帅",
        "version": 1,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": 1000}],
    }
    body.update(overrides)
    if body.get("kind") == "backfill":
        # 输入表必须真实存在且不越出需求目录：spec 读取时就校验这一条。
        book = folder / "input" / "清单.xlsx"
        book.parent.mkdir(parents=True, exist_ok=True)
        if not book.exists():
            openpyxl.Workbook().save(book)
    path = folder / "spec.yaml"
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    return load(path)


def fill_body(tmp_path: Path, **overrides: Any) -> dict[str, Any]:
    """一份最小回填口径。输入表由 make() 放进各自的需求目录，这里只写相对路径。"""
    body: dict[str, Any] = {
        "kind": "backfill",
        # 回填按 URL 清单逐条补，不筛条件；`make()` 的默认 body 带一条 criteria，
        # 不清掉会被 `_validate` 拒绝——那条校验正是 2026-11-16 审计补的。
        "criteria": [],
        "fill": {
            "source": {"path": "input/清单.xlsx", "header_row": 1, "key_column": "URL"},
            "output": {"name": "已补全.xlsx"},
            "columns": {"下载量": {"field": "downloads"}, "训练阶段": {"manual": "须读 README"}},
        },
    }
    body.update(overrides)
    return body


def test_改阈值只报那一处(tmp_path: Path) -> None:
    old = make(tmp_path, "a")
    new = make(tmp_path, "b", criteria=[{"metric": "下载量", "op": ">", "value": 800}])

    changes = specdiff.compare(old, new)

    assert [item.path for item in changes] == ["criteria[下载量 > #0].value"]
    assert changes[0].old == 1000
    assert changes[0].new == 800


def test_插一条条件不会牵连原有条件(tmp_path: Path) -> None:
    """按 metric 对齐：不这么做，插入一条会让后面每条都显示「变了」，差异表就废了。"""
    old = make(tmp_path, "a", criteria=[{"metric": "下载量", "op": ">", "value": 1000}])
    new = make(
        tmp_path,
        "b",
        criteria=[
            {"metric": "下载量", "op": ">", "value": 1000},
            {"metric": "点赞数", "op": ">", "value": 50},
        ],
    )

    changes = specdiff.compare(old, new)

    assert [item.path for item in changes] == [
        "criteria[点赞数 > #0].op",
        "criteria[点赞数 > #0].value",
    ]
    assert changes[0].old is specdiff._MISSING


def test_同词条的第二条条件被删也要报(tmp_path: Path) -> None:
    """区间筛选是常见写法：`下载量 > 100` 且 `下载量 < 1000`。

    只用 metric 作键时后一条会覆盖前一条，于是删掉一条 `request diff` 会打印
    「口径未变」——而唯一能发现它的 spec_digest 不在这条路径上显示。
    """
    old = make(
        tmp_path,
        "a",
        criteria=[
            {"metric": "下载量", "op": ">", "value": 100},
            {"metric": "下载量", "op": "<", "value": 1000},
        ],
    )
    new = make(
        tmp_path,
        "b",
        criteria=[{"metric": "下载量", "op": "<", "value": 1000}],
    )

    changes = specdiff.compare(old, new)

    assert [item.path for item in changes] == [
        "criteria[下载量 > #0].op",
        "criteria[下载量 > #0].value",
    ], "删掉的第一条必须报出来，且第二条不能被牵连"
    assert all(item.new is specdiff._MISSING for item in changes)


def test_改一列的取值方式只报那一列(tmp_path: Path) -> None:
    old = make(tmp_path, "a", **fill_body(tmp_path))
    changed = fill_body(tmp_path)
    changed["fill"]["columns"]["训练阶段"] = {"field": "training_stage"}
    new = make(tmp_path, "b", **changed)

    changes = specdiff.compare(old, new)

    paths = [item.path for item in changes]
    assert "fill.columns[训练阶段].field" in paths
    # 同一列的 manual 理由随规则消失，也应当报出来（否则读的人以为它还在）。
    assert "fill.columns[训练阶段].reason" in paths
    assert all(item.path.startswith("fill.columns[训练阶段]") for item in changes)


def test_只改标题时口径未变但要单独说一句(tmp_path: Path) -> None:
    old = make(tmp_path, "a")
    new = make(tmp_path, "b", title="高热度（改名）")

    assert specdiff.compare(old, new) == []
    notes = specdiff.side_notes(old, new)
    assert len(notes) == 1
    assert "标题" in notes[0] and "改名" in notes[0]


def test_只改列宽不算口径差异但要单独说一句(tmp_path: Path) -> None:
    """展示项会改产物字节、却不进口径指纹：混进「口径差异」会让人以为数字变了。"""
    old = make(tmp_path, "a", **fill_body(tmp_path))
    body = fill_body(tmp_path)
    body["fill"]["columns"]["下载量"]["width"] = 30
    new = make(tmp_path, "b", **body)

    assert specdiff.compare(old, new) == []
    notes = specdiff.display_notes(old, new)
    assert len(notes) == 1
    assert "下载量" in notes[0] and "宽 30" in notes[0]


def test_长文本截断且换行压平() -> None:
    long_sql = "SELECT " + "、".join("col" for _ in range(40)) + "\nFROM datasets"

    text = specdiff.cell(long_sql)

    assert "\n" not in text
    assert text.endswith("…")
    assert len(text) <= specdiff._CELL_LIMIT


def test_纯列名不能作筛选条件(tmp_path: Path) -> None:
    """`id`/`url` 只供 order_by 与 output.columns 使用。

    放行它们作条件时，`compile_sql` 会在 `lookup` 处抛 `UnknownMetric`，而该异常
    逃出 `DeliveryBlocked`，`request run-due` 会整批中断。
    """
    with pytest.raises(SpecError, match="纯列名"):
        make(tmp_path, "a", criteria=[{"metric": "id", "op": "=", "value": "acme/x"}])


def test_SQL模式下criteria必须删掉(tmp_path: Path) -> None:
    """SQL 模式下 criteria 不参与编译，却照样进 `spec_digest`。

    留着它会让 manifest 看起来筛过、实际没有。
    """
    with pytest.raises(SpecError, match="criteria 不生效"):
        make(
            tmp_path,
            "a",
            mode="sql",
            sql="SELECT id FROM datasets",
            criteria=[{"metric": "下载量", "op": ">", "value": 1000}],
        )


def test_回填不接受筛选条件(tmp_path: Path) -> None:
    """回填按输入表的 URL 清单逐条补，范围不是筛出来的。"""
    with pytest.raises(SpecError, match="不支持 criteria"):
        make(
            tmp_path,
            "a",
            **fill_body(tmp_path, criteria=[{"metric": "下载量", "op": ">", "value": 1}]),
        )
