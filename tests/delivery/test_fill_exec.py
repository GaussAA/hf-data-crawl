"""回填形态的档案口径：机器读的 `definition` 与人读的 `detail_lines`。

`backfill.py` 的测试管的是「值填得对不对」；这里管的是「填错了说不说得清」。
两者失败代价不同：值错是一格错，口径漏记则事后无法追查——对方问「这格为什么
是空的」，档案只能回答「就是空的」。

三处尤其要紧，且都只在出问题时才被打印，平时看着像死代码：

- 判读表里写错库名/列名：产物上只表现为「这一格空着」，与「没判」一模一样。
- 未命中底库：整行留空是**有意**的，不点名就会被当成漏填。
- 未声明的列：引擎一个都没碰，但「保持空白」的说法会让人以为它清空过。
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, cast

import openpyxl
import pytest

from hf_data_crawl.delivery import fill_exec
from hf_data_crawl.delivery.backfill import (
    ColumnRule,
    ColumnTally,
    FillReport,
    FillSpec,
    TableSource,
)
from hf_data_crawl.delivery.contract import DeliveryBlocked, RunContext
from hf_data_crawl.delivery.requirement import load
from hf_data_crawl.delivery.stamp import SourceStamp
from hf_data_crawl.platform.store import DatasetStore

STAMP = {"sha256": "a" * 64, "bytes": 4096, "mtime": "2026-11-19T10:00:00+00:00"}


def table_source(tmp_path: Path) -> TableSource:
    return TableSource(
        path=tmp_path / "input" / "清单.xlsx",
        sheet="数据",
        header_row=2,
        key_column="URL",
        relative="input/清单.xlsx",
    )


def fill_spec(tmp_path: Path, **overrides: object) -> FillSpec:
    base: dict[str, object] = {
        "source": table_source(tmp_path),
        "output_name": "回填结果.xlsx",
        "columns": (ColumnRule(name="下载量", kind="field", field="downloads"),),
    }
    base.update(overrides)
    return FillSpec(**base)  # type: ignore[arg-type]


def report(**overrides: object) -> FillReport:
    base: dict[str, object] = {
        "request_id": "REQ-0001",
        "output": None,
        "rows": 3,
        "tallies": {"下载量": ColumnTally(name="下载量", written=3)},
        "missing_ids": [],
        "undeclared": [],
        "manual": [],
        "header": ("URL", "数据集名称"),
    }
    base.update(overrides)
    return FillReport(**base)  # type: ignore[arg-type]


# ---------------------------------------------------------------- 机器读口径


def test_口径把对方表头与我们加的列分开记(tmp_path: Path) -> None:
    """只记 sha256 的话「换了一列」确实对不上，可对不上在哪一列没人知道。

    `appended` 必须与 `input.header` 分开：前者说我们往右加了什么，后者说对方
    原来有什么。混成一份，「原表长什么样」就再也答不出来。
    """
    spec = fill_spec(tmp_path)
    got = fill_exec._definition(
        spec,
        report(appended=("新增列",), header=("URL", "数据集名称")),
        STAMP,
    )

    fill = got["fill"]
    assert fill["input"]["header"]["columns"] == ["URL", "数据集名称"]
    assert fill["appended"] == ["新增列"]
    assert fill["input"]["path"] == "input/清单.xlsx", "记相对路径，别把本机绝对路径写进档案"


def test_口径里判读表对不上账的部分单独记(tmp_path: Path) -> None:
    spec = fill_spec(tmp_path, overrides_relative="judge/ov.json")
    got = fill_exec._definition(
        spec,
        report(override_unknown_ids=("acme/typo",), override_unknown_columns=("不存在列",)),
        STAMP,
    )

    unmatched = got["fill"]["overrides_unmatched"]
    assert unmatched == {"ids": ["acme/typo"], "columns": ["不存在列"]}
    assert got["fill"]["overrides"] == "judge/ov.json"


# ---------------------------------------------------------------- 人读口径


def test_预演在首行写明未写盘(tmp_path: Path) -> None:
    """预演与真跑的唯一区别是写没写盘；首行不说，两份档案读起来一样。"""
    lines = fill_exec._lines(fill_spec(tmp_path), report(), True, STAMP)

    assert "预演，未写盘" in lines[0]


def test_判读表对不上账时逐条点名(tmp_path: Path) -> None:
    """写错库名的结果是「这一格空着」，与「没判」在产物里长得一模一样。"""
    lines = fill_exec._lines(
        fill_spec(tmp_path),
        report(
            override_unknown_ids=("acme/typo",),
            override_unknown_columns=("不存在列",),
        ),
        False,
        STAMP,
    )
    text = "\n".join(lines)

    assert "判读表里 1 个标识对不上这批清单" in text and "`acme/typo`" in text
    assert "1 个列名不在 spec 声明里" in text and "`不存在列`" in text
    assert "整列判读白写" in text


def test_对不上账的条目超过八条时折起来(tmp_path: Path) -> None:
    """终端一行塞三十个库名没人读；列八个 + 总数，够定位就够。"""
    lines = fill_exec._lines(
        fill_spec(tmp_path),
        report(override_unknown_ids=tuple(f"acme/ds-{i}" for i in range(12))),
        False,
        STAMP,
    )
    text = "\n".join(lines)

    assert "12 个标识对不上" in text
    assert text.count("acme/ds-") == 8
    assert "…" in text


def test_未命中底库与未声明列各自成段(tmp_path: Path) -> None:
    """两件事常被混为一谈：前者是「底库没有」，后者是「我们没碰」。"""
    lines = fill_exec._lines(
        fill_spec(tmp_path),
        report(
            missing_ids=[f"acme/ds-{i}" for i in range(10)],
            undeclared=["序号", "备注"],
        ),
        False,
        STAMP,
    )
    text = "\n".join(lines)

    assert "未命中底库 10 条" in text and "等 10 条" in text
    assert "未声明的列（不填、原样保留）" in text
    assert "序号、备注" in text


def test_口径补充说明与来源说明都进档案(tmp_path: Path) -> None:
    """对方问「数据什么时候取的」，答案在这两行里，不在脑子里。"""
    spec = fill_spec(
        tmp_path,
        notes=(("下载量", "取 30 天口径"),),
        stamp_note="数据截至 2026-11-18",
    )

    text = "\n".join(fill_exec._lines(spec, report(), False, STAMP))

    assert "### 口径补充说明" in text
    assert "**下载量**：取 30 天口径" in text
    assert "> 数据来源说明：数据截至 2026-11-18" in text


def test_追加列在逐列规则里标出新增(tmp_path: Path) -> None:
    """同一张表两版对比时，得看得出哪一列是这次才有的。"""
    spec = fill_spec(
        tmp_path,
        columns=(
            ColumnRule(name="下载量", kind="field", field="downloads"),
            ColumnRule(name="结论", kind="const", const="在用", append=True),
        ),
    )
    spec_report = report(
        tallies={
            "下载量": ColumnTally(name="下载量", written=3),
            "结论": ColumnTally(name="结论", written=3),
        },
        appended=("结论",),
    )

    text = "\n".join(fill_exec._lines(spec, spec_report, False, STAMP))

    assert "| 结论（新增） |" in text
    assert "常量 `在用`" in text


def test_逐列规则的四种来源各自写法(tmp_path: Path) -> None:
    """来源写错会让人以为某一列是判读来的，而它其实是常量。"""
    spec = fill_spec(
        tmp_path,
        columns=(
            ColumnRule(name="下载量", kind="field", field="downloads", fmt="int"),
            ColumnRule(name="链接", kind="template", template="https://hf.co/{id}"),
            ColumnRule(name="状态", kind="const", const="在用"),
            ColumnRule(name="备注", kind="manual", reason="需人工核对许可"),
        ),
    )
    spec_report = report(
        tallies={
            "下载量": ColumnTally(name="下载量", written=3),
            "链接": ColumnTally(name="链接", written=3),
            "状态": ColumnTally(name="状态", written=3),
            "备注": ColumnTally(name="备注", from_override=3),
        },
        manual=[("备注", "需人工核对许可")],
    )

    text = "\n".join(fill_exec._lines(spec, spec_report, False, STAMP))

    assert "`downloads` · int" in text
    assert "模板 `https://hf.co/{id}`" in text
    assert "常量 `在用`" in text
    assert "人工判读：需人工核对许可" in text
    assert "### 需人工判读（引擎不猜）" in text


def test_输入快照的指纹取前十六位(tmp_path: Path) -> None:
    """档案里记全量 sha256 没有用——没人会逐字比对，记前缀够核对了。"""
    text = "\n".join(fill_exec._lines(fill_spec(tmp_path), report(), False, STAMP))

    assert "4,096 字节" in text
    assert "sha256 `aaaaaaaaaaaaaaaa…`" in text


# ---------------------------------------------------------------- 失败的翻译


SPEC_TEXT = """
id: REQ-0001
title: 测试
requester: 测试者
version: 1
cadence: once
kind: backfill
fill:
  source:
    path: 清单.xlsx
    sheet: 数据
    header_row: 2
    key_column: URL
  output:
    name: 回填结果.xlsx
  columns:
    下载量:
      field: downloads
"""


def run_context(tmp_path: Path) -> RunContext:
    """一份最小可用的执行上下文：输入表真实存在，底库与目录都不必真有数据。"""
    book = tmp_path / "清单.xlsx"
    made = openpyxl.Workbook()
    sheet = made.active
    assert sheet is not None
    sheet.title = "数据"
    sheet.cell(row=2, column=1, value="URL")
    made.save(book)
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(SPEC_TEXT, encoding="utf-8")

    return RunContext(
        store=DatasetStore(tmp_path / "data"),
        spec=load(spec_path),
        staging=tmp_path / "out",
        # 执行体在失败用例里根本走不到查询，故不必造目录库。
        catalog=cast("GoldCatalog", None),
        source=SourceStamp(current_md5="md5", current_rows=0),
    )


def test_回填失败翻译成阻断而不是栈(tmp_path: Path, monkeypatch) -> None:
    """引擎的异常类型不能漏到档案层：漏出去，用户看到的是 traceback。

    `FillError` 说的是「这张表与 spec 对不上」，是给用户看的话；档案层若认识它，
    加第三种形态时又得回去改档案层——「只认 Payload」就成了空话。
    """

    def boom(*_args: object, **_kwargs: object) -> object:
        raise fill_exec.backfill.FillError("第 3 行不是 HuggingFace 数据集链接")

    monkeypatch.setattr(fill_exec.backfill, "run", boom)

    with pytest.raises(DeliveryBlocked) as caught:
        fill_exec.payload(run_context(tmp_path))

    assert "REQ-0001 执行失败" in str(caught.value)
    assert "第 3 行不是" in str(caught.value)


def test_缺fill段时阻断并点名是哪一单(tmp_path: Path) -> None:
    """`_validate` 本应挡住它；真漏到这里时，报错要能直接指向那一单。"""
    ctx = run_context(tmp_path)
    broken = RunContext(
        store=ctx.store,
        spec=replace(ctx.spec, fill=None),
        staging=ctx.staging,
        catalog=ctx.catalog,
        source=ctx.source,
    )

    with pytest.raises(DeliveryBlocked) as caught:
        fill_exec.payload(broken)

    assert "REQ-0001" in str(caught.value)
    assert "没有 fill 段" in str(caught.value)


if TYPE_CHECKING:
    from hf_data_crawl.query.gold import GoldCatalog
