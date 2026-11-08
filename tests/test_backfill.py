"""表格回填引擎的行为测试。

真正要守的不是「能填上」，而是四条边界：**不猜、不改原件、不覆盖已有内容、
同样的输入给同样的字节**。这四条里任何一条破例，交付出去的表都会在几个月后
变成一份说不清来历的文件。
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any, cast

import openpyxl
import polars as pl
import pytest

from hf_data_crawl.backfill import (
    FillError,
    FillSpec,
    parse_reference,
    render,
    run,
)
from hf_data_crawl.delivery import SourceStamp
from hf_data_crawl.gold import GoldCatalog
from hf_data_crawl.requirement import SpecError
from hf_data_crawl.requirement import load as load_request
from hf_data_crawl.store import DatasetStore

HEADERS = ["URL", "数据集名称", "下载量", "数据模态", "论文URL", "训练阶段", "备注"]


def load_spec(path: Path) -> FillSpec:
    """从**完整 REQ spec** 里取出回填口径体。

    不再有「独立的 fill spec 文件」这种第二套入口：测试也走需求层统一读取，
    这样 spec 校验、重复键拒绝、相对路径基准都与真实运行时一模一样。
    """
    spec = load_request(path)
    assert spec.fill is not None
    return spec.fill


class FakeCatalog:
    """只回一张定表的伪查询口。真视图要 72 列百万行，测边界不必背这个包袱。"""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def run(self, sql: str) -> pl.DataFrame:  # noqa: ARG002 - 桩不解析 SQL
        return pl.DataFrame(self.rows)


STAMP = SourceStamp(current_md5="deadbeef", current_rows=10, newest_snapshot_date=date(2026, 11, 8))

ROW = {
    "id": "acme/ds-1",
    "author": "acme",
    "downloads": 1234,
    "likes": 7,
    "main_size": 2_500_000_000,
    "modalities": ["image", "text"],
    "languages": ["en"],
    "task_categories": ["robotics"],
    "arxiv_ids": ["2501.00001"],
    "created_at": datetime(2025, 3, 18, 6, 0),
    "last_modified": datetime(2026, 3, 6, 6, 0),
    "description": "第一行简介\n\n\t\n第二行简介",
    "has_readme": True,
    "training_stage": None,
}


def catalogue(rows: list[dict[str, Any]]) -> GoldCatalog:
    """把桩按真类型交出去。

    测试替身不继承 `GoldCatalog`（那要背 72 列百万行的视图），而调用方只用它的
    `.run()`。与其在每个调用点撒 `type: ignore`，不如在造桩这一处显式 cast——
    豁免只出现一次，且看得出为什么。
    """
    return cast(GoldCatalog, FakeCatalog(rows))


def run_it(spec: Any, rows: list[dict[str, Any]], *, tmp_path: Path, dry_run: bool = False) -> Any:
    """跑一遍回填。`catalog` 与 `source` 都注入后，`store` 完全不会被读到。

    `target_dir` 是档案层给的暂存目录；测试里直接用 tmp_path，于是产物仍落在
    各用例原本断言的那个路径上。
    """
    return run(
        DatasetStore(tmp_path / "unused"),
        spec,
        target_dir=tmp_path,
        catalog=catalogue(rows),
        source=STAMP,
        dry_run=dry_run,
    )


def build_book(path: Path, keys: list[str], *, header_row: int = 2) -> Path:
    """造一张与原表同构的书：第 header_row 行是列名，之上是阶段分组。"""
    book = openpyxl.Workbook()
    sheet = book.active
    assert sheet is not None  # openpyxl 新建工作簿必有活动表，类型上却可能是 None
    sheet.title = "数据"
    if header_row > 1:
        sheet.cell(row=1, column=1, value="基础信息")
    for column, name in enumerate(HEADERS, start=1):
        sheet.cell(row=header_row, column=column, value=name)
    for offset, key in enumerate(keys):
        sheet.cell(row=header_row + 1 + offset, column=1, value=key)
    path.parent.mkdir(parents=True, exist_ok=True)
    book.save(path)
    return path


def write_spec(path: Path, *, book: Path, out: Path, body: str) -> Path:
    """造一份完整的 REQ spec，回填口径体嵌在 `fill:` 下。

    `out` 既是档案内文件名，也是另存落点；`run_it` 把它的父目录当暂存目录，
    于是端到端用例仍能在老地方找到产物。
    """
    indented = "\n".join(f"  {line}" if line else "" for line in body.splitlines())
    path.write_text(
        f"""
id: REQ-0001
title: 测试
requester: 测试者
version: 1
cadence: once
kind: backfill
fill:
  source:
    path: {book.as_posix()}
    sheet: 数据
    header_row: 2
    key_column: URL
  output:
    name: {out.name}
    copy_to: {out.as_posix()}
{indented}
""",
        encoding="utf-8",
    )
    return path


# ---------------------------------------------------------------- URL 解析


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://huggingface.co/datasets/acme/ds-1", "acme/ds-1"),
        ("https://huggingface.co/datasets/acme/ds-1/", "acme/ds-1"),
        ("https://huggingface.co/datasets/acme/ds-1/tree/main", "acme/ds-1"),
        ("https://huggingface.co/datasets/acme/ds-1?config=default", "acme/ds-1"),
        ("https://hf.co/datasets/acme/ds-1", "acme/ds-1"),
        ("acme/ds-1", "acme/ds-1"),
        ("  acme/ds-1  ", "acme/ds-1"),
    ],
)
def test_各种链接写法都能解析出标识(raw: str, expected: str) -> None:
    assert parse_reference(raw) == expected


def test_标识在第一个斜杠处截断会切掉名字() -> None:
    """id 本身就是「作者/名称」两段；在首个斜杠截断会得到 `acme`，
    看着像解析成功，实际每一行都匹配不到。"""
    assert parse_reference("https://huggingface.co/datasets/nvidia/PhysicalAI-GR00T/tree/main") == (
        "nvidia/PhysicalAI-GR00T"
    )


@pytest.mark.parametrize(
    "raw", ["", "   ", "acme", "https://example.com/acme/ds-1", "acme/ds/extra"]
)
def test_不是数据集的写法一律报错(raw: str) -> None:
    with pytest.raises(FillError):
        parse_reference(raw)


# ---------------------------------------------------------------- spec 校验


def test_输出不能与输入相同(tmp_path: Path) -> None:
    """补全是产出，不是就地修改：原件是唯一可回退的凭据。"""
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    spec = write_spec(
        tmp_path / "spec.yaml", book=book, out=book, body='columns:\n  "下载量": {field: downloads}'
    )

    with pytest.raises(SpecError, match="不能与 source 相同"):
        load_spec(spec)


def test_一列只能有一种取值方式(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    spec = write_spec(
        tmp_path / "spec.yaml",
        book=book,
        out=tmp_path / "out.xlsx",
        body='columns:\n  "下载量": {field: downloads, const: 1}',
    )

    with pytest.raises(SpecError, match="之一"):
        load_spec(spec)


def test_同一列写两次在读入处就被拒(tmp_path: Path) -> None:
    """两类需求共用一份 spec 后，重复键防线也跟着上移到需求层。"""
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    spec = write_spec(
        tmp_path / "spec.yaml",
        book=book,
        out=tmp_path / "out.xlsx",
        body='columns:\n  "下载量": {field: downloads}\n  "下载量": {const: 1}',
    )

    with pytest.raises(SpecError, match="重复的键"):
        load_spec(spec)


def test_空columns要报错(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    spec = write_spec(
        tmp_path / "spec.yaml", book=book, out=tmp_path / "out.xlsx", body="columns: {}"
    )

    with pytest.raises(SpecError, match="columns 为空"):
        load_spec(spec)


def test_输入表不存在要报错(tmp_path: Path) -> None:
    """受理期就该发现：等到执行期才炸，人已经把这份需求当成建好了。"""
    spec = tmp_path / "spec.yaml"
    spec.write_text(
        """
id: REQ-0001
title: 测试
requester: 测试者
kind: backfill
fill:
  source: {path: nope.xlsx, header_row: 2, key_column: URL}
  output: {name: out.xlsx}
  columns: {"下载量": {field: downloads}}
""",
        encoding="utf-8",
    )

    with pytest.raises(SpecError, match="输入表不存在"):
        load_spec(spec)


# ---------------------------------------------------------------- 取值规则


def spec_of(tmp_path: Path, body: str) -> Any:
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    return load_spec(
        write_spec(tmp_path / "spec.yaml", book=book, out=tmp_path / "out.xlsx", body=body)
    )


def test_列表字段按顿号拼接并中文化(tmp_path: Path) -> None:
    spec = spec_of(
        tmp_path,
        'columns:\n  "数据模态": {field: modalities, join: "、", translate: true}',
    )

    assert render(spec.columns[0], ROW, spec) == "图像、文本"


def test_字节按十进制折GB(tmp_path: Path) -> None:
    """与 HF 页面、README 自述的 TB 数同一口径；用 1024³ 会系统性偏小 7%。"""
    spec = spec_of(tmp_path, 'columns:\n  "数据模态": {field: main_size, format: gb}')

    assert render(spec.columns[0], ROW, spec) == 2.5


def test_简介压平但不改写措辞(tmp_path: Path) -> None:
    spec = spec_of(tmp_path, 'columns:\n  "数据模态": {field: description, format: prose}')

    assert render(spec.columns[0], ROW, spec) == "第一行简介\n第二行简介"


def test_模板占位符缺值就整格留空(tmp_path: Path) -> None:
    """拼出半截链接比空着更坏：空着有人会来问，半截会被当成真的。"""
    spec = spec_of(
        tmp_path, 'columns:\n  "论文URL": {template: "https://arxiv.org/abs/{arxiv_ids}"}'
    )
    row = {**ROW, "arxiv_ids": []}

    assert render(spec.columns[0], row, spec) is None


def test_模板取列表首项(tmp_path: Path) -> None:
    spec = spec_of(
        tmp_path, 'columns:\n  "论文URL": {template: "https://arxiv.org/abs/{arxiv_ids}"}'
    )

    assert render(spec.columns[0], ROW, spec) == "https://arxiv.org/abs/2501.00001"


def test_底库为空算不出值而非空串(tmp_path: Path) -> None:
    """None 是「不知道」，空串是「确实是空」——两者混同会让下游判断失真。"""
    spec = spec_of(tmp_path, 'columns:\n  "训练阶段": {field: training_stage}')

    assert render(spec.columns[0], ROW, spec) is None


# ---------------------------------------------------------------- 端到端


def test_只填空格不覆盖已有内容(tmp_path: Path) -> None:
    """人家手填的可能比底库新，也可能有底库没有的判断。"""
    book = build_book(tmp_path / "in.xlsx", ["https://huggingface.co/datasets/acme/ds-1"])
    prepared = openpyxl.load_workbook(book)
    prepared["数据"].cell(row=3, column=3, value=999)  # 「下载量」已有人填
    prepared.save(book)

    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=tmp_path / "out.xlsx",
            body='columns:\n  "下载量": {field: downloads}\n  "备注": {const: 引擎填的}',
        )
    )
    report = run_it(spec, [ROW], tmp_path=tmp_path)

    out = openpyxl.load_workbook(tmp_path / "out.xlsx")["数据"]
    assert out.cell(row=3, column=3).value == 999
    assert out.cell(row=3, column=7).value == "引擎填的"
    assert report.tallies["下载量"].kept == 1


def test_未命中底库的行整行留空并点名(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["acme/known", "acme/unknown"])
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=tmp_path / "out.xlsx",
            body='columns:\n  "下载量": {field: downloads}',
        )
    )

    report = run_it(spec, [ROW | {"id": "acme/known"}], tmp_path=tmp_path)

    out = openpyxl.load_workbook(tmp_path / "out.xlsx")["数据"]
    assert out.cell(row=3, column=3).value == 1234
    assert out.cell(row=4, column=3).value is None
    assert "acme/unknown" in report.missing_ids


def test_坏链接不阻塞整批(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["https://example.com/x", "acme/known"])
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=tmp_path / "out.xlsx",
            body='columns:\n  "下载量": {field: downloads}',
        )
    )

    report = run_it(spec, [ROW | {"id": "acme/known"}], tmp_path=tmp_path)

    out = openpyxl.load_workbook(tmp_path / "out.xlsx")["数据"]
    assert out.cell(row=4, column=3).value == 1234
    assert "https://example.com/x" in report.missing_ids


def test_未声明的列一律不碰并在报告里点名(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=tmp_path / "out.xlsx",
            body='columns:\n  "下载量": {field: downloads}',
        )
    )

    report = run_it(spec, [ROW], tmp_path=tmp_path)

    assert "训练阶段" in report.undeclared
    assert "备注" in report.undeclared


def test_人工判读取自overrides并计数(tmp_path: Path) -> None:
    """引擎不猜；判读结果以数据形式进来，写没写、来自哪里都要能说清。"""
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    (tmp_path / "ov.json").write_text('{"acme/ds-1": {"训练阶段": "微调"}}', encoding="utf-8")
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=tmp_path / "out.xlsx",
            body='overrides: ov.json\ncolumns:\n  "训练阶段": {manual: 须读 README}',
        )
    )

    report = run_it(spec, [ROW], tmp_path=tmp_path)

    out = openpyxl.load_workbook(tmp_path / "out.xlsx")["数据"]
    assert out.cell(row=3, column=6).value == "微调"
    assert report.tallies["训练阶段"].from_override == 1
    assert report.manual == [("训练阶段", "须读 README")]


def test_没有overrides时判读列留空而非编值(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=tmp_path / "out.xlsx",
            body='columns:\n  "训练阶段": {manual: 须读 README}',
        )
    )

    report = run_it(spec, [ROW], tmp_path=tmp_path)

    assert openpyxl.load_workbook(tmp_path / "out.xlsx")["数据"].cell(row=3, column=6).value is None
    assert report.tallies["训练阶段"].empty == 1


def test_预演不写盘也不留产物(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    out = tmp_path / "out.xlsx"
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=out,
            body='columns:\n  "下载量": {field: downloads}',
        )
    )

    report = run_it(spec, [ROW], tmp_path=tmp_path, dry_run=True)

    assert not out.exists()
    assert report.output is None
    assert report.tallies["下载量"].written == 1


def test_两次运行产物逐字节相同(tmp_path: Path) -> None:
    """同输入同底库必须同字节：否则「这次和上次一样吗」只能靠人肉比对。"""
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    out = tmp_path / "out.xlsx"
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=out,
            body='columns:\n  "下载量": {field: downloads}\n  "数据模态": {field: modalities, join: "、" }',
        )
    )

    run_it(spec, [ROW], tmp_path=tmp_path)
    first = out.read_bytes()
    run_it(spec, [ROW], tmp_path=tmp_path)

    assert out.read_bytes() == first


def test_说明页列出判读列与未命中(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1", "acme/missing"])
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=tmp_path / "out.xlsx",
            body='columns:\n  "训练阶段": {manual: 须读 README}',
        )
    )

    run_it(spec, [ROW], tmp_path=tmp_path)

    note = openpyxl.load_workbook(tmp_path / "out.xlsx")["补全说明"]
    text = "\n".join(str(cell.value) for row in note.iter_rows() for cell in row if cell.value)
    assert "留空 · 训练阶段" in text
    assert "acme/missing" in text


def test_表里没有声明的列要报错而不是静默跳过(tmp_path: Path) -> None:
    book = build_book(tmp_path / "in.xlsx", ["acme/ds-1"])
    spec = load_spec(
        write_spec(
            tmp_path / "spec.yaml",
            book=book,
            out=tmp_path / "out.xlsx",
            body='columns:\n  "不存在的列": {field: downloads}',
        )
    )

    with pytest.raises(FillError, match="表里没有"):
        run_it(spec, [ROW], tmp_path=tmp_path)
