"""受理用例的直接测试。

CLI 那组验的是「命令说得对不对」，这里验的是「用例本身」：返回值结构、失败以
异常表达、时刻可注入。绕过 argparse 直接调 `create_request`，因为它就是模块的
接口面——命令层哪天换掉，这些断言不该跟着改。
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import openpyxl
import pytest

from hf_data_crawl.delivery.intake import CreatedRequest, IntakeError, create_request
from hf_data_crawl.delivery.requirement import Kind, load, read_status, resolve_dir


def book_at(path: Path) -> Path:
    openpyxl.Workbook().save(path)
    return path


def test_建好的需求返回目录与形态(tmp_path: Path) -> None:
    """返回值要能直接告诉调用方「建在哪、哪种形态、有没有快照」，不必回头再扫目录。"""
    root = tmp_path / "req"

    created = create_request(
        root, request_id="REQ-0001", title="高热度数据集", requester="张三", kind=Kind.QUERY
    )

    assert isinstance(created, CreatedRequest)
    assert created.folder == resolve_dir(root, "REQ-0001")
    assert created.kind is Kind.QUERY
    assert created.snapshot is None


def test_回填型把输入表复制进来并返回快照路径(tmp_path: Path) -> None:
    """取材一次、取完即断：返回的路径就是今后唯一的输入来源。"""
    source = book_at(tmp_path / "in.xlsx")

    created = create_request(
        tmp_path / "req",
        request_id="REQ-0002",
        title="清单补全",
        requester="大帅",
        kind=Kind.BACKFILL,
        source=str(source),
    )

    assert created.snapshot is not None
    assert created.snapshot == created.folder / "input" / "in.xlsx"
    assert created.snapshot.read_bytes() == source.read_bytes()


def test_重复编号以异常表达而不是静默返回(tmp_path: Path) -> None:
    """失败是一个事实，调用方必须处理；返回 0 会让批量导入把它当成功。"""
    root = tmp_path / "req"
    create_request(root, request_id="REQ-0003", title="甲", requester="张三")

    with pytest.raises(IntakeError, match="已有目录"):
        create_request(root, request_id="REQ-0003", title="乙", requester="李四")


def test_表头行只对回填型有意义(tmp_path: Path) -> None:
    with pytest.raises(IntakeError, match="只对 kind=backfill"):
        create_request(
            tmp_path / "req", request_id="REQ-0004", title="甲", requester="张三", header_row=2
        )


def test_回填缺输入表时不留半成品(tmp_path: Path) -> None:
    """建到一半失败要整目录收走，否则下次会被判重挡住，人还得手工删。"""
    root = tmp_path / "req"

    with pytest.raises(IntakeError, match="需要 --source"):
        create_request(
            root, request_id="REQ-0005", title="甲", requester="张三", kind=Kind.BACKFILL
        )

    assert not root.is_dir() or list(root.glob("REQ-0005*")) == []


def test_样例标记用注入的时刻(tmp_path: Path) -> None:
    """`now` 是为了让「标在什么时候」可断言；不注入就只能比对一个区间。"""
    moment = datetime(2026, 11, 18, 23, 0, tzinfo=UTC)

    created = create_request(
        tmp_path / "req",
        request_id="REQ-0006",
        title="夹具",
        requester="张三",
        sample=True,
        now=moment,
    )

    status = read_status(created.folder)
    assert status["sample"] is True
    assert status["sample_marked_at"] == moment.isoformat(timespec="seconds")


# ---------------------------------------------------------------- 骨架里的 sheet 名


def named_book(path: Path, sheet_name: str) -> Path:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    assert sheet is not None
    sheet.title = sheet_name
    workbook.save(path)
    return path


def test_回填型骨架的sheet名取自源表(tmp_path: Path) -> None:
    """写死占位会让 `request input` 读不到旧表头，字段差异只剩新表那一半。

    受理时文件就在手边，读一次索引只要几毫秒；放到交付时再发现，人已经离开
    现场，还得回去翻对方给的原表。
    """
    source = named_book(tmp_path / "清单.xlsx", "对方给的数据")

    created = create_request(
        tmp_path / "req",
        request_id="REQ-0007",
        title="清单补全",
        requester="大帅",
        kind=Kind.BACKFILL,
        source=str(source),
    )

    spec = (created.folder / "spec.yaml").read_text(encoding="utf-8")
    assert 'sheet: "对方给的数据"' in spec
    assert "Sheet1" not in spec


def test_源表读不出来时当场失败且不留目录(tmp_path: Path) -> None:
    """留一个读不出来的源表在需求目录里，代价是几天后才在交付时炸。

    这条还钉住顺序：读 sheet 名发生在建目录**之前**，所以失败时连回滚都不必。
    """
    broken = tmp_path / "坏表.xlsx"
    broken.write_text("这不是工作簿", encoding="utf-8")
    root = tmp_path / "req"

    with pytest.raises(IntakeError, match="读不出输入表的 sheet 名") as caught:
        create_request(
            root,
            request_id="REQ-0008",
            title="清单补全",
            requester="大帅",
            kind=Kind.BACKFILL,
            source=str(broken),
        )

    assert "坏表.xlsx" in str(caught.value)
    assert not root.exists() or list(root.iterdir()) == []


def test_sheet名里有井号也能原样读回(tmp_path: Path) -> None:
    """页名是对方给的任意字符串，而 `#` 在 Excel 里合法、在 YAML 里却是注释开头。

    裸写的话 `sheet: 数据 # 2026` 会被读成「数据」，不报错——骨架里就静静躺着
    一个错的页名，直到 `request input` 才说「没有这个 sheet」。这条钉住加引号。
    """
    source = named_book(tmp_path / "清单.xlsx", "数据 # 2026")

    created = create_request(
        tmp_path / "req",
        request_id="REQ-0009",
        title="清单补全",
        requester="大帅",
        kind=Kind.BACKFILL,
        source=str(source),
    )

    spec = load(created.folder / "spec.yaml")

    assert spec.fill is not None
    assert spec.fill.source.sheet == "数据 # 2026"
