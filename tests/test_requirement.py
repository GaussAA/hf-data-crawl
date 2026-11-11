"""需求定义的行为测试。

三件事要守：**不合法的 spec 在受理时就被拦下**（而不是交付跑到一半）、
**编译产物是确定性的**（两次交付才能逐字节比对）、**值不可能变成注入**。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import openpyxl
import pytest
import yaml

from hf_data_crawl.delivery.fields import DEFAULT_COLUMNS, projection_sql
from hf_data_crawl.delivery.requirement import (
    Cadence,
    Kind,
    Mode,
    SpecError,
    compile_sql,
    directory_name,
    history_versions,
    load,
    resolve_dir,
    skeleton,
    snapshot_history,
    spec_digest,
    version_conflict,
)


def write_spec(tmp_path: Path, **overrides: Any) -> Path:
    body: dict[str, Any] = {
        "id": "REQ-0001",
        "title": "高热度数据集",
        "requester": "张三",
        "version": 1,
        "cadence": "once",
        "criteria": [{"metric": "下载量", "op": ">", "value": 1000}],
    }
    body.update(overrides)
    path = tmp_path / "REQ-0001" / "spec.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")
    return path


def sql_of(tmp_path: Path, **overrides: Any) -> str:
    return compile_sql(load(write_spec(tmp_path, **overrides)))


# ---------------------------------------------------------------- 读取与校验


def test_骨架能原样读回来(tmp_path: Path) -> None:
    path = tmp_path / "REQ-0001" / "spec.yaml"
    path.parent.mkdir(parents=True)
    path.write_text(
        skeleton(request_id="REQ-0001", title="高热度数据集", requester="张三"),
        encoding="utf-8",
    )

    spec = load(path)

    assert spec.id == "REQ-0001"
    assert spec.cadence is Cadence.ONCE
    assert spec.mode is Mode.CRITERIA
    assert len(spec.criteria) == 2


@pytest.mark.parametrize("missing", ["id", "title", "requester"])
def test_缺必填项立刻报错(tmp_path: Path, missing: str) -> None:
    path = write_spec(tmp_path)
    body = yaml.safe_load(path.read_text(encoding="utf-8"))
    body.pop(missing)
    path.write_text(yaml.safe_dump(body, allow_unicode=True), encoding="utf-8")

    with pytest.raises(SpecError, match=missing):
        load(path)


def test_需求号格式不对报错(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="REQ-0001"):
        load(write_spec(tmp_path, id="REQ-1"))


def test_节奏写错报错并列出可选值(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="once"):
        load(write_spec(tmp_path, cadence="每小时"))


def test_criteria模式不给条件报错(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="mode: sql"):
        load(write_spec(tmp_path, criteria=[]))


def test_sql模式不给sql报错(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="必须给 sql"):
        load(write_spec(tmp_path, mode="sql"))


def test_未知词条在受理时就报错(tmp_path: Path) -> None:
    """等到编译才报，那时已经在跑交付了；受理时就该发现。"""
    with pytest.raises(SpecError, match="词表里没有"):
        load(write_spec(tmp_path, criteria=[{"metric": "热度", "op": ">", "value": 1}]))


def test_护栏自相矛盾报错(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="任何结果都过不去"):
        load(write_spec(tmp_path, guard={"min_rows": 100, "max_rows": 10}))


def test_版本号必须为正(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="version"):
        load(write_spec(tmp_path, version=0))


# ---------------------------------------------------------------- 编译


def test_单条件编译出只读查询(tmp_path: Path) -> None:
    assert sql_of(tmp_path, criteria=[{"metric": "下载量", "op": ">", "value": 1000}]) == (
        f'SELECT {projection_sql()}\nFROM datasets\nWHERE "downloads" > 1000\nORDER BY "id" ASC'
    )


def test_不指定字段时投默认契约而非全列(tmp_path: Path) -> None:
    """`SELECT *` 会把水印、抓取状态与近乎全空的补抓列一起倒给对方。"""
    sql = sql_of(tmp_path)

    assert "SELECT *" not in sql
    assert sql.startswith(f"SELECT {projection_sql()}\nFROM datasets")
    for column in DEFAULT_COLUMNS:
        assert f'"{column}"' in sql


def test_多条件用AND连接(tmp_path: Path) -> None:
    sql = sql_of(
        tmp_path,
        criteria=[
            {"metric": "下载量", "op": ">", "value": 1000},
            {"metric": "点赞数", "op": ">=", "value": 1000},
        ],
    )

    assert '"downloads" > 1000' in sql
    assert 'AND "likes" >= 1000' in sql


def test_词条的别名与字段名都编译成同一列(tmp_path: Path) -> None:
    by_term = sql_of(tmp_path, criteria=[{"metric": "累计下载量", "op": ">", "value": 1}])
    by_field = sql_of(tmp_path, criteria=[{"metric": "downloads_all_time", "op": ">", "value": 1}])

    assert by_term == by_field
    assert '"downloads_all_time"' in by_term


def test_未给排序时以id兜底保证确定性(tmp_path: Path) -> None:
    """没有唯一键兜底，两次交付的字节可能不同，sha256 比对就永远失败。"""
    assert sql_of(tmp_path).endswith('ORDER BY "id" ASC')


def test_显式排序仍追加id兜底(tmp_path: Path) -> None:
    sql = sql_of(tmp_path, order_by=[{"metric": "下载量", "direction": "desc"}])

    assert 'ORDER BY "downloads" DESC, "id" ASC' in sql


def test_排序已含id时不重复追加(tmp_path: Path) -> None:
    sql = sql_of(tmp_path, order_by=[{"metric": "id", "direction": "asc"}])

    # 默认投影里本来就有 `"id"`，不能再数全文出现次数；只看 ORDER BY 子句。
    assert sql.endswith('ORDER BY "id" ASC')
    assert sql.count("ORDER BY") == 1


def test_文本值里的单引号被转义(tmp_path: Path) -> None:
    sql = sql_of(tmp_path, criteria=[{"metric": "作者", "op": "=", "value": "o'brien"}])

    assert "'o''brien'" in sql


def test_布尔条件接受两种写法(tmp_path: Path) -> None:
    assert "TRUE" in sql_of(tmp_path, criteria=[{"metric": "是否私有", "op": "=", "value": True}])
    assert "FALSE" in sql_of(
        tmp_path, criteria=[{"metric": "是否私有", "op": "=", "value": "false"}]
    )


def test_日期条件编成DATE字面量(tmp_path: Path) -> None:
    sql = sql_of(tmp_path, criteria=[{"metric": "创建日期", "op": ">=", "value": date(2026, 1, 1)}])

    assert "DATE '2026-01-01'" in sql


def test_数字条件给字符串报错(tmp_path: Path) -> None:
    spec = load(write_spec(tmp_path, criteria=[{"metric": "下载量", "op": ">", "value": "很多"}]))

    with pytest.raises(SpecError, match="需要一?个?数字"):
        compile_sql(spec)


def test_文本列上用大于号在编译期就被拒(tmp_path: Path) -> None:
    """DuckDB 允许文本比较，但那是字典序，几乎从来不是提问者要的。"""
    spec = load(write_spec(tmp_path, criteria=[{"metric": "作者", "op": ">", "value": "a"}]))

    with pytest.raises(SpecError, match="文本"):
        compile_sql(spec)


def test_输出列可写词条也可写列名(tmp_path: Path) -> None:
    sql = sql_of(tmp_path, output={"columns": ["id", "下载量", "点赞数"]})

    assert sql.startswith('SELECT "id", "downloads", "likes"')


def test_sql模式原样使用并去掉尾分号(tmp_path: Path) -> None:
    spec = load(write_spec(tmp_path, mode="sql", criteria=[], sql="SELECT id FROM datasets;\n"))

    assert compile_sql(spec) == "SELECT id FROM datasets"


# ---------------------------------------------------------------- 历史快照


def test_快照复制原文件字节且幂等(tmp_path: Path) -> None:
    path = write_spec(tmp_path)
    original = path.read_bytes()

    first = snapshot_history(path, 1)
    path.write_text("id: REQ-0001\n", encoding="utf-8")
    second = snapshot_history(path, 1)

    assert first == second
    assert first.read_bytes() == original


def test_快照版本可列举(tmp_path: Path) -> None:
    path = write_spec(tmp_path)
    snapshot_history(path, 1)
    snapshot_history(path, 2)

    assert history_versions(path) == [1, 2]


# ---------------------------------------------------------------- 统一的「变」


def test_未知形态在受理期报出可选值(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="query"):
        load(write_spec(tmp_path, kind="对账"))


def test_重复键在读入处被拒(tmp_path: Path) -> None:
    """两类需求共用一份 spec，这道防线不能只挂在回填口径体上。"""
    path = tmp_path / "REQ-0001" / "spec.yaml"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "id: REQ-0001\ntitle: 第一次\nrequester: 张三\ntitle: 又改了一次\n"
        "criteria: [{metric: 下载量, op: '>', value: 1}]\n",
        encoding="utf-8",
    )

    with pytest.raises(SpecError, match="重复的键"):
        load(path)


def test_回填骨架能原样读回(tmp_path: Path) -> None:
    """骨架建完就得按真规矩回读通过——输入快照必须在需求目录里，否则新建即自毁。"""
    folder = tmp_path / "REQ-0001"
    (folder / "input").mkdir(parents=True)
    book = folder / "input" / "in.xlsx"
    openpyxl.Workbook().save(book)
    path = folder / "spec.yaml"
    path.write_text(
        skeleton(
            request_id="REQ-0001",
            title="清单补全",
            requester="大帅",
            kind=Kind.BACKFILL,
            source_path=book.as_posix(),
        ),
        encoding="utf-8",
    )

    spec = load(path)

    assert spec.kind is Kind.BACKFILL
    assert spec.criteria == ()
    assert spec.fill is not None
    assert len(spec.fill.columns) == 4
    assert spec.guard.min_rows == 1


def test_两种骨架的公共头一致(tmp_path: Path) -> None:
    """变的只有 kind 与口径体；节奏、护栏、标疑、编号都得是同一套。"""
    book = tmp_path / "in.xlsx"
    openpyxl.Workbook().save(book)
    query = skeleton(request_id="REQ-0001", title="x", requester="y")
    fill = skeleton(
        request_id="REQ-0001",
        title="x",
        requester="y",
        kind=Kind.BACKFILL,
        source_path=book.as_posix(),
    )

    for key in ("id: REQ-0001", "requester: y", "version: 1", "cadence: once", "guard:", "alert:"):
        assert key in query
        assert key in fill
    assert "kind: backfill" in fill
    assert "kind:" not in query


# ---------------------------------------------------------------- 语义化目录


def test_目录名带编号形态与标题且不含路径分隔符() -> None:
    name = directory_name("REQ-0002", "具身感知 数据集/信息补充", Kind.BACKFILL)

    assert name.startswith("REQ-0002-backfill-")
    assert "具身感知" in name
    assert not any(bad in name for bad in ("/", "\\", " ", ":", "*"))


def test_按编号解析语义目录(tmp_path: Path) -> None:
    root = tmp_path / "req"
    folder = root / directory_name("REQ-0002", "清单补全", Kind.BACKFILL)
    folder.mkdir(parents=True)
    (folder / "spec.yaml").write_text("id: REQ-0002\n", encoding="utf-8")

    assert resolve_dir(root, "REQ-0002") == folder


def test_旧的纯编号目录仍可解析(tmp_path: Path) -> None:
    """兼容存量需求，不必为了改名搬动交付台账。"""
    root = tmp_path / "req"
    folder = root / "REQ-0002"
    folder.mkdir(parents=True)
    (folder / "spec.yaml").write_text("id: REQ-0002\n", encoding="utf-8")

    assert resolve_dir(root, "REQ-0002") == folder


def test_同号多目录要报歧义而不是猜一个(tmp_path: Path) -> None:
    root = tmp_path / "req"
    for suffix in ("backfill-甲", "backfill-乙"):
        folder = root / f"REQ-0002-{suffix}"
        folder.mkdir(parents=True)
        (folder / "spec.yaml").write_text("id: REQ-0002\n", encoding="utf-8")

    with pytest.raises(SpecError, match="多个目录"):
        resolve_dir(root, "REQ-0002")


def test_找不到需求时报得清位置(tmp_path: Path) -> None:
    with pytest.raises(SpecError, match="找不到需求"):
        resolve_dir(tmp_path / "req", "REQ-9999")


# ---------------------------------------------------------------- 口径变更


def test_指纹只认口径不认注释与展示字段(tmp_path: Path) -> None:
    """改注释、补一个默认值、改标题都不算一版变更——否则「改标题不搬目录」自相矛盾。"""
    path = write_spec(tmp_path)
    before = spec_digest(path)

    text = path.read_text(encoding="utf-8")
    text = "# 补一行注释\n" + text.replace("cadence: once", "kind: query\ncadence: once")
    text = text.replace("高热度数据集", "高热度数据集（改名）")
    path.write_text(text, encoding="utf-8")

    assert spec_digest(path) == before


def test_改条件即改指纹(tmp_path: Path) -> None:
    path = write_spec(tmp_path)
    before = spec_digest(path)

    write_spec(tmp_path, criteria=[{"metric": "下载量", "op": ">", "value": 800}])

    assert spec_digest(path) != before


def test_版本号被别的口径占用时要拦住(tmp_path: Path) -> None:
    path = write_spec(tmp_path)
    snapshot_history(path, 1)  # v1 记的是「下载量 > 1000」
    assert version_conflict(path, 1) is None

    write_spec(tmp_path, criteria=[{"metric": "下载量", "op": ">", "value": 800}])
    conflict = version_conflict(path, 1)
    assert conflict is not None
    assert "版本号不能复用" in conflict

    # 升版即解：同一个内容换个号，不再与历史冲突。
    write_spec(tmp_path, version=2, criteria=[{"metric": "下载量", "op": ">", "value": 800}])
    assert version_conflict(path, 2) is None


def test_历史快照按需求目录解析相对路径(tmp_path: Path) -> None:
    """回填快照躺在 history/ 下；按它自己的父目录解析，`input/…` 就成了 `history/input/…`。"""
    folder = tmp_path / "REQ-0002"
    (folder / "input").mkdir(parents=True)
    openpyxl.Workbook().save(folder / "input" / "清单.xlsx")
    path = folder / "spec.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "id": "REQ-0002",
                "title": "清单补全",
                "requester": "大帅",
                "version": 1,
                "kind": "backfill",
                "fill": {
                    "source": {"path": "input/清单.xlsx", "header_row": 1, "key_column": "URL"},
                    "output": {"name": "已补全.xlsx"},
                    "columns": {"下载量": {"field": "downloads"}},
                },
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    snapshot = snapshot_history(path, 1)

    assert load(snapshot, base_dir=folder).fill is not None
    with pytest.raises(SpecError, match="输入表不存在|不是文件"):
        load(snapshot)
