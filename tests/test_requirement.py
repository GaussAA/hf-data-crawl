"""需求定义的行为测试。

三件事要守：**不合法的 spec 在受理时就被拦下**（而不是交付跑到一半）、
**编译产物是确定性的**（两次交付才能逐字节比对）、**值不可能变成注入**。
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import pytest
import yaml

from hf_data_crawl.requirement import (
    Cadence,
    Mode,
    SpecError,
    compile_sql,
    history_versions,
    load,
    skeleton,
    snapshot_history,
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
        'SELECT *\nFROM datasets\nWHERE "downloads" > 1000\nORDER BY "id" ASC'
    )


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

    assert sql.count('"id"') == 1


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
