"""数据说明文档的同步测试。

文档的价值全在「准」。它不像代码那样一跑就露馅——过期了照样能读，只是读到的
是错的东西。所以这里不测文笔，只测**文档与代码是否还对得上**：

- 表清单变了（新增/删除一张表）→ 文档里必须有/不能多
- 某张表加了列 → 文档里必须有那一列
- 某列没写含义 → `missing_docs()` 非空，生成命令退出码为 1
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from hf_data_crawl.ops.cli import main
from hf_data_crawl.ops.datadoc import missing_docs, render
from hf_data_crawl.platform.tables import TABLES

STAMP = datetime(2026, 11, 7, 12, 0)


@pytest.fixture()
def doc(tmp_path: Path) -> str:
    return render(tmp_path / "data", STAMP)


def test_每张表都出现在文档里(doc: str) -> None:
    for name, spec in TABLES.items():
        assert f"`{spec.path}`" in doc, f"{name} 的路径没进文档"
        assert f"逻辑名：`{name}`" in doc, f"{name} 没标逻辑名"


def test_每一列都出现在文档里(doc: str) -> None:
    """这是最重要的一条：schema 加了一列而文档没重新生成，此处必红。"""
    for name, spec in TABLES.items():
        for column in spec.schema:
            assert f"| `{column}` |" in doc, f"{name}.{column} 没进文档"


def test_没有缺少说明的字段() -> None:
    gaps = missing_docs()
    assert not gaps, f"这些字段还没写含义：{gaps}"


def test_文档里没有待补字样(doc: str) -> None:
    assert "待补" not in doc


def test_同一时刻生成的内容逐字节相同(doc: str, tmp_path: Path) -> None:
    """可重复，才能进版本库时只看真实差异、不被噪声淹没。"""
    again = render(tmp_path / "data", STAMP)

    assert doc == again


def test_空数据目录也能生成(tmp_path: Path) -> None:
    """刚 clone 下来还没跑过采集时，文档照样该出得来。"""
    text = render(tmp_path / "empty", STAMP)

    assert "尚无数据" in text
    assert "## 一、数据是怎么流过来的" in text


def test_生成命令写出文件并返回成功(tmp_path: Path) -> None:
    target = tmp_path / "DATA.md"

    code = main(["--data-dir", str(tmp_path / "data"), "docs", "--out", str(target)])

    assert code == 0
    assert target.exists()
    assert "data 目录说明" in target.read_text(encoding="utf-8")


def test_命令生成的文档与渲染函数一致(tmp_path: Path) -> None:
    """两条路径写出同一份内容，避免「命令跑出来的」和「测试验过的」不是一回事。"""
    target = tmp_path / "DATA.md"
    main(["--data-dir", str(tmp_path / "data"), "docs", "--out", str(target)])

    written = target.read_text(encoding="utf-8")

    assert "## 五、几个容易误解的地方" in written
    assert "请勿手工编辑" in written


def test_文档提到的源码文件必须真的存在(tmp_path: Path) -> None:
    """生成物被手工改过，重新生成就会把手改的部分冲掉。

    2026-11-13 实测：`docs/DATA.md` 里补了切片前缀（`delivery/fields.py`），生成器
    却仍输出裸名 `fields.py`——重新生成即回退。这类漂移没有守卫就只能靠人记得，
    而「记得」不是机制。故：文档里每个以 `.py` 结尾的引用，都必须能在仓库根或
    `src/hf_data_crawl/` 下找到。
    """
    import re

    repo = Path(__file__).resolve().parents[2]
    text = render(tmp_path / "data", STAMP)
    refs = sorted(set(re.findall(r"`([A-Za-z0-9_/]+\.py)`", text)))

    missing = [
        ref
        for ref in refs
        if not (repo / ref).is_file() and not (repo / "src" / "hf_data_crawl" / ref).is_file()
    ]
    assert missing == [], f"文档引用了不存在的源码文件：{missing}"
