"""表头纯核：列序、空列、重名，以及指纹与差异。

这里不 import openpyxl：`parse` 吃的是「单元格的值」，不是单元格本身。
开文件、选 sheet、拒公式都在 `test_sheet.py`。
"""

from __future__ import annotations

import pytest

from hf_data_crawl.delivery.header import (
    Header,
    HeaderError,
    header_change,
    header_digest,
    parse,
)


def test_按列序解析并跳过空列名() -> None:
    parsed = parse(["URL", "", None, "下载量", " 备注 "])

    assert isinstance(parsed, Header)
    assert parsed.names == ("URL", "下载量", "备注")
    assert parsed.columns == {"URL": 1, "下载量": 4, "备注": 5}


def test_列号从1开始且与列名一一对应() -> None:
    parsed = parse(["作者", "下载量"])

    assert [parsed.columns[name] for name in parsed.names] == [1, 2]


def test_重名列直接报错并指出两处位置() -> None:
    """两个「下载量」谁是谁，下游没有任何办法猜对；猜错的后果是写进错误的列。"""
    with pytest.raises(HeaderError, match="重名列 '下载量'.*第 2 列与第 5 列"):
        parse(["URL", "下载量", "名称", "", "下载量"])


def test_整行没有列名时报错() -> None:
    with pytest.raises(HeaderError, match="表头是空的"):
        parse([None, "", "  "])


def test_表头指纹认列名也认顺序() -> None:
    """只按列名集合算，挪个次序就看不出来了——而次序变了，人看表会串行。"""
    assert header_digest(("URL", "名称")) == header_digest(("URL", "名称"))
    assert header_digest(("URL", "名称")) != header_digest(("名称", "URL"))
    assert header_digest(("URL", "名称")) != header_digest(("URL", "名称", "下载量"))


def test_表头差异分加列减列与挪位() -> None:
    change = header_change(("URL", "名称", "下载量"), ("URL", "下载量", "名称", "新增"))
    assert change["added"] == ["新增"]
    assert change["removed"] == []
    assert change["reordered"] is True

    assert header_change(("a", "b"), ("a", "b")) == {
        "added": [],
        "removed": [],
        "reordered": False,
    }
