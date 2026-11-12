"""列编码规则的测试。

核心是一条：**取不到就留 None，绝不猜**。

`private: "true"` 是字符串不是布尔，猜成 True 会让「是否受限」失去可信度；
这正是 2026-11-04 那次 `private` 整列为空的邻接事故——一次是没请求，一次是
请求到了却按错误规则解释，两者都不会报错。
"""

from __future__ import annotations

import json

import pytest

from hf_data_crawl.platform.coerce import as_bool, as_float, as_int, as_json, as_list, as_text

# ---------------------------------------------------------------- 整数


@pytest.mark.parametrize("value", [42, 0, -7])
def test_整数原样保留(value: int) -> None:
    assert as_int(value) == value


def test_整值浮点收为整数() -> None:
    """JSON 里 483.0 与 483 是同一个数，不该因为写法不同就丢掉。"""
    assert as_int(483.0) == 483


def test_布尔不算整数() -> None:
    """True 会被 Python 当成 1，但数据集条数不可能是「是」。"""
    assert as_int(True) is None


@pytest.mark.parametrize("value", [1.5, "很大", None, [], {}])
def test_非整数一律留空(value: object) -> None:
    assert as_int(value) is None


# ---------------------------------------------------------------- 布尔


def test_只认同真正的布尔() -> None:
    assert as_bool(True) is True
    assert as_bool(False) is False


@pytest.mark.parametrize("value", ["true", "false", 1, 0, None])
def test_字符串与数字都不当布尔(value: object) -> None:
    """`gated` 的 "manual"/"auto" 与 `private` 的 true/false 是两种语义，混用即失真。"""
    assert as_bool(value) is None


# ---------------------------------------------------------------- 浮点


def test_趋势分取浮点() -> None:
    assert as_float(483) == 483.0
    assert as_float(1.5) == 1.5


def test_布尔不算浮点() -> None:
    assert as_float(True) is None


# ---------------------------------------------------------------- 文本


def test_列表用逗号拼接() -> None:
    """`license` 常见多值，直接 str() 会得到 "['mit', 'cc0']"。"""
    assert as_text(["mit", "cc0"]) == "mit,cc0"


def test_文本去空白且空串留空() -> None:
    assert as_text("  hello  ") == "hello"
    assert as_text("   ") is None
    assert as_text("") is None


def test_布尔转小写文本() -> None:
    assert as_text(True) == "true"
    assert as_text(False) == "false"


def test_缺失留空() -> None:
    assert as_text(None) is None


# ---------------------------------------------------------------- 列表


def test_标量包成单元素列表() -> None:
    assert as_list("en") == ["en"]


def test_空列表与缺失同义() -> None:
    """空列表不携带信息，与「没有」在下游统计里应当一致。"""
    assert as_list([]) is None
    assert as_list(None) is None
    assert as_list(["  ", None]) is None


# ---------------------------------------------------------------- 复合结构


def test_复合结构编码为可再解析的json() -> None:
    """绝不用 str()：那会产出单引号字面量，json.loads 必然失败。"""
    encoded = as_json({"id": "rg-1", "name": "默认"})

    assert encoded is not None
    assert json.loads(encoded) == {"id": "rg-1", "name": "默认"}


def test_空结构留空() -> None:
    assert as_json({}) is None
    assert as_json([]) is None


def test_标量走文本() -> None:
    assert as_json("plain") == "plain"
