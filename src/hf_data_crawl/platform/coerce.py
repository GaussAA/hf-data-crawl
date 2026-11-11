"""把外部 JSON 值落成一列时的编码规则。

为什么要统一
------------
「一个 JSON 值怎么落进一列」是本项目里最高频的决策，也是两次事故的现场：
`private` 整列为空、`resourceGroup` 被 str() 写成单引号字面量。而它原本散在
store / card / sizes 三处——`_as_int` 逐字相同三份，`_as_text` 同名而异义
（一处 `str()`、一处逗号拼接）。

同一个名字在一处拒绝字符串布尔、在另一处接受，是最难查的那类不一致：不会报错，
只会让两个模块对同一字段产生不同的解释。

一条铁律
--------
**取不到就留 None，绝不猜。** `private: "true"` 是字符串不是布尔，猜成 True 会让
「是否受限」这件事失去可信度；`mainSize: "很大"` 更不该变成 0。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any


def as_int(value: Any) -> int | None:
    """只认整数与「整值浮点」。布尔不算——`True` 会被当成 1，那是猜。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return None


def as_float(value: Any) -> float | None:
    """趋势分取浮点：接口当前给整数，但不把「今天恰好是整数」当成契约。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def as_bool(value: Any) -> bool | None:
    """只认同真正的布尔。

    刻意不接受 "true"/"false"：`gated` 的 "manual"/"auto" 与 `private` 的
    true/false 是两种语义，混用会让「是否受限」无法区分。
    """
    return value if isinstance(value, bool) else None


def as_text(value: Any) -> str | None:
    """标量转文本，列表用逗号拼接（`license` 常见多值）。

    去空白后为空的一律留 None：`""` 与「没有」在下游统计里应当同义，
    否则字段覆盖率会被空串虚高。
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if item is not None]
        parts = [part for part in parts if part]
        return ",".join(parts) if parts else None
    text = str(value).strip()
    return text or None


def as_list(value: Any) -> list[str] | None:
    """标量包成单元素列表。空列表与缺失合并为 None：空列表不携带信息。"""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else None
    if isinstance(value, list):
        items = [str(item).strip() for item in value if item is not None]
        items = [item for item in items if item]
        return items or None
    return None


def as_json(value: Any) -> str | None:
    """复合结构编码为 JSON，标量走文本。

    **绝不用 `str()`**：那会产出单引号的 Python 字面量，之后 `json.loads` 必然失败。
    空结构返回 None——`{}` 和 `[]` 不携带信息。
    """
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True) if value else None
    return as_text(value)


def as_datetime(value: Any) -> datetime | None:
    """解析 HF 的 ISO 8601 时间戳（`2026-06-14T13:56:50.000Z`），统一为 UTC。

    为什么不原样留字符串：按时间排序、比较、算差是这两列的全部用途，而字符串
    比较只在格式永远规整时才等于时间序——上游哪天少写毫秒或换了时区表示，
    排序会静默错位，且没人会发现。实测 112 万条全部解析成功、零例外，
    说明这个格式是稳定的，可以放心转成真正的类型。
    """
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip())
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)
