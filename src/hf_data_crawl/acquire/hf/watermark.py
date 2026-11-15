"""水位线机制：基于时间戳的增量停止判断。

为什么需要它
------------
原 watermark 基于"连续 N 页无新 id"判定，存在严重漏采风险：
假设今天 00:01 启动 collect，HF 上有 10 个今天新上架的库（lastModified 都是今天）
排在列表最前面，连续 3 页都没有"昨天晚上更新的库"，watermark 提前触发。
但实际上昨天 23:58 更新的库还没扫到（它们的 lastModified 排在今天的新库后面）。

新机制：基于 lastModified 时间戳判断。列表按 lastModified 倒序翻页，只有当页内
最老的有效 lastModified **严格早于**上次完成运行时记录的最新值时，才认为已经越过
旧数据边界；整页都晚于水位线时仍在未扫过的区间内，必须继续翻页。水位线在整轮里
固定不动，本轮新记录只用于决定下一轮的边界。

异常值处理
----------
GLOSSARY 43 条记录了"lastModified 存在 2028 年异常值"。如果持久化水位线是 2028 年，
下次运行会永久停止（所有正常记录都早于 2028）。因此：

1. 记录水位线时，过滤掉"当前时间 + 7 天"之后的异常值
2. 判定时，对异常值也做同样过滤
3. 如果一页内全是异常值，视为该页无有效记录、不更新水位线

折衷
----
如果 HF 修复了异常时间戳（2028 年改回正确日期），这些记录的 lastModified 会突然
变旧，可能再次漏采。但这比"永久卡住"或"永久漏采正常记录"要好。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from ...platform.atomic import write_text
from .checkpoint import Checkpoint


@dataclass(frozen=True)
class Watermark:
    """水位线：上次运行记录的最新 lastModified（已过滤异常值）。"""

    #: ISO 8601 格式的 UTC 时间戳，如 "2026-11-06T15:30:45Z"
    latest_modified: str | None

    def is_stale(self, last_modified_list: list[str]) -> bool:
        """判断一页记录是否已过水位线。

        页内最老的有效 lastModified 严格早于水位线，说明已经越过旧数据边界；
        整页都晚于水位线时尚未扫到旧数据，必须继续翻页。

        比较走解析后的时间点，不是 ISO 字符串字典序：`...:00Z` 与
        `...:00.000Z` 是同一刻，字典序却分得出大小；带 `+09:00` 的值更是
        连日期都可能与前一天等价。用字符串比会静默改掉停页时机。
        """
        baseline = _parse_timestamp(self.latest_modified or "")
        if baseline is None:
            # 首次运行，或水位线损坏：继续扫描而不是永久卡住
            return False

        valid = _valid_timestamps(last_modified_list)
        if not valid:
            # 本页全是异常值，视为未越过水位线（继续扫描）
            return False

        return min(item[1] for item in valid) < baseline

    def update(self, last_modified_list: list[str]) -> Watermark:
        """根据本页记录更新水位线，返回新水位线。"""
        valid = _valid_timestamps(last_modified_list)
        if not valid:
            # 本页无有效时间戳，水位线不变
            return self

        page_latest, page_time = max(valid, key=lambda item: item[1])
        baseline = _parse_timestamp(self.latest_modified or "")
        if baseline is None or page_time > baseline:
            # 写回保留原始字符串写法，不擅自改写上游格式；同一刻则不动水线。
            return Watermark(page_latest)
        return self


def _parse_timestamp(timestamp: str) -> datetime | None:
    """把 HF 的 lastModified 解析成 aware UTC；naive 值按 UTC 处理，坏值返回 None。

    水位线文件与 API 响应的写法不总一致（`Z` / `+00:00` / 微秒 / 无后缀）。
    统一成时间点再比较，格式差异才不会改变「哪一页该停」的判断；naive 值
    按 UTC 处理也避免与 aware 边界相减时抛 TypeError。
    """
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _valid_timestamps(values: list[str]) -> list[tuple[str, datetime]]:
    """过滤异常值，给出 (原始字符串, 时间点) 对：写回时保留上游格式。"""
    out: list[tuple[str, datetime]] = []
    for text in values:
        if not text or not _is_reasonable_timestamp(text):
            continue
        parsed = _parse_timestamp(text)
        if parsed is not None:
            out.append((text, parsed))
    return out


def _is_reasonable_timestamp(timestamp: str) -> bool:
    """检查时间戳是否在合理范围内：2015-01-01 ~ 当前时间 + 7 天。"""
    dt = _parse_timestamp(timestamp)
    if dt is None:
        return False
    now = datetime.now(UTC)
    return datetime(2015, 1, 1, tzinfo=UTC) <= dt <= now + timedelta(days=7)


class WatermarkCheckpoint:
    """游标与水位线各自持久化，互不推断对方的生命周期。

    每页两者都要落盘：只存游标不存水线，续跑会从旧游标起步却拿新水线判断，
    上半段已扫过的数据会被再扫一次。但「一起保存」不等于「一起清除」——
    旧实现把 `cursor=None` 同时当作「一轮完成、清掉水位线」的信号，而
    collect 完成态恰好调 save(None)，首轮每页的 cursor 本就是 None，
    于是水位线永远写不进或刚写完就被删，下轮 is_stale() 恒为 False，
    时间戳机制形同虚设。今拆成三个入口：save 管游标、
    save_watermark 管水线、finish 管完成。
    """

    def __init__(self, checkpoint: Checkpoint, watermark_path: Path) -> None:
        self._checkpoint = checkpoint
        self._watermark_path = watermark_path
        self._watermark = self._load_watermark()

    @property
    def cursor(self) -> str | None:
        return self._checkpoint.cursor

    @property
    def watermark(self) -> Watermark:
        return self._watermark

    def save(self, cursor: str | None, watermark: Watermark | None = None) -> None:
        """保存游标；传入水位线时一并保存。不再从 cursor 推断完成态。"""
        self._checkpoint.save(cursor)
        if watermark is not None:
            self._save_watermark(watermark)

    def save_watermark(self, watermark: Watermark) -> None:
        """只写水位线。游标由 ScanRun 在写盘之后统一保存，勿在此处代劳。"""
        self._save_watermark(watermark)

    def finish(self) -> None:
        """一轮完成：游标作废，水位线留给下一轮判断新旧边界。"""
        self._checkpoint.save(None)

    def _save_watermark(self, watermark: Watermark) -> None:
        write_text(self._watermark_path, watermark.latest_modified or "")
        self._watermark = watermark

    def _load_watermark(self) -> Watermark:
        """从文件加载水位线；不存在或损坏返回空水位线。"""
        if not self._watermark_path.exists():
            return Watermark(None)

        try:
            content = self._watermark_path.read_text(encoding="utf-8").strip()
            if not content:
                return Watermark(None)

            # 验证是否是合理的时间戳
            if _is_reasonable_timestamp(content):
                return Watermark(content)
            else:
                # 异常值，忽略
                return Watermark(None)
        except (OSError, UnicodeDecodeError):
            return Watermark(None)


def extract_last_modified(records: list[dict[str, Any]]) -> list[str]:
    """从记录列表提取 lastModified 时间戳。"""
    result = []
    for record in records:
        last_modified = record.get("lastModified")
        if last_modified is None:
            result.append("")
        elif isinstance(last_modified, str):
            result.append(last_modified)
        else:
            result.append("")
    return result
