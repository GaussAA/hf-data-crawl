"""Watermark 机制测试。

测试基于时间戳的 watermark 是否正确：
- 跨日运行不漏采
- 异常值过滤
- 持久化和恢复
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from hf_data_crawl.hf.checkpoint import MemoryCheckpoint
from hf_data_crawl.hf.watermark import (
    Watermark,
    WatermarkCheckpoint,
    _is_reasonable_timestamp,
    extract_last_modified,
)


def test_合理时间戳检查() -> None:
    """测试时间戳合理性检查。"""
    now = datetime.now(UTC)

    # 当前时间合理
    assert _is_reasonable_timestamp(now.isoformat())

    # 昨天合理
    yesterday = now - timedelta(days=1)
    assert _is_reasonable_timestamp(yesterday.isoformat())

    # 未来 6 天合理（在 7 天阈值内）
    future = now + timedelta(days=6)
    assert _is_reasonable_timestamp(future.isoformat())

    # 未来 8 天不合理（超过 7 天阈值）
    far_future = now + timedelta(days=8)
    assert not _is_reasonable_timestamp(far_future.isoformat())

    # 2028 年不合理
    assert not _is_reasonable_timestamp("2028-12-10T00:00:00Z")

    # 2014 年不合理（早于 HF 成立）
    assert not _is_reasonable_timestamp("2014-01-01T00:00:00Z")

    # 无效格式
    assert not _is_reasonable_timestamp("not-a-timestamp")
    assert not _is_reasonable_timestamp("")


def test_空水位线不触发停止() -> None:
    """首次运行没有水位线，不应停止。"""
    watermark = Watermark(None)

    timestamps = [
        "2026-11-06T15:30:00Z",
        "2026-11-06T15:00:00Z",
        "2026-11-06T14:30:00Z",
    ]

    assert not watermark.is_stale(timestamps)


def test_整页新于水位线不触发停止() -> None:
    """整页记录都晚于水位线，说明还没扫到旧数据，必须继续翻页。"""
    # 水位线：昨天 23:50
    watermark = Watermark("2026-11-06T23:50:00Z")

    # 本页记录：今天 00:05 ~ 00:20（全部晚于水位线）
    timestamps = [
        "2026-11-07T00:20:00Z",
        "2026-11-07T00:15:00Z",
        "2026-11-07T00:05:00Z",
    ]

    assert not watermark.is_stale(timestamps)


def test_页内有早于水位线的记录触发停止() -> None:
    """页内最老记录早于水位线，说明已经越过旧数据边界。"""
    watermark = Watermark("2026-11-06T23:50:00Z")

    # 本页记录：有一条 23:45（早于水位线）
    timestamps = [
        "2026-11-07T00:05:00Z",
        "2026-11-06T23:58:00Z",
        "2026-11-06T23:45:00Z",  # 早于水位线
    ]

    assert watermark.is_stale(timestamps)


def test_全异常值页面不触发停止() -> None:
    """如果页内全是异常值，应继续扫描。"""
    watermark = Watermark("2026-11-06T23:50:00Z")

    # 本页全是异常值
    timestamps = [
        "2028-12-10T00:00:00Z",  # 未来异常值
        "2029-01-01T00:00:00Z",
        "2014-01-01T00:00:00Z",  # 过去异常值
    ]

    assert not watermark.is_stale(timestamps)


def test_部分异常值被过滤() -> None:
    """异常值应被过滤，只用有效时间戳判断。"""
    watermark = Watermark("2026-11-06T23:50:00Z")

    # 混合：有效值 + 异常值
    timestamps = [
        "2028-12-10T00:00:00Z",  # 异常，过滤
        "2026-11-07T00:05:00Z",  # 有效，最老的
        "2026-11-07T00:10:00Z",  # 有效
    ]

    # 有效值中最老的（00:05）晚于水位线（23:50），尚未越过边界，继续扫描
    assert not watermark.is_stale(timestamps)

    # 如果最老的早于水位线，则停止
    timestamps2 = [
        "2028-12-10T00:00:00Z",  # 异常，过滤
        "2026-11-07T00:05:00Z",  # 有效
        "2026-11-06T23:45:00Z",  # 有效，最老的，早于水位线
    ]
    assert watermark.is_stale(timestamps2)


def test_更新水位线取最新时间戳() -> None:
    """水位线应更新为本页最新的有效时间戳。"""
    watermark = Watermark("2026-11-06T23:00:00Z")

    timestamps = [
        "2026-11-07T00:20:00Z",  # 最新
        "2026-11-07T00:10:00Z",
        "2026-11-06T23:50:00Z",
    ]

    new_watermark = watermark.update(timestamps)
    assert new_watermark.latest_modified == "2026-11-07T00:20:00Z"


def test_更新时过滤异常值() -> None:
    """更新水位线时应过滤异常值。"""
    watermark = Watermark("2026-11-06T23:00:00Z")

    timestamps = [
        "2028-12-10T00:00:00Z",  # 异常，过滤
        "2026-11-07T00:10:00Z",  # 最新有效值
        "2026-11-06T23:50:00Z",
    ]

    new_watermark = watermark.update(timestamps)
    assert new_watermark.latest_modified == "2026-11-07T00:10:00Z"


def test_全异常值不更新水位线() -> None:
    """如果本页全是异常值，水位线保持不变。"""
    watermark = Watermark("2026-11-06T23:00:00Z")

    timestamps = [
        "2028-12-10T00:00:00Z",
        "2029-01-01T00:00:00Z",
    ]

    new_watermark = watermark.update(timestamps)
    assert new_watermark.latest_modified == "2026-11-06T23:00:00Z"


def test_提取记录的lastModified() -> None:
    """测试从记录列表提取时间戳。"""
    records: list[dict[str, Any]] = [
        {"id": "a/b", "lastModified": "2026-11-07T00:20:00Z"},
        {"id": "c/d", "lastModified": "2026-11-07T00:10:00Z"},
        {"id": "e/f", "lastModified": None},  # null 值
        {"id": "g/h"},  # 缺失字段
    ]

    timestamps = extract_last_modified(records)
    assert timestamps == [
        "2026-11-07T00:20:00Z",
        "2026-11-07T00:10:00Z",
        "",
        "",
    ]


def test_watermark持久化和加载(tmp_path: Path) -> None:
    """测试 WatermarkCheckpoint 的持久化。"""
    watermark_path = tmp_path / "watermark.txt"

    base_checkpoint = MemoryCheckpoint()
    wm_checkpoint = WatermarkCheckpoint(base_checkpoint, watermark_path)

    # 初始状态：空水位线
    assert wm_checkpoint.watermark.latest_modified is None

    # 保存
    new_watermark = Watermark("2026-11-07T00:20:00Z")
    wm_checkpoint.save("cursor123", new_watermark)

    # 验证持久化
    assert watermark_path.exists()
    content = watermark_path.read_text(encoding="utf-8")
    assert content == "2026-11-07T00:20:00Z"

    # 重新加载
    wm_checkpoint2 = WatermarkCheckpoint(base_checkpoint, watermark_path)
    assert wm_checkpoint2.watermark.latest_modified == "2026-11-07T00:20:00Z"


def test_完成时保留水位线且清除游标(tmp_path: Path) -> None:
    """完成态只作废游标；水位线必须留给下一轮判断边界。

    旧实现把 cursor=None 当作清线信号，而 collect 每轮完成都调 save(None)，
    结果是下轮加载到的永远是空水线，is_stale() 恒 False。
    """
    watermark_path = tmp_path / "watermark.txt"

    base_checkpoint = MemoryCheckpoint()
    wm_checkpoint = WatermarkCheckpoint(base_checkpoint, watermark_path)
    wm_checkpoint.save("cursor123", Watermark("2026-11-07T00:20:00Z"))
    assert watermark_path.exists()

    wm_checkpoint.finish()

    assert base_checkpoint.cursor is None
    assert watermark_path.exists(), "完成即清水线会让时间戳机制永远不生效"
    reloaded = WatermarkCheckpoint(base_checkpoint, watermark_path)
    assert reloaded.watermark.latest_modified == "2026-11-07T00:20:00Z"


def test_水位线可独立写入且不受游标影响(tmp_path: Path) -> None:
    """首轮 cursor 本就是 None，写水线不应被误判成「一轮完成」。"""
    watermark_path = tmp_path / "watermark.txt"
    base_checkpoint = MemoryCheckpoint()
    wm_checkpoint = WatermarkCheckpoint(base_checkpoint, watermark_path)

    wm_checkpoint.save_watermark(Watermark("2026-11-07T00:20:00Z"))

    assert base_checkpoint.cursor is None
    assert wm_checkpoint.watermark.latest_modified == "2026-11-07T00:20:00Z"
    assert watermark_path.read_text(encoding="utf-8") == "2026-11-07T00:20:00Z"


def test_损坏的水位线文件返回空水位线(tmp_path: Path) -> None:
    """损坏的水位线文件应返回空水位线，不应崩溃。"""
    watermark_path = tmp_path / "watermark.txt"

    # 写入损坏内容
    watermark_path.write_text("not-a-timestamp", encoding="utf-8")

    base_checkpoint = MemoryCheckpoint()
    wm_checkpoint = WatermarkCheckpoint(base_checkpoint, watermark_path)

    # 应返回空水位线
    assert wm_checkpoint.watermark.latest_modified is None


def test_跨日场景真实模拟() -> None:
    """模拟真实跨日场景：00:01 启动，昨天 23:58 的更新不应被跳过。

    关键理解：watermark 记录的是上一轮完成时的最新时间戳，整轮固定不动。
    is_stale 判断：页内最老的时间戳是否严格早于 watermark。
    - 如果否：说明这页全部落在尚未扫过的区间内，继续翻页
    - 如果是：说明这页已经触及水位线之前的旧数据，越过边界
    """
    # 上次运行的水位线：昨天 22:00
    baseline = Watermark("2026-11-06T22:00:00Z")

    # 第一页：今天 00:11 ~ 00:20 的新库
    page1 = [f"2026-11-07T00:{i:02d}:00Z" for i in range(20, 10, -1)]
    # 最老的是 00:11，晚于 22:00，尚未越过边界
    assert not baseline.is_stale(page1)

    # 第二页：今天 00:01 ~ 00:10 的新库
    page2 = [f"2026-11-07T00:{i:02d}:00Z" for i in range(10, 0, -1)]
    # 最老的是 00:01，仍晚于 22:00，继续
    assert not baseline.is_stale(page2)

    # 第三页：混合，包含昨天 23:58 的更新
    page3 = [
        "2026-11-07T00:05:00Z",
        "2026-11-07T00:01:00Z",
        "2026-11-06T23:58:00Z",  # 昨天晚上的更新！
        "2026-11-06T23:55:00Z",
    ]
    # 最老的是 23:55，仍晚于 22:00，继续
    assert not baseline.is_stale(page3)

    # 第四页：开始接近原始水位线
    page4 = [
        "2026-11-06T23:50:00Z",
        "2026-11-06T23:45:00Z",
        "2026-11-06T22:30:00Z",
    ]
    # 最老的是 22:30，仍晚于 22:00，继续
    assert not baseline.is_stale(page4)

    # 第五页：最老的早于原始水位线（22:00）
    page5 = [
        "2026-11-06T22:10:00Z",
        "2026-11-06T22:05:00Z",
        "2026-11-06T21:50:00Z",  # 最老的，早于原始水位线 22:00
    ]
    # 最老 21:50 严格早于基线 22:00，越过边界
    assert baseline.is_stale(page5)

    # 结论：is_stale 必须始终拿“开始时的基线”比较；本轮累积的新时间戳
    # 只用于下一轮边界，不能改变本轮是否越界。
