"""采集预设配置。

提供不同场景的采集配置预设,用户可通过 --preset 参数快速选择。
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CollectPreset:
    """采集配置预设。

    Attributes:
        name: 预设名称
        description: 预设描述
        max_pages: 主扫描最多页数
        refresh_top: 额外刷新的Top数据集数量
        pages_without_new: 连续多少页无新记录即判定到达水位线
    """

    name: str
    description: str
    max_pages: int | None
    refresh_top: int
    pages_without_new: int


#: 预设配置字典
PRESETS: dict[str, CollectPreset] = {
    "default": CollectPreset(
        name="default",
        description="默认配置 - 平衡性能与新鲜度 (Top 5000, ~105k数据集/天, 同步75s)",
        max_pages=100,
        refresh_top=5000,
        pages_without_new=5,
    ),
    "aggressive": CollectPreset(
        name="aggressive",
        description="激进配置 - 高数据新鲜度 (Top 10000, ~210k数据集/天, 同步150s, 推荐异步)",
        max_pages=200,
        refresh_top=10000,
        pages_without_new=5,
    ),
    "maximum": CollectPreset(
        name="maximum",
        description="极致配置 - 最高新鲜度 (Top 20000, ~520k数据集/天, 需异步模式)",
        max_pages=500,
        refresh_top=20000,
        pages_without_new=10,
    ),
    "minimal": CollectPreset(
        name="minimal",
        description="最小配置 - 快速测试 (Top 1000, ~11k数据集/天, 同步15s)",
        max_pages=10,
        refresh_top=1000,
        pages_without_new=3,
    ),
    "conservative": CollectPreset(
        name="conservative",
        description="保守配置 - 类似旧默认行为 (不刷新Top, 依赖watermark)",
        max_pages=None,
        refresh_top=0,
        pages_without_new=3,
    ),
}


def get_preset(name: str) -> CollectPreset:
    """获取预设配置。

    Args:
        name: 预设名称

    Returns:
        预设配置对象

    Raises:
        ValueError: 预设名称不存在
    """
    if name not in PRESETS:
        available = ", ".join(PRESETS.keys())
        raise ValueError(f"Unknown preset: {name}. Available: {available}")
    return PRESETS[name]


def list_presets() -> list[CollectPreset]:
    """列出所有预设配置。

    Returns:
        预设配置列表
    """
    return list(PRESETS.values())
