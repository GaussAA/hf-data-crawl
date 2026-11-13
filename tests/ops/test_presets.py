"""测试采集预设配置。"""

import argparse

from hf_data_crawl.platform.presets import PRESETS, get_preset, list_presets


def test_预设数量():
    """验证预设数量。"""
    assert len(PRESETS) == 5
    assert len(list_presets()) == 5


def test_获取default预设():
    """验证default预设。"""
    preset = get_preset("default")
    assert preset.name == "default"
    assert preset.max_pages == 100
    assert preset.refresh_top == 5000
    assert preset.pages_without_new == 5


def test_获取aggressive预设():
    """验证aggressive预设。"""
    preset = get_preset("aggressive")
    assert preset.name == "aggressive"
    assert preset.max_pages == 200
    assert preset.refresh_top == 10000


def test_获取maximum预设():
    """验证maximum预设。"""
    preset = get_preset("maximum")
    assert preset.name == "maximum"
    assert preset.max_pages == 500
    assert preset.refresh_top == 20000


def test_获取minimal预设():
    """验证minimal预设。"""
    preset = get_preset("minimal")
    assert preset.name == "minimal"
    assert preset.max_pages == 10
    assert preset.refresh_top == 1000


def test_获取conservative预设():
    """验证conservative预设(旧默认行为)。"""
    preset = get_preset("conservative")
    assert preset.name == "conservative"
    assert preset.max_pages is None
    assert preset.refresh_top == 0
    assert preset.pages_without_new == 3


def test_获取不存在的预设():
    """验证错误处理。"""
    import pytest

    with pytest.raises(ValueError, match="Unknown preset"):
        get_preset("nonexistent")


def test_所有预设有描述():
    """验证所有预设都有描述。"""
    for preset in list_presets():
        assert preset.description
        assert len(preset.description) > 10


# ---------------------------------------------------------------- 预设与显式参数


def parse_collect(*argv: str) -> argparse.Namespace:
    """按真实解析器取一份 collect 参数，避免手搓 Namespace 与 CLI 脱节。"""
    from hf_data_crawl.ops.cli import build_parser

    return build_parser().parse_args(["collect", *argv])


def test_未指定时填内置默认值() -> None:
    from hf_data_crawl.acquire.commands import apply_collect_defaults

    args = parse_collect()
    apply_collect_defaults(args)

    assert args.refresh_top == 5000
    assert args.pages_without_new == 3
    # 主扫描默认不设页数上限：靠水位线停，而不是靠页数。
    assert args.max_pages is None


def test_预设只填空位() -> None:
    from hf_data_crawl.acquire.commands import apply_collect_defaults

    args = parse_collect("--preset", "maximum")
    apply_collect_defaults(args)

    assert args.refresh_top == 20000
    assert args.max_pages == 500
    assert args.pages_without_new == 10


def test_显式参数不被预设覆盖() -> None:
    """`--refresh-top 5000` 恰好等于默认值，过去会被预设悄悄换掉。

    判据不能用「值等于默认值」——argparse 分不清「没给」与「给了默认值」，
    只能让这两个参数没有默认值，靠 None 作哨兵。
    """
    from hf_data_crawl.acquire.commands import apply_collect_defaults

    args = parse_collect("--preset", "conservative", "--refresh-top", "5000")
    apply_collect_defaults(args)

    assert args.refresh_top == 5000
    # 未指定的那项才归预设管。
    assert args.pages_without_new == 3
