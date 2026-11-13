"""测试采集预设配置。"""

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
