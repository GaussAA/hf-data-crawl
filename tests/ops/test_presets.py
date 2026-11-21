"""测试采集预设配置。"""

import argparse

import pytest

from hf_data_crawl.platform.presets import get_preset, list_presets


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
    from hf_data_crawl.ops.parsers import build_parser

    return build_parser().parse_args(["collect", *argv])


def test_不传预设等于default预设() -> None:
    """「默认」只有一处定义：不传 `--preset` 就是 `default`。

    此前命令层另有一套游离默认值，于是同一个参数上「不传」给 3、「传 default」给 5，
    `max_pages` 更是 `None`（扫到末页）对 100——命令行上没有任何提示。
    """
    from hf_data_crawl.acquire.commands import apply_collect_defaults

    bare = parse_collect()
    apply_collect_defaults(bare)
    explicit = parse_collect("--preset", "default")
    apply_collect_defaults(explicit)

    preset = get_preset("default")
    assert (bare.refresh_top, bare.pages_without_new, bare.max_pages) == (
        preset.refresh_top,
        preset.pages_without_new,
        preset.max_pages,
    )
    assert (bare.refresh_top, bare.pages_without_new, bare.max_pages) == (
        explicit.refresh_top,
        explicit.pages_without_new,
        explicit.max_pages,
    )


def test_full_scan不填页数上限() -> None:
    """`--full-scan` 说的就是「扫到末页」，页数护栏必须让位。

    链上跑的正是 `collect --full-scan`（不传 preset）。若它被默认的 100 页截断，
    `stopped_reason` 会记成 `max_pages`，而刷新链只认 `end`——整条链的下游全部
    跳过，且不报错。
    """
    from hf_data_crawl.acquire.commands import apply_collect_defaults

    args = parse_collect("--full-scan")
    apply_collect_defaults(args)

    assert args.max_pages is None
    # 水位线在 full_scan 下也失效（由 collect.run 处理），但仍要填一个合法值。
    assert args.pages_without_new == get_preset("default").pages_without_new


def test_full_scan时显式的max_pages仍然作数() -> None:
    """用户自己给了页数，就是他要的有界试跑；不许被 full_scan 悄悄改掉。"""
    from hf_data_crawl.acquire.commands import apply_collect_defaults

    args = parse_collect("--full-scan", "--max-pages", "50")
    apply_collect_defaults(args)

    assert args.max_pages == 50


def test_帮助里的默认值与预设表一致(capsys: pytest.CaptureFixture[str]) -> None:
    """help 里写的数字若手抄，改预设时它不会跟着变——正是两套默认的由来。

    取真实用户看到的那份输出（`collect --help`），比对数值。
    """
    from hf_data_crawl.ops.parsers import build_parser

    preset = get_preset("default")
    with pytest.raises(SystemExit):
        build_parser().parse_args(["collect", "--help"])
    # argparse 会按宽度折行，先把空白压平再比对数值。
    text = " ".join(capsys.readouterr().out.split())

    assert f"{preset.pages_without_new} 页" in text
    assert f"{preset.refresh_top:,}" in text
    assert f"{preset.max_pages} 页" in text


def test_默认预设必须在登记表里() -> None:
    from hf_data_crawl.platform.presets import DEFAULT_PRESET_NAME, PRESETS

    assert DEFAULT_PRESET_NAME in PRESETS


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
