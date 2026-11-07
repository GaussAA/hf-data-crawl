"""对账 sha 变更检测测试。

测试 null sha 的正确处理：
- 两方都有 sha 且不同 → sha_changed
- 至少一方 sha 为 null → undetectable_change
"""

from __future__ import annotations

from hf_data_crawl.session import diff_sets
from hf_data_crawl.status import Status


def test_两方都有sha且不同算变更() -> None:
    """两方都有非 null sha 且不同，应算 sha_changed。"""
    baseline = {
        "dataset/a": ("sha123", Status.ACTIVE.value),
        "dataset/b": ("sha456", Status.ACTIVE.value),
    }
    seen = {
        "dataset/a": "sha123",  # 相同
        "dataset/b": "sha789",  # 不同
    }

    diff = diff_sets(baseline, seen)

    assert diff.sha_changed == ["dataset/b"]
    assert diff.undetectable_change == []


def test_两方都有sha且相同不算变更() -> None:
    """两方都有非 null sha 且相同，不算变更。"""
    baseline = {
        "dataset/a": ("sha123", Status.ACTIVE.value),
    }
    seen = {
        "dataset/a": "sha123",
    }

    diff = diff_sets(baseline, seen)

    assert diff.sha_changed == []
    assert diff.undetectable_change == []


def test_基线sha为null归入无法检测() -> None:
    """基线 sha 为 null，应归入 undetectable_change。"""
    baseline = {
        "dataset/a": (None, Status.ACTIVE.value),
        "dataset/b": (None, Status.ACTIVE.value),
    }
    seen = {
        "dataset/a": "sha123",  # 已见有 sha，但基线没有
        "dataset/b": None,  # 两方都 null
    }

    diff = diff_sets(baseline, seen)

    assert diff.sha_changed == []
    assert set(diff.undetectable_change) == {"dataset/a", "dataset/b"}


def test_已见sha为null归入无法检测() -> None:
    """已见 sha 为 null，应归入 undetectable_change。"""
    baseline = {
        "dataset/a": ("sha123", Status.ACTIVE.value),
        "dataset/b": ("sha456", Status.ACTIVE.value),
    }
    seen = {
        "dataset/a": None,  # 基线有 sha，但已见没有
        "dataset/b": None,
    }

    diff = diff_sets(baseline, seen)

    assert diff.sha_changed == []
    assert set(diff.undetectable_change) == {"dataset/a", "dataset/b"}


def test_两方都是null归入无法检测() -> None:
    """两方 sha 都是 null，应归入 undetectable_change。"""
    baseline = {
        "dataset/a": (None, Status.ACTIVE.value),
    }
    seen = {
        "dataset/a": None,
    }

    diff = diff_sets(baseline, seen)

    assert diff.sha_changed == []
    assert diff.undetectable_change == ["dataset/a"]


def test_混合场景() -> None:
    """混合场景：同时有变更、无法检测、不变、新增、消失。"""
    baseline = {
        "dataset/a": ("sha123", Status.ACTIVE.value),  # 不变
        "dataset/b": ("sha456", Status.ACTIVE.value),  # 变更
        "dataset/c": (None, Status.ACTIVE.value),  # 无法检测（基线 null）
        "dataset/d": ("sha789", Status.ACTIVE.value),  # 无法检测（已见 null）
        "dataset/e": ("shaXYZ", Status.ACTIVE.value),  # 消失
        "dataset/f": ("sha111", Status.RESTRICTED.value),  # 恢复（但 sha 变了）
    }
    seen = {
        "dataset/a": "sha123",  # 不变
        "dataset/b": "sha999",  # 变更
        "dataset/c": "sha777",  # 无法检测
        "dataset/d": None,  # 无法检测
        "dataset/f": "sha222",  # 恢复 + 变更
        "dataset/z": "shaNew",  # 新增
    }

    diff = diff_sets(baseline, seen)

    # 变更：b（sha 不同）、f（sha 不同，虽然是恢复但也算变更）
    assert set(diff.sha_changed) == {"dataset/b", "dataset/f"}

    # 无法检测：c（基线 null）、d（已见 null）
    assert set(diff.undetectable_change) == {"dataset/c", "dataset/d"}

    # 新增
    assert diff.new == ["dataset/z"]

    # 消失
    assert diff.missing == ["dataset/e"]

    # 恢复：f（从 restricted 恢复）
    assert diff.reactivated == ["dataset/f"]


def test_不在基线的库不参与sha检测() -> None:
    """新增的库（不在基线）不参与 sha 检测。"""
    baseline = {
        "dataset/a": ("sha123", Status.ACTIVE.value),
    }
    seen = {
        "dataset/a": "sha123",
        "dataset/b": None,  # 新增，sha 为 null
        "dataset/c": "sha999",  # 新增，sha 有值
    }

    diff = diff_sets(baseline, seen)

    # 新增的库不算 undetectable_change
    assert diff.undetectable_change == []
    assert diff.sha_changed == []
    assert set(diff.new) == {"dataset/b", "dataset/c"}


def test_消失的库不参与sha检测() -> None:
    """消失的库不参与 sha 检测。"""
    baseline = {
        "dataset/a": ("sha123", Status.ACTIVE.value),
        "dataset/b": (None, Status.ACTIVE.value),
    }
    seen = {
        # a 和 b 都消失了
    }

    diff = diff_sets(baseline, seen)

    # 消失的库不算 sha_changed 或 undetectable_change
    assert diff.sha_changed == []
    assert diff.undetectable_change == []
    assert set(diff.missing) == {"dataset/a", "dataset/b"}


def test_恢复但sha相同() -> None:
    """从非在架状态恢复，且 sha 相同。"""
    baseline = {
        "dataset/a": ("sha123", Status.RESTRICTED.value),
    }
    seen = {
        "dataset/a": "sha123",  # sha 相同
    }

    diff = diff_sets(baseline, seen)

    # 恢复，但 sha 未变
    assert diff.reactivated == ["dataset/a"]
    assert diff.sha_changed == []
    assert diff.undetectable_change == []


def test_恢复且sha变更() -> None:
    """从非在架状态恢复，且 sha 变更。"""
    baseline = {
        "dataset/a": ("sha123", Status.RENAMED.value),
    }
    seen = {
        "dataset/a": "sha999",  # sha 变了
    }

    diff = diff_sets(baseline, seen)

    # 既恢复，也变更
    assert diff.reactivated == ["dataset/a"]
    assert diff.sha_changed == ["dataset/a"]
    assert diff.undetectable_change == []


def test_恢复但sha为null() -> None:
    """从非在架状态恢复，但 sha 为 null。"""
    baseline = {
        "dataset/a": (None, Status.DISAPPEARED.value),
    }
    seen = {
        "dataset/a": "sha123",
    }

    diff = diff_sets(baseline, seen)

    # 既恢复，也无法检测
    assert diff.reactivated == ["dataset/a"]
    assert diff.sha_changed == []
    assert diff.undetectable_change == ["dataset/a"]


def test_空基线空已见() -> None:
    """空基线、空已见，所有列表都应为空。"""
    baseline = {}
    seen = {}

    diff = diff_sets(baseline, seen)

    assert diff.new == []
    assert diff.sha_changed == []
    assert diff.reactivated == []
    assert diff.missing == []
    assert diff.undetectable_change == []


def test_只有新增() -> None:
    """只有新增的库。"""
    baseline = {}
    seen = {
        "dataset/a": "sha123",
        "dataset/b": None,
    }

    diff = diff_sets(baseline, seen)

    assert set(diff.new) == {"dataset/a", "dataset/b"}
    assert diff.sha_changed == []
    assert diff.undetectable_change == []


def test_只有消失() -> None:
    """只有消失的库。"""
    baseline = {
        "dataset/a": ("sha123", Status.ACTIVE.value),
        "dataset/b": (None, Status.ACTIVE.value),
    }
    seen = {}

    diff = diff_sets(baseline, seen)

    assert set(diff.missing) == {"dataset/a", "dataset/b"}
    assert diff.sha_changed == []
    assert diff.undetectable_change == []


def test_结果排序() -> None:
    """所有列表应该是排序的（便于测试和报告）。"""
    baseline = {
        "z/z": ("shaZ", Status.ACTIVE.value),
        "a/a": ("shaA", Status.ACTIVE.value),
        "m/m": (None, Status.ACTIVE.value),
    }
    seen = {
        "z/z": "shaZ2",  # 变更
        "a/a": "shaA",  # 不变
        "m/m": "shaM",  # 无法检测
        "x/x": "shaX",  # 新增
    }

    diff = diff_sets(baseline, seen)

    # 检查排序
    assert diff.sha_changed == ["z/z"]  # 已排序
    assert diff.undetectable_change == ["m/m"]  # 已排序
    assert diff.new == ["x/x"]  # 已排序
