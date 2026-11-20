"""口径词表的行为测试。

要守的不是「能查到」，而是三件事：**歧义有默认**（下载量取 30 天而非累计）、
**写错立刻报**（不返回 None 让调用方去猜）、**拼错列名当场露馅**。
"""

from __future__ import annotations

import pytest

from hf_data_crawl.delivery.metrics import (
    METRICS,
    Metric,
    MetricKind,
    UnknownMetric,
    _build_index,
    lookup,
    mismatched_fields,
)


def test_下载量默认取近30天而非累计() -> None:
    """同一句话两种理解，差两个数量级——这是词表存在的全部理由。"""
    assert lookup("下载量").field == "downloads"
    assert lookup("累计下载量").field == "downloads_all_time"


def test_字段名与别名都能查到同一条() -> None:
    assert lookup("downloads").term == "下载量"
    assert lookup("30天下载量").term == "下载量"
    assert lookup("likes").term == "点赞数"


def test_未知词条报错并列出已知词条() -> None:
    with pytest.raises(UnknownMetric) as caught:
        lookup("热度")

    assert caught.value.term == "热度"
    assert "下载量" in caught.value.known
    assert "点赞数" in caught.value.known


def test_每个词条都有定义且是给人看的() -> None:
    """定义会原样进交付包的 README，空定义等于交出去一份没有说明的文件。"""
    for metric in METRICS:
        assert metric.definition, f"{metric.term} 缺定义"
        # 4 字即可：「点赞总数」没有歧义，不需要长篇；有歧义的（下载量）自己会写长。
        assert len(metric.definition) >= 4, f"{metric.term} 的定义太短，说不清是什么"


def test_词条字段不重复() -> None:
    fields = [metric.field for metric in METRICS]
    assert len(fields) == len(set(fields)), "两个词条指同一列，等于让口径分叉"


def test_词表冲突在构建期就炸() -> None:
    """冲突若放到运行时，错的交付单会一直错下去而没人发现。"""
    a = Metric("甲", "col_a", MetricKind.NUMBER, "第一个", aliases=("别名",))
    b = Metric("乙", "col_b", MetricKind.NUMBER, "第二个", aliases=("别名",))

    with pytest.raises(ValueError, match="词表冲突"):
        _build_index([a, b])


def test_拼错的列名会被当场指出来() -> None:
    """加词最容易犯的错；等到查询时才报，那时已经在跑交付了。"""
    bad = mismatched_fields(["downloads", "likes", "id"])

    assert "下载量" not in bad
    assert "点赞数" not in bad
    assert bad["累计下载量"] == "downloads_all_time"


def test_全部词条都能在视图定义里找到() -> None:
    """词表指向的列必须真在 `datasets` 视图里。

    用视图定义（`wide_column_names()`，由登记表拼出）而不是本机底库验证：后者要
    `data/` 存在，CI 上恒 skip——一个永不执行的判据等于没有。
    """
    from hf_data_crawl.query.gold import wide_column_names

    assert mismatched_fields(wide_column_names()) == {}
