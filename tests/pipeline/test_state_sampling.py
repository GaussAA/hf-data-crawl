"""观测分层采样的边界。

采样若写错，不会报错，只会让某个库的增速在榜单上凭空消失或凭空翻倍。所以这里
测的不是「行数变少了」，而是三条可证伪的性质：

- **头部每天都记**：榜单主力不能有时效缺口；
- **长尾一周恰好一次**：既不能漏（间隔变成 14 天，增速翻倍），也不能多（白记）；
- **边界与首见必记**：这两类一旦被采样筛掉，是「数据消失」，不是「精度下降」。
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl

from hf_data_crawl.pipeline.state import _sample_observations

#: 锚点库：prev 从 1000 递减，取 head_size=2 时 cutoff 恰为 999。
_ANCHOR = pl.DataFrame(
    {
        "downloads_prev": list(range(1000, 900, -1)),
    }
)


def changed_frame(rows: list[tuple[str, int, int | None]]) -> pl.DataFrame:
    """(id, downloads, downloads_prev) → 采样函数要的输入帧。"""
    return pl.DataFrame(
        {
            "id": [row[0] for row in rows],
            "downloads": [row[1] for row in rows],
            "downloads_all_time": [row[1] * 10 for row in rows],
            "likes": [0 for _ in rows],
            "downloads_prev": [row[2] for row in rows],
        },
        schema_overrides={"downloads_prev": pl.Int64},
    )


def kept(frame: pl.DataFrame, moment: date) -> set[str]:
    out = _sample_observations(frame, _ANCHOR, snapshot_date=moment, head_size=2, buckets=7)
    return set(out["id"].to_list())


def test_库数不足头部容量时整体不采样() -> None:
    """小库集合（测试、试运行）行为必须与采样前完全一致，否则是给省几行引入随机性。"""
    previous = pl.DataFrame({"downloads_prev": [10, 20, 30]})
    frame = changed_frame([("a", 11, 10), ("b", 21, 20), ("c", 31, 30)])

    out = _sample_observations(frame, previous, snapshot_date=date(2026, 11, 22), head_size=100)

    assert out.height == 3


def test_头部每天都有观测() -> None:
    """头部是榜单主力，不能因为采样出现时效缺口。"""
    frame = changed_frame([("acme/head", 999, 998)])
    start = date(2026, 11, 16)

    hits = sum(1 for i in range(7) if "acme/head" in kept(frame, start + timedelta(days=i)))

    assert hits == 7


def test_长尾一周恰好被采一次() -> None:
    """少一次则间隔变 14 天、增速翻倍；多一次则等于没采样。"""
    frame = changed_frame([("acme/tail", 5, 4)])
    start = date(2026, 11, 16)

    days = [start + timedelta(days=i) for i in range(7)]
    hits = sum(1 for day in days if "acme/tail" in kept(frame, day))

    assert hits == 1, f"7 天里应恰好命中一次，实际 {hits} 次"


def test_长尾的采样日跨周固定() -> None:
    """按 id 哈希分桶，不是按日期轮转：同一个库每周都是同一天被采。"""
    frame = changed_frame([("acme/tail", 5, 4)])
    start = date(2026, 11, 16)

    first = [i for i in range(7) if "acme/tail" in kept(frame, start + timedelta(days=i))]
    second = [i for i in range(7) if "acme/tail" in kept(frame, start + timedelta(days=i + 7))]

    assert first == second


def test_首次出现的库必记_哪怕不是它的采样日() -> None:
    """跳过的代价不是「这一天没数据」，而是「这个库从此没有观测来源」：
    层间勾稽会报红，且下一次的增量锚点变成 null，第一段增长被整段吞掉。"""
    frame = changed_frame([("acme/new", 3, None)])
    start = date(2026, 11, 16)

    hits = [
        day
        for day in (start + timedelta(days=i) for i in range(7))
        if "acme/new" in kept(frame, day)
    ]

    assert len(hits) == 7, "锚点缺失的 id 不受采样约束"


def test_掉出头部的那一天仍要记() -> None:
    """跨在阈值上的库，离开头部那一刻恰好是记录最后一段增长的唯一机会。

    只按新值判会把它当长尾筛掉，那一段增量就永远不在流水里了。
    """
    frame = changed_frame([("acme/fall", 1, 999)])

    out = _sample_observations(
        frame, _ANCHOR, snapshot_date=date(2026, 11, 16), head_size=2, buckets=7
    )

    assert out.height == 1


def test_同一天重复采样得到同一批行() -> None:
    """同日幂等：重跑派生不能因为哈希或分桶抖动而写出另一批行。"""
    frame = changed_frame([("acme/tail", 5, 4), ("acme/head", 999, 998)])
    moment = date(2026, 11, 22)

    assert kept(frame, moment) == kept(frame, moment)
