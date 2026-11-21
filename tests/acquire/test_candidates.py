"""候选集的唯一判据：陈旧 ∪ 缺口，再按名额截断。

这个文件是「哪些库该重取」的规格。此前同一件事有四份写法——`fetch-sizes` 与
`fetch-readme` 各在自己命令里拼一遍、`sizes.stale_ids` 与 `readme.stale_readme_ids`
各一份、体检里再抄一遍 join——四份里改一份，另外三份不会报错、不会变红，只会
静默漂移。漂移的表现就是 2026-11-20 那次：体检 30 项全 ok，而 readme 落后 5,416 个。

故判据收进 `acquire/candidates.py`，由 `EnrichPolicy` 声明「这张表怎么算新鲜」，
算法只此一份。测试分三组：陈旧、缺口、名额。
"""

from __future__ import annotations

from datetime import UTC, date, datetime

import polars as pl
import pytest

from hf_data_crawl.acquire.candidates import (
    FILES_POLICY,
    GAP_BUDGET,
    NONE,
    README_POLICY,
    SIZE_POLICY,
    EnrichPolicy,
    refresh_candidates,
)

DAY = date(2026, 11, 21)


def at(hour: int, minute: int = 0) -> datetime:
    """当天某时刻（UTC），省得每处都写一遍 tzinfo。"""
    return datetime(2026, 11, 21, hour, minute, tzinfo=UTC)


# ---------------------------------------------------------------- 陈旧：按 sha


def test_sha变了算陈旧_同值与denied不算() -> None:
    """README 的判据是 commit：`source_sha` 与 `current.sha` 不等才算旧。

    `denied` 虽有旧 sha，但它是明确结论（受门禁），不归陈旧管——它走
    `--retry-denied` 那条独立开关。把它算进陈旧，等于每轮白撞一次 403。
    """
    current = pl.DataFrame(
        {
            "id": ["acme/changed", "acme/same", "acme/denied"],
            "sha": ["new", "same", "new"],
            # current 自己也有 status（active/disappeared）。判据若取错这一列，
            # 过滤恒不成立、永远「无陈旧」——静默的那种错。
            "status": ["active", "active", "active"],
            "downloads": [3, 2, 1],
        }
    )
    existing = pl.DataFrame(
        {
            "id": ["acme/changed", "acme/same", "acme/denied"],
            "status": ["ok", "ok", "denied"],
            "source_sha": ["old", "same", "old"],
        }
    )

    found = refresh_candidates(current, existing, README_POLICY)

    assert found.stale["id"].to_list() == ["acme/changed"]
    assert found.gap.height == 0, "denied 是结论，不该被算成缺口"


def test_旧行没有source_sha一律算陈旧() -> None:
    """加列之前抓的行没有 sha，无从证明新鲜——一律重取一次，之后走精确判据。"""
    current = pl.DataFrame({"id": ["acme/a"], "sha": ["new"], "status": ["active"]})
    existing = pl.DataFrame(
        {"id": ["acme/a"], "status": ["ok"], "source_sha": [None]}
    ).with_columns(pl.col("source_sha").cast(pl.Utf8))

    found = refresh_candidates(current, existing, README_POLICY)

    assert found.stale["id"].to_list() == ["acme/a"]


# ---------------------------------------------------------------- 陈旧：按时刻


def test_同日再次改动的库算陈旧() -> None:
    """库在被抓取的同一天又改一次：只比日期会永久漏判。

    两侧日期相等时 `>` 恒不成立；而该行只有判据成立才会被重抓，也就不会产生新的
    `snapshot_date`，于是永久停在旧值，连体检的「规模待重取」都看不见。
    """
    current = pl.DataFrame({"id": ["acme/a"], "last_modified": [at(18)], "downloads": [1]})
    existing = pl.DataFrame(
        {
            "id": ["acme/a"],
            "status": ["ok"],
            "snapshot_date": [DAY],
            "fetched_at": [at(10)],
        }
    )

    assert refresh_candidates(current, existing, SIZE_POLICY).stale["id"].to_list() == ["acme/a"]


def test_抓取之后没再改动的不算陈旧() -> None:
    """收紧判据不能把本来就新鲜的行也拖进来——那会天天白烧配额。"""
    current = pl.DataFrame({"id": ["acme/a"], "last_modified": [at(9)], "downloads": [1]})
    existing = pl.DataFrame(
        {"id": ["acme/a"], "status": ["ok"], "snapshot_date": [DAY], "fetched_at": [at(10)]}
    )

    assert refresh_candidates(current, existing, SIZE_POLICY).stale.height == 0


def test_没有fetched_at时回退到快照日零点() -> None:
    """`fetched_at` 是后加的列，历史行没有它。

    回退成「该日零点」而不是跳过：跳过会让那批历史行永远判不出陈旧。代价是第一轮
    把「最后改动日 == 抓取日」的库补抓一次，之后走精确判据。
    """
    current = pl.DataFrame(
        {
            "id": ["acme/same-day", "acme/older"],
            "last_modified": [at(0, 30), datetime(2026, 11, 20, 23, tzinfo=UTC)],
            "downloads": [2, 1],
        }
    )
    existing = pl.DataFrame(
        {
            "id": ["acme/same-day", "acme/older"],
            "status": ["ok", "ok"],
            "snapshot_date": [DAY, DAY],
            "fetched_at": [None, None],
        }
    ).with_columns(pl.col("fetched_at").cast(pl.Datetime("us", "UTC")))

    found = refresh_candidates(current, existing, SIZE_POLICY)

    assert found.stale["id"].to_list() == ["acme/same-day"]


# ---------------------------------------------------------------- 缺口


def test_没有记录的库算缺口() -> None:
    """`--changed` 的候选若只来自 inner join，新库永远进不来。

    2026-11-19 实测：2,945 个库卡在这里，而 `size` 的同类问题早就有缺口补抓。
    """
    current = pl.DataFrame({"id": ["acme/a", "acme/b", "acme/c"]})
    existing = pl.DataFrame({"id": ["acme/a"], "status": ["ok"], "source_sha": ["x"]})

    found = refresh_candidates(current, existing, README_POLICY)

    assert set(found.gap["id"].to_list()) == {"acme/b", "acme/c"}


def test_瞬时失败算缺口而明确结论不算() -> None:
    """`error` 不是结论（下次该重试）；`denied` / `unavailable` 是结论。

    这一条正是「error 可见」的来源：此前 `--changed` 只看陈旧集合，readme 表里
    那些 `error` 行既不算陈旧也不算缺口，日常那趟永远轮不到它们。
    """
    current = pl.DataFrame({"id": ["acme/err", "acme/denied", "acme/absent", "acme/ok"]})
    existing = pl.DataFrame(
        {
            "id": ["acme/err", "acme/denied", "acme/absent", "acme/ok"],
            "status": ["error", "denied", "absent", "ok"],
            "source_sha": [None, None, None, "s"],
        }
    ).with_columns(pl.col("source_sha").cast(pl.Utf8))

    found = refresh_candidates(current, existing, README_POLICY)

    assert found.gap["id"].to_list() == ["acme/err"]


def test_规模表的不可用算结论而错误不算() -> None:
    """`unavailable` 是「上游没做 parquet 转换」，逐日重烧只是白费预算。"""
    current = pl.DataFrame({"id": ["acme/bad", "acme/err", "acme/new"]})
    existing = pl.DataFrame({"id": ["acme/bad", "acme/err"], "status": ["unavailable", "error"]})

    found = refresh_candidates(current, existing, SIZE_POLICY)

    assert set(found.gap["id"].to_list()) == {"acme/err", "acme/new"}


def test_无凭据的表只判缺口() -> None:
    """`files` / `preview` 不记版本凭据：内容变了判不出来，只能判「没看过」。

    这是刻意的取舍：两者都是覆盖面任务，名额该给「没看过」的库，不是「看过又变了」
    的库——否则头部天天有人改，长尾永远排不到。要判陈旧，先给表加 `source_sha`。
    """
    current = pl.DataFrame({"id": ["a/x", "a/y"], "sha": ["new", "new"], "downloads": [2, 1]})
    existing = pl.DataFrame({"id": ["a/x"], "status": ["ok"], "source_sha": ["old"]})

    found = refresh_candidates(current, existing, FILES_POLICY)

    assert found.stale.height == 0, "没有凭据就不该声称谁陈旧"
    assert found.gap["id"].to_list() == ["a/y"]


def test_无凭据的策略不许声明新鲜状态() -> None:
    """声明了 `evidence=NONE` 又列新鲜状态，是自相矛盾——那份名单永远不会被用到。"""
    with pytest.raises(ValueError):
        EnrichPolicy(
            table="files",
            evidence=NONE,
            fresh_states=frozenset({"ok"}),
            resolved_states=frozenset({"ok"}),
        )


def test_截断的库是结论而不是缺口() -> None:
    """`truncated=true` 的清单只是下限，但它已是结论：默认不该天天重抓。

    一页重抓只会把截断重新确认一遍，白烧请求与配额；补全须显式
    `--refresh-truncated --max-pages>1`。
    """
    current = pl.DataFrame({"id": ["a/full", "a/partial"], "downloads": [2, 1]})
    existing = pl.DataFrame(
        {
            "id": ["a/full", "a/partial"],
            "status": ["ok", "ok"],
            "truncated": [False, True],
        }
    )

    found = refresh_candidates(current, existing, FILES_POLICY)

    assert found.gap.height == 0, "截断不是缺口"
    assert found.picked.height == 0, "默认不重抓截断"


def test_点名重试把结论行拉回候选并排在缺口之前() -> None:
    """`--retry-unavailable` / `--refresh-truncated` 若不进候选，开关就是空转。

    命令照常成功、请求一个没发——候选集化之后最容易漏的一类静默回归。
    """
    current = pl.DataFrame(
        {
            "id": ["a/partial", "a/unavailable", "a/new"],
            "downloads": [30, 20, 10],
        }
    )
    existing = pl.DataFrame(
        {
            "id": ["a/partial", "a/unavailable"],
            "status": ["ok", "unavailable"],
            "truncated": [True, False],
        }
    )

    found = refresh_candidates(
        current, existing, FILES_POLICY, retry=frozenset({"unavailable", "ok_partial"})
    )

    assert sorted(found.retried["id"].to_list()) == ["a/partial", "a/unavailable"]
    assert set(found.picked["id"].to_list()) == {"a/partial", "a/unavailable", "a/new"}
    assert found.gap["id"].to_list() == ["a/new"]


def test_重试先吃名额() -> None:
    """点名的重试是这一轮的目的，名额先给它；缺口排后面。"""
    current = pl.DataFrame(
        {
            "id": ["a/partial", "a/new-high", "a/new-low"],
            "downloads": [10, 100, 5],
        }
    )
    existing = pl.DataFrame({"id": ["a/partial"], "status": ["ok"], "truncated": [True]})

    found = refresh_candidates(
        current, existing, FILES_POLICY, budget=1, retry=frozenset({"ok_partial"})
    )

    assert found.picked["id"].to_list() == ["a/partial"]
    assert found.deferred == 2


def test_缺口状态不能借重试通道绕过判据() -> None:
    """把 `error` 传进 `retry` 只会被 `resolved_states` 收窄：它本来就是缺口。

    不收窄就会既算重试又算缺口，同一行重复请求、`deferred` 也对不上。
    """
    current = pl.DataFrame({"id": ["a/err"], "downloads": [1]})
    existing = pl.DataFrame({"id": ["a/err"], "status": ["error"]})

    found = refresh_candidates(current, existing, FILES_POLICY, retry=frozenset({"error"}))

    assert found.retried.height == 0
    assert found.gap["id"].to_list() == ["a/err"]
    assert found.deferred == 0


def test_旧表没有截断列时不影响判定() -> None:
    """加列之前写入的行没有 `truncated`：不能因此整表算缺口，也不能算不完整。"""
    current = pl.DataFrame({"id": ["a/x"], "downloads": [1]})
    existing = pl.DataFrame({"id": ["a/x"], "status": ["ok"]})

    found = refresh_candidates(current, existing, FILES_POLICY)

    assert found.gap.height == 0
    assert found.retried.height == 0


# ---------------------------------------------------------------- 名额


def _budget_frames() -> tuple[pl.DataFrame, pl.DataFrame]:
    """5 个陈旧 + 5 个缺口，缺口下载量普遍高于陈旧。

    故意让缺口排在前面：若实现「先按下载量取前 N、再看是不是陈旧」，名额会被缺口
    吃掉，`--changed N` 就退化成「按下载量抓 N 个」。陈旧优先才是这个开关的本意。
    """
    current = pl.DataFrame(
        {
            "id": [f"acme/g{index}" for index in range(5)]
            + [f"acme/s{index}" for index in range(5)],
            "sha": ["new"] * 10,
            "status": ["active"] * 10,
            "downloads": [100, 99, 98, 97, 96, 95, 94, 93, 92, 91],
        }
    )
    existing = pl.DataFrame(
        {
            "id": [f"acme/s{index}" for index in range(5)],
            "status": ["ok"] * 5,
            "source_sha": ["old"] * 5,
        }
    )
    return current, existing


def test_名额是总数而不是陈旧的名额() -> None:
    """`--changed 8000` 曾经会发出最多 28,000 个请求（8000 陈旧 + 20,000 缺口）。

    用户看着 8000 做容量预算，实际跑了三倍半——名义额度与实际消耗必须是一个数。
    """
    current, existing = _budget_frames()

    found = refresh_candidates(current, existing, README_POLICY, budget=3, gap_budget=GAP_BUDGET)

    assert found.picked.height == 3
    assert set(found.picked["id"].to_list()) == {"acme/s0", "acme/s1", "acme/s2"}
    assert found.deferred == 7, "2 个陈旧 + 5 个缺口没排进来，要如实报"
    assert found.stale.height == 5 and found.gap.height == 5, "明细不受名额影响"


def test_陈旧不足时余额给缺口() -> None:
    """名额是「这一轮最多发多少个请求」，不是「最多补多少陈旧」。"""
    current = pl.DataFrame(
        {
            "id": ["acme/s0"] + [f"acme/g{index}" for index in range(5)],
            "sha": ["new"] * 6,
            "status": ["active"] * 6,
            "downloads": [100, 99, 98, 97, 96, 95],
        }
    )
    existing = pl.DataFrame({"id": ["acme/s0"], "status": ["ok"], "source_sha": ["old"]})

    found = refresh_candidates(current, existing, README_POLICY, budget=4, gap_budget=GAP_BUDGET)

    assert found.picked.height == 4
    assert set(found.picked["id"].to_list()) == {"acme/s0", "acme/g0", "acme/g1", "acme/g2"}
    assert found.deferred == 2, "缺口还剩 2 个没排进来"
    assert found.deferred == 2


def test_缺口另有独立上限() -> None:
    """缺口上限防的是「某轮全量失败后缺口涨到几十万」，与名额是两回事。"""
    current, existing = _budget_frames()

    found = refresh_candidates(current, existing, README_POLICY, budget=None, gap_budget=2)

    assert found.picked.height == 7
    assert found.deferred == 3


def test_都不设上限时全取() -> None:
    current, existing = _budget_frames()

    found = refresh_candidates(current, existing, README_POLICY)

    assert found.picked.height == 10
    assert found.deferred == 0


def test_候选按下载量降序() -> None:
    """抓取顺序跟着下载量走：中断时先把头部抓完。"""
    current = pl.DataFrame(
        {
            "id": ["acme/low", "acme/high", "acme/mid"],
            "sha": ["x"] * 3,
            "status": ["active"] * 3,
            "downloads": [1, 300, 20],
        }
    )

    found = refresh_candidates(current, pl.DataFrame(schema={"id": pl.Utf8}), README_POLICY)

    assert found.picked["id"].to_list() == ["acme/high", "acme/mid", "acme/low"]


# ---------------------------------------------------------------- 边界


def test_暂存分片与正式表重复的id只算一次() -> None:
    """`existing` 常是「正式表 + 残留分片」拼起来的，同一个 id 会出现两次。

    不去重的话，陈旧 join 会把同一个库复制成两行，同一个请求发两次；而分片里的
    行更新（正是它让这个库不再陈旧），必须以它为准。
    """
    current = pl.DataFrame(
        {"id": ["acme/a"], "sha": ["new"], "status": ["active"], "downloads": [1]}
    )
    existing = pl.DataFrame(
        {
            "id": ["acme/a", "acme/a"],
            "status": ["ok", "ok"],
            "source_sha": ["old", "new"],
        }
    )

    found = refresh_candidates(current, existing, README_POLICY)

    assert found.stale.height == 0, "分片里的行已是当前版本，不该再抓"
    assert found.gap.height == 0


def test_current为空时返回空候选() -> None:
    current = pl.DataFrame(schema={"id": pl.Utf8, "sha": pl.Utf8, "downloads": pl.Int64})
    existing = pl.DataFrame({"id": ["acme/a"], "status": ["ok"], "source_sha": ["x"]})

    found = refresh_candidates(current, existing, README_POLICY, budget=10)

    assert found.picked.height == 0 and found.stale.height == 0 and found.gap.height == 0
    assert found.deferred == 0


def test_existing为空时全是缺口() -> None:
    current = pl.DataFrame({"id": ["acme/a", "acme/b"], "sha": ["x", "y"], "downloads": [2, 1]})

    found = refresh_candidates(current, pl.DataFrame(schema={"id": pl.Utf8}), README_POLICY)

    assert found.picked["id"].to_list() == ["acme/a", "acme/b"]
    assert found.stale.height == 0


def test_策略自检_重试状态必须是结论的一部分() -> None:
    """`denied` 既是「重试开关能救的」，就必须算「已有结论」——否则它两头不落地：
    既不算缺口（被 resolved 挡住），又不在重试名单里，永远没人管。"""
    with pytest.raises(ValueError):
        EnrichPolicy(
            table="readme",
            evidence="source_sha",
            fresh_states=frozenset({"ok"}),
            resolved_states=frozenset({"ok"}),
            retry_states=frozenset({"denied"}),
        )
