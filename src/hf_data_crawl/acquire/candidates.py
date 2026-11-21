"""「这一轮该重取哪些库」的唯一判据：陈旧 ∪ 缺口，再按名额截断。

为什么要有这个模块
------------------
同一件事此前有四份写法：`fetch-sizes` 与 `fetch-readme` 各自在命令里拼一遍候选集，
`sizes.stale_ids` 与 `readme.stale_readme_ids` / `missing_readme_ids` 各一份判据，
体检里再抄一遍 join。四份里改一份，另外三份不会报错、不会变红，只会静默漂移——
漂移的形态正是 2026-11-20 那次：体检 30 项全 ok，而 readme 落后 5,416 个、
size 落后 6,731 个。判据只留这一份。

两个词
------
**陈旧**：表里有结论，但结论对应的内容已经变了（README 的 commit 换了、库在上次
抓取之后又改过）。判据由 `EnrichPolicy.evidence` 声明，不在这里按表名分支。

**缺口**：压根没有可用结论——从没抓过，或上次是 `error`（`error` 不是结论，
下次该重试）。`denied` / `unavailable` 是明确结论，不算缺口；要重试它们得显式
开开关（`--retry-denied` / `--retry-unavailable`），那是另一件事。

名额
----
`budget` 是**这一轮最多发多少个请求**，不是「最多补多少陈旧」。此前
`--changed 8000` 实际会发出最多 28,000 个请求（8000 陈旧 + 20,000 缺口）——
用户看着 8000 做容量预算，跑出来三倍半。现在陈旧先吃名额（开关叫 `--changed`，
它命名的就是陈旧），余额给缺口；`budget=None` 表示陈旧全取，缺口此时受
`gap_budget` 单独限制（防「某轮全量失败后缺口涨到几十万」）。

两个明细（`stale` / `gap`）不受名额影响：体检要的是「欠了多少」，不是「今天排了
多少」。只有 `picked` 是截断后的。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import polars as pl

from ..platform.tables import TIMESTAMP

#: 证据种类：README 按 commit 判新旧。
SHA: Final = "source_sha"

#: 证据种类：规模是内容的函数，只能按「库最后一次改动 vs 上次抓取时刻」判。
MODIFIED: Final = "last_modified"

#: 证据种类：这张表不记版本凭据，只能判缺口。
#:
#: `files` 既没有 `source_sha` 也没有抓取时刻，拿 `snapshot_date` 凑一个按日判据
#: 只会得到「同日改动永久漏判」的老问题；`preview` 虽有 `fetched_at`，但它抓的是
#: 某一刻的切片，本身就是快照。两者又都是**覆盖面任务**——名额该给「没看过」的库，
#: 不是「看过又变了」的库，否则头部天天变，长尾永远排不到。故显式声明「无凭据」。
NONE: Final = "none"

#: 缺口在一轮里的上限。与 `--top` 默认值同量级：16 并发实测约 22 req/s，
#: 两万个约 15 分钟，仍在夜链给的预算之内。设上限防的是「某轮全量失败后缺口涨到
#: 几十万」——那种情况该由周日的 `--top 0` 兜底，不该让一次日常刷新跑成几小时。
GAP_BUDGET: Final = 20_000


@dataclass(frozen=True)
class EnrichPolicy:
    """一张富化表「怎么算新鲜」的静态声明。算法在 `refresh_candidates`，不在这里。

    `evidence` 只说**比什么**，不说怎么比：`source_sha` 与 `current.sha` 逐字比对，
    `last_modified` 与抓取时刻比大小——比较方式属于算法，跟着算法走。

    `fresh_states` 是「内容对得上当前版本」的状态，`resolved_states` 是「已有结论」
    的状态（缺口看它），`retry_states` 是「明确结论但允许显式重试」的那些。
    """

    table: str
    evidence: str
    fresh_states: frozenset[str]
    resolved_states: frozenset[str]
    retry_states: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if self.evidence not in (SHA, MODIFIED, NONE):
            raise ValueError(f"{self.table}：未知的证据种类 {self.evidence!r}")
        if self.evidence == NONE and self.fresh_states:
            raise ValueError(f"{self.table}：没有版本凭据的表不该声明新鲜状态")
        if not self.fresh_states <= self.resolved_states:
            raise ValueError(f"{self.table}：新鲜状态必须是结论的一部分")
        if not self.retry_states <= self.resolved_states:
            # 两头不落地的状态最坏：既不算缺口（被 resolved 挡住），又不在重试范围，
            # 于是永远没人管——静默漏抓。
            raise ValueError(f"{self.table}：可重试状态必须算作已有结论")


#: README：仓库里的一个文件，属于哪个版本由 commit 决定。`absent`（作者没写）
#: 与 `denied`（门禁读不到）都是结论——前者重试仍是 404，后者带令牌实测零命中。
README_POLICY = EnrichPolicy(
    table="readme",
    evidence=SHA,
    fresh_states=frozenset({"ok", "absent"}),
    resolved_states=frozenset({"ok", "absent", "denied"}),
    retry_states=frozenset({"denied"}),
)

#: 规模：`/size` 的 `unavailable`（未做 parquet 转换、私有、不支持）是永久性缺失，
#: 逐日重烧只是白费预算；重试要显式 `--retry-unavailable`。
SIZE_POLICY = EnrichPolicy(
    table="size",
    evidence=MODIFIED,
    fresh_states=frozenset({"ok"}),
    resolved_states=frozenset({"ok", "unavailable"}),
    retry_states=frozenset({"unavailable"}),
)

#: 文件清单：只判缺口。`truncated`（清单没取完）不是状态而是「结论不完整」，
#: 由 `fetch-files --refresh-truncated` 单独点名，不进这里的判据——那是另一根轴。
FILES_POLICY = EnrichPolicy(
    table="files",
    evidence=NONE,
    fresh_states=frozenset(),
    resolved_states=frozenset({"ok", "unavailable"}),
    retry_states=frozenset({"unavailable"}),
)

#: 预览样例：只判缺口。`ok` 与 `unavailable`（没有 viewer / 受限）都是终局。
PREVIEW_POLICY = EnrichPolicy(
    table="preview",
    evidence=NONE,
    fresh_states=frozenset(),
    resolved_states=frozenset({"ok", "unavailable"}),
    retry_states=frozenset({"unavailable"}),
)


@dataclass(frozen=True)
class Candidates:
    """一轮的候选集。三个表都是 `current` 的列，便于调用方直接取 `sha` / `downloads`。

    `picked` 已按下载量降序、已截断；`stale` / `gap` 是未截断的明细，供体检与
    摘要报数。`truncated` 是本该重取却被名额挡下的陈旧数，`deferred` 是被挡下的
    缺口数——「排了但没排完」必须说出来，否则下一轮看着一切正常。
    """

    picked: pl.DataFrame
    stale: pl.DataFrame
    gap: pl.DataFrame
    truncated: int = 0
    deferred: int = 0


def refresh_candidates(
    current: pl.DataFrame,
    existing: pl.DataFrame,
    policy: EnrichPolicy,
    *,
    budget: int | None = None,
    gap_budget: int | None = None,
) -> Candidates:
    """算出这一轮该抓谁。

    `budget` 为 None 表示陈旧全取（链上的 `--changed-all`），否则是**总数**上限；
    `gap_budget` 为 None 表示缺口不设上限（体检要真实欠账，不要排期后的数）。
    """
    empty = current.head(0)
    if current.height == 0:
        return Candidates(picked=empty, stale=empty, gap=empty)

    observed = _observations(existing, policy)
    stale = _ranked(_stale(current, observed, policy))
    gap = _ranked(_gap(current, observed, policy))

    picked_stale = stale if budget is None else stale.head(budget)
    # 「名额」是总请求数：陈旧先吃，剩下的才轮到缺口。
    room = None if budget is None else max(0, budget - picked_stale.height)
    if gap_budget is None:
        limit = room
    elif room is None:
        limit = gap_budget
    else:
        limit = min(gap_budget, room)
    picked_gap = gap if limit is None else gap.head(limit)

    picked = picked_stale
    if picked_gap.height:
        picked = _ranked(pl.concat([picked_stale, picked_gap]))

    return Candidates(
        picked=picked,
        stale=stale,
        gap=gap,
        truncated=stale.height - picked_stale.height,
        deferred=gap.height - picked_gap.height,
    )


def _observations(existing: pl.DataFrame, policy: EnrichPolicy) -> pl.DataFrame | None:
    """把富化表压成「id → 状态 + 证据」两列，并去掉重复 id。

    必须去重：`existing` 常是「正式表 + 残留分片」拼起来的，同一个 id 会出现两次。
    不去重的话陈旧 join 会把同一个库复制成两行、同一个请求发两次。保留最后一行——
    分片里的行更新，正是它让这个库不再陈旧。
    """
    if existing.height == 0 or "status" not in existing.columns:
        return None
    keep = existing.unique(subset=["id"], keep="last")
    columns = [pl.col("id"), pl.col("status").alias("_status")]
    if policy.evidence == SHA:
        evidence = (
            pl.col("source_sha") if "source_sha" in keep.columns else pl.lit(None, dtype=pl.Utf8)
        )
    else:
        fetched = (
            pl.col("fetched_at") if "fetched_at" in keep.columns else pl.lit(None, dtype=TIMESTAMP)
        )
        # 历史行没有 `fetched_at`：回退成快照日零点，而不是跳过——跳过会让那批行
        # 永远判不出陈旧。代价是第一轮把「最后改动日 == 抓取日」的库补抓一次。
        snapshot = (
            pl.col("snapshot_date").cast(pl.Datetime("us", "UTC"))
            if "snapshot_date" in keep.columns
            else pl.lit(None, dtype=TIMESTAMP)
        )
        evidence = pl.coalesce([fetched, snapshot])
    columns.append(evidence.alias("_evidence"))
    return keep.select(columns)


def _stale(
    current: pl.DataFrame, observed: pl.DataFrame | None, policy: EnrichPolicy
) -> pl.DataFrame:
    """有结论、但结论对应的内容已经变了。"""
    empty = current.head(0)
    if observed is None or policy.evidence == NONE:
        return empty
    if policy.evidence == SHA:
        if "sha" not in current.columns:
            return empty
        # 旧行没有 `source_sha`（加列之前抓的）一律算陈旧：无从证明新鲜。
        changed = pl.col("_evidence").is_null() | (pl.col("_evidence") != pl.col("sha"))
    else:
        if "last_modified" not in current.columns:
            return empty
        # 两侧都取真时刻。只比日期会留下盲区：库在「被抓取的那一天」又改一次时，
        # 两侧日期恒相等、`>` 永不成立，而该行只有判据成立才会被重抓——永久停在旧值。
        changed = pl.col("last_modified") > pl.col("_evidence")
    fresh = pl.col("_status").is_in(sorted(policy.fresh_states))
    joined = current.join(observed, on="id", how="left")
    return joined.filter(fresh & changed).select(current.columns)


def _gap(
    current: pl.DataFrame, observed: pl.DataFrame | None, policy: EnrichPolicy
) -> pl.DataFrame:
    """没有可用结论：从没抓过，或上次是 `error`。

    anti join 而非 `is_in`：`existing` 是百万行，collect 成 set 再查会慢一个量级。
    """
    if observed is None:
        return current
    resolved = observed.filter(pl.col("_status").is_in(sorted(policy.resolved_states))).select("id")
    if resolved.height == 0:
        return current
    return current.join(resolved, on="id", how="anti")


def _ranked(frame: pl.DataFrame) -> pl.DataFrame:
    """按下载量降序：中断时先把头部抓完。没有下载量列（单测的窄表）就原序。"""
    if frame.height <= 1 or "downloads" not in frame.columns:
        return frame
    return frame.sort("downloads", descending=True, nulls_last=True)
