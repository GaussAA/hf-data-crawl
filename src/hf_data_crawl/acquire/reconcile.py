"""全量对账：按 createdAt 稳定序扫全站，产出新增 / 下架 / sha 变更三类差异。

铁律：只有「扫到末页」才允许判定下架。有界扫描（max_pages）属于没扫完，
此时若照常判定，会把整站未扫到的部分全标成下架——这是不可逆的误判。

续跑所需的游标、已见集合与基线由 `session.ReconcileSession` 拥有，本模块不碰文件。
"""

from __future__ import annotations

import json
import re
import time
from collections.abc import Mapping
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from ..pipeline import state
from ..pipeline.derive import derive_day
from ..platform.atomic import write_csv, write_text
from ..platform.store import DatasetStore
from ..platform.tables import PolarsType
from ..platform.vocab import INACTIVE, Status
from .collect import LEAN_EXPAND
from .hf.api import MAX_LIMIT, HfDatasetApi, Page
from .hf.scan import ScanRun, ScanSpec
from .session import ReconcileSession

#: 探测结论 → 落库状态。`unknown`（网络异常等）不在表内，表示「先别动」。
STATUS_BY_VERDICT: dict[str, Status] = {
    "deleted": Status.DISAPPEARED,
    "restricted": Status.RESTRICTED,
    "renamed": Status.RENAMED,
    "present": Status.ACTIVE,
}

RECONCILE_CHECKPOINT = "reconcile-checkpoint.json"

REPORT_SCHEMA: dict[str, PolarsType] = {
    "kind": pl.Utf8,
    "id": pl.Utf8,
    # `detail` 是机器可读的编码（404 / 401 / sha_null / probe），保留给程序判断；
    # `note` 是给人看的中文说明。Polars 按显式 schema 建表时会静默丢掉多出的键，
    # 所以两列都必须登记，否则行里的 `note` 永远进不了 CSV。
    "detail": pl.Utf8,
    "note": pl.Utf8,
}


#: 每轮最多探测多少个「没扫到」的库。
#:
#: 全站扫描已用掉约 1123 次请求，而配额是 1000 请求 / 5 分钟。missing 常年数千，
#: 若一次全探，剩余配额必然不够，大批库落到 `unknown`（先别动）——状态就此冻住。
#: 故按日限额、轮转推进：宁可三天轮完一轮，也不要每天探一批、天天探不完。
PROBE_DAILY_LIMIT = 2000

#: 轮转游标：记「下一轮从排序后的第几个开始」，让每天都探到不同的库。
PROBE_CURSOR_NAME = "probe-cursor.json"

#: 探测排序键的版本。判据一改（如从纯 id 改成「在架优先」），旧游标指向的就是
#: 另一个序列里的位置，续用只会从中间切开；故版本不符即归零，从头扫一遍。
PROBE_ORDER_VERSION = 2

#: 探测阶段的时间预算（秒）。
#:
#: 探测是唯一「结果可以不要」的环节：判不出来的库本就留待下轮，状态不动。
#: 但它持有写锁——2026-11-13 实测，扫描把配额吃光后，探测逐个撞 429 并按
#: `retry-after` 睡满整个窗口（最长 300 秒），2000 个探测 × 最多 5 次重试 ÷ 8 线程
#: 足以把进程挂上数小时，整条日常链就此堵死（当晚卡了 2 小时 50 分，导出未跑）。
#: 故给探测一个墙钟上限：到点就收工，剩下的顺延，绝不为了几个库占着锁过夜。
PROBE_TIME_BUDGET = 15 * 60.0

#: 探测分批提交的批大小。批与批之间检查预算，才停得下来；一次提交几千个，
#: 线程池会把它们全部跑完，预算形同虚设。
PROBE_CHUNK = 128


def probe_cursor_path(store: DatasetStore) -> Path:
    """放在 `meta/` 而非 `state/`：它不属于任何登记表，重建清不到它。"""
    return store.root / "meta" / PROBE_CURSOR_NAME


@dataclass(frozen=True)
class ReconcileSummary:
    """一轮对账的摘要。

    `pages` / `records` 是**本轮**的数；`new_ids` / `sha_changed` / `reactivated`
    等差异计数反映**整个会话**（续跑时包含上半段），与单次跑完一致。
    """

    snapshot_date: date
    pages: int
    records: int
    new_ids: int
    disappeared: int
    sha_changed: int
    reactivated: int
    #: 每轮对账必写差异清单，`_write_report` 恒返回 Path，故不是可选值。
    report_path: Path
    stopped_reason: str
    elapsed_seconds: float
    resumed: bool = False
    restricted: int = 0
    renamed: int = 0
    #: 发现了旧版本留下的会话（有游标、无基线），已丢弃并从头开始。
    discarded_stale_session: bool = False
    #: 无法检测变更的库数量（sha 为 null）
    undetectable_change: int = 0
    #: 本轮实际探测的库数（`missing` 里取出的那批）。
    probe_probed: int = 0
    #: 探测没拿到结论的库数（重试耗尽、仍被限流）。这些不落状态，留待下轮。
    probe_failed: int = 0
    #: 本轮未探测、顺延到下一轮的库数。积压量长期不降，说明限额或配额要调。
    probe_deferred: int = 0


class Reconciler:
    """全量核对入口。"""

    def __init__(
        self,
        api: HfDatasetApi,
        store: DatasetStore,
        *,
        report_dir: Path | None = None,
        checkpoint_filename: str = RECONCILE_CHECKPOINT,
        probe_workers: int = 8,
        probe_limit: int = PROBE_DAILY_LIMIT,
        probe_budget: float = PROBE_TIME_BUDGET,
    ) -> None:
        self._api = api
        self._store = store
        self._report_dir = report_dir or (store.root / "reports")
        self._checkpoint_filename = checkpoint_filename
        self._probe_workers = max(1, probe_workers)
        self._probe_limit = max(1, probe_limit)
        # 0 或负数表示不设预算（测试与手动排障用），生产一律给正数。
        self._probe_budget = probe_budget

    def run(
        self,
        snapshot_date: date,
        *,
        limit: int = MAX_LIMIT,
        max_pages: int | None = None,
    ) -> ReconcileSummary:
        started = time.monotonic()

        # 基线在会话开始时固定：有界停止那一轮也会更新 current，续跑若再读 current，
        # 上半段的新增与 sha 变更就被当成「本来就有」而丢掉。
        session = ReconcileSession.open(self._store, checkpoint_filename=self._checkpoint_filename)

        def remember(page: Page, _index: int) -> str | None:
            """把本页 id 追加进已见集合。幂等：ScanRun 在存游标之前调用它。"""
            session.remember(page.records)
            return None

        scan = ScanRun(self._api, self._store).run(
            snapshot_date,
            ScanSpec(
                sort="createdAt",
                expand=LEAN_EXPAND,
                mode="reconcile",
                limit=limit,
                max_pages=max_pages,
                checkpoint=session.checkpoint,
                on_page=remember,
            ),
        )
        stopped_reason = scan.stopped_reason

        derive_day(self._store, snapshot_date)

        diff = session.diff()
        new_ids = diff.new
        sha_changed = diff.sha_changed
        reactivated = diff.reactivated
        undetectable = diff.undetectable_change

        disappeared: list[str] = []
        restricted: list[str] = []
        renamed: list[str] = []
        probe_reactivated: list[str] = []
        probed = failed = deferred = 0
        if stopped_reason == "end":
            verdicts, probed, failed, deferred = self._classify_missing(
                diff.missing, statuses=session.status_of(diff.missing)
            )
            disappeared = [i for i, s in verdicts.items() if s is Status.DISAPPEARED]
            restricted = [i for i, s in verdicts.items() if s is Status.RESTRICTED]
            renamed = [i for i, s in verdicts.items() if s is Status.RENAMED]
            # 探测证明库还在、id 没变：此前被标成非在架的，恢复为 active（自愈）。
            probe_reactivated = [
                i
                for i, s in verdicts.items()
                if s is Status.ACTIVE and session.status_before(i) in INACTIVE
            ]
            state.mark_status(self._store, verdicts)
            session.close()

        report_path = self._write_report(
            snapshot_date,
            stopped_reason,
            new_ids,
            disappeared,
            sha_changed,
            reactivated,
            restricted=restricted,
            renamed=renamed,
            probe_reactivated=probe_reactivated,
            undetectable=undetectable,
            probe=(probed, failed, deferred),
        )

        return ReconcileSummary(
            snapshot_date=snapshot_date,
            pages=scan.pages,
            records=scan.records,
            new_ids=len(new_ids),
            disappeared=len(disappeared),
            sha_changed=len(sha_changed),
            reactivated=len(reactivated) + len(probe_reactivated),
            report_path=report_path,
            stopped_reason=stopped_reason,
            elapsed_seconds=time.monotonic() - started,
            resumed=session.resumed,
            restricted=len(restricted),
            renamed=len(renamed),
            discarded_stale_session=session.discarded,
            undetectable_change=len(undetectable),
            probe_probed=probed,
            probe_failed=failed,
            probe_deferred=deferred,
        )

    def _classify_missing(
        self,
        missing: list[str],
        *,
        statuses: Mapping[str, str] | None = None,
    ) -> tuple[dict[str, Status], int, int, int]:
        """复核「没扫到」的真实原因，返回 (判定, 已探, 失败, 顺延)。

        列表扫描看不见一个库，可能是删除、转私有或改名。直接问一次详情接口即可
        分辨：404 是删除，401 是转私有，307 跟随到 200 则是改名。

        每轮只探 `probe_limit` 个：全站扫描已花掉上千次请求，总量才 1000 请求 /
        5 分钟，一次探完几千个必然大批限流。顺延不是偷懒——探测失败本就不落状态，
        本来要留待下轮；明确排队至少让「下一轮探谁」可预期，也让积压量被体检看见。

        按 `statuses` 把在架的排前面，再按 id 稳定排序、从游标处取。已判定为非在架
        的不必天天复核：它们本就不在列表里，每轮都探等于拿一半配额去确认旧结论。
        """
        if not missing:
            return {}, 0, 0, 0

        batch, start, total = self._probe_batch(missing, statuses or {})
        verdicts: dict[str, Status] = {}
        failed = 0
        probed = 0
        deadline = time.monotonic() + self._probe_budget if self._probe_budget > 0 else None
        with ThreadPoolExecutor(max_workers=self._probe_workers) as pool:
            while probed < len(batch):
                # 预算按批检查：一批 128 个，最坏多跑一批就停，不会几千个一起冲出去。
                if deadline is not None and time.monotonic() >= deadline:
                    break
                chunk = batch[probed : probed + PROBE_CHUNK]
                for dataset_id, probe in zip(chunk, pool.map(self._api.probe, chunk), strict=True):
                    status = STATUS_BY_VERDICT.get(probe.verdict)
                    if status is None:
                        failed += 1
                        continue
                    verdicts[dataset_id] = status
                probed += len(chunk)
        # 游标只前进「已探到」的部分：超预算没探的仍留在队首，下一轮先探它们，
        # 而不是被整批跳过、白等一整个轮转周期。
        if total:
            self._write_probe_cursor((start + probed) % total)
        return verdicts, probed, failed, len(missing) - probed

    def _probe_batch(
        self, missing: list[str], statuses: Mapping[str, str]
    ) -> tuple[list[str], int, int]:
        """按轮转游标取本轮要探的一批，返回 (这批 id, 起点, 序列总数)。

        排序键是「在架优先」：active 排前面，已判为非在架的排后面。后者仍会被探到
        （在架的凑不满限额时），但不再占每天的黄金配额——实测 6579 个待复核里有
        2933 个是早已判过的，全探一遍等于四成半的请求白花。
        """
        ordered = sorted(missing, key=lambda item: (statuses.get(item, "") != "active", item))
        total = len(ordered)
        if self._probe_limit >= total:
            return ordered, 0, total
        start = self._read_probe_cursor() % total
        return (
            (ordered + ordered)[start : start + self._probe_limit],
            start,
            total,
        )

    def _read_probe_cursor(self) -> int:
        """读轮转游标；档案缺失、损坏、或排序版本不符一律当 0，不因此中断对账。"""
        try:
            body = json.loads(probe_cursor_path(self._store).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return 0
        if not isinstance(body, dict) or body.get("order") != PROBE_ORDER_VERSION:
            # 没有 order 的是本字段引入前的旧档案；排序键已换，位置不可沿用。
            return 0
        value = body.get("offset")
        return value if isinstance(value, int) and value >= 0 else 0

    def _write_probe_cursor(self, offset: int) -> None:
        """写轮转游标。写不进去最多让下轮从同一处开始，不该让整轮判定作废。"""
        path = probe_cursor_path(self._store)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            write_text(
                path,
                json.dumps({"offset": offset, "order": PROBE_ORDER_VERSION}, ensure_ascii=False)
                + "\n",
            )
        except OSError:
            return

    def _write_report(
        self,
        snapshot_date: date,
        stopped_reason: str,
        new_ids: list[str],
        disappeared: list[str],
        sha_changed: list[str],
        reactivated: list[str],
        *,
        restricted: list[str] | None = None,
        renamed: list[str] | None = None,
        probe_reactivated: list[str] | None = None,
        undetectable: list[str] | None = None,
        probe: tuple[int, int, int] | None = None,
    ) -> Path:
        rows: list[dict[str, Any]] = [
            {"kind": "run", "id": "", "detail": f"stopped_reason={stopped_reason}", "note": ""}
        ]
        if probe is not None:
            probed, failed, deferred = probe
            if probed or deferred:
                rows.append(
                    {
                        "kind": "probe",
                        "id": "",
                        "detail": f"probed={probed} failed={failed} deferred={deferred}",
                        "note": "探测限额；顺延的库下一轮继续" if deferred else "",
                    }
                )
        rows += [{"kind": "new", "id": item, "detail": "", "note": ""} for item in new_ids]
        rows += [
            {"kind": "disappeared", "id": item, "detail": "404", "note": ""} for item in disappeared
        ]
        rows += [
            {"kind": "restricted", "id": item, "detail": "401", "note": ""}
            for item in restricted or []
        ]
        rows += [
            {"kind": "renamed", "id": item, "detail": "", "note": ""} for item in renamed or []
        ]
        rows += [
            {"kind": "sha_changed", "id": item, "detail": "", "note": ""} for item in sha_changed
        ]
        rows += [
            {"kind": "reactivated", "id": item, "detail": "", "note": ""} for item in reactivated
        ]
        rows += [
            {"kind": "reactivated", "id": item, "detail": "probe", "note": ""}
            for item in probe_reactivated or []
        ]
        rows += [
            {
                "kind": "undetectable_change",
                "id": item,
                "detail": "sha_null",
                "note": "无法检测变更（sha为null）",
            }
            for item in undetectable or []
        ]

        self._report_dir.mkdir(parents=True, exist_ok=True)
        path = self._report_dir / f"reconcile-{snapshot_date.isoformat()}.csv"
        write_csv(pl.DataFrame(rows, schema=REPORT_SCHEMA), path)
        prune_reports(self._report_dir)
        return path


#: 对账差异清单的保留份数。对账已改为每日一次，30 份约一个月；更早的差异已体现在
#: `current` 的状态与 `observations` 里，清单只是当时的可读副本。
REPORT_KEEP = 30

_REPORT_PATTERN = re.compile(r"^reconcile-\d{4}-\d{2}-\d{2}\.csv$")


def prune_reports(report_dir: Path, keep: int = REPORT_KEEP) -> list[Path]:
    """只保留日期最新的 `keep` 份对账清单，返回被删的文件。

    只认 `reconcile-YYYY-MM-DD.csv` 这个文件名：同目录的榜单页、用户放进来的
    其他文件一概不碰。按文件名里的日期排序，不看 mtime（复制目录后会变）。
    """
    reports = sorted(
        (path for path in report_dir.iterdir() if _REPORT_PATTERN.match(path.name)),
        key=lambda path: path.name,
    )
    stale = reports[: max(0, len(reports) - max(1, keep))]
    for path in stale:
        path.unlink(missing_ok=True)
    return stale
