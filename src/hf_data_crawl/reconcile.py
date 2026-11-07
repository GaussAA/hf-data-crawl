"""全量对账：按 createdAt 稳定序扫全站，产出新增 / 下架 / sha 变更三类差异。

铁律：只有「扫到末页」才允许判定下架。有界扫描（max_pages）属于没扫完，
此时若照常判定，会把整站未扫到的部分全标成下架——这是不可逆的误判。

续跑所需的游标、已见集合与基线由 `session.ReconcileSession` 拥有，本模块不碰文件。
"""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from .api import MAX_LIMIT, HfDatasetApi, Page
from .atomic import write_csv
from .collect import LEAN_EXPAND
from .derive import derive_day
from .scan import ScanRun, ScanSpec
from .session import ReconcileSession
from .status import INACTIVE, STATUS_BY_VERDICT, Status
from .store import DatasetStore
from .tables import PolarsType

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
    ) -> None:
        self._api = api
        self._store = store
        self._report_dir = report_dir or (store.root / "reports")
        self._checkpoint_filename = checkpoint_filename
        self._probe_workers = max(1, probe_workers)

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
        if stopped_reason == "end":
            verdicts = self._classify_missing(diff.missing)
            disappeared = [i for i, s in verdicts.items() if s is Status.DISAPPEARED]
            restricted = [i for i, s in verdicts.items() if s is Status.RESTRICTED]
            renamed = [i for i, s in verdicts.items() if s is Status.RENAMED]
            # 探测证明库还在、id 没变：此前被标成非在架的，恢复为 active（自愈）。
            probe_reactivated = [
                i
                for i, s in verdicts.items()
                if s is Status.ACTIVE and session.status_before(i) in INACTIVE
            ]
            self._store.mark_status(verdicts)
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
        )

    def _classify_missing(self, missing: list[str]) -> dict[str, Status]:
        """复核「没扫到」的真实原因，返回 id → 状态。

        列表扫描看不见一个库，可能是删除、转私有或改名。直接问一次详情接口即可
        分辨：404 是删除，401 是转私有，307 跟随到 200 则是改名。
        463 条并发探测约 20 秒，比把三种情况混成一个数字便宜得多。

        探测失败（网络异常）不标记，留待下轮——把网络抖动写成「下架」是不可逆的脏数据。
        """
        if not missing:
            return {}

        verdicts: dict[str, Status] = {}
        with ThreadPoolExecutor(max_workers=self._probe_workers) as pool:
            for dataset_id, probe in zip(missing, pool.map(self._api.probe, missing), strict=True):
                status = STATUS_BY_VERDICT.get(probe.verdict)
                if status is not None:
                    verdicts[dataset_id] = status
        return verdicts

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
    ) -> Path:
        rows: list[dict[str, Any]] = [
            {"kind": "run", "id": "", "detail": f"stopped_reason={stopped_reason}", "note": ""}
        ]
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
