"""全量对账：按 createdAt 稳定序扫全站，产出新增 / 下架 / sha 变更三类差异。

铁律：只有「扫到末页」才允许判定下架。有界扫描（max_pages）属于没扫完，
此时若照常判定，会把整站未扫到的部分全标成下架——这是不可逆的误判。
"""

from __future__ import annotations

import csv
import time
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from .api import MAX_LIMIT, HfDatasetApi
from .checkpoint import FileCheckpoint
from .collect import LEAN_EXPAND
from .store import DatasetStore

RECONCILE_CHECKPOINT = "reconcile-checkpoint.json"

REPORT_SCHEMA: dict[str, pl.DataType] = {
    "kind": pl.Utf8,
    "id": pl.Utf8,
    "detail": pl.Utf8,
}


@dataclass(frozen=True)
class ReconcileSummary:
    snapshot_date: date
    pages: int
    records: int
    new_ids: int
    disappeared: int
    sha_changed: int
    reactivated: int
    report_path: Path | None
    stopped_reason: str
    elapsed_seconds: float


class Reconciler:
    """全量核对入口。"""

    def __init__(
        self,
        api: HfDatasetApi,
        store: DatasetStore,
        *,
        report_dir: Path | None = None,
        checkpoint_filename: str = RECONCILE_CHECKPOINT,
    ) -> None:
        self._api = api
        self._store = store
        self._report_dir = report_dir or (store.root / "reports")
        self._checkpoint_filename = checkpoint_filename

    def run(
        self,
        snapshot_date: date,
        *,
        limit: int = MAX_LIMIT,
        max_pages: int | None = None,
    ) -> ReconcileSummary:
        started = time.monotonic()
        checkpoint = FileCheckpoint(self._store.state_dir / self._checkpoint_filename)

        before = self._store.read_current()
        before_ids = set(before["id"].to_list())
        before_sha = dict(zip(before["id"].to_list(), before["sha"].to_list()))
        before_status = dict(zip(before["id"].to_list(), before["status"].to_list()))

        # 已扫描过的 id 必须与游标一同持久化：只存游标的话，续跑后「见过谁」是空的，
        # 前半段会被整片误判为下架（实测一次就误杀了 4.6 万条）。
        session_path = self._store.state_dir / f"reconcile-seen-{snapshot_date.isoformat()}.csv"
        if checkpoint.cursor is None:
            session_path.unlink(missing_ok=True)

        pages = 0
        records = 0
        stopped_reason = "end"

        for current_page in self._api.scan(
            sort="createdAt",
            direction="-1",
            limit=limit,
            expand=LEAN_EXPAND,
            checkpoint=checkpoint,
        ):
            pages += 1
            records += len(current_page.records)
            self._store.write_raw(
                snapshot_date=snapshot_date,
                page_index=pages,
                records=current_page.records,
                meta={"sort": "createdAt", "direction": "-1", "mode": "reconcile"},
            )
            self._append_session(session_path, current_page.records)
            checkpoint.save(current_page.next_cursor)

            if max_pages is not None and pages >= max_pages:
                stopped_reason = "max_pages"
                break

        self._store.build_bronze(snapshot_date)
        self._store.build_silver(snapshot_date)
        self._store.update_state(snapshot_date)

        seen, seen_sha = self._load_session(session_path)

        new_ids = sorted(seen - before_ids)
        reactivated = sorted(
            dataset_id for dataset_id in seen if before_status.get(dataset_id) == "disappeared"
        )
        sha_changed = sorted(
            dataset_id
            for dataset_id in seen & before_ids
            if before_sha.get(dataset_id) != seen_sha.get(dataset_id)
        )

        disappeared: list[str] = []
        if stopped_reason == "end":
            disappeared = sorted(before_ids - seen)
            self._store.mark_disappeared(disappeared)
            checkpoint.save(None)
            session_path.unlink(missing_ok=True)

        report_path = self._write_report(
            snapshot_date,
            stopped_reason,
            new_ids,
            disappeared,
            sha_changed,
            reactivated,
        )

        return ReconcileSummary(
            snapshot_date=snapshot_date,
            pages=pages,
            records=records,
            new_ids=len(new_ids),
            disappeared=len(disappeared),
            sha_changed=len(sha_changed),
            reactivated=len(reactivated),
            report_path=report_path,
            stopped_reason=stopped_reason,
            elapsed_seconds=time.monotonic() - started,
        )

    @staticmethod
    def _append_session(path: Path, records: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not path.exists()
        with path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if write_header:
                writer.writerow(["id", "sha"])
            for record in records:
                writer.writerow([record["id"], record.get("sha") or ""])

    @staticmethod
    def _load_session(path: Path) -> tuple[set[str], dict[str, str | None]]:
        if not path.exists():
            return set(), {}
        frame = pl.read_csv(
            path,
            schema={"id": pl.Utf8, "sha": pl.Utf8},
            truncate_ragged_lines=True,
        )
        ids = frame["id"].to_list()
        shas = frame["sha"].to_list()
        return set(ids), dict(zip(ids, [item or None for item in shas]))

    def _write_report(
        self,
        snapshot_date: date,
        stopped_reason: str,
        new_ids: list[str],
        disappeared: list[str],
        sha_changed: list[str],
        reactivated: list[str],
    ) -> Path:
        rows: list[dict[str, Any]] = [
            {"kind": "run", "id": "", "detail": f"stopped_reason={stopped_reason}"}
        ]
        rows += [{"kind": "new", "id": item, "detail": ""} for item in new_ids]
        rows += [{"kind": "disappeared", "id": item, "detail": ""} for item in disappeared]
        rows += [{"kind": "sha_changed", "id": item, "detail": ""} for item in sha_changed]
        rows += [{"kind": "reactivated", "id": item, "detail": ""} for item in reactivated]

        self._report_dir.mkdir(parents=True, exist_ok=True)
        path = self._report_dir / f"reconcile-{snapshot_date.isoformat()}.csv"
        pl.DataFrame(rows, schema=REPORT_SCHEMA).write_csv(path)
        return path
