"""对账会话：游标、已见集合、基线，同键、同生命周期。

为什么需要它
------------
一次全量对账可能分多段跑完（有界停止后续跑）。它依赖三样东西，原本各管各的：

- **游标**：下一页从哪开始。按一份固定文件名存。
- **已见集合**：前半段见过谁。按 `snapshot_date` 命名——隔天续跑时游标还在、
  已见集合却是空的，前半段全被当成「没扫到」送去探测，旧文件还成了孤儿。
- **基线**：「相对什么算新增 / sha 变更 / 恢复」。原本每次 `run()` 现读 current，
  而有界停止那一轮也会更新 current，续跑时基线已含上半段，差异被悄悄低估
  （实测：单次跑 new=2、sha_changed=1，分段跑只剩 new=1、sha_changed=0）。

三者必须一起开、一起续、一起删，所以由本 module 拥有。调用方只做四件事：
`open` / `remember` / `diff` / `close`。

基线固定在会话开始那一刻。会话期间若有别的命令往 current 里加了库，
这些库在对账里被扫到时算「新增」——这是「相对会话开始时」的定义，不另做处理。

旧会话
------
有游标、却没有基线（或已见集合）的，是旧版本留下的会话，基线无法还原；
续跑只会重现旧缺陷，故丢弃并从头开始，`discarded` 标出这件事让调用方告知用户。
"""

from __future__ import annotations

import csv
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import polars as pl

from .atomic import write_parquet
from .checkpoint import FileCheckpoint
from .status import INACTIVE
from .store import DatasetStore

BASELINE_FILENAME = "reconcile-baseline.parquet"
SEEN_FILENAME = "reconcile-seen.csv"

#: 旧格式的已见集合（按日期命名）。启动时清掉，不留孤儿。
LEGACY_SEEN_GLOB = "reconcile-seen-*.csv"

BASELINE_SCHEMA: dict[str, pl.DataType] = {
    "id": pl.Utf8,
    "sha": pl.Utf8,
    "status": pl.Utf8,
}


@dataclass(frozen=True)
class SetDiff:
    """基线与已见集合的差。"""

    #: 已见、基线里没有。
    new: list[str]
    #: 两边都有、sha 不同。
    sha_changed: list[str]
    #: 已见、且基线里是非在架状态（扫描直接证明它回来了）。
    reactivated: list[str]
    #: 基线里有、没见到。是否真下架要靠探测，且只有扫到末页才可判。
    missing: list[str]


def diff_sets(
    baseline: Mapping[str, tuple[str | None, str]],
    seen: Mapping[str, str | None],
) -> SetDiff:
    """集合差的纯函数：基线 id → (sha, status)，已见 id → sha。"""
    known = set(baseline)
    seen_ids = set(seen)
    return SetDiff(
        new=sorted(seen_ids - known),
        sha_changed=sorted(i for i in seen_ids & known if baseline[i][0] != seen[i]),
        reactivated=sorted(i for i in seen_ids & known if baseline[i][1] in INACTIVE),
        missing=sorted(known - seen_ids),
    )


class ReconcileSession:
    """一次（可能分段的）全量对账的持久状态。"""

    def __init__(
        self,
        *,
        checkpoint: FileCheckpoint,
        baseline: dict[str, tuple[str | None, str]],
        baseline_path: Path,
        seen_path: Path,
        resumed: bool,
        discarded: bool,
    ) -> None:
        self._checkpoint = checkpoint
        self._baseline = baseline
        self._baseline_path = baseline_path
        self._seen_path = seen_path
        self._resumed = resumed
        self._discarded = discarded

    @classmethod
    def open(cls, store: DatasetStore, *, checkpoint_filename: str) -> ReconcileSession:
        state = store.state_dir
        baseline_path = state / BASELINE_FILENAME
        seen_path = state / SEEN_FILENAME
        checkpoint = FileCheckpoint(state / checkpoint_filename)

        for legacy in state.glob(LEGACY_SEEN_GLOB):
            legacy.unlink(missing_ok=True)

        resumed = checkpoint.cursor is not None
        discarded = False
        if resumed and not (baseline_path.exists() and seen_path.exists()):
            # 旧会话：基线无法还原，丢弃重来。
            checkpoint.save(None)
            resumed = False
            discarded = True

        if resumed:
            baseline = _load_baseline(baseline_path)
        else:
            # 先固定基线，再清已见集合：中途被打断，下次仍会重新开始，不会拿着
            # 新基线配旧的已见集合。
            frame = store.read_current().select(list(BASELINE_SCHEMA))
            write_parquet(frame, baseline_path)
            seen_path.unlink(missing_ok=True)
            baseline = _to_baseline(frame)

        return cls(
            checkpoint=checkpoint,
            baseline=baseline,
            baseline_path=baseline_path,
            seen_path=seen_path,
            resumed=resumed,
            discarded=discarded,
        )

    @property
    def checkpoint(self) -> FileCheckpoint:
        return self._checkpoint

    @property
    def resumed(self) -> bool:
        return self._resumed

    @property
    def discarded(self) -> bool:
        return self._discarded

    def status_before(self, dataset_id: str) -> str | None:
        """会话开始时该库的状态；基线里没有则为 None。"""
        entry = self._baseline.get(dataset_id)
        return entry[1] if entry else None

    def remember(self, records: list[dict[str, Any]]) -> None:
        """追加本页见过的 id 与 sha。

        已见集合是集合：同一页重复追加无害，所以调用方可以放心把它排在保存游标
        之前——被打断时最坏是多记一页，而不是少记一页。
        """
        self._seen_path.parent.mkdir(parents=True, exist_ok=True)
        write_header = not self._seen_path.exists()
        with self._seen_path.open("a", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            if write_header:
                writer.writerow(["id", "sha"])
            for record in records:
                writer.writerow([record["id"], record.get("sha") or ""])

    def seen(self) -> dict[str, str | None]:
        """已见 id → sha。"""
        if not self._seen_path.exists():
            return {}
        frame = pl.read_csv(
            self._seen_path,
            schema={"id": pl.Utf8, "sha": pl.Utf8},
            truncate_ragged_lines=True,
        )
        return {
            dataset_id: sha or None
            for dataset_id, sha in zip(frame["id"].to_list(), frame["sha"].to_list(), strict=True)
        }

    def diff(self) -> SetDiff:
        return diff_sets(self._baseline, self.seen())

    def close(self) -> None:
        """会话完成：游标归零，基线与已见集合一起删除。"""
        self._checkpoint.save(None)
        self._baseline_path.unlink(missing_ok=True)
        self._seen_path.unlink(missing_ok=True)


def _to_baseline(frame: pl.DataFrame) -> dict[str, tuple[str | None, str]]:
    return {
        dataset_id: (sha, status)
        for dataset_id, sha, status in zip(
            frame["id"].to_list(),
            frame["sha"].to_list(),
            frame["status"].to_list(),
            strict=True,
        )
    }


def _load_baseline(path: Path) -> dict[str, tuple[str | None, str]]:
    return _to_baseline(pl.read_parquet(path))
