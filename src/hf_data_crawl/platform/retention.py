"""Raw 保留策略的单一真相源。

为什么单独成模块
----------------
此前「7 天 / 30 天」这几个数字散在 `cli archive --older-than-days`、`doctor`
的告警、`rebuild` 的重放范围里，各写一份。改一处不改另一处不会报错，只会
让「build 重放的日期」与「doctor 认为该压缩的日期」悄悄错开——这类漂移只有
等数据攒够才会显形。

三层语义
--------
- **热区**（近 `REPLAY_WINDOW_DAYS` 天）：`data/raw/dt=*` 原样保留，`build`
  默认按天重放，写入序由 `written_at` 或归档分片的 `groups` 冻结。
- **冷区权威层**：`silver` / `observations` / `current` 的历史分区不再清空，
  它们是历史事实来源（属性与指标本就「变化才追加」）。
- **冷区 Raw**：由 `compact` 跨天去重压成一份基线，存 `data/cold/raw/`；
  原分块移入 `data/backup/raw/` **不删除**，可逆，也是 `build --full` 的逃生舱。

`manifest.json` 记录哪些日期已压缩：既供 `doctor` 判断断日（部分压缩时残留的早分区会把检查窗口拉长，
已压缩的日子不登记就被误判成「那天没采集」），也供回滚时按图索骥。
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .store import DatasetStore

#: `build` 默认只重放最近这些天；超过即视为冷区，不再逐日重放。
REPLAY_WINDOW_DAYS = 7
#: `archive` 命令的默认门槛（同天去重、冻结写入序）。compact 会先移走冷区，
#: 因此 30 天前仍留在 data/raw 的分区说明治理没跟上。
ARCHIVE_AFTER_DAYS = 30
#: 归档/压缩只移不删；backup 超过这些天未人工清理，磁盘就没有真正释放。
BACKUP_PRUNE_AFTER_DAYS = 7

COLD_SUBDIR = ("cold", "raw")
BACKUP_SUBDIR = ("backup", "raw")
MANIFEST_NAME = "manifest.json"


def cold_dir(store: DatasetStore) -> Path:
    return store.root.joinpath(*COLD_SUBDIR)


def backup_raw_dir(store: DatasetStore, snapshot_date: date) -> Path:
    return store.root.joinpath(*BACKUP_SUBDIR, f"dt={snapshot_date.isoformat()}")


def manifest_path(store: DatasetStore) -> Path:
    return cold_dir(store) / MANIFEST_NAME


def read_compressed_dates(store: DatasetStore) -> set[date]:
    """已压缩进冷区基线的日期；档案缺失或损坏按「没有」处理。

    体检是只读诊断，不能因为一份坏档案就崩掉——宁可少算几天、由断日告警
    把问题暴露出来，也不要让 `doctor` 自己成为故障点。
    """
    path = manifest_path(store)
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        return {date.fromisoformat(item) for item in payload["compressed_dates"]}
    except (OSError, ValueError, TypeError, KeyError):
        return set()
