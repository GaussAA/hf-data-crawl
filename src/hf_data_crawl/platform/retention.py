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

#: `prune` 实际使用的**判龄阈值**。
#:
#: 为什么不是 `BACKUP_PRUNE_AFTER_DAYS` 本身：备份目录名记的是「数据日期」而不是
#: 「搬入日期」，而 `compact` 只压早于 `today - REPLAY_WINDOW_DAYS` 的分区——一份
#: 备份落地那一刻，它的数据日期就已经有 8 天了。若清理阈值也取 7，同一晚 00:30
#: 刚搬进 `backup/raw/` 的分区会被 03:10 的 `prune` 立刻删掉，备份的实际寿命只有
#: 2.6 小时，「留出回滚余量」名存实亡（2026-11-16 审计实测）。
#:
#: 判龄阈值 = 想保留的天数 + `compact` 的窗口。两处共用这一个常量：`prune.plan`
#: 的默认值，与 `doctor._backup_backlog` 的文案都从它取。
BACKUP_KEEP_DAYS = BACKUP_PRUNE_AFTER_DAYS + REPLAY_WINDOW_DAYS

COLD_SUBDIR = ("cold", "raw")
BACKUP_SUBDIR = ("backup", "raw")
MANIFEST_NAME = "manifest.json"

#: 冷区基线的文件名前缀。不带后缀：新基线是 `.json.zst`、历史基线是 `.json.gz`。
#:
#: `compact` 用它扫当前基线、`prune` 用它扫 `backup/cold` 的旧基线。各写一份
#: 字面量的话，改了命名只有一处跟着变，另一处会**静默**看不见文件——2026-11-21
#: 的体积审计就是这么发现的：`prune` 只匹配 `*.gz`，`.zst` 旧基线全部漏网，
#: 一年多攒到 1.58 GB。
BASELINE_PREFIX = "baseline-"

#: `backup/cold` 保留的**代**数。一代 = 一次 `compact` 搬入的整批旧基线。
#:
#: 为什么不按天数：一代约 150 MB 且代际内容高度重合（都是「全量去重后的旧态」），
#: 留 14 天等于留 14 份几乎相同的快照。回滚只需「上一版」，留 2 代即
#: 「当前 + 上一代 + 上上代」，够用且不再每天涨 175 MB。
COLD_BASELINE_GENERATIONS = 2

#: 同代分片由一次 `compact` 连续写出，不同代之间至少隔一天。
#: 实测 11-11~11-20 九代：代内最大间隔 6.9 秒、代间最小 23 小时；取 1 小时，
#: 两侧余量都够。误聚只可能把两代并成一代，那只会少删，不会误删。
COLD_GENERATION_GAP_SECONDS = 3600.0


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
