"""Platform 切片公开接口：基础设施服务。

提供:
- 数据目录入口 (DatasetStore)
- 锁管理 (exclusive, LockBusy)
- 运行记录 (Outcome, write_run)
- 保留窗口常量 (BACKUP_PRUNE_AFTER_DAYS, REPLAY_WINDOW_DAYS)
"""

from .lock import LockBusy, exclusive
from .retention import BACKUP_PRUNE_AFTER_DAYS, REPLAY_WINDOW_DAYS
from .runs import Outcome, write_run
from .store import DatasetStore

__all__ = [
    # 数据目录
    "DatasetStore",
    # 锁
    "exclusive",
    "LockBusy",
    # 运行记录
    "Outcome",
    "write_run",
    # 常量
    "BACKUP_PRUNE_AFTER_DAYS",
    "REPLAY_WINDOW_DAYS",
]
