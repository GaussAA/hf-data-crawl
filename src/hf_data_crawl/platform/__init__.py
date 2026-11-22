"""Platform 切片公开接口：基础设施服务。

这里登记的是**跨切片契约**，不是「这层有什么」的目录。判断标准只有一条：
别的切片真的 import 了它。子模块内部的其余名字仍可自由改，因为没有外部依赖。

分层（由下至上）：atomic / tables / vocab / coerce / env / presets 是叶子，
store 建立在 atomic 与 tables 之上，runs / retention / lock 建立在 store 之上。

`from ..platform import write_bytes` 与 `from ..platform.atomic import write_bytes`
取到的是同一个对象；门面不转发、不包装，只登记。这样「谁能依赖什么」有据可查，
而调用点仍保留「这个名字出自哪一层」的信息。
"""

from .atomic import replace, write_bytes, write_csv, write_parquet, write_text
from .coerce import as_bool, as_datetime, as_float, as_int, as_json, as_list, as_text
from .deadline import DeadlineError, deadline_epoch, resolve_deadline
from .env import load_env_file
from .lock import LockBusy, exclusive, read_holder
from .presets import DEFAULT_PRESET_NAME, get_preset
from .retention import (
    ARCHIVE_AFTER_DAYS,
    BACKUP_KEEP_DAYS,
    BACKUP_PRUNE_AFTER_DAYS,
    COLD_BASELINE_GENERATIONS,
    COLD_SUBDIR,
    REPLAY_WINDOW_DAYS,
    backup_raw_dir,
    cold_dir,
    manifest_path,
    read_compressed_dates,
)
from .runs import (
    COLLECT_COMPLETE_REASONS,
    FULL_SCAN,
    Outcome,
    last_success,
    read_runs,
    write_run,
)
from .store import DatasetStore
from .tables import (
    BRONZE_SCHEMA,
    BRONZE_SOURCE_FIELDS,
    CARD_CONFIGS_SCHEMA,
    CURRENT_SCHEMA,
    DATASET_URL,
    DERIVED_DAY_TABLES,
    ENRICH_SCHEMA,
    FILES_SCHEMA,
    PREVIEW_SCHEMA,
    README_FIELDS_SCHEMA,
    README_SCHEMA,
    SILVER_SCHEMA,
    SIZE_SCHEMA,
    TABLES,
    TIMESTAMP,
    Origin,
    PolarsType,
    TableSpec,
    table,
    url_for,
)
from .vocab import INACTIVE, Status

__all__ = [
    # 数据目录：全仓唯一的数据根入口
    "DatasetStore",
    # 原子写：所有落盘都必须经这里
    "replace",
    "write_bytes",
    "write_csv",
    "write_parquet",
    "write_text",
    # 表契约：逻辑表名、schema、URL 拼接
    "TABLES",
    "table",
    "url_for",
    "TableSpec",
    "Origin",
    "PolarsType",
    "TIMESTAMP",
    "DATASET_URL",
    "BRONZE_SCHEMA",
    "BRONZE_SOURCE_FIELDS",
    "SILVER_SCHEMA",
    "CURRENT_SCHEMA",
    "ENRICH_SCHEMA",
    "CARD_CONFIGS_SCHEMA",
    "SIZE_SCHEMA",
    "FILES_SCHEMA",
    "PREVIEW_SCHEMA",
    "README_SCHEMA",
    "README_FIELDS_SCHEMA",
    "DERIVED_DAY_TABLES",
    # 标量收敛：把接口回来的任意 JSON 值收成确定的类型
    "as_int",
    "as_float",
    "as_bool",
    "as_text",
    "as_list",
    "as_json",
    "as_datetime",
    # 状态词表
    "Status",
    "INACTIVE",
    # 运行台账
    "Outcome",
    "write_run",
    "read_runs",
    "COLLECT_COMPLETE_REASONS",
    "FULL_SCAN",
    "last_success",
    # 保留窗口与冷区
    "REPLAY_WINDOW_DAYS",
    "ARCHIVE_AFTER_DAYS",
    "BACKUP_KEEP_DAYS",
    "BACKUP_PRUNE_AFTER_DAYS",
    "COLD_BASELINE_GENERATIONS",
    "COLD_SUBDIR",
    "cold_dir",
    "backup_raw_dir",
    "manifest_path",
    "read_compressed_dates",
    # 锁
    "exclusive",
    "LockBusy",
    "read_holder",
    # 采集预设与 .env
    "DEFAULT_PRESET_NAME",
    "get_preset",
    "load_env_file",
    # 收工时刻（`--until`）
    "resolve_deadline",
    "deadline_epoch",
    "DeadlineError",
]
