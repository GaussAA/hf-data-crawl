"""运行摘要落盘：每轮一条 JSON，便于连续两轮对比、失败留痕。

失败必须写、且要带原因——静默失败比跑得慢更糟。
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .store import DatasetStore

#: runs/ 保留的最近运行记录条数。按目前每天十余条，500 条约一个多月；更早的
#: 细节已没有对比价值。上限存在的意义是防止目录随计划任务无限增长。
RUNS_KEEP = 500

#: collect 的完成态停止原因。`--skip-if-done` 与 doctor 的完成检查共用这一份枚举，
#: 避免新增停止原因时两处各自维护、判定脱节。`watermark` 是历史 run 记录里的旧值。
COLLECT_COMPLETE_REASONS = frozenset(
    {"end", "watermark", "watermark_timestamp", "watermark_no_new"}
)


@dataclass(frozen=True)
class Outcome:
    """一条命令的结局。

    **成败只有一个判定点**：`failed` 由 `code` 推出，调用方不再各自声明 status。
    此前 `report` 在无数据时返回 1、`runs/` 里却写着 success——同一条命令的结局
    被劈成两半，没有任何一处能回答「这轮到底成功了吗」。
    """

    payload: Any = None
    code: int = 0
    reason: str | None = None

    @property
    def failed(self) -> bool:
        return self.code != 0


def _scrub(reason: str | None, root: Path) -> str | None:
    """台账落盘前把本机绝对路径换成占位。

    `reason` 直接来自异常消息，而 `FileNotFoundError` 这类会带上完整路径
    （`C:\\WorkSpace\\...\\data\\state\\current.parquet`）。台账是机读面，也是
    排障时会被贴进 issue 的东西，本机路径对它没有信息量——`cli.py` 里那句
    「台账保持机读，不夹带本机路径」说的就是这件事，但只在注释里，异常消息照旧
    原样写了进去。
    """
    if not reason:
        return reason
    for needle in (str(root.resolve()), str(root)):
        # 太短的相对名（如 `data`）换成占位会误伤正常词句，只替换真正的绝对路径。
        if len(needle) > 8 and needle in reason:
            reason = reason.replace(needle, "<data>")
    return reason


def write_run(
    store: DatasetStore,
    command: str,
    started_at: datetime,
    outcome: Outcome,
) -> Path | None:
    runs_dir = store.root / "runs"
    try:
        runs_dir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        print(f"[警告] 运行台账目录无法创建（不影响命令结果）：{error}")
        return None
    stamp = started_at.strftime("%Y%m%dT%H%M%S")
    path = runs_dir / f"{stamp}-{command}.json"
    body = {
        "command": command,
        # status 由 outcome 推出，不接受外部传入：可传的 status 一定有传错的那天。
        "status": "failed" if outcome.failed else "success",
        "reason": _scrub(outcome.reason, store.root),
        # 精度到微秒：同一秒内连跑两轮时，秒级时间戳无法区分先后，
        # 而「连续两轮对比」正是靠这个顺序成立的。
        "started_at": started_at.isoformat(timespec="microseconds"),
        "finished_at": datetime.now().isoformat(timespec="microseconds"),
        "payload": _jsonable(outcome.payload),
    }
    content = json.dumps(body, ensure_ascii=False, indent=2)

    # 同一秒内连跑两条会撞名。旧写法先 `path.exists()` 再 `write_text`（内部
    # `os.replace`）：两个进程同秒启动时双双判「不存在」，后写的那份静默盖掉
    # 先写的，台账少一条而 `_collected_today` / 体检都看不出来。`x` 是原子的
    # 独占创建，抢不到名字就换序号重试；几百字节一次写完、`read_runs` 对坏行
    # 已有容错，故此处刻意不用会覆盖目标的 `os.replace`。
    suffix = 1
    while True:
        try:
            with path.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(content)
            break
        except FileExistsError:
            suffix += 1
            path = runs_dir / f"{stamp}-{command}-{suffix}.json"
        except OSError as error:
            # 台账写不进去是「少了一份记录」，不是「命令失败了」：数据已经落盘，
            # 不能因为记录写失败就让一次成功的 collect 以异常栈收场、退出码变成 1。
            # 原先异常直接穿过 `_execute`（`write_run` 在它 try 之外），后果是
            # 命令成功了却报失败、且台账里查不到这次运行。降级为告警。
            print(f"[警告] 运行台账写入失败（不影响命令结果）：{error}")
            return None
    # 清理时机：每写一条就检查一次上限。这里不做「超过很多再批量清」——
    # 一次多解析几百个小 JSON 的代价可以忽略，却能让目录大小始终有界。
    prune_runs(store, RUNS_KEEP)
    return path


def prune_runs(store: DatasetStore, keep: int = RUNS_KEEP) -> list[Path]:
    """只保留最近 `keep` 条运行摘要，返回被删除的文件。

    排序键与 `read_runs` 一致，取记录里的 `started_at` / `finished_at`，**不按
    文件名**：同一秒内第二轮的文件名 `...-build-2.json` 字典序反而在前。
    读不出来的损坏记录按最旧处理，优先清掉。
    """
    if keep <= 0:
        raise ValueError("keep 必须为正数")
    runs_dir = store.root / "runs"
    if not runs_dir.exists():
        return []
    paths = list(runs_dir.glob("*.json"))
    if len(paths) <= keep:
        return []

    entries: list[tuple[tuple[str, str, str], Path]] = []
    for path in paths:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
            key = (
                str(record.get("started_at") or ""),
                str(record.get("finished_at") or ""),
                path.name,
            )
        except (json.JSONDecodeError, OSError):
            key = ("", "", path.name)
        entries.append((key, path))

    entries.sort(key=lambda item: item[0])
    stale = [path for _, path in entries[:-keep]]
    for path in stale:
        with contextlib.suppress(OSError):
            path.unlink()
    return stale


def _jsonable(value: Any) -> Any:
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        # asdict 会递归展开，但 Path/date 仍需兜底转字符串
        return json.loads(json.dumps(dataclasses.asdict(value), default=str))
    return json.loads(json.dumps(value, default=str))


def read_runs(store: DatasetStore) -> list[dict[str, Any]]:
    """按时间升序读取全部运行摘要。

    排序依据是记录里的 `started_at`，**不是文件名**：同一秒内的第二轮会被写成
    `...-build-2.json`，而 `-` 的字典序排在 `.` 之前，按文件名排会把后一轮
    排到前一轮之前——「连续两轮对比」于是读到颠倒的一对。
    """
    runs_dir = store.root / "runs"
    if not runs_dir.exists():
        return []
    records: list[dict[str, Any]] = []
    for path in sorted(runs_dir.glob("*.json")):
        try:
            records.append(json.loads(path.read_text(encoding="utf-8")))
        except (json.JSONDecodeError, OSError):
            # 单条损坏不影响其余摘要的读取
            continue
    records.sort(
        key=lambda item: (str(item.get("started_at") or ""), str(item.get("finished_at") or ""))
    )
    return records
