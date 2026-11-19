"""刷新鲜：每日 00:30 起把全部例行跑一遍，09:00 是软边界。

为什么在 Python 里编排，而不是继续用 PowerShell
------------------------------------------------
锁、runs 台账、退出码、日志前缀全在 CLI 里，`_common.ps1` 又已经实现过一遍
「超时 / 墙钟 / 摘要日志」。再叠一层 `Start-Job` 只会得到两份事实。编排器只做
三件事：**排顺序、开并发、判续跑**，每一步仍是一次独立的 `hf-data-crawl` 子进程
——锁与台账由它自己管，并发段里各步骤不会互相踩。

三段式（ADR-0008）
==================
① 串行头：`compact` → `collect --full-scan` → `build --stale`
   跑完这一步，`current` 才叫「采集定稿」（DECISIONS #134）。锁外抓取的候选集
   取自 `current`，早于它启动就会漏掉当天新增的库。

② 并行段：`reconcile`（持锁全站扫）‖ `fetch-sizes` / `fetch-readme` /
   `fetch-preview`（锁外抓取）。`fetch-files` 排在 `reconcile` 之后——两者同走
   `/api` 配额（约 1000 请求/5 分钟），并行只会互相拖慢。

③ 串行尾：`export-wide`（`sizes` 合并后即跑，35 列契约只依赖它）→
   `request run-due` → `prune` → `doctor`。

续跑与边界
==========
- **续跑**：启动时读当天的 runs 台账，「命令成功过」就跳过。`collect` 只认
  `stopped_reason == "end"`——水位线早停不算全站扫完。
- **软边界**：超过 `--launch-hours`（默认 8 小时）不再启动新的抓取；已在跑的
  会自然收尾，总时长可能略超 `--budget-hours`（默认 8.5 小时）。到边界后
  `doctor` 照常跑，它是唯一能说明「今天哪里没跑完」的环节。
- **失败不重试**：某步非零退出就记失败，依赖它的步骤跳过，不相关的并行分支
  照常跑完。staging 分片保留，次日续。
"""

from __future__ import annotations

import subprocess
import sys
import time
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from ..platform.runs import Outcome, read_runs
from ..platform.store import DatasetStore

#: 整条链的时间预算（小时）。超过它只停止「还没启动」的步骤。
DEFAULT_BUDGET_HOURS = 8.5
#: 到这个点之后不再启动新的抓取，只等正在跑的收尾。
DEFAULT_LAUNCH_HOURS = 8.0
#: 子进程等锁的上限：并行段里 reconcile 持锁，锁外抓取的收尾合并要排在它后面。
LOCK_WAIT_SECONDS = 1800.0
#: 日志目录（相对数据根），与 `scripts/_common.ps1` 的 `data/logs` 是同一处。
LOG_DIR = "logs"


@dataclass(frozen=True)
class Step:
    """链条里的一步。`args` 是 `hf-data-crawl` 之后的参数。"""

    name: str
    args: tuple[str, ...]
    #: 必须先成功的步骤名。缺依赖就跳过，不带着半截状态硬跑。
    needs: tuple[str, ...] = ()


#: ① 采集定稿。`--card-pages 0` 表示卡片不设页数上限（变化 ∪ 缺口全刷）。
SERIAL_HEAD: tuple[Step, ...] = (
    Step("compact", ("compact",)),
    Step(
        "collect",
        (
            "collect",
            "--full-scan",
            "--refresh-top",
            "20000",
            "--cards",
            "--card-pages",
            "0",
            "--skip-if-done",
        ),
    ),
    Step("build", ("build", "--stale"), needs=("collect",)),
)

#: ② 并行段：对账持锁，三个抓取在锁外。各自走不同端点，限速互不干扰。
PARALLEL: tuple[Step, ...] = (
    Step("reconcile", ("reconcile",)),
    Step("sizes", ("fetch-sizes", "--changed-all")),
    Step("readme", ("fetch-readme", "--changed-all")),
    Step("preview", ("fetch-preview", "--top", "20000")),
)

#: ②-b 等 reconcile 释放锁后再跑：同走 /api 配额，并行会互相拖慢。
AFTER_RECONCILE: tuple[Step, ...] = (
    Step("files", ("fetch-files", "--top", "20000"), needs=("reconcile",)),
)

#: ③ 收尾。导出只依赖 sizes：35 列契约里的 `server_*` 四列来自 `size` 表。
SERIAL_TAIL: tuple[Step, ...] = (
    # `fetch-readme` 只抓正文：`readme_fields` 的字段要靠 `parse-readme` 派生。
    # 必须排在导出前——`paper_url` / `citation_bibtex` 就来自那张表，不解析就会
    # 静默停在旧正文上（2026-11-19 复核发现刷新链漏了这一步）。
    Step("parse", ("parse-readme",), needs=("readme",)),
    Step("export", ("export-wide",), needs=("build", "reconcile", "sizes", "parse")),
    Step("deliver", ("request", "run-due"), needs=("build", "reconcile")),
    Step("prune", ("prune",)),
    Step("doctor", ("doctor",)),
)


@dataclass
class StepResult:
    """一步的结果。`code is None` 表示没跑（今天已完成 / 依赖未满足 / 超窗口）。"""

    name: str
    code: int | None = None
    seconds: float = 0.0
    note: str = ""
    log: Path | None = None


def today_successes(store: DatasetStore) -> set[str]:
    """今天已经成功跑过的命令名。`collect` 只认「扫到末页」那次。"""
    today = date.today().isoformat()
    done: set[str] = set()
    for item in read_runs(store):
        if item.get("status") != "success":
            continue
        if str(item.get("started_at", ""))[:10] != today:
            continue
        command = str(item.get("command"))
        if command == "collect":
            payload = item.get("payload") or {}
            if payload.get("stopped_reason") != "end":
                # 水位线早停那次不是全站扫完，不算采集定稿。
                continue
        done.add(command)
    return done


def _command(step: Step, root: Path) -> list[str]:
    """子进程命令行：复用 CLI 的锁、台账与退出码，编排器不自己实现这些。

    用 `-c` 而不是 `-m hf_data_crawl.ops.cli`：包的 `__init__` 会先把 `cli` 拉进
    `sys.modules`，`-m` 随后再执行它时 runpy 会警告「found in sys.modules」——
    每步一行 stderr，日志会被噪声填满。
    """
    entry = (
        "import sys; from hf_data_crawl.ops.cli import main; raise SystemExit(main(sys.argv[1:]))"
    )
    return [
        sys.executable,
        "-c",
        entry,
        "--data-dir",
        str(root),
        "--lock-wait",
        str(int(LOCK_WAIT_SECONDS)),
        *step.args,
    ]


def run_step(step: Step, root: Path) -> StepResult:
    """跑一步，输出落到 `data/logs/<ts>-<name>.log`，终端只收摘要。"""
    started = time.monotonic()
    logs = root / LOG_DIR
    logs.mkdir(parents=True, exist_ok=True)
    log_path = logs / f"{datetime.now():%Y%m%dT%H%M%S}-refresh-{step.name}.log"
    with log_path.open("w", encoding="utf-8") as handle:
        result = subprocess.run(
            _command(step, root),
            stdout=handle,
            stderr=subprocess.STDOUT,
            check=False,
        )
    return StepResult(step.name, result.returncode, time.monotonic() - started, log=log_path)


def _plan(
    steps: tuple[Step, ...],
    done: set[str],
    failed: set[str],
    only: frozenset[str] | None = None,
) -> tuple[list[Step], list[StepResult]]:
    """把步骤分成「要跑」与「跳过」，跳过的带上原因。

    `only` 非空时是**手动补跑**模式：只跑点名的步骤，且**不看依赖**——
    `--only export` 的人知道自己在干什么，让他先拼一串前置只会把「补一步」
    变成「补整条」。代价写在 `--only` 的 help 里：导出的口径取决于底层数据
    今天是否真的更新过。
    """
    ready: list[Step] = []
    skipped: list[StepResult] = []
    for step in steps:
        if only is not None and step.name not in only:
            skipped.append(StepResult(step.name, note="不在 --only 里"))
            continue
        if step.name in done:
            skipped.append(StepResult(step.name, note="今天已完成"))
            continue
        missing: list[str] = []
        if only is None:
            missing = [need for need in step.needs if need not in done or need in failed]
        if missing:
            skipped.append(StepResult(step.name, note=f"依赖未完成：{','.join(missing)}"))
            continue
        ready.append(step)
    return ready, skipped


#: 全部步骤名，供 `--only` 校验。写在这里而不是扫描三个元组，是为了让拼错的步骤名
#: 在参数解析期就报错——`--only exprot` 静默什么都不跑，比报错坏得多。
STEP_NAMES: frozenset[str] = frozenset(
    step.name for step in (*SERIAL_HEAD, *PARALLEL, *AFTER_RECONCILE, *SERIAL_TAIL)
)


@dataclass
class Plan:
    """一次刷新跑完后的事实。"""

    results: list[StepResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)

    def by_name(self, name: str) -> StepResult | None:
        for item in self.results:
            if item.name == name:
                return item
        return None

    @property
    def failed(self) -> list[StepResult]:
        return [item for item in self.results if item.code not in (None, 0)]


def _collect_failure(plan: Plan, done: set[str], failed: set[str], result: StepResult) -> None:
    plan.results.append(result)
    if result.code == 0:
        done.add(result.name)
    else:
        failed.add(result.name)


def execute(
    store: DatasetStore,
    *,
    budget_hours: float = DEFAULT_BUDGET_HOURS,
    launch_hours: float = DEFAULT_LAUNCH_HOURS,
    only: frozenset[str] | None = None,
    log: Callable[[str], None] | None = None,
) -> Plan:
    """跑完整条链，或（`only` 非空时）只跑点名的步骤。

    `log` 是逐步摘要的输出口（默认打印）。

    `only` 是**手动补跑**模式：不读「今天已完成」台账（明确要求跑就不该被跳过）、
    不检查依赖、不套用软边界与预算——手跑一步是几分钟到几小时的事，把它当整条链
    来限时没有意义，超时由 `refresh.ps1` 的墙钟兜底。
    """
    emit = log or print
    started = time.monotonic()
    plan = Plan()
    done = today_successes(store) if only is None else set()
    failed: set[str] = set()

    def over_launch() -> bool:
        return only is None and time.monotonic() - started > launch_hours * 3600

    def over_budget() -> bool:
        return only is None and time.monotonic() - started > budget_hours * 3600

    def announce(result: StepResult) -> None:
        if result.code is None:
            emit(f"[跳过] {result.name}：{result.note}")
            return
        state = "ok" if result.code == 0 else f"退出 {result.code}"
        emit(f"[{state}] {result.name}（{result.seconds:.0f}s，日志 {result.log}）")

    # ① 串行头：采集定稿。**逐步推进**，不能一次性 `_plan`——build 依赖同批的
    # collect，一次性评估会把「还没跑的 collect」判成依赖未完成，build 直接被跳过，
    # 然后导出的依赖链整条塌掉（2026-11-19 首测就是这个形状）。
    for step in SERIAL_HEAD:
        if only is not None and step.name not in only:
            result = StepResult(step.name, note="不在 --only 里")
            plan.results.append(result)
            announce(result)
            continue
        if step.name in done:
            result = StepResult(step.name, note="今天已完成")
            plan.results.append(result)
            announce(result)
            continue
        missing = (
            []
            if only is not None
            else [need for need in step.needs if need not in done or need in failed]
        )
        if missing:
            result = StepResult(step.name, note=f"依赖未完成：{','.join(missing)}")
            plan.results.append(result)
            announce(result)
            failed.add(step.name)
            continue
        result = run_step(step, store.root)
        _collect_failure(plan, done, failed, result)
        announce(result)

    # ② 并行段：对账与三个锁外抓取同时跑；files 等 reconcile。
    ready, skipped = _plan(PARALLEL, done, failed, only)
    for result in skipped:
        plan.results.append(result)
        announce(result)

    if ready:
        if over_launch():
            for step in ready:
                result = StepResult(step.name, note="超过启动窗口，留给次日续跑")
                plan.results.append(result)
                announce(result)
        else:
            with ThreadPoolExecutor(max_workers=len(ready) + len(AFTER_RECONCILE)) as pool:
                futures: dict[Future[StepResult], Step] = {
                    pool.submit(run_step, step, store.root): step for step in ready
                }
                for future, step in list(futures.items()):
                    if step.name != "reconcile":
                        continue
                    wait([future])
                    result = future.result()
                    _collect_failure(plan, done, failed, result)
                    announce(result)
                    del futures[future]
                    # reconcile 释放锁之后，files 才能不抢 /api 配额。
                    later_ready, later_skipped = _plan(AFTER_RECONCILE, done, failed)
                    for item in later_skipped:
                        plan.results.append(item)
                        announce(item)
                    for later in later_ready:
                        futures[pool.submit(run_step, later, store.root)] = later

                for future, _step in list(futures.items()):
                    wait([future])
                    result = future.result()
                    _collect_failure(plan, done, failed, result)
                    announce(result)
                    del futures[future]

    # ③ 串行尾：导出、交付、清理、体检。
    # ③ 串行尾：**逐步推进**，理由同串行头——`export` 依赖同批的 `parse`，
    # 一次性 `_plan` 会把「还没跑的 parse」判成依赖未完成，导出与交付整条塌掉。
    for step in SERIAL_TAIL:
        if only is not None and step.name not in only:
            result = StepResult(step.name, note="不在 --only 里")
            plan.results.append(result)
            announce(result)
            continue
        if step.name in done:
            result = StepResult(step.name, note="今天已完成")
            plan.results.append(result)
            announce(result)
            continue
        missing = (
            []
            if only is not None
            else [need for need in step.needs if need not in done or need in failed]
        )
        if missing:
            result = StepResult(step.name, note=f"依赖未完成：{','.join(missing)}")
            plan.results.append(result)
            announce(result)
            failed.add(step.name)
            continue
        if step.name in {"export", "deliver"} and over_budget():
            result = StepResult(step.name, note="超过时间预算，留给次日续跑")
            plan.results.append(result)
            announce(result)
            continue
        result = run_step(step, store.root)
        _collect_failure(plan, done, failed, result)
        announce(result)

    return plan


def summary_payload(plan: Plan, budget_hours: float) -> dict[str, object]:
    """台账里记的那份摘要：每步的退出码与耗时，跳过的也记。"""
    return {
        "budget_hours": budget_hours,
        "elapsed_seconds": round((datetime.now() - plan.started_at).total_seconds(), 1),
        "steps": [
            {
                "name": item.name,
                "code": item.code,
                "seconds": round(item.seconds, 1),
                "note": item.note,
            }
            for item in plan.results
        ],
        "failed": [item.name for item in plan.failed],
    }


def run(args, store: DatasetStore) -> Outcome:
    """`refresh` 子命令入口。"""
    only: frozenset[str] | None = None
    if getattr(args, "only", None):
        names = frozenset(item.strip() for item in args.only.split(",") if item.strip())
        unknown = sorted(names - STEP_NAMES)
        if unknown:
            raise SystemExit(
                f"未知步骤名：{', '.join(unknown)}；可选：{', '.join(sorted(STEP_NAMES))}"
            )
        only = names
    plan = execute(
        store,
        budget_hours=args.budget_hours,
        launch_hours=args.launch_hours,
        only=only,
    )
    # 体检非零只代表**有告警**（导出落后、Raw 断日这类待办也计入非零），不让它
    # 拖垮整条链——告警与故障挤进同一个退出码，铃声很快就没人听。详情在
    # `data/ALERT.txt`。这条与旧 daily.ps1 的 `Report = $true` 是同一判断。
    failures = [item for item in plan.failed if item.name != "doctor"]
    payload = summary_payload(plan, args.budget_hours)
    if failures:
        reason = "、".join(f"{item.name}(退出 {item.code})" for item in failures)
        return Outcome(payload, code=1, reason=f"刷新鲜未全部成功：{reason}")
    return Outcome(payload)
