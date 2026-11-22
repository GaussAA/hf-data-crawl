"""刷新鲜：每日 00:30 起把全部例行跑一遍，09:00 是软边界。

为什么在 Python 里编排，而不是继续用 PowerShell
------------------------------------------------
锁、runs 台账、退出码、日志前缀全在 CLI 里，`_common.ps1` 又已经实现过一遍
「超时 / 墙钟 / 摘要日志」。再叠一层 `Start-Job` 只会得到两份事实。编排器只做
三件事：**排顺序、开并发、判续跑**，每一步仍是一次独立的 `hf-data-crawl` 子进程
——锁与台账由它自己管，并发段里各步骤不会互相踩。

四段式（ADR-0008；④ 于 2026-11-23 增补）
==========================================
① 串行头：`compact` → `collect --full-scan` → `build --stale`
   跑完这一步，`current` 才叫「采集定稿」（DECISIONS #134）。锁外抓取的候选集
   取自 `current`，早于它启动就会漏掉当天新增的库。

② 并行段：`reconcile`（持锁全站扫）‖ `fetch-sizes` / `fetch-readme`（锁外抓取）。
   三段各走不同端点，限速互不干扰。

③ 串行尾：`export-wide`（`sizes` 合并后即跑，契约里的 `server_*` 只依赖它）→
   `request run-due` → `archive` → `prune` → `doctor`。

④ 兜底段：`fetch-files` ‖ `fetch-preview`，`--top 0` 不限名额，`--until 00:20` 收工。

`fetch-files` 与 `fetch-preview` 曾在 2026-11-20 被移出链：两者都是「按下载量取头部」
的富化资产，全量一轮要几百 GB 下载与几十小时，塞进每日窗口只会挤掉采集本身。当时
的处理是改成**纯手工**。2026-11-23 以**末段兜底**的方式挂回来——位置在整条链之后，
所以永远不挤采集；`--top 0` 表达「有多少剩余时间就填多少」；`--until 00:20` 钉死
收工点，赶在次日 00:30 的 `collect` 之前让出同一个 `api` 桶。抓满之后缺口为空，
`--top 0` 自然退化成「只刷陈旧」，与 `sizes` / `readme` 的日常增量是同一个语义，
不必再加开关。

这一段**不套启动窗口**（见 `execute` 里的说明）：链跑得越久它越晚开始，而那正是
它存在的意义。

续跑与边界
==========
- **续跑**：启动时读当天的 runs 台账，「命令成功过」就跳过。`collect` 只认
  `stopped_reason == "end"`——水位线早停不算全站扫完。
- **依赖封闭**：「今天已完成」只在它**每个前置今天也完成、且完成时刻不晚于自己**时
  成立。台账记不住一条命令用的是哪一版数据——白天手动跑过 `build`/`export`、夜里才补
  采集时，若不收缩，导出会停在旧采集上，而摘要只写一行「今天已完成」。
  光比名字不够：2026-11-20 的 `sizes` 00:32 跑完、`collect` 23:08 才定稿，两者都
  「今天完成」，但前者用的是前一天的底库。所以还要比时刻（见 `_dependency_closed`）。
- **软边界**：超过 `--launch-hours`（默认 8 小时）不再启动新的抓取；已在跑的
  会自然收尾，总时长可能略超 `--budget-hours`（默认 8.5 小时）。到边界后
  `doctor` 照常跑，它是唯一能说明「今天哪里没跑完」的环节。
- **失败不重试**：某步非零退出就记失败，依赖它的步骤跳过，不相关的并行分支
  照常跑完。staging 分片保留，次日续。
"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from collections.abc import Callable, Iterable
from concurrent.futures import Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path

from ..platform.runs import FULL_SCAN, Outcome, last_success, read_runs
from ..platform.store import DatasetStore

#: 整条链的时间预算（小时）。超过它只停止「还没启动」的步骤。
DEFAULT_BUDGET_HOURS = 8.5
#: 到这个点之后不再启动新的抓取，只等正在跑的收尾。
DEFAULT_LAUNCH_HOURS = 8.0

#: 兜底段的收工钟点（次日）。
#:
#: 必须早于次日 00:30 的链起点：`collect --full-scan` 打 `huggingface.co/api`，
#: 与 `fetch-files` 是**同一个桶**（1000 次 / 300 秒）。两个进程各持一份节流额度，
#: 合起来就是两倍超发，双方一起 429、各自退避几十秒——2026-11-22 实测过这个形状。
#: 留 10 分钟，是给收尾合并的短锁（它要把几千个分片并进 `enrich/files.parquet`）。
FILL_UNTIL = "00:20"
#: 子进程等锁的上限：并行段里 reconcile 持锁，锁外抓取的收尾合并要排在它后面。
LOCK_WAIT_SECONDS = 1800.0
#: 日志目录（相对数据根），与 `scripts/_common.ps1` 的 `data/logs` 是同一处。
LOG_DIR = "logs"


@dataclass(frozen=True)
class Step:
    """链条里的一步。`args` 是 `hf-data-crawl` 之后的参数。

    `name` 是编排器内部用的短名（`--only` 收它、`needs` 写它）；`ledger` 才是
    `runs/` 台账里那条记录的 `command`。**两者必须分开**：台账记的是真正执行的
    子命令（`fetch-sizes` / `export-wide` / `request-run-due`），拿短名去比对
    永远比不上，续跑就退化成「每天从头再跑一遍」——而输出里看不出任何异常。
    """

    name: str
    args: tuple[str, ...]
    #: 必须先成功的步骤名。缺依赖就跳过，不带着半截状态硬跑。
    needs: tuple[str, ...] = ()
    #: 台账里的 `command`。留空表示与 `name` 相同；不同时必须显式写出。
    command: str = ""

    @property
    def ledger(self) -> str:
        """这一步写进 `runs/` 的命令名。"""
        return self.command or self.name


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

#: ② 并行段：对账持锁，两个抓取在锁外。各自走不同端点，限速互不干扰。
#:
#: 两个抓取都 `needs=("build",)`：它们的**候选集来自 `current`**（陈旧 = 内容变过的、
#: 缺口 = 还没有记录的），而 `current` 由 `collect` + `build` 定稿。不写这条依赖，
#: 它们就只是「今天跑过」——2026-11-20 实际发生：00:32 按前一天的底库刷了 163 个
#: 规模 / 6,202 个 README，23:08 全站采集才定稿，摘要里这两步却写着「今天已完成」，
#: 于是当晚新采到与刚变更的库，一个都没轮到。
PARALLEL: tuple[Step, ...] = (
    Step("reconcile", ("reconcile",)),
    Step("sizes", ("fetch-sizes", "--changed-all"), needs=("build",), command="fetch-sizes"),
    Step("readme", ("fetch-readme", "--changed-all"), needs=("build",), command="fetch-readme"),
)

#: ③ 收尾。导出紧邻的前置只有 sizes：契约里的 `server_*` 四列来自 `size` 表
#: （`paper_url` / `citation_bibtex` 来自更早的 `parse-readme`，不在这里等）。
SERIAL_TAIL: tuple[Step, ...] = (
    # `fetch-readme` 只抓正文：`readme_fields` 的字段要靠 `parse-readme` 派生。
    # 必须排在导出前——`paper_url` / `citation_bibtex` 就来自那张表，不解析就会
    # 静默停在旧正文上（2026-11-19 复核发现刷新链漏了这一步）。
    Step("parse", ("parse-readme",), needs=("readme",), command="parse-readme"),
    Step(
        "export",
        ("export-wide",),
        needs=("build", "reconcile", "sizes", "parse"),
        command="export-wide",
    ),
    # 榜单页是 `reports/` 的产物（`query/report.py` 渲染 `assets/` 的 css/js），
    # 13 秒的事——但 2026-11-20 实测它停在 4 天前，因为刷新链里没有这一步。
    # 只依赖 `build`：它读 `datasets` 视图与观测流水，不读导出物。
    Step("report", ("report",), needs=("build",), command="report"),
    Step(
        "deliver",
        ("request", "run-due"),
        needs=("build", "reconcile"),
        command="request-run-due",
    ),
    # 归档把 30 天前的 Raw 冻结成分片（写入序进 groups、同天重复压掉）。旧链靠
    # `hf-archive` 每周一跑；ADR-0008 取消全部周任务时**漏了给它排期**，于是
    # `doctor` 的 `archive_backlog` 一过 30 天就永久报红。放串行尾是因为它要
    # `build` 之后的派生状态判「归档前是否已滞后」，且改的是 Raw 而非 `current`。
    Step("archive", ("archive",), needs=("build",)),
    Step("prune", ("prune",)),
    Step("doctor", ("doctor",)),
)

#: ④ 兜底段：把链外的时间用来填 files 与 preview 的缺口。
#:
#: 与前三段目的不同：前三段是「今天该做完的事」，做完就散；这一段是「还剩多少时间
#: 就填多少」，所以三条约束都跟前面相反——
#:
#: - **不设名额**（`--top 0`）：缺口全取。抓满之后缺口为空，候选自然只剩下陈旧项，
#:   行为就跟 `fetch-sizes --changed-all` 一样了，不需要另设开关。
#: - **自己找死线**（`--until`）：它是唯一会跨过次日 00:30 的步骤，必须自己停。
#: - **不套启动窗口**：链跑得越久它越晚开始，而这正是它的用途（把剩余时间用掉）。
#:   套上 `launch_hours` 会得到反效果——最忙的那天直接跳过兜底。
#:
#: 两者**必须并行**：串行的话先跑的那个会吃满整个窗口，后一个一条都跑不到。
#: 并行也不冲突：files 打 `api` 桶，preview 打 `datasets-server`（没有配额头）。
FILL: tuple[Step, ...] = (
    Step(
        "files",
        ("fetch-files", "--top", "0", "--until", FILL_UNTIL, "--workers", "12"),
        needs=("build",),
        # 台账记的是真跑的子命令。不写这一行，`name` 会顶上去记成 `files`，
        # 而 cli 落盘的是 `fetch-files`——续跑判「今天跑过没有」永远对不上，
        # 表现是「每天都重跑一遍」，输出里一个字都看不出来。
        command="fetch-files",
    ),
    Step(
        "preview",
        # `--throttle-wait` 必须给：preview 默认「撞一次 429 就整轮中止」，手工跑
        # 十来分钟无所谓，挂在链上要跑七八个小时，撞一次就白跑一整晚。
        ("fetch-preview", "--top", "0", "--until", FILL_UNTIL, "--throttle-wait", "600"),
        needs=("build",),
        command="fetch-preview",
    ),
)


@dataclass
class StepResult:
    """一步的结果。`code is None` 表示没跑（今天已完成 / 依赖未满足 / 超窗口）。"""

    name: str
    code: int | None = None
    seconds: float = 0.0
    note: str = ""
    log: Path | None = None


#: 各步骤的完成态判据。没列出的表示「成功即完整」——`build` / `export` 这类没有
#: 「扫到哪」的概念，退出码 0 就是跑完了。
#:
#: `reconcile` 也要 `FULL_SCAN`：手动跑过的 `--max-pages 100` 是 success、却只扫了
#: 前 100 页。只认 status 会让半轮顶替真正的全量轮（doctor 的 `_last_sweep_date`
#: 早就按这个判据办，编排器此前没有）。
STEP_DONE_REASONS: dict[str, frozenset[str]] = {
    "collect": FULL_SCAN,
    "reconcile": FULL_SCAN,
}


def _success_times(store: DatasetStore) -> dict[str, datetime]:
    """今天成功跑过的**步骤名 → 它最后一次成功的完成时刻**。

    台账记的是子命令名（`fetch-sizes` / `export-wide` / `request-run-due`），步骤名
    是它的短名（`sizes` / `export` / `deliver`）。这一步换算不能省：直接拿短名去
    比对台账，七个步骤永远比不上，续跑每天白跑一遍，而输出里看不出异常。

    同一命令今天跑过多轮时取**最晚**完成的那次——最晚那次才是当前数据的来源。
    时间归一与「最近一次成功」都交给 `platform.runs`（`last_success`）：这两件事
    doctor 与编排器此前各写一份，且两份的完成态判据不一致。
    """
    today = date.today().isoformat()
    # 按 `started_at` 归日：跨零点的运行属于它开始的那个班次。
    runs = [item for item in read_runs(store) if str(item.get("started_at", ""))[:10] == today]
    times: dict[str, datetime] = {}
    for step in ALL_STEPS:
        stamp = last_success(runs, step.ledger, done_reasons=STEP_DONE_REASONS.get(step.name))
        if stamp is not None:
            times[step.name] = stamp
    return times


def today_successes(store: DatasetStore) -> set[str]:
    """今天已经成功跑过的**步骤名**（不含先后关系，供 `--only` 的提示用）。"""
    return set(_success_times(store))


def _dependency_closed(store: DatasetStore) -> set[str]:
    """把「今天已完成」收成依赖封闭集：上游没用今天的结果，下游也不算完成。

    台账只记「这条命令今天成功过」，记不住它**用的是哪一版数据**。2026-11-20
    白天手动跑过 `build --stale` 与 `export-wide`，夜里 `collect` 才补上今天的
    采集；若照台账办，这两个会被「今天已完成」挡掉，导出的口径就停在旧采集上，
    而摘要里只有一行「今天已完成」，看不出它已经过期。

    规则有两条，缺一不可：

    1. 前置今天也要完成（`needs` 里每一个都在）；
    2. 前置的**完成时刻不晚于**自己——同一份台账里，`sizes` 可以 00:32 跑完、
       `collect` 23:08 才跑完，两者都在「今天」，但 `sizes` 用的是昨夜那批库。
       只比「在不在集合里」会把它当成新鲜产物（2026-11-20 实际发生）。

    链条无环，逐轮剔除即可收敛。
    """
    keep = _success_times(store)
    changed = True
    while changed:
        changed = False
        for step in ALL_STEPS:
            if step.name not in keep:
                continue
            if any(need not in keep or keep[need] > keep[step.name] for need in step.needs):
                del keep[step.name]
                changed = True
    return set(keep)


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


def _skip_closure(steps: tuple[Step, ...], skip: frozenset[str]) -> frozenset[str]:
    """把 `--skip` 扩成「这些步骤 + 依赖它们的所有下游」。

    只跳上游不跳下游，下游就是在旧数据上拼出来的结果，跑它只是白占预算：
    `--skip collect` 若只跳过 collect，build 会拿昨天的 silver 重建一遍并报成成功。
    """
    dropped = set(skip)
    changed = True
    while changed:
        changed = False
        for step in steps:
            if step.name in dropped:
                continue
            if any(need in dropped for need in step.needs):
                dropped.add(step.name)
                changed = True
    return frozenset(dropped)


@dataclass(frozen=True)
class Decision:
    """一步的去留。`note` 为空表示该跑，否则就是跳过的原因。

    单独成类型而不共用 `StepResult`：决策发生在**执行之前**，此时没有退出码、
    没有耗时、没有日志——拿 `StepResult` 凑合，`code=None` 就会同时表示
    「还没跑」与「跳过了」，下游再也没法分辨。
    """

    step: Step
    note: str = ""
    #: 不是用户主动跳过的：被上游挡下、或到点没跑。这一天该做的没做完。
    #: 它与「今天已完成」「不属于 --only」不同——那两种是设计内的，不必惊动人。
    blocked: bool = False

    @property
    def run(self) -> bool:
        return not self.note


def decide_one(
    step: Step,
    done: set[str],
    failed: set[str],
    *,
    only: frozenset[str] | None = None,
    dropped: frozenset[str] = frozenset(),
    requested: frozenset[str] = frozenset(),
) -> Decision:
    """这一步跑不跑。判据只此一处：串行头、并行段、串行尾全调它。

    跳过原因有四种，顺序即优先级——用户主动点名跳过的要先说，否则
    `--skip readme` 的 `parse` 会显示「依赖未完成」，看着像故障。

    1. `--skip` 点名，或上游被它带下水（`dropped`）；
    2. 不在 `--only` 里（手动补跑模式）；
    3. 今天已完成（`done` 已按依赖封闭收窄过）；
    4. 依赖未完成（上游没进 `done`，或进了 `failed`）。

    **第四种既不算失败、也不算正常跳过**：`blocked=True`。上游没跑成，下游没动是
    后果，不是独立故障——此前串行两段把它标进 `failed`，于是 `collect` 一失败，
    `build`/`export`/`deliver` 全成了「失败步骤」，失败列表被下游淹没，真正该看的
    那一条反而要翻。但反过来当作「无事发生」也不行：超窗口导致导出没跑，链条会
    全绿退出（#149：排了但没跑完，比不排更坏）。故单列一类：不报失败，但必须报
    「未完成」。
    """
    if step.name in dropped:
        return Decision(step, "被 --skip 跳过" if step.name in requested else "上游被 --skip 跳过")
    if only is not None and step.name not in only:
        return Decision(step, "不在 --only 里")
    if step.name in done:
        return Decision(step, "今天已完成")
    if only is None:
        missing = [need for need in step.needs if need not in done or need in failed]
        if missing:
            return Decision(step, f"依赖未完成：{','.join(missing)}", blocked=True)
    return Decision(step)


def decide(
    steps: Iterable[Step],
    done: set[str],
    failed: set[str],
    *,
    only: frozenset[str] | None = None,
    dropped: frozenset[str] = frozenset(),
    requested: frozenset[str] = frozenset(),
) -> list[Decision]:
    """一批步骤的决策。**纯函数**：不读时钟、不碰文件、不跑子进程。

    串行段仍须逐步调用 `decide_one`——`build` 依赖同一批里还没跑的 `collect`，
    一次性评估会把它判成依赖未完成，导出链整条塌掉（2026-11-19 首测的形状）。
    并行段没有批内依赖，才用这个批量版。
    """
    return [
        decide_one(step, done, failed, only=only, dropped=dropped, requested=requested)
        for step in steps
    ]


#: 全部步骤，按链上的出现顺序。`--only` 的合法名与续跑的台账映射都从这一份派生。
ALL_STEPS: tuple[Step, ...] = (*SERIAL_HEAD, *PARALLEL, *SERIAL_TAIL, *FILL)

#: 全部步骤名，供 `--only` 校验。写在这里而不是扫描四个元组，是为了让拼错的步骤名
#: 在参数解析期就报错——`--only exprot` 静默什么都不跑，比报错坏得多。
STEP_NAMES: frozenset[str] = frozenset(step.name for step in ALL_STEPS)

#: 步骤名 → 定义。`--only` 的依赖提示按它取 `needs`。
STEP_BY_NAME: dict[str, Step] = {step.name: step for step in ALL_STEPS}


@dataclass
class Plan:
    """一次刷新跑完后的事实。"""

    results: list[StepResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    #: 被挡下或到点没跑的步骤名。不是故障，但这一天该做的没做完。
    #: 与「今天已完成」「不在 --only 里」区分开：那两种是设计内的，不必报。
    blocked: set[str] = field(default_factory=set)

    @property
    def failed(self) -> list[StepResult]:
        return [item for item in self.results if item.code not in (None, 0)]


def _record(
    plan: Plan, done: set[str], failed: set[str], result: StepResult, *, blocked: bool = False
) -> None:
    """记一步的结果。**只有真跑过且有退出码的**才进 `done` / `failed`。

    「没跑」与「跑失败」是两件事：前者 `code is None`，不计入这两者。此前串行
    两段把「依赖未完成」也塞进 `failed`，一次 `collect` 失败会连带把 `build` /
    `export` / `deliver` 全算成失败步骤。

    没跑的原因分两类：设计内的（今天已完成、不在 `--only` 里、用户 `--skip`）
    不必惊动人；被上游挡下或到点没跑的进 `plan.blocked`——链条会以非零退出，
    摘要里单列一段。
    """
    plan.results.append(result)
    if result.code is None:
        if blocked:
            plan.blocked.add(result.name)
        return
    if result.code == 0:
        done.add(result.name)
    else:
        failed.add(result.name)


def _advance(
    steps: tuple[Step, ...],
    root: Path,
    plan: Plan,
    done: set[str],
    failed: set[str],
    decide_step: Callable[[Step], Decision],
    announce: Callable[[StepResult], None],
    *,
    budget: Callable[[Step], bool] | None = None,
) -> None:
    """串行推进：一步决策、一步执行、一步记账。

    必须**逐步**决策：`build` 依赖同一批里还没跑的 `collect`，一次性评估会把它判成
    依赖未完成，导出链整条塌掉（2026-11-19 首测就是这个形状）。

    串行头与串行尾此前的循环逐字相同（连注释都抄了一份），唯一的差别是串行尾多一条
    「导出 / 交付到点就留给次日」——现在由 `budget` 参数表达。
    """
    for step in steps:
        decision = decide_step(step)
        if not decision.run:
            result = StepResult(step.name, note=decision.note)
            _record(plan, done, failed, result, blocked=decision.blocked)
            announce(result)
            continue
        if budget is not None and budget(step):
            result = StepResult(step.name, note="超过时间预算，留给次日续跑")
            _record(plan, done, failed, result, blocked=True)
            announce(result)
            continue
        result = run_step(step, root)
        _record(plan, done, failed, result)
        announce(result)


def _run_parallel(
    steps: list[Step],
    root: Path,
    plan: Plan,
    done: set[str],
    failed: set[str],
    announce: Callable[[StepResult], None],
) -> None:
    """并行跑一批已经决定要跑的步骤，完成一个记一个。

    ② 与 ④ 两段共用这段骨架，差别只在「跑之前要不要过启动窗口」——那由调用方判，
    因为两段的答案正好相反：② 该过（太晚了就别启动三小时的抓取），④ 不该过
    （它本来就是吃剩余时间的）。
    """
    if not steps:
        return
    with ThreadPoolExecutor(max_workers=len(steps)) as pool:
        futures: dict[Future[StepResult], Step] = {
            pool.submit(run_step, step, root): step for step in steps
        }
        for future, _step in list(futures.items()):
            wait([future])
            result = future.result()
            _record(plan, done, failed, result)
            announce(result)
            del futures[future]


def execute(
    store: DatasetStore,
    *,
    budget_hours: float = DEFAULT_BUDGET_HOURS,
    launch_hours: float = DEFAULT_LAUNCH_HOURS,
    only: frozenset[str] | None = None,
    skip: frozenset[str] = frozenset(),
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
    done = _dependency_closed(store) if only is None else set()
    failed: set[str] = set()
    # `--skip` 的下游扩散对整条链算一次：`readme` 在并行段、`parse` 在串行尾，
    # 只在段内算会漏掉跨段的那条依赖。
    dropped = _skip_closure(ALL_STEPS, skip) if skip else frozenset()

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

    def decide_step(step: Step) -> Decision:
        # 每次现取 `done` / `failed`：上一步刚跑完的结果要立刻影响下一步的判定。
        return decide_one(step, done, failed, only=only, dropped=dropped, requested=skip)

    # ① 串行头：采集定稿。
    _advance(SERIAL_HEAD, store.root, plan, done, failed, decide_step, announce)

    # ② 并行段：对账与两个锁外抓取同时跑。各走不同端点，互不干扰。
    decisions = decide(PARALLEL, done, failed, only=only, dropped=dropped, requested=skip)
    for decision in decisions:
        if decision.run:
            continue
        result = StepResult(decision.step.name, note=decision.note)
        _record(plan, done, failed, result, blocked=decision.blocked)
        announce(result)

    batch = [decision.step for decision in decisions if decision.run]
    if batch:
        if over_launch():
            for step in batch:
                result = StepResult(step.name, note="超过启动窗口，留给次日续跑")
                _record(plan, done, failed, result, blocked=True)
                announce(result)
        else:
            _run_parallel(batch, store.root, plan, done, failed, announce)

    # ③ 串行尾：导出、交付、清理、体检。到点的导出与交付留给次日。
    _advance(
        SERIAL_TAIL,
        store.root,
        plan,
        done,
        failed,
        decide_step,
        announce,
        budget=lambda step: step.name in {"export", "deliver"} and over_budget(),
    )

    # ④ 兜底段：files 与 preview 用剩余时间填缺口，跑到次日 `FILL_UNTIL` 为止。
    #
    # 刻意不判 `over_launch` / `over_budget`：`launch_hours` 的本意是「太晚了别再
    # 启动耗时步骤，免得挤掉次日的班次」，而这一段存在的理由恰好是把那些剩余时间
    # 用掉——套上它等于「链越忙越不填」，与设计正好相反。它自己有 `--until` 兜底，
    # 不会跑过次日 00:30 的链起点。
    #
    # 依赖只挂 `build`：files 的候选来自 `current`，与导出物无关。`export` 万一
    # 失败，不该连带把今晚的兜底也停掉——那正是唯一还能干点活的时候。
    fill_decisions = decide(FILL, done, failed, only=only, dropped=dropped, requested=skip)
    for decision in fill_decisions:
        if decision.run:
            continue
        result = StepResult(decision.step.name, note=decision.note)
        _record(plan, done, failed, result, blocked=decision.blocked)
        announce(result)
    _run_parallel(
        [decision.step for decision in fill_decisions if decision.run],
        store.root,
        plan,
        done,
        failed,
        announce,
    )

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
        "blocked": sorted(plan.blocked),
    }


def _report_dependencies(
    store: DatasetStore,
    only: frozenset[str] | None,
    *,
    strict: bool,
) -> None:
    """`--only` 时把「前置今天跑过没有」摆到台面上。

    `--only` 刻意不看依赖（手动补一步的人知道自己在干什么），但**静默**与
    **知情**是两件事：2026-11-19 之前，`--only export` 可以在 `build` 没跑过时
    导出一份「新鲜的旧数据」，而输出里一个字都没提。

    判据用「今天是否成功过」而不是「数据是否一致」：这里只是提示，真正的硬校验
    在 `export` 自己身上（它比 `current` 与最近一次成功采集的日期，对不上就拒绝）。

    `strict` 时从提示升级为拒绝——代价是手动补跑要先拼前置，换来的是「不会误跑」。
    """
    if not only:
        return
    done = today_successes(store)
    stale: list[str] = []
    for name in sorted(only):
        step = STEP_BY_NAME.get(name)
        if step is None:
            continue
        missing = [need for need in step.needs if need not in done]
        if missing:
            stale.append(f"{name} 缺前置 {', '.join(missing)}")
    if not stale:
        return
    detail = "；".join(stale)
    if strict:
        raise SystemExit(
            f"--strict：{detail}（今天没有成功记录）。去掉 --strict 可以照跑，但产物可能基于旧数据"
        )
    print(f"[提示] {detail}——今天没有成功记录，产物可能基于旧数据；加 --strict 可改为拒绝")


def run(args: argparse.Namespace, store: DatasetStore) -> Outcome:
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
    skip: frozenset[str] = frozenset()
    if getattr(args, "skip", None):
        names = frozenset(item.strip() for item in args.skip.split(",") if item.strip())
        unknown = sorted(names - STEP_NAMES)
        if unknown:
            raise SystemExit(
                f"未知步骤名：{', '.join(unknown)}；可选：{', '.join(sorted(STEP_NAMES))}"
            )
        skip = names
    if only is not None and skip:
        raise SystemExit("--only 与 --skip 不能同时给：一个说只跑这些，一个说别跑这些")
    _report_dependencies(store, only, strict=getattr(args, "strict", False))
    plan = execute(
        store,
        budget_hours=args.budget_hours,
        launch_hours=args.launch_hours,
        only=only,
        skip=skip,
    )
    # 体检非零只代表**有告警**（导出落后、Raw 断日这类待办也计入非零），不让它
    # 拖垮整条链——告警与故障挤进同一个退出码，铃声很快就没人听。详情在
    # `data/ALERT.txt`。这条与旧 daily.ps1 的 `Report = $true` 是同一判断。
    failures = [item for item in plan.failed if item.name != "doctor"]
    # 被挡下或到点没跑的，单列一段：它们不是故障，但这一天该做的没做完。
    # 退出码仍非零——「全绿」与「跑完了」必须是两件事（#149）。
    blocked = sorted(plan.blocked)
    payload = summary_payload(plan, args.budget_hours)
    if failures:
        reason = "、".join(f"{item.name}(退出 {item.code})" for item in failures)
        if blocked:
            reason += f"；另有 {len(blocked)} 步未完成：{'、'.join(blocked)}"
        return Outcome(payload, code=1, reason=f"刷新鲜未全部成功：{reason}")
    if blocked:
        return Outcome(
            payload, code=1, reason=f"刷新鲜有 {len(blocked)} 步未完成：{'、'.join(blocked)}"
        )
    return Outcome(payload)
