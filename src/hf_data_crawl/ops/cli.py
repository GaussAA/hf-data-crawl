"""命令行入口：把解析好的参数分派到各切片的公开处理函数。

本模块是唯一的组合根：独占锁、派生崩溃恢复与运行台账都在这里统一施加。
写命令与 report/docs 经 `_execute` 写 runs/（失败也写并带原因）；
query、doctor 与 request 直接返回，不写台账。

参数表在 `parsers`：那边只管「命令行长什么样」，这边只管「谁加锁、谁记账」。
"""

from __future__ import annotations

import argparse
import traceback
from collections.abc import Callable
from datetime import datetime
from pathlib import Path
from typing import Any

from ..acquire.commands import (
    run_collect,
    run_enrich,
    run_fetch_files,
    run_fetch_preview,
    run_fetch_readme,
    run_fetch_sizes,
    run_parse_readme,
    run_reconcile,
)
from ..delivery.commands import (
    run_export_wide,
    run_request,
    run_request_run,
    run_request_run_due,
)
from ..pipeline.commands import run_bronze_view, run_build
from ..pipeline.recovery import check_and_recover
from ..platform import DatasetStore, LockBusy, Outcome, exclusive, write_run
from ..query.commands import run_query, run_report
from .commands import run_archive, run_compact, run_docs, run_doctor, run_prune
from .parsers import DOCTOR_LOCK_WAIT, SNAPSHOT_LOCK_WAIT, build_parser

#: 会改写数据目录的命令，执行期间持有独占锁。query 只读且单次查询很短，
#: 不在此列；report/docs 不改数据，但要同时读多张表，与写命令并发会读到
#: 新旧混合的快照，因此也纳入锁集合。
WRITE_COMMANDS = frozenset(
    {
        "collect",
        "reconcile",
        "build",
        "enrich",
        "fetch-sizes",
        "fetch-files",
        "fetch-readme",
        "parse-readme",
        "fetch-preview",
        "archive",
        "compact",
        "prune",
        "export-wide",
    }
)

#: 只读一批表、需要一致快照的生成命令。它们与写命令共用同一把独占锁——
#: 只有互斥才能保证读到的是某一刻的完整快照；Windows 的文件锁不提供共享模式。
SNAPSHOT_COMMANDS = frozenset({"report", "docs"})

#: 需要持锁的命令全集。
#: 需求交付也要独占：它读多张表出一个一致快照，且要写 `history/`、`deliveries/`。
#: 与写命令放同一把锁，而不是自己另开一把——「同一时刻只有一个进程在动底库或档案」。
REQUEST_COMMANDS = frozenset({"request-run", "request-run-due"})

#: 拿到锁的命令：写命令互斥，生成命令等一个完整快照。需求交付同属此列。
LOCKED_COMMANDS = WRITE_COMMANDS | SNAPSHOT_COMMANDS | REQUEST_COMMANDS

#: 自己安排锁的命令：长耗时的网络抓取在锁外做，只把「并入正式表」包在短锁里。
#: 它们仍在 WRITE_COMMANDS 里（启动检查/恢复未完成的派生照旧），但不走
#: `_execute` 的整段锁。原因见 `sizes.StagingWriter`：全量抓取要跑三小时，
#: 整段持锁会直接让当天的 collect 等锁失败（2026-11-07 实际事故）。
#: `parse-readme` 同理：78 万条正文逐行解析要二十几分钟，全程持锁会挡住当天的
#: 采集；它只在最后写 `readme_fields` 时拿一次短锁。2026-11-18 忘了登记这一条，
#: 结果自锁死锁——`_execute` 持着锁，内部又调 `exclusive`，等了二十多分钟后
#: 以 `LockBusy: ... 被另一个写命令占用：parse-readme（pid 自己）` 收场。
SELF_LOCKED_COMMANDS = frozenset({"fetch-sizes", "fetch-readme", "fetch-preview", "parse-readme"})


def main(argv: list[str] | None = None, *, transport: Any = None) -> int:
    """命令行入口。

    Args:
        argv: 命令行参数,None 时使用 sys.argv[1:]
        transport: 网络传输抽象,仅供测试注入假实现。生产环境传 None,
            由各命令内部创建真实传输。类型标注为 Any 以避免 CLI 层耦合
            acquire 切片的内部抽象。
    """
    args = build_parser().parse_args(argv)
    store = DatasetStore(Path(args.data_dir))
    # 未显式给 --lock-wait 时按命令取默认值：写命令立即失败，生成命令先等一会儿；
    # 体检只等 2 秒，等不到就降级为非快照而不是阻塞排障。
    lock_wait = args.lock_wait
    if lock_wait is None:
        if args.command == "doctor":
            lock_wait = DOCTOR_LOCK_WAIT
        elif args.command in SNAPSHOT_COMMANDS:
            lock_wait = SNAPSHOT_LOCK_WAIT
        else:
            lock_wait = 0.0

    # 启动时自动检查和恢复未完成的派生
    # 只有写命令才做：恢复本身会改派生层，只读快照命令（report/docs）不应顺手写数据。
    # 恢复会写 current / lineage / 各派生分区，必须与命令本体共用同一把锁——否则
    # 两个写命令各自在锁外重放，后写静默盖掉先写（此前是全项目唯一绕过锁的写路径）。
    # 这段在 `_execute` 之外，故 `LockBusy` 要自己兜住，不能让命令无痕崩溃。
    if args.command in WRITE_COMMANDS:
        try:
            with exclusive(store.root, "recovery", lock_wait):
                recovered = check_and_recover(store)
        except LockBusy:
            # 别的写命令正持锁，它启动时已经做过恢复；本轮跳过即可。
            recovered = []
            print("[恢复] 另一个写命令正持有数据锁，本轮跳过启动恢复")
        if recovered:
            print(f"⚠️  检测到 {len(recovered)} 个未完成的派生，已自动恢复：{recovered}")
            print("   提示：残留派生层已清理；有 Raw 的日期已重新派生，无 Raw 的仅清理")

    if args.command == "collect":
        return _execute(store, "collect", lambda: run_collect(args, store, transport), lock_wait)
    if args.command == "reconcile":
        return _execute(
            store, "reconcile", lambda: run_reconcile(args, store, transport), lock_wait
        )
    if args.command == "build":
        return _execute(store, "build", lambda: run_build(args, store), lock_wait)
    if args.command == "archive":
        return _execute(store, "archive", lambda: run_archive(args, store), lock_wait)
    if args.command == "compact":
        return _execute(store, "compact", lambda: run_compact(args, store), lock_wait)
    if args.command == "prune":
        return _execute(store, "prune", lambda: run_prune(args, store), lock_wait)
    if args.command == "enrich":
        return _execute(store, "enrich", lambda: run_enrich(args, store, transport), lock_wait)
    if args.command == "fetch-sizes":
        return _execute(
            store,
            "fetch-sizes",
            lambda: run_fetch_sizes(args, store, transport, lock_wait),
            lock_wait,
        )
    if args.command == "fetch-files":
        return _execute(
            store, "fetch-files", lambda: run_fetch_files(args, store, transport), lock_wait
        )
    if args.command == "fetch-readme":
        return _execute(
            store,
            "fetch-readme",
            lambda: run_fetch_readme(args, store, transport, lock_wait),
            lock_wait,
        )
    if args.command == "parse-readme":
        return _execute(
            store,
            "parse-readme",
            lambda: run_parse_readme(args, store, transport, lock_wait),
            lock_wait,
        )
    if args.command == "fetch-preview":
        return _execute(
            store,
            "fetch-preview",
            lambda: run_fetch_preview(args, store, transport, lock_wait),
            lock_wait,
        )
    if args.command == "report":
        return _execute(store, "report", lambda: run_report(args, store), lock_wait)
    if args.command == "export-wide":
        return _execute(store, "export-wide", lambda: run_export_wide(args, store), lock_wait)
    if args.command == "query":
        return run_query(args, store)
    if args.command == "bronze":
        # 只读 Raw、不落盘、不改派生层：与 query 同待遇，不进数据锁、不写 runs 台账
        # （「看一眼某天」不该挤掉采集记录的位置）。
        return run_bronze_view(args, store).code
    if args.command == "request":
        # new / list / show 只读写 requests/，不碰数据目录，不参与数据锁；
        # run 要读多张表出一个一致快照，与 report / docs 同规矩——先等一会儿锁。
        if args.request_action in {"run", "run-due"}:
            wait = SNAPSHOT_LOCK_WAIT if args.lock_wait is None else args.lock_wait
            # 预演与预览不记账：它们不产出交付，写进 runs/ 只会让「最近的失败运行」
            # 混进一堆草稿。真正那次交付次次留痕，失败也留——需求侧的失败原先只活在
            # 终端里，体检的「近 7 天失败」根本看不见（ADR-0003 审计发现）。
            if getattr(args, "dry_run", False) or getattr(args, "preview", False):
                try:
                    with exclusive(store.root, "request-run", wait):
                        return run_request_run(args, store).code
                except LockBusy as error:
                    print(f"交付失败：{error}")
                    return 1
            command = "request-run" if args.request_action == "run" else "request-run-due"
            return _execute(store, command, lambda: _request_outcome(args, store), wait)
        return run_request(args)
    if args.command == "doctor":
        return run_doctor(store, lock_wait, requests_root=Path(args.requests_dir))
    if args.command == "docs":
        # 与 report 同路：加锁与落台账都交给 `_execute`。缺字段说明时退出码为 1，
        # 失败因此也会留在 runs/ 里——原先这里自己开一次锁并直接返回，等于在
        # `_execute` 之外又造了一条「有加锁、没台账」的路径。
        return _execute(store, "docs", lambda: run_docs(args, store), lock_wait)

    return 1


def _request_outcome(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """把「交付一次 / 跑一批」收敛成一条运行记录。

    原因用 `Attempt.reason`：那是「被哪道闸门拦住」的完整一句话。台账是机读面，
    写「见上方说明」等于没写——几天后再看，终端早滚没了。
    """
    if args.request_action == "run":
        attempt = run_request_run(args, store)
    else:
        attempt = run_request_run_due(args, store)
    return Outcome(payload={"exit": attempt.code}, code=attempt.code, reason=attempt.reason)


def _execute(
    store: DatasetStore,
    command: str,
    action: Callable[[], Outcome],
    lock_wait: float = 0.0,
) -> int:
    """执行一条命令并留下唯一一份运行记录。

    成败只在这里判定一次：异常与业务失败都收敛成 `Outcome`，然后走同一条
    `write_run`。此前异常路径写 failed、正常路径无条件写 success，于是
    「退出码 1 但记录成功」成为可能。
    """
    started = datetime.now()
    try:
        if command in LOCKED_COMMANDS and command not in SELF_LOCKED_COMMANDS:
            # 写命令互斥，生成命令则等一个完整快照：拿不到锁就如实失败并留下
            # runs 记录，而不是并发读到新旧混合的表。自己排锁的命令（fetch-sizes）
            # 由动作内部只锁真正写表的短段。
            with exclusive(store.root, command, lock_wait):
                outcome = action()
        else:
            outcome = action()
    except LockBusy as error:
        # 锁被占用是**预期状态**，不是程序错误：不打堆栈，也不该与真正的崩溃
        # 长一个样。计划任务里它意味着「另一个写命令还在跑」，人要读的是谁占着、
        # 该等还是该查，而不是一段 traceback。
        # reason 仍带 `LockBusy: ` 前缀：台账是机读面，类型名让「锁冲突」与
        # 「真崩溃」在 `doctor._recent_failures` 里能分开统计。
        reason = f"{type(error).__name__}: {error}"
        outcome = Outcome(payload={"error": reason}, code=1, reason=reason)
    except Exception as error:
        reason = f"{type(error).__name__}: {error}"
        outcome = Outcome(payload={"error": reason}, code=1, reason=reason)
        # catch-all 是台账一致性的需要，不等于可以吞掉现场：业务失败走的是
        # Outcome(code=1) 正常返回，能到这里的多半是未预期异常（含编程错误）。
        # 完整堆栈只进 stderr，由 scripts/_common.ps1 收进 logs/，不写进 runs/
        # 的 payload——台账保持机读，不夹带本机路径。
        traceback.print_exc()

    write_run(store, command, started, outcome)
    # 失败只在这里说一次。原先 except 分支也打印一句，同一条原因出现两遍
    # （「失败：…」+「未完成：…」），而两处措辞还不一样。
    # 需求交付自己已经把原因打得更细，台账那句不再重复一遍。
    if outcome.failed and outcome.reason and command not in REQUEST_COMMANDS:
        print(f"{command} 未完成：{outcome.reason}")
    return outcome.code


# ---------------------------------------------------------------- 需求管理
