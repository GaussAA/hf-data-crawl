"""刷新鲜编排器：续跑判断、依赖跳过、软边界、并行段与台账。

这些测试不打网络：`run_step` 被换成假的，只验证**编排**——哪一步该跑、哪一步
该跳过、跳过的原因写没写对。真子进程的行为由各自的命令测试覆盖。
"""

from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from hf_data_crawl.ops import refresh
from hf_data_crawl.ops.refresh import StepResult
from hf_data_crawl.platform.store import DatasetStore


def seed_run(
    store: DatasetStore,
    command: str,
    *,
    status: str = "success",
    payload: dict[str, object] | None = None,
    when: datetime | None = None,
) -> None:
    """写一条运行记录，模拟上一次跑过这一步。`when` 默认此刻。"""
    runs = store.root / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    stamp = when or datetime.now()
    body = {
        "command": command,
        "status": status,
        "reason": None,
        "started_at": stamp.isoformat(timespec="microseconds"),
        "finished_at": stamp.isoformat(timespec="microseconds"),
        "payload": payload or {},
    }
    (runs / f"{stamp:%Y%m%dT%H%M%S}-{command}.json").write_text(
        json.dumps(body, ensure_ascii=False), encoding="utf-8"
    )


def _result(plan: refresh.Plan, name: str) -> StepResult | None:
    """按步骤名取结果。`Plan` 不再自带查询方法：生产代码不按名回查，测试自己取。"""
    return next((item for item in plan.results if item.name == name), None)


def fake_steps(monkeypatch, codes: dict[str, int] | None = None) -> list[str]:
    """把 `run_step` 换成记账的假实现，返回被调用过的步骤名。"""
    calls: list[str] = []

    def fake(step, root: Path) -> StepResult:
        calls.append(step.name)
        return StepResult(step.name, (codes or {}).get(step.name, 0), 0.0)

    monkeypatch.setattr(refresh, "run_step", fake)
    return calls


def test_今天已成功的步骤会被跳过(tmp_path: Path, monkeypatch) -> None:
    store = DatasetStore(tmp_path / "data")
    seed_run(store, "compact")
    seed_run(store, "collect", payload={"stopped_reason": "end"})
    seed_run(store, "build")

    calls = fake_steps(monkeypatch)
    plan = refresh.execute(store, log=lambda _: None)

    assert "compact" not in calls
    assert "collect" not in calls
    assert "build" not in calls
    compact = _result(plan, "compact")
    assert compact is not None and compact.note == "今天已完成"
    # 采集基于前一步的 current，后两段照常跑。
    assert "reconcile" in calls


def test_水位线早停的采集不算采集定稿(tmp_path: Path, monkeypatch) -> None:
    """`stopped_reason == "end"` 才是扫完全站；水位线早停要重跑。"""
    store = DatasetStore(tmp_path / "data")
    seed_run(store, "collect", payload={"stopped_reason": "watermark_no_new"})

    calls = fake_steps(monkeypatch)
    refresh.execute(store, log=lambda _: None)

    assert "collect" in calls


def test_依赖失败的步骤被跳过(tmp_path: Path, monkeypatch) -> None:
    store = DatasetStore(tmp_path / "data")
    calls = fake_steps(monkeypatch, {"build": 1})

    plan = refresh.execute(store, log=lambda _: None)

    export = _result(plan, "export")
    assert export is not None
    assert export.code is None, "build 失败时导出不该跑"
    assert "依赖未完成" in export.note
    assert "export" not in calls


def test_依赖齐全时导出与交付都会跑(tmp_path: Path, monkeypatch) -> None:
    store = DatasetStore(tmp_path / "data")
    calls = fake_steps(monkeypatch)

    plan = refresh.execute(store, log=lambda _: None)

    assert "export" in calls
    assert "deliver" in calls
    assert plan.failed == []


def test_超过启动窗口时并行段跳过(tmp_path: Path, monkeypatch) -> None:
    """软边界：到点不再启动新抓取，但 doctor 这类收尾照常。"""
    store = DatasetStore(tmp_path / "data")
    calls = fake_steps(monkeypatch)

    plan = refresh.execute(store, launch_hours=0, budget_hours=0, log=lambda _: None)

    assert "reconcile" not in calls
    reconcile = _result(plan, "reconcile")
    assert reconcile is not None and reconcile.note.startswith("超过启动窗口")
    assert "doctor" in calls, "体检是唯一能说明哪里没跑完的环节"


def test_失败步骤体现在退出码与摘要里(tmp_path: Path, monkeypatch) -> None:
    store = DatasetStore(tmp_path / "data")
    fake_steps(monkeypatch, {"readme": 1})

    outcome = refresh.run(Namespace(budget_hours=8.5, launch_hours=8.0), store)

    assert outcome.failed
    assert outcome.reason is not None and "readme" in outcome.reason
    steps = {item["name"]: item for item in outcome.payload["steps"]}
    assert steps["readme"]["code"] == 1
    assert steps["readme"]["note"] == ""


def test_refresh子命令解析出默认预算() -> None:
    from hf_data_crawl.ops.parsers import build_parser

    args = build_parser().parse_args(["refresh"])

    assert args.budget_hours == refresh.DEFAULT_BUDGET_HOURS
    assert args.launch_hours == refresh.DEFAULT_LAUNCH_HOURS


# ---------------------------------------------------------------- --only 与 --strict


def test_only模式只跑点名的步骤(tmp_path: Path, monkeypatch) -> None:
    """手动补一步时，其余步骤记为「不在 --only 里」而不是「依赖未完成」——
    两种跳过原因指向不同的下一步动作，写混了人就不知道该查哪里。"""
    store = DatasetStore(tmp_path / "data")
    calls = fake_steps(monkeypatch)

    plan = refresh.execute(store, only=frozenset({"doctor"}), log=lambda _: None)

    assert calls == ["doctor"]
    export = _result(plan, "export")
    assert export is not None and export.note == "不在 --only 里"


def test_only的依赖提示指出缺哪个前置(tmp_path: Path, capsys) -> None:
    """`--only` 刻意不看依赖，但**静默**与**知情**是两件事。

    2026-11-19 之前，`--only export` 可以在 build 没跑过时导出一份「新鲜的旧数据」，
    而输出里一个字都没提。这条盯的就是那句话。
    """
    store = DatasetStore(tmp_path / "data")

    refresh._report_dependencies(store, frozenset({"export"}), strict=False)

    out = capsys.readouterr().out
    assert "export 缺前置" in out
    assert "build" in out and "reconcile" in out
    assert "--strict" in out


def test_only的前置今天都跑过就不提示(tmp_path: Path, capsys) -> None:
    """提示只在真缺前置时出现；天天提示等于没提示。"""
    store = DatasetStore(tmp_path / "data")
    seed_run(store, "build")
    seed_run(store, "reconcile")
    seed_run(store, "fetch-sizes")
    seed_run(store, "fetch-readme", payload={"stopped_reason": "end"})
    seed_run(store, "parse-readme")

    refresh._report_dependencies(store, frozenset({"export"}), strict=False)

    assert capsys.readouterr().out == ""


def test_strict在缺前置时拒绝且说清代价(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")

    with pytest.raises(SystemExit) as caught:
        refresh._report_dependencies(store, frozenset({"export"}), strict=True)

    message = str(caught.value)
    assert "--strict" in message and "可能基于旧数据" in message


def test_未知步骤名在参数层就报错(tmp_path: Path) -> None:
    """`--only exprot` 静默什么都不跑，比报错坏得多——人以为补完了。"""
    store = DatasetStore(tmp_path / "data")
    args = Namespace(only="exprot", strict=False, budget_hours=8.5, launch_hours=8.0)

    with pytest.raises(SystemExit) as caught:
        refresh.run(args, store)

    message = str(caught.value)
    assert "未知步骤名" in message and "exprot" in message
    assert "export" in message, "要列出可选步骤名，否则还得去翻源码"


# ---------------------------------------------------------------- 失败传播与预算


def test_采集失败时下游整条跳过(tmp_path: Path, monkeypatch) -> None:
    """采集没定稿，build 就没有可信的 current；再往下导出的是半截数据。"""
    store = DatasetStore(tmp_path / "data")
    fake_steps(monkeypatch, {"collect": 1})

    plan = refresh.execute(store, log=lambda _: None)

    build = _result(plan, "build")
    assert build is not None and build.code is None
    assert build.note.startswith("依赖未完成")
    export = _result(plan, "export")
    assert export is not None and export.code is None


def test_超过时间预算时导出与交付留给次日(tmp_path: Path, monkeypatch) -> None:
    """预算是硬线：导出与交付宁可留到明天，也不要在窗口末尾赶出一份没人看过的档案。"""
    store = DatasetStore(tmp_path / "data")
    fake_steps(monkeypatch)

    plan = refresh.execute(store, budget_hours=0, launch_hours=8, log=lambda _: None)

    for name in ("export", "deliver"):
        result = _result(plan, name)
        assert result is not None and result.code is None
        assert result.note == "超过时间预算，留给次日续跑"
    assert _result(plan, "doctor") is not None


def test_今天的失败记录不算已完成(tmp_path: Path, monkeypatch) -> None:
    store = DatasetStore(tmp_path / "data")
    seed_run(store, "compact", status="failed")

    calls = fake_steps(monkeypatch)
    refresh.execute(store, log=lambda _: None)

    assert "compact" in calls


def test_全部成功时退出码为零(tmp_path: Path, monkeypatch) -> None:
    """全成功时不该带着退出码 1 回去。"""
    store = DatasetStore(tmp_path / "data")
    fake_steps(monkeypatch)
    args = Namespace(budget_hours=8.5, launch_hours=8.0)

    outcome = refresh.run(args, store)

    assert outcome.code == 0
    assert outcome.reason is None


def test_doctor非零只算告警不算链条失败(tmp_path: Path, monkeypatch) -> None:
    """体检把「导出落后」「Raw 断日」这类待办也计入非零；若让它拖垮链条，
    整条链会天天报红，铃声很快就没人听。详情由 `data/ALERT.txt` 承载。"""
    store = DatasetStore(tmp_path / "data")
    fake_steps(monkeypatch, {"doctor": 1})
    args = Namespace(budget_hours=8.5, launch_hours=8.0)

    outcome = refresh.run(args, store)

    assert outcome.code == 0, outcome.reason


def test_run_step把子进程输出写进日志(tmp_path: Path) -> None:
    """终端只收摘要，细节进 `logs/`：跑失败时要能回去看，而不是靠终端滚回去找。

    用一条不存在的子命令，退出码必然非零——这条测的是「有没有落盘」，不是命令本身。
    """
    step = refresh.Step(name="probe", args=("no-such-subcommand",))

    result = refresh.run_step(step, tmp_path)

    assert result.code != 0
    assert result.log is not None and result.log.is_file()
    assert result.log.name.endswith("-refresh-probe.log")
    assert result.log.read_text(encoding="utf-8").strip(), "日志不能是空的"


# ---------------------------------------------------------------- 续跑的边角


def test_昨天成功的记录不算今天完成(tmp_path: Path, monkeypatch) -> None:
    """台账跨天延续，判据必须带日期；只看「成功过」会让今天的步骤一个都不跑。"""
    store = DatasetStore(tmp_path / "data")
    seed_run(store, "compact", when=datetime.now() - timedelta(days=1))

    calls = fake_steps(monkeypatch)
    refresh.execute(store, log=lambda _: None)

    assert "compact" in calls


def test_并行段里已完成的那一步不再跑(tmp_path: Path, monkeypatch) -> None:
    # 台账记的是子命令名 `fetch-sizes`，步骤短名是 `sizes`。这条用例原先按短名
    # 播种，于是「续跑」在真实台账上从来没有生效过，测试却一直绿。
    store = DatasetStore(tmp_path / "data")
    seed_run(store, "fetch-sizes")

    calls = fake_steps(monkeypatch)
    plan = refresh.execute(store, log=lambda _: None)

    assert "sizes" not in calls
    sizes = _result(plan, "sizes")
    assert sizes is not None and sizes.note == "今天已完成"


def test_收尾段里已完成的那一步不再跑(tmp_path: Path, monkeypatch) -> None:
    """导出已交过就不再导：同一天重导一次，档案里会多出一条没人要的版本。

    前置也要一并种上——只种 `export-wide` 而 build/parse/sizes/readme 都没种时，
    「已完成」会因依赖封闭被收回（那份导出用的是没更新的底层数据），见下一条。
    """
    store = DatasetStore(tmp_path / "data")
    seed_run(store, "collect", payload={"stopped_reason": "end"})
    seed_run(store, "build")
    seed_run(store, "reconcile")
    seed_run(store, "fetch-sizes")
    seed_run(store, "fetch-readme")
    seed_run(store, "parse-readme")
    seed_run(store, "export-wide")

    calls = fake_steps(monkeypatch)
    plan = refresh.execute(store, log=lambda _: None)

    assert "export" not in calls
    export = _result(plan, "export")
    assert export is not None and export.note == "今天已完成"


def test_上游重跑时下游不算已完成(tmp_path: Path, monkeypatch) -> None:
    """台账只记「今天成功过」，记不住它用的是哪一版数据。

    2026-11-20：白天手动跑过 build / export，夜里 collect 才补上当天采集。照台账
    办，这两个会被「今天已完成」挡掉，导出就停在旧采集上，而摘要只写一行「今天
    已完成」——看起来一切正常。
    """
    store = DatasetStore(tmp_path / "data")
    seed_run(store, "build")
    seed_run(store, "export-wide")
    seed_run(store, "fetch-sizes")
    seed_run(store, "fetch-readme")
    seed_run(store, "parse-readme")
    # collect 今天没有一次「扫到末页」，所以 build 的前置没成。

    calls = fake_steps(monkeypatch)
    refresh.execute(store, log=lambda _: None)

    assert "collect" in calls
    assert "build" in calls, "上游重跑，build 必须重建"
    assert "export" in calls, "导出要基于重建后的 current"


def test_only点名一步时正常跑完并返回零(tmp_path: Path, monkeypatch) -> None:
    store = DatasetStore(tmp_path / "data")
    calls = fake_steps(monkeypatch)
    args = Namespace(only="doctor", strict=False, budget_hours=8.5, launch_hours=8.0)

    outcome = refresh.run(args, store)

    assert calls == ["doctor"]
    assert outcome.code == 0


# ---------------------------------------------------------------- 并行段的分支


def test_only点名并行步骤时不看依赖(tmp_path: Path, monkeypatch) -> None:
    """`--only sizes` 只在锁外抓规模，不该因为 reconcile 今天没跑就不动。"""
    store = DatasetStore(tmp_path / "data")
    calls = fake_steps(monkeypatch)

    refresh.execute(store, only=frozenset({"sizes"}), log=lambda _: None)

    assert calls == ["sizes"]


def test_依赖提示忽略不认识的名字(tmp_path: Path, capsys) -> None:
    """`run` 之前会校验步骤名，但这条提示是公开函数——直接调用时不能崩。"""
    store = DatasetStore(tmp_path / "data")

    refresh._report_dependencies(store, frozenset({"nope"}), strict=False)

    assert capsys.readouterr().out == ""


def test_归档在链上且有排期(tmp_path: Path, monkeypatch) -> None:
    """`doctor` 的 archive_backlog 一过 30 天就报红；没人跑 archive 就是永久红。"""
    store = DatasetStore(tmp_path / "data")
    calls = fake_steps(monkeypatch)

    refresh.execute(store, log=lambda _: None)

    assert "archive" in calls


# ---------------------------------------------------------------- 台账与步骤名


#: 步骤短名 → 台账里真正记的命令名。只列对不上的，对得上的默认同名。
LEDGER_ALIASES = {
    "sizes": "fetch-sizes",
    "readme": "fetch-readme",
    "parse": "parse-readme",
    "export": "export-wide",
    "deliver": "request-run-due",
}


def test_每一步的台账命令名与它实际跑的子命令一致() -> None:
    """`Step.args[0]` 就是 cli 写进台账的 `command`（`request` 例外）。

    这条不测行为，测的是**声明**：改了 args 却忘了改 `command`，续跑就从
    「跳过今天已完成的」退化成「每天重跑一遍」，两种情况的输出一模一样。
    """
    for step in refresh.ALL_STEPS:
        if step.args[0] == "request":
            # cli 按 `args[1]` 记命令名（request-run / request-run-due），不是写死一个。
            assert step.ledger == "-".join(step.args[:2]), step.name
        else:
            assert step.ledger == step.args[0], step.name


def test_对不上名的步骤都能被台账认出来(tmp_path: Path) -> None:
    """真实台账按子命令名落盘；续跑必须把这些名字换算回步骤名。"""
    store = DatasetStore(tmp_path / "data")
    for command in LEDGER_ALIASES.values():
        seed_run(store, command)

    done = refresh.today_successes(store)

    assert set(LEDGER_ALIASES) <= done
    assert "compact" not in done, "没跑过的步骤不能算完成"


def test_窗口外不启动新抓取但摘要里有留痕(tmp_path: Path, monkeypatch) -> None:
    """窗口超时时并行段的每一步都必须出现在摘要里。

    不标的话它在摘要里连一行都没有——正是「静默少跑一步」的形状，doctor 与
    翻日志的人都看不出来。
    """
    store = DatasetStore(tmp_path / "data")
    calls = fake_steps(monkeypatch)

    plan = refresh.execute(store, launch_hours=0.0, log=lambda _: None)

    assert "reconcile" not in calls
    sizes = _result(plan, "sizes")
    assert sizes is not None and sizes.code is None
    assert "启动窗口" in sizes.note


def test_skip把下游一起跳过() -> None:
    """只跳上游不跳下游，下游就是在旧数据上拼出来的结果，跑它只是白占预算。"""
    dropped = refresh._skip_closure(refresh.ALL_STEPS, frozenset({"readme"}))

    assert "readme" in dropped
    assert "parse" in dropped, "parse 依赖 readme，上游不跑它就是在旧正文上重算"
    assert "sizes" not in dropped


def test_plan里被skip的步骤带原因且不参与依赖判断() -> None:
    ready, skipped = refresh._plan(
        refresh.ALL_STEPS, done=set(), failed=set(), skip=frozenset({"readme"})
    )

    names = [step.name for step in ready]
    notes = {item.name: item.note for item in skipped}
    assert "readme" not in names
    assert notes["readme"] == "被 --skip 跳过"
    # 下游（依赖 readme 的）也一起被跳过，标的是上游原因
    downstream = [step.name for step in refresh.ALL_STEPS if "readme" in step.needs]
    assert downstream, "readme 至少有一个下游（parse）"
    for name in downstream:
        assert notes.get(name) == "上游被 --skip 跳过"


def test_文件清单与样例不在每日链上() -> None:
    """`fetch-files` / `fetch-preview` 是自由抓取：每日链不该再点名它们。

    两者都是「按下载量取头部」的富化资产，全量一轮几百 GB 下载、几十小时；
    塞进每日窗口只会挤掉采集本身，还会隔着锁与 /api 配额互相拖慢。
    """
    assert "files" not in refresh.STEP_NAMES
    assert "preview" not in refresh.STEP_NAMES

    commands = {step.args[0] for step in refresh.ALL_STEPS}
    assert "fetch-files" not in commands
    assert "fetch-preview" not in commands


def test_only点名已下线的文件清单会报未知步骤(tmp_path: Path) -> None:
    """`--only files` 必须报错，而不是「跑完了、什么都没做」。

    静默成功比报错坏得多：人会以为今天的文件清单已经补过了。
    """
    store = DatasetStore(tmp_path / "data")
    args = Namespace(only="files", strict=False, budget_hours=8.5, launch_hours=8.0)

    with pytest.raises(SystemExit, match="未知步骤名"):
        refresh.run(args, store)
