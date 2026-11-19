"""刷新鲜编排器：续跑判断、依赖跳过、软边界、并行段与台账。

这些测试不打网络：`run_step` 被换成假的，只验证**编排**——哪一步该跑、哪一步
该跳过、跳过的原因写没写对。真子进程的行为由各自的命令测试覆盖。
"""

from __future__ import annotations

import json
from argparse import Namespace
from datetime import datetime
from pathlib import Path

from hf_data_crawl.ops import refresh
from hf_data_crawl.ops.refresh import StepResult
from hf_data_crawl.platform.store import DatasetStore


def seed_run(
    store: DatasetStore,
    command: str,
    *,
    status: str = "success",
    payload: dict[str, object] | None = None,
) -> None:
    """写一条「今天的」运行记录，模拟上一次跑过这一步。"""
    runs = store.root / "runs"
    runs.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now()
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
    compact = plan.by_name("compact")
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

    export = plan.by_name("export")
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
    reconcile = plan.by_name("reconcile")
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
