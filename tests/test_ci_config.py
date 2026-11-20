"""CI 与本地钩子的口径契约：同一组工具、同一把尺子。

本地 pre-commit 与 CI 各写一份命令，任何一边漏掉一个工具，另一边就退化成
「推送后才知道」。这里不比对说明文案（改一句注释就红、真实语义却没变），而是把
两边 `uv run <tool>` 抽成集合比相等：新增或删除工具时，两边必须一起动。

`ruff` 覆盖全仓另有一条单独判据：2026-11 曾出现 CI 的 test job 只查 `src tests`、
lint job 查全仓，而钩子带 `--fix` 会自动改写文件。
"""

from __future__ import annotations

import shlex
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _ci_commands() -> list[str]:
    config = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    return [
        step["run"]
        for job in config["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step.get("run"), str)
    ]


def _hook_commands() -> list[str]:
    config = yaml.safe_load((ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))
    return [
        entry
        for repo in config["repos"]
        for hook in repo["hooks"]
        if (entry := hook.get("entry")) is not None
    ]


def _tool(command: str) -> str | None:
    """`uv run <tool> ...` → 工具标识；不是 `uv run` 命令就返回 None。"""
    parts = shlex.split(command)
    if parts[:2] != ["uv", "run"]:
        return None
    words = [part for part in parts[2:] if not part.startswith("-")]
    if not words:
        return None
    tool, *rest = words
    if tool == "python":
        return "docs-links" if any("check_docs_links" in word for word in rest) else "python"
    if tool == "ruff" and rest:
        return f"ruff {rest[0]}"
    return tool


def _tools(commands: list[str]) -> set[str]:
    found = {_tool(line) for command in commands for line in command.splitlines()}
    return {tool for tool in found if tool}


def test_本地钩子与CI跑同一组工具() -> None:
    """两边只是 `--fix` / `-n 8` 这类参数不同；工具集合必须相同。"""
    assert _tools(_hook_commands()) == _tools(_ci_commands())


def test_ci里每一次ruff检查都覆盖全仓() -> None:
    """`src tests` 会漏掉 scripts/*.py；钩子检查全仓，两者必须对齐。"""
    ruff_runs = [command for command in _ci_commands() if "ruff" in command]

    assert ruff_runs, "CI 里没有 ruff 检查"
    assert all("src tests" not in command for command in ruff_runs)
    assert any("ruff check ." in command for command in ruff_runs)
    assert any("ruff format --check ." in command for command in ruff_runs)


def test_远端钩子版本都钉死() -> None:
    """远端 hook 不写 `rev` 会跟着默认分支漂，本地与 CI 就不再是同一份工具。"""
    config = yaml.safe_load((ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8"))

    for repo in config["repos"]:
        if repo["repo"] == "local":
            continue
        assert repo.get("rev"), f"{repo['repo']} 没有钉 rev"
