"""CI 与本地钩子的口径契约：ruff 覆盖全仓，注释不许声称「完全同一动作」。

CI 的 test job 曾只查 `src tests`、lint job 查全仓，而 pre-commit 钩子带
`--fix` 会自动改写文件——三处口径分裂时，「本地过了 CI 却挂」就成了玄学。
"""

from __future__ import annotations

from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def _run_commands() -> list[str]:
    config = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    return [
        step["run"]
        for job in config["jobs"].values()
        for step in job.get("steps", [])
        if isinstance(step.get("run"), str)
    ]


def test_ruff在CI里一律覆盖全仓() -> None:
    """`src tests` 会漏掉 scripts/*.py；钩子检查全仓，两者必须对齐。"""
    ruff_runs = [command for command in _run_commands() if "ruff" in command]

    assert ruff_runs, "CI 里没有 ruff 检查"
    assert all("src tests" not in command for command in ruff_runs)
    assert any("ruff check ." in command for command in ruff_runs)
    assert any("ruff format --check ." in command for command in ruff_runs)


def test_文档不再声称本地与CI完全同一动作() -> None:
    """钩子带 --fix 会改写文件，CI 只读检查；注释不能再写成「无从发生」。"""
    hooks = (ROOT / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    standards = (ROOT / "CODING_STANDARDS.md").read_text(encoding="utf-8")

    assert "--fix" in hooks
    assert "钩子版本不会悄悄漂移" in hooks
    assert "无从发生" not in hooks
    assert "`uv run ruff check .`" in standards
