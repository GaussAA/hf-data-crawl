"""工具缓存忽略规则的契约测试。

钉住两件事：工具链里每个会写本地缓存的工具，其缓存目录必须在**根**
`.gitignore` 显式登记；工作区里新冒出的 `.*_cache` 目录必须同步登记。

只靠工具自建的目录内 `.gitignore`（mypy 就会这么做）不算数——那是工具行为，
不是仓库契约。新增工具或改用 `--cache-dir` 时，须一并更新 `TOOL_CACHES`、
根 `.gitignore` 与 `CODING_STANDARDS.md` 的收尾清单。
"""

from __future__ import annotations

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

#: 工具链里会生成缓存的工具 → 缓存目录。新增工具须一并登记。
TOOL_CACHES: dict[str, str] = {
    "pytest": ".pytest_cache",
    "ruff": ".ruff_cache",
    "mypy": ".mypy_cache",
}


def _root_rules() -> set[str]:
    """根 .gitignore 去掉注释与末尾斜杠后的规则集合。"""
    lines = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    return {
        line.strip().rstrip("/")
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    }


def test_每个工具的缓存目录都在根_gitignore_显式登记() -> None:
    """新增工具忘了写忽略规则，此处必红，而非等缓存被误提交才发现。"""
    expected = set(TOOL_CACHES.values())
    rules = _root_rules()

    assert expected <= rules, f"根 .gitignore 缺少：{sorted(expected - rules)}"


def test_缓存目录确实被_git_忽略() -> None:
    """登记了不算数，还得真的被 git 判为忽略。"""
    for directory in TOOL_CACHES.values():
        result = subprocess.run(
            ["git", "check-ignore", "-q", f"{directory}/"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=False,
        )

        assert result.returncode == 0, f"{directory}/ 未被 git 忽略"


def test_工作区新出现的缓存目录必须登记() -> None:
    """工具悄悄生成了新缓存目录，不能从清单旁边溜过去。"""
    actual = {
        path.name
        for path in ROOT.iterdir()
        if path.is_dir() and path.name.startswith(".") and path.name.endswith("_cache")
    }

    assert actual <= set(TOOL_CACHES.values()), (
        f"未登记的缓存目录：{sorted(actual - set(TOOL_CACHES.values()))}"
    )
