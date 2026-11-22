"""工具缓存与交付档案的忽略规则契约。

钉住两件事：

1. 工具链里会写本地缓存的工具，其目录必须在**根** `.gitignore` 显式登记，且
   `git check-ignore -v` 的来源确实是这份文件——只判「被忽略」不够：全局
   `core.excludesFile` 也能让 `check-ignore` 返回 0，仓库规则被删照样绿。
2. 交付档案与项目技能的白名单逐项生效（`deliveries/**` 只放行话、不放行产物字节）。

缓存只靠工具自建的目录内 `.gitignore`（mypy 就会这么做）不算数：那是工具行为，
不是仓库契约；换 `--cache-dir` 或工具升级时就会漏。这张 `TOOL_CACHES` 表是契约的
机器可读版本，新增工具时人工登记一条——「新工具会不会偷偷写缓存」无法从配置推出来。
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

#: 交付档案目录的放行名单：`**/deliveries/**` 排除整目录后再逐项放行。
DELIVERY_WHITELIST = ("manifest.json", "README.md", "INDEX.md", "AUDIT.md")


def _root_rules() -> set[str]:
    """根 .gitignore 去掉注释与末尾斜杠后的规则集合。"""
    lines = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    return {
        line.strip().rstrip("/")
        for line in lines
        if line.strip() and not line.lstrip().startswith("#")
    }


def _check_ignore(path: str) -> tuple[bool, str]:
    """(是否被忽略, 命中的规则来源)。

    `-v` 的退出码对「白名单放行」也返回 0（它表示「命中了某条规则」），所以不能
    拿退出码当忽略判定：要看命中行里的模式是否以 `!` 开头。来源为空表示没命中
    任何规则。
    """
    result = subprocess.run(
        ["git", "check-ignore", "-v", "--no-index", path],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    line = result.stdout.strip()
    if not line:
        return False, ""
    location = line.split("\t", 1)[0]
    parts = location.split(":", 2)
    if len(parts) < 3:
        return False, ""
    source, lineno, pattern = parts
    return not pattern.startswith("!"), f"{source}:{lineno}"


def test_工具缓存目录都登记在根gitignore且确实生效() -> None:
    """登记了不算数，还得由仓库规则真的忽略。"""
    expected = set(TOOL_CACHES.values())
    rules = _root_rules()
    assert expected <= rules, f"根 .gitignore 缺少：{sorted(expected - rules)}"

    for directory in TOOL_CACHES.values():
        ignored, source = _check_ignore(f"{directory}/")
        assert ignored, f"{directory}/ 未被 git 忽略"
        assert source.startswith(".gitignore:"), (
            f"{directory}/ 由 {source} 忽略，来源不是仓库 .gitignore"
        )


def test_覆盖率产物也被忽略() -> None:
    """coverage.py 写的是文件，扫 `.*_cache` 目录的那条规则管不到。

    2026-11-18 引入 pytest-cov 时补的：`--cov` 一跑就在根下留 `.coverage`，
    而 `TOOL_CACHES` 只登记目录。不单独钉住，迟早有人 `git add -A` 把它带进提交
    ——那正是 `.workbuddy/` 踩过的那类事故。
    """
    for path in (
        ".coverage",
        ".coverage.desktop.1234",
        "coverage.xml",
        "htmlcov/index.html",
    ):
        ignored, source = _check_ignore(path)
        assert ignored, f"{path} 未被 git 忽略"
        assert source.startswith(".gitignore:"), f"{path} 的忽略来源不是仓库 .gitignore"


def test_交付档案与项目技能的白名单确实生效() -> None:
    """两处「先排除整目录、再放行子项」最容易写错：目录没放行时文件白名单不起作用。

    放行名单逐项验，不抽一两个代表：删掉 INDEX/AUDIT 任一条白名单而不验，
    交付索引与审计记录就会被静默挡在版本库外。
    """
    ignored_cases = [
        ("requests/REQ-9999-query-x/deliveries/20261115T000000/data.parquet", True),
        (".agents/memory/2026-11-15.md", True),
    ]
    kept_cases = [
        *(
            (f"requests/REQ-9999-query-x/deliveries/20261115T000000/{name}", False)
            for name in DELIVERY_WHITELIST
        ),
        (".agents/skills/hf-request/SKILL.md", False),
    ]

    for path, expected_ignored in [*ignored_cases, *kept_cases]:
        ignored, _ = _check_ignore(path)
        assert ignored is expected_ignored, (
            f"{path} 的忽略判定不符合白名单契约（ignored={ignored}）"
        )


def test_临时探针脚本也被忽略() -> None:
    """`.tmp_` 前缀与 `.tmp` 后缀是「当场用完即废」的约定，不是命名风格。

    2026-11-22 真发生过：一份 `.tmp_probe.py`（硬编码本机代理端口、import 即发请求）
    被 `git add -A` 带进了 `ab5b063` 的功能提交，直到有人问「这个需要提交吗」才发现。
    与 `.coverage` 同一类：**规则缺一条，事故就会自己找上门**——所以这里连「来源是
    仓库 .gitignore」也一并验，免得将来靠全局 excludesFile 蒙混过关。
    """
    for path in (".tmp_probe.py", "probe.tmp"):
        ignored, source = _check_ignore(path)
        assert ignored, f"{path} 未被 git 忽略"
        assert source.startswith(".gitignore:"), f"{path} 的忽略来源不是仓库 .gitignore"
