"""文档引用校验器自身的回归测试。

钉住四件事：内联链接与行内代码路径的断链都必须被检出并报出位置；有效引用
不能误报；v1.0 需求→切片追溯链不得被静默删除；非 UTF-8 控制台（Windows
runner 的 cp1252）不能让「通过」变成崩溃。否则校验器很容易退化成「只打印
文件名」的假检查。
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_docs_links.py"


def _run(root: Path, io_encoding: str | None = None) -> subprocess.CompletedProcess[str]:
    env = None
    if io_encoding is not None:
        env = {**os.environ, "PYTHONIOENCODING": io_encoding}
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        env=env,
        check=False,
    )


def test_当前仓库的文档引用全部有效() -> None:
    result = _run(ROOT)

    assert result.returncode == 0, result.stderr
    assert "UnicodeEncodeError" not in result.stderr
    assert "0 条断链" in result.stdout


def test_断链必须被检出并报出文件与位置(tmp_path: Path) -> None:
    (tmp_path / "ok.md").write_text("# OK\n", encoding="utf-8")
    (tmp_path / "README.md").write_text(
        "[有效](ok.md) [失效](missing.md) [外链](https://example.com)\n"
        "```\n[代码块示例](not-a-link.md)\n```\n",
        encoding="utf-8",
    )

    result = _run(tmp_path)

    assert result.returncode == 1
    assert "UnicodeEncodeError" not in result.stderr  # 不能靠崩溃凑出非零退出码
    assert "README.md" in result.stderr
    assert "missing.md" in result.stderr
    assert "not-a-link.md" not in result.stderr  # 代码块内不参与校验
    assert "https://example.com" not in result.stderr  # 外链不参与校验


def test_修好断链后校验转为通过(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("[指向 b](b.md)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("# B\n", encoding="utf-8")

    result = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "UnicodeEncodeError" not in result.stderr


def test_行内代码的仓库路径失效必须被检出(tmp_path: Path) -> None:
    """Legacy 段用反引号引用文件，文件删了也不能静默通过。"""
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "tracker.md").write_text(
        "v1.0 切片表见 `docs/ISSUES.md`。\n", encoding="utf-8"
    )

    result = _run(tmp_path)

    assert result.returncode == 1
    assert "UnicodeEncodeError" not in result.stderr
    assert "tracker.md" in result.stderr
    assert "docs/ISSUES.md" in result.stderr
    assert "行内代码" in result.stderr


def test_行内代码路径修好后转为通过(tmp_path: Path) -> None:
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "tracker.md").write_text(
        "v1.0 切片表见 `docs/ISSUES.md`。\n", encoding="utf-8"
    )
    (tmp_path / "docs" / "ISSUES.md").write_text("# Issues\n", encoding="utf-8")

    result = _run(tmp_path)

    assert result.returncode == 0, result.stderr
    assert "0 条断链" in result.stdout


def test_裸文件名占位符与命令片段不误报(tmp_path: Path) -> None:
    (tmp_path / "note.md").write_text(
        "`derive.py` 是源码；`silver/dt={dt}/silver.parquet` 是数据布局；\n"
        "`.scratch/<slug>/spec.md` 是模板；`python scripts/check_docs_links.py` 是命令。\n"
        "```\n`docs/missing-in-fence.md`\n```\n",
        encoding="utf-8",
    )

    result = _run(tmp_path)

    assert result.returncode == 0, result.stderr


def test_v1_0_需求到切片的追溯链必须完整() -> None:
    """两份档案互为参照：连文档带索引一起删，也不能悄悄通过。"""
    prd = ROOT / "docs" / "PRD-hf-data-crawl-v1.0.md"
    issues = ROOT / "docs" / "ISSUES.md"
    index = (ROOT / "docs" / "README.md").read_text(encoding="utf-8")

    assert prd.is_file() and issues.is_file()
    assert "User Stories" in prd.read_text(encoding="utf-8")
    assert "覆盖故事" in issues.read_text(encoding="utf-8")
    assert prd.name in index and issues.name in index


def test_cp1252_控制台下仍能输出正确结论() -> None:
    """Windows runner 默认 cp1252：中文提示不得让校验通过变成崩溃。"""
    result = _run(ROOT, io_encoding="cp1252")

    assert result.returncode == 0, result.stderr
    assert "UnicodeEncodeError" not in result.stderr
    assert "0 条断链" in result.stdout
