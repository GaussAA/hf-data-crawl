"""文档链接校验器自身的回归测试。

钉住三件事：真断链必须被检出并报出位置；有效链接不能误报；
非 UTF-8 控制台（Windows runner 的 cp1252）不能让「通过」变成崩溃。
否则校验器很容易退化成「只打印文件名」的假检查。
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


def test_当前仓库的文档链接全部有效() -> None:
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


def test_cp1252_控制台下仍能输出正确结论() -> None:
    """Windows runner 默认 cp1252：中文提示不得让校验通过变成崩溃。"""
    result = _run(ROOT, io_encoding="cp1252")

    assert result.returncode == 0, result.stderr
    assert "UnicodeEncodeError" not in result.stderr
    assert "0 条断链" in result.stdout
