"""文档链接校验器自身的回归测试。

钉住两件事：真断链必须被检出并报出位置；有效链接不能误报。
否则校验器很容易退化成「只打印文件名」的假检查。
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "check_docs_links.py"


def _run(root: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), "--root", str(root)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )


def test_当前仓库的文档链接全部有效() -> None:
    result = _run(ROOT)

    assert result.returncode == 0, result.stderr
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
    assert "README.md" in result.stderr
    assert "missing.md" in result.stderr
    assert "not-a-link.md" not in result.stderr  # 代码块内不参与校验
    assert "https://example.com" not in result.stderr  # 外链不参与校验


def test_修好断链后校验转为通过(tmp_path: Path) -> None:
    (tmp_path / "a.md").write_text("[指向 b](b.md)\n", encoding="utf-8")
    (tmp_path / "b.md").write_text("# B\n", encoding="utf-8")

    result = _run(tmp_path)

    assert result.returncode == 0, result.stderr
