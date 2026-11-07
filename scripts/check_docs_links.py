"""校验仓库内 Markdown 的相对链接，防止文档漂移。

CI 的 ``Check documentation`` 与 pre-commit 共用这一入口；判定标准是目标文件
真的存在，而不是把文件名打印一遍。纯标准库实现，不依赖项目环境。

用法::

    python scripts/check_docs_links.py [--root DIR]
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

#: Markdown 内联链接 / 图片，捕获括号内的目标部分。
_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
#: 围栏代码块边界；块内的示例路径不是真链接，跳过以免误报。
_FENCE = re.compile(r"^\s*(?:```|~~~)")
#: 不参与扫描的目录：产物、依赖、缓存与工作区内部记录。
_EXCLUDED = {".git", ".venv", "node_modules", "data", "__pycache__", ".workbuddy", "build", "dist"}


def markdown_files(root: Path) -> list[Path]:
    """仓库内需要检查的 Markdown 文件，按路径稳定排序。"""
    return sorted(
        path for path in root.rglob("*.md") if not (_EXCLUDED & set(path.relative_to(root).parts))
    )


def link_targets(text: str) -> list[str]:
    """取出行内链接目标，跳过围栏代码块。"""
    targets: list[str] = []
    in_fence = False
    for line in text.splitlines():
        if _FENCE.match(line):
            in_fence = not in_fence
            continue
        if in_fence:
            continue
        targets.extend(match.group(1) for match in _LINK.finditer(line))
    return targets


def _relative_target(raw: str) -> str | None:
    """把链接目标规约成相对路径；外部 URL、纯锚点返回 None。"""
    target = raw.strip()
    if target.startswith("<") and target.endswith(">"):
        target = target[1:-1].strip()
    target = target.split("#", 1)[0].strip()
    # 可选的链接标题：("path" "title") / ('path' 'title')
    if len(target) > 1 and target[0] in "\"'":
        closing = target.find(target[0], 1)
        target = target[1:closing] if closing > 0 else target
    if not target:
        return None  # 纯锚点
    if re.match(r"^[A-Za-z][A-Za-z0-9+.-]*:", target):
        return None  # http/https/mailto 等外部协议
    return target.lstrip("/") if target.startswith("/") else target


def broken_links(root: Path) -> list[str]:
    """返回 ``文件: 目标`` 形式的断链清单，空列表表示全部有效。"""
    problems: list[str] = []
    for path in markdown_files(root):
        for raw in link_targets(path.read_text(encoding="utf-8", errors="replace")):
            target = _relative_target(raw)
            if target is None:
                continue
            base = root if raw.strip().startswith("/") else path.parent
            if not (base / target).resolve().exists():
                problems.append(f"{path.relative_to(root)}: {raw.strip()} -> {target}")
    return problems


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="校验 Markdown 相对链接")
    parser.add_argument(
        "--root",
        type=Path,
        default=Path(__file__).resolve().parent.parent,
        help="仓库根目录，默认取本脚本所在仓库",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    files = markdown_files(root)
    if not files:
        print(f"未找到 Markdown 文件：{root}", file=sys.stderr)
        return 1

    problems = broken_links(root)
    if problems:
        print(f"发现 {len(problems)} 条失效相对链接：", file=sys.stderr)
        for item in problems:
            print(f"  {item}", file=sys.stderr)
        return 1
    print(f"链接校验通过：{len(files)} 个 Markdown 文件，0 条断链")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
