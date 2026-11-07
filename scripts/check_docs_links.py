"""校验仓库内 Markdown 的文档引用，防止文档漂移。

检查两类引用：

1. Markdown 内联链接 ``[...](target)``，按所在文件目录解析；
2. 行内代码里**带仓库前缀**的路径，如 `` `docs/ISSUES.md` ``，按仓库根解析。

第二类曾经是盲区：文档的 Legacy 段用反引号引用 `` `docs/ISSUES.md` ``，
文件被删后引用悬空，而只查内联链接的校验器与 CI 依旧全绿。裸文件名、
数据布局示例、``.scratch/<slug>/spec.md`` 这类模板占位符不视为路径，
否则会产生大量误报。

CI 的 ``Check documentation`` 与 pre-commit 共用这一入口；判定标准是目标
文件真的存在，而不是把文件名打印一遍。纯标准库实现，不依赖项目环境。

用法::

    python scripts/check_docs_links.py [--root DIR]
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Iterator
from pathlib import Path

#: Markdown 内联链接 / 图片，捕获括号内的目标部分。
_LINK = re.compile(r"!?\[[^\]]*\]\(([^)]+)\)")
#: 行内代码；是否为「仓库路径」由 _repo_relative_target 判定。
_CODE_PATH = re.compile(r"`([^`\n]+)`")
#: 围栏代码块边界；块内的示例路径不是真引用，跳过以免误报。
_FENCE = re.compile(r"^\s*(?:```|~~~)")
#: 不参与扫描的目录：产物、依赖、缓存与工作区内部记录。
_EXCLUDED = {".git", ".venv", "node_modules", "data", "__pycache__", ".workbuddy", "build", "dist"}
#: 写在行内代码里、且以这些前缀开头的路径，按仓库根解析。
_REPO_PREFIXES = ("docs/", "scripts/", "src/", "tests/", ".github/")
#: 路径里出现空白、占位符或通配符，说明是命令片段/模板而非单一路径。
_NOT_A_PATH = re.compile(r"[\s<>{}*?$]")


def markdown_files(root: Path) -> list[Path]:
    """仓库内需要检查的 Markdown 文件，按路径稳定排序。"""
    return sorted(
        path for path in root.rglob("*.md") if not (_EXCLUDED & set(path.relative_to(root).parts))
    )


def _scanned_lines(text: str) -> Iterator[str]:
    """逐行产出参与校验的文本，跳过围栏代码块内容。"""
    in_fence = False
    for line in text.splitlines():
        if _FENCE.match(line):
            in_fence = not in_fence
            continue
        if not in_fence:
            yield line


def link_targets(text: str) -> list[str]:
    """取出行内链接目标。"""
    return [match.group(1) for line in _scanned_lines(text) for match in _LINK.finditer(line)]


def code_paths(text: str) -> list[str]:
    """取出行内代码内容，由调用方筛出真正的仓库路径。"""
    return [match.group(1) for line in _scanned_lines(text) for match in _CODE_PATH.finditer(line)]


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


def _repo_relative_target(raw: str) -> str | None:
    """行内代码里带仓库前缀的单一路径；其余写法返回 None。

    只认「前缀 + 单一路径」：`docs/ISSUES.md` 会被校验，`derive.py`（裸文件名）、
    `silver/dt={dt}/silver.parquet`（数据布局示例）、`.scratch/<slug>/spec.md`
    （模板占位符）、`python scripts/check_docs_links.py`（命令片段）都不会。
    """
    target = raw.strip().split("#", 1)[0].strip()
    if not target.startswith(_REPO_PREFIXES) or _NOT_A_PATH.search(target):
        return None
    if ".." in Path(target).parts:
        return None  # 带 .. 的写法语义依所在目录而定，不按仓库根解析
    return target


def broken_references(root: Path) -> list[str]:
    """返回 ``文件: 目标`` 形式的失效引用清单，空列表表示全部有效。"""
    problems: list[str] = []
    for path in markdown_files(root):
        text = path.read_text(encoding="utf-8", errors="replace")
        relative = path.relative_to(root)
        for raw in link_targets(text):
            target = _relative_target(raw)
            if target is None:
                continue
            base = root if raw.strip().startswith("/") else path.parent
            if not (base / target).resolve().exists():
                problems.append(f"{relative}: 链接 {raw.strip()} -> {target}")
        for raw in code_paths(text):
            target = _repo_relative_target(raw)
            if target is None:
                continue
            if not (root / target).resolve().exists():
                problems.append(f"{relative}: 行内代码 {raw.strip()} -> {target}")
    return problems


def _force_utf8_output() -> None:
    """把标准输出固定为 UTF-8。

    Windows runner 的 stdout 默认 cp1252 + strict，成功时打印中文会抛
    UnicodeEncodeError，把「校验通过」误判成失败；stderr 默认 backslashreplace，
    中文被转义得难以阅读。文本流支持 reconfigure 时统一切换；被重定向为内存流
    （如测试捕获）时本就不做编码，跳过即可。
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is not None:
            reconfigure(encoding="utf-8", errors="replace")


def main(argv: list[str] | None = None) -> int:
    _force_utf8_output()
    parser = argparse.ArgumentParser(description="校验 Markdown 文档引用")
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

    problems = broken_references(root)
    if problems:
        print(f"发现 {len(problems)} 条失效文档引用：", file=sys.stderr)
        for item in problems:
            print(f"  {item}", file=sys.stderr)
        return 1
    print(f"链接校验通过：{len(files)} 个 Markdown 文件，0 条断链")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
