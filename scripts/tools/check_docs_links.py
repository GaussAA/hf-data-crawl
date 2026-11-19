"""校验仓库内 Markdown 的文档引用，防止文档漂移。

检查三类问题：

1. Markdown 内联链接 ``[...](target)``，按所在文件目录解析；
2. 行内代码里**带仓库前缀**的路径，如 `` `docs/ISSUES.md` ``，按仓库根解析；
3. ``DECISIONS #nn`` 引用必须指向真实条目，且词表编号不得重号、不得断号。

第二类曾经是盲区：文档的 Legacy 段用反引号引用 `` `docs/ISSUES.md` ``，
文件被删后引用悬空，而只查内联链接的校验器与 CI 依旧全绿。裸文件名、
数据布局示例、``.scratch/<slug>/spec.md`` 这类模板占位符不视为路径，
否则会产生大量误报。

第三类针对引用键：编号是跨文档的稳定键，重号会让两个读者落到不同决策上，
而两处都读得通。词条与编号 2026-11-19 拆成两份文件（``GLOSSARY.md`` 放词、
``DECISIONS.md`` 放编号条目），所以这条同时兼作迁移检查。

CI 的 ``Check documentation`` 与 pre-commit 共用这一入口；判定标准是目标
文件真的存在、引用真的落地，而不是把名字打印一遍。纯标准库实现，不依赖项目环境。

用法::

    python scripts/tools/check_docs_links.py [--root DIR]
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
#: `.agents/skills` 是跟仓库走的项目技能，仍要参与校验；只跳过
#: `.agents/worktrees`（DSH 的工作树副本，每个都是一份完整 checkout）。
#:
#: `.workbuddy` 是本机 agent 的对话记忆与逐日工作日志（见 `.gitignore`）。它必须在这里
#: 再列一次：本校验按文件系统递归，不读 `.gitignore`，而记忆文件里满是当时的旧路径，
#: 于是同一份代码在 CI 上全绿、在本机永远报十几条断链——「本机红、CI 绿」比不校验更坏，
#: 它会训练人忽略这条命令。
#:
#: `.scratch` 同理（`.gitignore` 第 17 行）：临时探查脚本、别处的草稿与 issue 存档都堆在
#: 这里，写的是当时的相对路径。2026-11-19 一份 `.scratch/<slug>/issues/*.md` 里的
#: `` `docs/DATA.md` `` 就这样让本地全量测试红了两条，而 CI 上根本没有这个目录。
_EXCLUDED = {
    ".git",
    ".workbuddy",
    ".scratch",
    ".venv",
    "node_modules",
    "data",
    "__pycache__",
    "build",
    "dist",
}
#: `.agents/` 下唯一要跳过的子树；其余（尤其 skills）仍是仓库文档。
_WORKTREE_PARTS = (".agents", "worktrees")
#: 写在行内代码里、且以这些前缀开头的路径，按仓库根解析。
_REPO_PREFIXES = ("docs/", "scripts/", "src/", "tests/", ".github/")
#: 路径里出现空白、占位符或通配符，说明是命令片段/模板而非单一路径。
_NOT_A_PATH = re.compile(r"[\s<>{}*?$]")
#: DECISIONS 的条目行：`1. **...`。
_GLOSSARY_ENTRY = re.compile(r"^(\d+)\.\s", re.MULTILINE)
#: 引用键：`#74` 或范围 `#62–#66` / `#62-66`。
_GLOSSARY_REF = re.compile(r"#(\d+)(?:\s*[–—-]\s*#?(\d+))?")
#: 外部 PR 编号长得像引用键，但不是——`PR #27` 不该被当成词表条目。
_PR_REF = re.compile(r"\bPR\s*#\d+")


def markdown_files(root: Path) -> list[Path]:
    """仓库内需要检查的 Markdown 文件，按路径稳定排序。"""
    files: list[Path] = []
    for path in root.rglob("*.md"):
        relative = path.relative_to(root)
        if _EXCLUDED & set(relative.parts):
            continue
        if relative.parts[:2] == _WORKTREE_PARTS:
            continue
        files.append(path)
    return sorted(files)


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
    （模板占位符）、`python scripts/tools/check_docs_links.py`（命令片段）都不会。
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


def _scanned_numbered(text: str) -> Iterator[tuple[int, str]]:
    """逐行产出 (行号, 文本)，跳过围栏代码块。"""
    in_fence = False
    for number, line in enumerate(text.splitlines(), 1):
        if _FENCE.match(line):
            in_fence = not in_fence
            continue
        if not in_fence:
            yield number, line


def glossary_entries(text: str) -> list[int]:
    """按出现顺序取词表编号。"""
    return [int(match.group(1)) for match in _GLOSSARY_ENTRY.finditer(text)]


def _glossary_refs(line: str) -> Iterator[tuple[int, int]]:
    """取一行里的编号引用，返回 (起, 止)；单号时止 == 起。"""
    for match in _GLOSSARY_REF.finditer(line):
        start = int(match.group(1))
        end = int(match.group(2)) if match.group(2) else start
        yield start, end


def decisions_problems(root: Path) -> list[str]:
    """检查编号与引用键，返回 ``文件:行号: 问题`` 清单。

    编号在 `DECISIONS.md`（2026-11-19 从 `GLOSSARY.md` 拆出）：词表只放词，
    带编号的决策与接口事实放在这里。写成 `GLOSSARY #nn` 的引用一律报错——
    那是迁移前的写法，读者按它去词表里找会扑空。
    """
    path = root / "DECISIONS.md"
    if not path.is_file():
        return []
    text = path.read_text(encoding="utf-8", errors="replace")
    entries = glossary_entries(text)
    known = set(entries)
    problems: list[str] = []

    for number in sorted(known):
        count = entries.count(number)
        if count > 1:
            problems.append(f"DECISIONS.md: 编号 {number} 出现 {count} 次")
    expected = set(range(1, len(entries) + 1))
    missing = sorted(expected - known)
    extra = sorted(known - expected)
    if missing:
        problems.append(f"DECISIONS.md: 缺号 {', '.join(map(str, missing))}")
    if extra:
        problems.append(f"DECISIONS.md: 编号超出连续范围 {', '.join(map(str, extra))}")

    def check(target: Path, line_number: int, after: str) -> None:
        relative = target.relative_to(root)
        for start, end in _glossary_refs(after):
            for number in (start, end):
                if number not in known:
                    problems.append(f"{relative}:{line_number}: DECISIONS #{number} 不存在")
            if start > end:
                problems.append(f"{relative}:{line_number}: 引用范围 #{start}–{end} 顺序反了")

    # 文件内部用裸 `#nn` 引用；`PR #27` 不是引用键，先摘掉。
    for number, line in _scanned_numbered(text):
        check(path, number, _PR_REF.sub("PR", line))
    # 其他文档：同一行里出现 DECISIONS 后，其后的 `#nn` 视为编号引用。
    for target in markdown_files(root):
        if target == path:
            continue
        body = target.read_text(encoding="utf-8", errors="replace")
        for number, line in _scanned_numbered(body):
            if "GLOSSARY #" in line:
                # 文档里讲「这个旧写法要报错」时，行内本身就会出现 `GLOSSARY #`。
                # 与 doctor 的 _HISTORY_MARKERS 同一思路：讲规则的行不算违规。
                if any(marker in line for marker in ("已迁到", "旧写法", "这种写法")):
                    continue
                relative = target.relative_to(root)
                problems.append(
                    f"{relative}:{number}: 写成 `GLOSSARY #…` 了，编号已迁到 DECISIONS.md"
                )
                continue
            if "DECISIONS" in line:
                tail = _PR_REF.sub("PR", line.split("DECISIONS", 1)[1])
                check(target, number, tail)
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
        default=Path(__file__).resolve().parents[2],
        help="仓库根目录，默认取本脚本所在仓库",
    )
    args = parser.parse_args(argv)
    root = args.root.resolve()

    files = markdown_files(root)
    if not files:
        print(f"未找到 Markdown 文件：{root}", file=sys.stderr)
        return 1

    problems = broken_references(root)
    problems.extend(decisions_problems(root))
    if problems:
        print(f"发现 {len(problems)} 条失效文档引用：", file=sys.stderr)
        for item in problems:
            print(f"  {item}", file=sys.stderr)
        return 1
    print(f"链接校验通过：{len(files)} 个 Markdown 文件，0 条断链")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
