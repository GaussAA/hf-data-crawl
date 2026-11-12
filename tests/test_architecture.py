"""架构依赖规则：切片之间只能单向依赖，且不得成环。

次序（小者在下）：platform < pipeline < {acquire, query} < delivery < ops。`n`acquire/hf` 是 acquire 内部的子包，不单独成片。
每个切片只能 import 次序严格低于自己的切片，或自己内部的模块；同一档的两个切片
（acquire 与 query）互不 import。检查覆盖所有导入：顶层、函数内延迟导入、
`TYPE_CHECKING` 块里的导入，一视同仁，否则延迟导入会把环藏起来。
"""

from __future__ import annotations

import ast
from pathlib import Path

PACKAGE = "hf_data_crawl"
SRC = Path(__file__).resolve().parent.parent / "src" / PACKAGE

#: 切片次序。同值表示同档，档内互不依赖。
RANK: dict[str, int] = {
    "platform": 0,
    "pipeline": 1,
    "acquire": 2,
    "query": 2,
    "delivery": 3,
    "ops": 4,
}


def _module_name(path: Path) -> str:
    parts = list(path.relative_to(SRC.parent).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _resolve(path: Path, node: ast.ImportFrom) -> list[str]:
    """把一条 from-import 解析成它指向的绝对模块名列表。"""
    if node.level == 0:
        base = node.module or ""
    else:
        package = _module_name(path).split(".")
        if path.name != "__init__.py":
            package = package[:-1]
        package = package[: len(package) - (node.level - 1)]
        base = ".".join([*package, *([node.module] if node.module else [])])
    # `from pkg import a, b` 里的 a、b 可能是子模块：两种解释都收下。
    return [base, *(f"{base}.{alias.name}" for alias in node.names)]


def _imports(path: Path) -> list[tuple[int, str]]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            found.extend(
                (node.lineno, alias.name) for alias in node.names if alias.name.startswith(PACKAGE)
            )
        elif isinstance(node, ast.ImportFrom):
            found.extend(
                (node.lineno, name) for name in _resolve(path, node) if name.startswith(PACKAGE)
            )
    return found


def _slice_of(module: str) -> str | None:
    parts = module.split(".")
    return parts[1] if len(parts) > 1 and parts[1] in RANK else None


def _sources() -> list[Path]:
    return sorted(path for path in SRC.rglob("*.py") if "__pycache__" not in path.parts)


def test_every_module_lives_in_a_known_slice() -> None:
    """包根只留入口；其余模块必须落在已登记的切片里。"""
    strays = [
        path.name for path in SRC.glob("*.py") if path.name not in {"__init__.py", "__main__.py"}
    ]
    allowed = {*RANK, "__pycache__"}
    unknown = [p.name for p in SRC.iterdir() if p.is_dir() and p.name not in allowed]
    assert strays == [] and unknown == []


def test_slices_only_depend_downwards() -> None:
    """切片只依赖次序更低的切片；同档切片互不依赖。"""
    violations: list[str] = []
    for path in _sources():
        mine = _slice_of(_module_name(path))
        if mine is None:
            continue
        seen: set[tuple[int, str]] = set()
        for lineno, target in _imports(path):
            theirs = _slice_of(target)
            if theirs is None or theirs == mine or (lineno, theirs) in seen:
                continue
            seen.add((lineno, theirs))
            if RANK[theirs] >= RANK[mine]:
                relative = path.relative_to(SRC)
                violations.append(f"{relative}:{lineno} {mine} -> {theirs}")
    assert violations == [], "\n".join(violations)


def test_platform_depends_on_nothing_else() -> None:
    """platform 是最底层：不得 import 任何其他切片。"""
    leaks = [
        f"{path.relative_to(SRC)}:{lineno} -> {target}"
        for path in (SRC / "platform").rglob("*.py")
        for lineno, target in _imports(path)
        if (_slice_of(target) or "platform") != "platform"
    ]
    assert leaks == [], "\n".join(leaks)


def test_no_import_cycles_between_modules() -> None:
    """模块级无环：含函数内延迟导入与 TYPE_CHECKING 导入。"""
    known = {_module_name(path) for path in _sources()}
    graph: dict[str, set[str]] = {}
    for path in _sources():
        name = _module_name(path)
        edges = graph.setdefault(name, set())
        for _, target in _imports(path):
            if target in known and target != name:
                edges.add(target)

    cycles: list[str] = []
    state: dict[str, int] = {}

    def visit(node: str, stack: list[str]) -> None:
        state[node] = 1
        for nxt in sorted(graph.get(node, ())):
            if state.get(nxt) == 1:
                cycles.append(" -> ".join([*stack[stack.index(nxt) :], node, nxt]))
            elif nxt not in state:
                visit(nxt, [*stack, node])
        state[node] = 2

    for module in sorted(graph):
        if module not in state:
            visit(module, [])
    assert cycles == [], "\n".join(cycles)
