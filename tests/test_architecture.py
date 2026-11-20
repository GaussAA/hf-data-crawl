"""架构依赖规则：切片之间只能单向依赖，且不得成环。

次序（小者在下）：platform < pipeline < {acquire, query} < delivery < ops。`acquire/hf` 是 acquire 内部的子包，不单独成片。
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

#: 切片之间**登记在案**的依赖边。`RANK` 只保证方向合法且无环，它允许
#: `delivery -> acquire`（3 > 2）——而这条边在语义上是反的：数据流里 `acquire`
#: 在 `delivery` 上游，`delivery` 也从不消费 `acquire`。所以「方向合法」不等于
#: 「关系合理」：实际存在的边得单独登记，新增一条就要在这里过一目。
EDGES: dict[str, set[str]] = {
    "platform": set(),
    "pipeline": {"platform"},
    "acquire": {"pipeline", "platform"},
    "query": {"pipeline", "platform"},
    "delivery": {"query", "platform"},
    "ops": {"delivery", "query", "acquire", "pipeline", "platform"},
}


def _module_name(path: Path) -> str:
    parts = list(path.relative_to(SRC.parent).with_suffix("").parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def _base_module(path: Path, node: ast.ImportFrom) -> str:
    """一条 from-import 的来源模块（不含导入的名字）。"""
    if node.level == 0:
        return node.module or ""
    package = _module_name(path).split(".")
    if path.name != "__init__.py":
        package = package[:-1]
    package = package[: len(package) - (node.level - 1)]
    return ".".join([*package, *([node.module] if node.module else [])])


def _resolve(path: Path, node: ast.ImportFrom) -> list[str]:
    """把一条 from-import 解析成它指向的绝对模块名列表。"""
    base = _base_module(path, node)
    # `from pkg import a, b` 里的 a、b 可能是子模块：两种解释都收下。
    return [base, *(f"{base}.{alias.name}" for alias in node.names)]


def _imported_names(path: Path) -> list[tuple[int, str, str]]:
    """(行号, 来源模块, 被导入的名字)。用于检查跨切片导入是否用了私有名。

    `import pkg.slice._impl` 与 `from pkg.slice import _impl` 是同一件事的两种
    写法：只认 `ImportFrom`，规则就有一半是漏的——绕过去不需要动一个字符的语义。
    """
    tree = ast.parse(path.read_text(encoding="utf-8"))
    found: list[tuple[int, str, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom):
            base = _base_module(path, node)
            found.extend((node.lineno, base, alias.name) for alias in node.names)
        elif isinstance(node, ast.Import):
            for alias in node.names:
                head, _, tail = alias.name.rpartition(".")
                found.append((node.lineno, head, tail))
    return found


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


def _actual_edges() -> dict[str, set[str]]:
    """从源码汇总切片之间真实存在的依赖边。"""
    edges: dict[str, set[str]] = {name: set() for name in RANK}
    for path in _sources():
        mine = _slice_of(_module_name(path))
        if mine is None:
            continue
        for _, target in _imports(path):
            theirs = _slice_of(target)
            if theirs is not None and theirs != mine:
                edges[mine].add(theirs)
    return edges


def _edge_diff(actual: dict[str, set[str]]) -> str:
    lines: list[str] = []
    for name in sorted(RANK):
        for theirs in sorted(actual[name] - EDGES[name]):
            lines.append(f"未登记：{name} -> {theirs}（先想清楚合理与否，再写进 EDGES）")
        for theirs in sorted(EDGES[name] - actual[name]):
            lines.append(f"已消失：{name} -> {theirs}（有意删除就同步 EDGES）")
    return "\n".join(lines)


def test_跨切片依赖边与登记表一致() -> None:
    """`RANK` 只挡向上与同档，挡不住语义反过来的边。

    `delivery -> acquire` 方向合法（3 > 2），却从不存在也不该存在——数据流里
    `acquire` 在 `delivery` 上游。哪天有人图省事从 `delivery` 抓一个 `acquire`
    的逻辑，只靠 `RANK` 的那条规则会全绿。真实存在的边因此单独登记：新增一条
    就在这里写下来，删掉一条也同步，否则表会退化成一张没人看的清单。
    """
    actual = _actual_edges()

    assert actual == EDGES, _edge_diff(actual)


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


def test_cross_slice_imports_use_public_names() -> None:
    """跨切片只许导入公开名。

    切片内的下划线开头是实现细节：别的切片一旦依赖它，任何一次内部重命名都会
    在 import 期炸掉，而报错只会指向「模块里没有这个名字」，与真正的改动隔了
    好几层。命令入口尤其容易犯这个错——组合根图省事就直接抓私有处理器。
    """
    violations: list[str] = []
    for path in _sources():
        mine = _slice_of(_module_name(path))
        if mine is None:
            continue
        for lineno, module, name in _imported_names(path):
            if not module.startswith(PACKAGE):
                continue
            theirs = _slice_of(module)
            if theirs is None or theirs == mine:
                continue
            if name.startswith("_") and not (name.startswith("__") and name.endswith("__")):
                violations.append(f"{path.relative_to(SRC)}:{lineno} {mine} -> {module}.{name}")
    assert violations == [], "\n".join(violations)


def test_档案层只认契约不认形态() -> None:
    """`delivery/delivery.py` 只许看见契约，不许看见任何一种形态。

    这条比「别 import 私有名」更严：连公开名也不行。`_execute` 曾经写着
    `except backfill.FillError` —— 一行就足以把档案层与回填绑死，于是「加一种形态
    只加一行注册」成了空话（另外两百行也得写进同一个文件）。现在档案层的全部形态
    知识只有 `from . import fill_exec, query_exec` 这一句「点名」——它导进来只为
    让执行体在各自模块末尾把自己写进 `EXECUTORS`，不从这里取任何名字。

    契约（`contract.py`）反过来也不许 import 任何执行体，否则环会从另一头长回来。

    判据用 `_base_module()` 解析后的绝对模块名，而不是「相对导入且 level==1」：
    后者会被 `from hf_data_crawl.delivery.backfill import FillError` 整条跳过，
    档案层重新与回填绑死仍然全绿。
    """
    executor_modules = {"backfill", "query_exec", "fill_exec"}
    package = f"{PACKAGE}.delivery"
    violations: list[str] = []

    for name, forbidden in (
        ("delivery/delivery.py", executor_modules),
        ("delivery/contract.py", executor_modules | {"delivery"}),
    ):
        path = SRC / name
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                module = _base_module(path, node)
                # `from . import fill_exec` 是登记，允许；`from .fill_exec import X`
                # 与绝对导入 `from hf_data_crawl.delivery.fill_exec import X` 都不是。
                if not module.startswith(f"{package}."):
                    continue
                short = module.removeprefix(f"{package}.")
                if short in forbidden:
                    imported = ", ".join(alias.name for alias in node.names)
                    violations.append(f"{name}:{node.lineno} from {module} import {imported}")
            elif isinstance(node, ast.Import):
                # `import hf_data_crawl.delivery.backfill` 一样把档案层绑死在执行体上，
                # 效果与 `from ... import X` 无异；只遍历 ImportFrom 就漏了另外一半。
                for alias in node.names:
                    if not alias.name.startswith(f"{package}."):
                        continue
                    if alias.name.removeprefix(f"{package}.") in forbidden:
                        violations.append(f"{name}:{node.lineno} import {alias.name}")

    assert violations == [], "\n".join(violations)


def test_不使用动态导入绕开依赖检查() -> None:
    """AST 规则只认 `Import` / `ImportFrom`；`importlib` 与 `__import__` 对它们隐身。

    当前代码一处都没有用。这条把「零使用」钉成规则：一旦要用，先想清楚它绕过了
    哪一层检查（切片方向、模块环、跨切片私有名），再改成静态导入或在这里显式
    登记——而不是让四条规则同时失效而测试全绿。
    """
    found: list[str] = []
    for path in _sources():
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if isinstance(func, ast.Attribute):
                name = func.attr
            elif isinstance(func, ast.Name):
                name = func.id
            else:
                continue
            if name in {"import_module", "__import__"}:
                found.append(f"{path.relative_to(SRC)}:{node.lineno}")

    assert found == [], "动态导入会绕开切片与环检查：" + "、".join(found)


def test_普通import语句也逃不过私有名检查(tmp_path: Path) -> None:
    """`import pkg.slice._impl` 不走 `from`，只遍历 ImportFrom 的实现会整条跳过。"""
    probe = tmp_path / "probe.py"
    probe.write_text("import hf_data_crawl.acquire._session\n", encoding="utf-8")

    assert (1, "hf_data_crawl.acquire", "_session") in _imported_names(probe)


def _module_exists(parts: list[str]) -> bool:
    """`SRC/<parts...>` 是否真的是个模块（.py 或包）。"""
    base = SRC.joinpath(*parts)
    return base.with_suffix(".py").is_file() or (base / "__init__.py").is_file()


def test_组合根只经各切片的公开入口() -> None:
    """`ops/cli.py` 只许 import `<切片>` 或 `<切片>.commands`。

    「唯一组合根」原先只写在 docstring 里：多抓一个 `<切片>.recovery` 不会有任何
    测试变红，于是「只调公开 run_*」是一句没有机制的话。`pipeline.recovery` 是
    登记在案的唯一例外——`pipeline/__init__.py` 为避环不导出 `check_and_recover`。
    """
    path = SRC / "ops" / "cli.py"
    violations: list[str] = []
    for lineno, target in _imports(path):
        if not target.startswith(PACKAGE):
            continue
        parts = target.split(".")[1:]
        if not parts or parts[0] == "ops":
            continue
        # `from ..platform import DatasetStore` 里的 DatasetStore 不是模块，`_imports`
        # 会把每个别名都当作可能的子模块；只有真实存在的子模块才算越界。
        if not _module_exists(parts) or len(parts) == 1:
            continue
        if parts[1:] == ["commands"]:
            continue
        if parts[0] == "pipeline" and parts[1:] == ["recovery"]:
            continue
        violations.append(f"{path.name}:{lineno} import {target}")

    assert violations == [], "组合根只能走 <切片> 与 <切片>.commands：\n" + "\n".join(violations)


def test_参数表只声明参数不承担执行() -> None:
    """`ops/parsers.py` 只许 import 常量与枚举，不许碰各切片的执行函数。

    上一条规则只盯着 `cli.py` 一个文件；参数表若偷偷 import `run_*`，它就变成
    第二个组合根，而没有任何测试会说话。参数表与「谁加锁、谁记账」分开的全部
    意义，就在于前者不承担执行——所以这条边界同样要落成机制。
    """
    path = SRC / "ops" / "parsers.py"
    offenders = [
        f"{path.name}:{lineno} from {base} import {name}"
        for lineno, base, name in _imported_names(path)
        if name.startswith("run_") or base.split(".")[-1] == "commands"
    ]

    assert offenders == [], "参数表里出现了执行细节：\n" + "\n".join(offenders)


def _declared(slice_name: str) -> set[str]:
    """读出某切片 `__init__.py` 里 `__all__` 登记的名字（只解析，不执行导入）。"""
    tree = ast.parse((SRC / slice_name / "__init__.py").read_text(encoding="utf-8"))
    names: set[str] = set()
    for node in tree.body:
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.List):
            if not any(isinstance(t, ast.Name) and t.id == "__all__" for t in node.targets):
                continue
            names |= {
                item.value
                for item in node.value.elts
                if isinstance(item, ast.Constant) and isinstance(item.value, str)
            }
    return names


def test_跨切片只能依赖登记过的符号() -> None:
    """别的切片能用的名字，必须在目标切片的 `__all__` 里登记过。

    原来的九条规则管住了「依赖谁」，管不住「依赖到什么程度」：`delivery` 抓一个
    `query.gold.share_text` 与抓 `query.gold.GoldCatalog` 一样畅通，于是哪个是
    契约、哪个只是眼下顺手拿的，事后无从分辨。这条要求前者登记、后者也登记，
    区别只在于——登记时要停下来想一次「这算不算我的接口」。

    登记的粒度由各切片的公开面自己定，两种写法都算数：

    - 符号级（`platform` / `query`）：`from ..platform.atomic import write_bytes`
      里 `write_bytes` 出现在 `__all__`；
    - 子模块级（`pipeline`）：`from ..pipeline.rebuild import discover_raw_dates`
      里 `rebuild` 出现在 `__all__`——「我依赖重建这一整块」本身就是一句可审的话。

    判据取「路径上的子模块名或末端的符号名，任一登记即可」：这两种写法表达的是
    同一件事，只认一种的话，换个写法就绕过去了。

    `ops` 不在约束范围内。它是组合根兼运维层，职责就是横跨所有切片：CLI 要调
    每个 `run_*`，体检要读各切片的诊断函数。要求它「只碰登记符号」只会逼着
    各切片把诊断函数一并登记，登记的筛选作用就没了。
    """
    declared = {name: _declared(name) for name in RANK}
    violations: list[str] = []
    for path in _sources():
        mine = _slice_of(_module_name(path))
        if mine is None or mine == "ops":
            continue
        for lineno, module, name in _imported_names(path):
            if not module.startswith(PACKAGE):
                continue
            theirs = _slice_of(module)
            if theirs is None or theirs == mine:
                continue
            parts = module.split(".")
            allowed = {name} if len(parts) == 2 else {parts[2], name}
            if allowed & declared[theirs]:
                continue
            violations.append(
                f"{path.relative_to(SRC)}:{lineno} {mine} -> {module}.{name}"
                f"（要就登记进 {theirs}/__init__.py 的 __all__，要就别依赖）"
            )
    assert violations == [], "跨切片用到了未登记的名字：\n" + "\n".join(violations)


def test_门面登记的名字都真实存在() -> None:
    """`__all__` 里写了却取不到，等于给外部一张空头契约。

    子模块名（如 `pipeline.rebuild`）按文件是否存在判断，不用 `hasattr`：包内
    子模块要等第一次 import 才会成为父模块的属性，用 `hasattr` 会在「还没人
    import 过」时误报。
    """
    from importlib import import_module

    missing: list[str] = []
    for name in RANK:
        module = import_module(f"{PACKAGE}.{name}")
        for attr in _declared(name):
            if _module_exists([name, attr]) or hasattr(module, attr):
                continue
            missing.append(f"{name}.{attr}")
    assert missing == [], "__all__ 里登记了取不到的名字：" + "、".join(missing)
