"""`delivery` 切片的命令处理函数，参数解析与分派在 ops/cli。"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from ..platform.runs import Outcome
from ..platform.store import DatasetStore
from . import compare, specdiff
from .backfill import FillError, header_change, header_of
from .delivery import DeliveryBlocked, DeliveryResult, Preview, deliver, preview, run_due
from .method import METHOD_DIR, METHOD_DOC, METHOD_INDEX, problems
from .method import sync as sync_method
from .metrics import lookup
from .requirement import (
    DELIVERIES_DIR,
    HISTORY_DIR,
    INPUT_DIR,
    MANIFEST_NAME,
    SPEC_NAME,
    Cadence,
    Kind,
    Mode,
    Spec,
    SpecError,
    compile_sql,
    directory_name,
    history_versions,
    resolve_dir,
    skeleton,
    spec_digest,
    version_conflict,
)
from .requirement import load as load_spec


def _export_wide(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """导出宽表。失败由 `_execute` 统一记进 runs/，不留半截文件。"""
    from .wide import export_wide

    target = Path(args.out) if args.out else None
    summary = export_wide(store, target)
    print(
        f"宽表已导出：{summary.path}（{summary.columns} 列 · "
        f"{summary.rows:,} 行 · {summary.bytes / 1e6:.1f}MB）"
    )
    print("  可重建产物：不参与状态更新，删掉后重跑本命令即可")
    return Outcome(summary)


def deliveries_of(request_dir: Path) -> list[Path]:
    """一次需求的交付目录，按名字升序——名字是时间戳，字典序即时间序。

    以 `.` 开头的是原子写留下的 `.tmp-*` 暂存目录，它**不是**一次交付：算进台账
    会让「once 已交付过」凭空成立，把真正需要的那份挡在门外。
    """
    base = request_dir / DELIVERIES_DIR
    if not base.is_dir():
        return []
    return sorted(
        path for path in base.iterdir() if path.is_dir() and not path.name.startswith(".")
    )


def criteria_text(spec: Spec) -> str:
    """把口径说成一句话，供 list 打印。形态不同，但都只报「这次要什么」。"""
    if spec.kind is Kind.BACKFILL:
        fill = spec.fill
        if fill is None:
            return "回填表格（fill 段待补）"
        return (
            f"回填 {fill.source.path.name}：{len(fill.columns)} 列，标识列 {fill.source.key_column}"
        )
    if spec.mode is Mode.SQL:
        return "手写 SQL（见 spec.yaml）"
    return " 且 ".join(f"{item.metric} {item.op} {item.value}" for item in spec.criteria)


def manifest_summary(directory: Path) -> str:
    """交付目录的一行摘要。读不到就留空——台账缺一份不该让 show 整体失败。"""
    path = directory / MANIFEST_NAME
    if not path.exists():
        return "  （缺 manifest）"
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return "  （manifest 读不出来）"
    rows = (body.get("result") or {}).get("rows")
    status = (body.get("health") or {}).get("status", "?")
    return f"  {rows} 行 · {status}"


def _request(args: argparse.Namespace) -> int:
    """需求管理里不写数据的几个动作，都不持数据目录的锁。"""
    root = Path(args.requests_dir)
    if args.request_action == "new":
        return _request_new(args, root)
    if args.request_action == "list":
        return _request_list(root)
    if args.request_action == "method":
        return _request_method(root, args.id, check=args.check)
    if args.request_action == "input":
        return _request_input(root, args)
    if args.request_action == "diff":
        return _request_diff(root, args)
    if args.request_action == "compare":
        return _request_compare(root, args)
    return _request_show(root, args.id)


def _brief(text: str, limit: int = 42) -> str:
    flat = text.replace("\r", " ").replace("\n", " ").strip()
    if not flat:
        return "（空）"
    return flat if len(flat) <= limit else flat[: limit - 1] + "…"


def _request_compare(root: Path, args: argparse.Namespace) -> int:
    """逐列比对两次交付。默认最近两次——「刚改完这版动了什么」最常问的就是这一对。"""
    try:
        request_dir = resolve_dir(root, args.id)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    done = deliveries_of(request_dir)
    if args.left and args.right:
        left, right = args.left, args.right
    elif len(done) < 2:
        print(f"{args.id} 只有 {len(done)} 次交付，没有可比对象。")
        return 1
    else:
        left, right = done[-2].name, done[-1].name

    base = request_dir / DELIVERIES_DIR
    for name in (left, right):
        if not (base / name).is_dir():
            existing = "、".join(path.name for path in done) or "无"
            print(f"失败：没有交付 {name}（现有：{existing}）")
            return 1

    try:
        result = compare.compare_deliveries(base / left, base / right)
    except compare.CompareError as error:
        print(f"失败：{error}")
        return 1

    print(f"{args.id} 交付对比（{result.kind}）：{result.left} → {result.right}")
    print(
        f"  行数：{result.left_rows:,} → {result.right_rows:,}"
        f"（共同 {result.common_rows:,} · 新增 {len(result.added)} · 消失 {len(result.removed)}）"
    )
    for label, items in (("新增", result.added), ("消失", result.removed)):
        if items:
            head = "、".join(items[:6])
            tail = f" 等 {len(items)} 条" if len(items) > 6 else ""
            print(f"  {label}：{head}{tail}")
    if result.left_only_columns:
        print(f"  仅旧版有的列：{'、'.join(result.left_only_columns)}")
    if result.right_only_columns:
        print(f"  仅新版有的列：{'、'.join(result.right_only_columns)}")

    changed = [item for item in result.columns if item.changed]
    if not changed:
        print(f"  共同列逐格相同（共 {len(result.columns)} 列）。")
        return 0
    print(f"  改值 {result.changed_cells:,} 格，涉及 {len(changed)} 列：")
    for item in changed:
        print(f"    {item.name}  {item.changed:,} 格")
        for key, before, after in item.samples:
            print(f"      {key}：{_brief(before)} → {_brief(after)}")
    return 0


def _version_of(text: str) -> int:
    """`v2` 与 `2` 都认。认不出来就报错——猜一个版本号会让人看错两版。"""
    value = text.strip().lower().removeprefix("v")
    if not value.isdigit():
        raise ValueError(f"版本号应形如 v2 或 2，实得 {text!r}")
    return int(value)


def _request_diff(root: Path, args: argparse.Namespace) -> int:
    """比对两版口径。默认最近两版——那是「刚改了什么」最常被问的一对。"""
    try:
        request_dir = resolve_dir(root, args.id)
    except SpecError as error:
        print(f"失败：{error}")
        return 1
    versions = history_versions(request_dir / SPEC_NAME)
    try:
        if args.old and args.new:
            left, right = _version_of(args.old), _version_of(args.new)
        elif len(versions) < 2:
            print(f"{args.id} 只有 {len(versions)} 版快照，没有可比对象。")
            print("  改过口径并交付一次之后，才会留下下一版快照。")
            return 1
        else:
            left, right = versions[-2], versions[-1]
    except ValueError as error:
        print(f"失败：{error}")
        return 1

    for version in (left, right):
        if version not in versions:
            existing = "、".join(f"v{item}" for item in versions) or "无"
            print(f"失败：没有 v{version} 的快照（现有：{existing}）")
            return 1

    try:
        # 历史快照躺在 history/ 下，相对路径必须按需求目录解析。
        old = load_spec(request_dir / HISTORY_DIR / f"v{left}.yaml", base_dir=request_dir)
        new = load_spec(request_dir / HISTORY_DIR / f"v{right}.yaml", base_dir=request_dir)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    changes = specdiff.compare(old, new)
    print(f"{args.id} 口径 v{left} → v{right}")
    print()
    if changes:
        for line in specdiff.table(changes):
            print(f"  {line}")
        print()
        print(f"共 {len(changes)} 处差异（只比决定产物的部分）。")
    else:
        print("  口径未变：条件、排序、投影、护栏、回填规则逐项相同。")
    notes = specdiff.side_notes(old, new)
    if notes:
        print()
        print("  另（不影响产物，也不进口径指纹）：")
        for line in notes:
            print(f"  {line}")
    return 0


def _next_input_version(request_dir: Path) -> int:
    """下一版输入表的序号。顶层还有快照时，那一版就算 v1。"""
    base = request_dir / INPUT_DIR
    if not base.is_dir():
        return 1
    versions = [
        int(path.name[1:])
        for path in base.iterdir()
        if path.is_dir() and path.name.startswith("v") and path.name[1:].isdigit()
    ]
    if versions:
        return max(versions) + 1
    return 2 if any(path.is_file() for path in base.iterdir()) else 1


def _print_header_change(old: tuple[str, ...], new: tuple[str, ...]) -> None:
    """报字段差异。人看的是「哪几列不一样」，不是「怎么挪最省」。"""
    if not old:
        print(f"  新表共 {len(new)} 列（读不到当前表头，无从比对）")
        return
    change = header_change(old, new)
    if not (change["added"] or change["removed"] or change["reordered"]):
        print(f"  表头与当前完全一致（{len(new)} 列）")
        return
    if change["added"]:
        print(f"  新增 {len(change['added'])} 列：{'、'.join(change['added'])}")
    if change["removed"]:
        print(f"  少了 {len(change['removed'])} 列：{'、'.join(change['removed'])}")
    if change["reordered"]:
        print("  共同列的次序变了（按列名定位不受影响，但人眼看会串行）")


def _request_input(root: Path, args: argparse.Namespace) -> int:
    """把新一版输入收进需求目录，并报出字段差异。

    只搬文件、只报告，**不改 spec**：改 `fill.source.path` 等于换口径，得升版本、
    得在 METHOD.md 里记一句为什么换。这个判断不能交给命令替人做——它是人的活，
    命令只负责把「新表和旧表差在哪」摆到明面上。
    """
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1
    fill = spec.fill
    if fill is None:
        print(f"失败：{spec.id} 是 {spec.kind.value} 型需求，没有输入表可换。")
        return 1

    given = Path(args.add).expanduser()
    if not given.is_file():
        print(f"失败：新输入不存在或不是文件：{given}")
        return 1

    sheet = args.sheet or fill.source.sheet
    header_row = args.header_row or fill.source.header_row
    try:
        new_header = header_of(given, sheet=sheet, header_row=header_row)
    except (FillError, OSError) as error:
        print(f"失败：读不出新表表头——{error}")
        return 1
    if fill.source.key_column not in new_header:
        print(
            f"失败：新表第 {header_row} 行没有标识列 {fill.source.key_column!r}，接进来也没法用。"
        )
        print(f"  现有列：{'、'.join(new_header[:10])}{'…' if len(new_header) > 10 else ''}")
        return 1

    target = request_dir / INPUT_DIR / f"v{_next_input_version(request_dir)}" / given.name
    if target.exists():
        print(f"失败：{target} 已存在，不覆盖。")
        return 1
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(given, target)

    try:
        old_header = header_of(
            fill.source.path, sheet=fill.source.sheet, header_row=fill.source.header_row
        )
    except (FillError, OSError):
        old_header = ()
        print("  [!] 当前输入表读不出表头（文件或 sheet 有问题），只报新表结构")

    relative = target.relative_to(request_dir).as_posix()
    print(f"已收进 {relative}（{given.stat().st_size:,} 字节，原件未改动）")
    _print_header_change(old_header, new_header)

    missing = [item.name for item in fill.columns if item.name not in new_header]
    if missing:
        print(
            f"  [!] spec 声明了 {len(missing)} 列、新表里没有：{'、'.join(missing[:8])}"
            f"{'…' if len(missing) > 8 else ''}"
        )
        print("      直接交付会失败；要么补回这些列，要么把对应规则改成 manual 或删掉")

    print()
    print("下一步（换输入等于换口径，这几步不代劳）：")
    print(f"  1. spec.yaml：fill.source.path 改成 {relative}")
    print(f"  2. spec.yaml：version 改成 {spec.version + 1}")
    print("  3. method/METHOD.md：记一句换了哪版表、字段怎么变、为什么换")
    print(f"  4. 先 `request run {spec.id} --dry-run` 看逐列覆盖，再正式交付")
    return 0


def _request_method(root: Path, request_id: str, *, check: bool) -> int:
    """过程记录的入口：缺骨架就补，脚本有增删改就重算指纹。

    索引只钉「哪个脚本、指纹多少」，不替人编用途——所以新增脚本后第一次重算会
    报「待补」，那是有意的：机器猜得出 sha256，猜不出这个脚本为什么存在。
    """
    try:
        request_dir = resolve_dir(root, request_id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    if not check:
        report = sync_method(
            request_dir, request_id=spec.id, title=spec.title, kind=spec.kind.value
        )
        if report.doc_created:
            print(f"已补过程记录骨架：{request_dir / METHOD_DIR / METHOD_DOC}")
            print("  填完删掉 `<!-- 待填：` 那一行，否则交付会被拦下")
        for name in report.added:
            print(f"  新登记：{name}（用途待补，请回 METHOD.md 与索引里写清楚）")
        for name in report.refreshed:
            print(f"  指纹更新：{name}")
        for name in report.dropped:
            print(f"  已移除：{name}（文件不在 method/scripts/ 了）")
        if report.written and not (report.added or report.refreshed or report.dropped):
            print(f"  已生成脚本索引：{METHOD_DIR}/{METHOD_INDEX}")
        if not report.changed and not report.written:
            print(f"{spec.id} 方法索引已是最新，无需改动")

    found = problems(request_dir)
    if found:
        print(f"\n{spec.id} 过程记录不合规（{len(found)} 处）：")
        for text in found:
            print(f"  - {text}")
        return 1
    print(f"{spec.id} 过程记录合规：{METHOD_DIR}/{METHOD_DOC} + 脚本索引一致")
    return 0


def _request_new(args: argparse.Namespace, root: Path) -> int:
    """建需求：目录语义化，外部输入表当场复制进目录，之后与外部再无瓜葛。"""
    kind = Kind(args.kind)
    folder = root / directory_name(args.id, args.title, kind)
    # 目录名带语义后缀，判重不能只看同名目录，得看编号前缀——否则同一个
    # REQ-0002 换个标题就能再建一份，编号的唯一性就废了。
    taken = (
        [
            path
            for path in root.iterdir()
            if path.is_dir() and (path.name == args.id or path.name.startswith(f"{args.id}-"))
        ]
        if root.is_dir()
        else []
    )
    if taken or folder.exists():
        names = "、".join(path.name for path in taken) or folder.name
        print(
            f"失败：需求号 {args.id} 已有目录（{names}）。要改口径请直接编辑它并把 version 加一。"
        )
        return 1

    given: Path | None = None
    if kind is Kind.BACKFILL:
        if not args.source:
            print("失败：kind=backfill 需要 --source 指向对方给的原始表格（文件须已存在）。")
            return 1
        given = Path(args.source).expanduser()
        if not given.is_file():
            print(f"失败：输入表不存在或不是文件：{given}")
            return 1

    folder.mkdir(parents=True)
    snapshot: Path | None = None
    try:
        if given is not None:
            # 取材一次，取完即断：外部文件之后被删被改，都影响不到这份需求。
            snapshot = folder / INPUT_DIR / given.name
            snapshot.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(given, snapshot)
        target = folder / SPEC_NAME
        target.write_text(
            skeleton(
                request_id=args.id,
                title=args.title,
                requester=args.requester,
                purpose=args.purpose,
                cadence=Cadence(args.cadence),
                kind=kind,
                source_path=given.name if given else "",
            ),
            encoding="utf-8",
        )
        # 建完立刻按真规矩读一遍：不合法就整目录收走，好过等下次 run 才发现。
        load_spec(target)
        # 过程记录一并建好骨架：新需求从第一天起就有 method/ 这个坑位，
        # 不必等人想起来该记「怎么做的」——想不起来，是这类记录最常见的死法。
        sync_method(folder, request_id=args.id, title=args.title, kind=kind.value)
    except (SpecError, OSError) as error:
        shutil.rmtree(folder, ignore_errors=True)
        print(f"失败：{error}")
        return 1

    print(f"已建需求：{folder}（{kind.value}）")
    if snapshot is not None:
        print(f"  输入快照：{snapshot}（原件未改动；此后本需求不再依赖外部文件）")
        print(
            "  下一步：核对 fill.source 的 sheet / header_row / key_column 与逐列规则，"
            "改完把 version 加一，先跑 `request run <id> --dry-run`"
        )
    else:
        print("  下一步：编辑 criteria 与 output（metric 只写词表词条），再跑 `request run`")
    print(
        f"  过程记录：{METHOD_DIR}/{METHOD_DOC} 已建骨架（含 `<!-- 待填：` 标记）。"
        "按步骤写清做法、删掉那行标记，交付才会放行"
    )
    return 0


def _request_list(root: Path) -> int:
    specs = sorted(root.glob(f"*/{SPEC_NAME}"))
    if not specs:
        print(f"{root} 下还没有需求。")
        print("  用 `request new REQ-0001 --title 标题 --requester 姓名` 建第一个。")
        return 0
    for path in specs:
        try:
            spec = load_spec(path)
        except SpecError as error:
            # 一个坏 spec 不该挡住其余需求：列出来标红，让人去修。
            print(f"[!] {path.parent.name}：{error}")
            continue
        runs = deliveries_of(path.parent)
        last = runs[-1].name if runs else "从未交付"
        print(f"{spec.id}  {spec.title}")
        print(
            f"  {spec.kind.value} · {spec.cadence.value} · v{spec.version} · "
            f"交付 {len(runs)} 次 · 最后 {last}"
        )
        print(f"  目录：{path.parent.name}")
        print(f"  口径：{criteria_text(spec)}")
    return 0


def _show_fill(spec: Spec) -> None:
    """打印回填口径体。与「编译后的 SQL」并列，同属「这次到底怎么算」。"""
    fill = spec.fill
    if fill is None:
        print("口径：（fill 段缺失，spec 校验应已拦下）")
        return
    sheet = fill.source.sheet or "首个 sheet"
    print("口径：")
    print(
        f"  输入快照：{fill.source.relative}（需求目录内；{sheet}，"
        f"表头第 {fill.source.header_row} 行，标识列 {fill.source.key_column}）"
    )
    print(f"  产物：deliveries/<时刻>/{fill.output_name}；只写在需求目录内")
    print(
        f"  声明 {len(fill.columns)} 列 · overwrite={fill.overwrite} · "
        f"overrides={fill.overrides or '无'}"
    )
    for item in fill.columns:
        origin = {
            "field": f"底库 `{item.field}`" + (f" · {item.fmt}" if item.fmt else ""),
            "template": f"模板 `{item.template}`",
            "const": f"常量 `{item.const}`",
            "manual": f"人工判读：{item.reason}",
        }[item.kind]
        print(f"    {item.name}  ← {origin}")
    print()


def _request_show(root: Path, request_id: str) -> int:
    try:
        path = resolve_dir(root, request_id) / SPEC_NAME
        spec = load_spec(path)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    print(f"{spec.id}  {spec.title}")
    print(f"  提出人：{spec.requester}")
    if spec.purpose:
        print(f"  用途：{spec.purpose}")
    print(f"  形态：{spec.kind.value} · 节奏：{spec.cadence.value} · 版本：v{spec.version}")
    limit = spec.guard.max_rows if spec.guard.max_rows is not None else "不限"
    print(f"  护栏：{spec.guard.min_rows} ≤ 行数 ≤ {limit}")
    print(
        f"  标疑：与上次相比行数变化超 {spec.alert.row_delta_pct:g}% "
        f"且超 {spec.alert.row_delta_abs} 行"
    )
    print()

    if spec.kind is Kind.BACKFILL:
        _show_fill(spec)
    else:
        if spec.criteria:
            print("口径：")
            for index, item in enumerate(spec.criteria, 1):
                metric = lookup(item.metric)
                print(f"  {index}. {item.metric} {item.op} {item.value}")
                print(f"     落成列 {metric.field} —— {metric.definition}")
            print()
        print("编译后的 SQL：")
        for line in compile_sql(spec).splitlines():
            print(f"  {line}")
        print()

    versions = history_versions(path)
    if versions:
        print(f"历史版本：{'、'.join(f'v{version}' for version in versions)}")
    else:
        print("历史版本：（尚未快照，首次交付时自动留存）")

    print(f"口径指纹：{spec_digest(path)[:16]}…（只算口径内容，改注释与标题不算）")
    conflict = version_conflict(path, spec.version)
    if conflict:
        print(f"  [!] {conflict}")
        print("      在改好并升版之前，下一次交付会被拦下。")

    print(f"过程记录：{METHOD_DIR}/{METHOD_DOC}")
    found = problems(path.parent)
    if found:
        print(f"  不合规 {len(found)} 处，交付会被拦下（`request method {spec.id}` 可看详情）：")
        for text in found:
            print(f"    - {text}")
    else:
        print("  合规：步骤齐备，脚本索引与文件指纹一致")
    runs = deliveries_of(path.parent)
    print(f"交付台账：{len(runs)} 次")
    for run in runs:
        print(f"  {run.name}{manifest_summary(run)}")
    return 0


def _request_run(args: argparse.Namespace, store: DatasetStore) -> int:
    """交付一次。一次性需求交付过就不再自动重复，加 --force 才重交。"""
    root = Path(args.requests_dir)
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1

    if args.dry_run:
        # 预演不写盘、也不占「once 已交付」的名额——它本来就是给人先看一眼的。
        try:
            looked = preview(store, root, args.id)
        except (DeliveryBlocked, SpecError) as error:
            print(f"预演失败：{error}")
            return 1
        _print_preview(looked)
        return 0

    done = deliveries_of(request_dir)
    if spec.cadence is Cadence.ONCE and done and not args.force:
        print(f"{spec.id} 是一次性需求，已于 {done[-1].name} 交付过。")
        print("  确实要再交一份（比如对方要求重出），加 --force。")
        return 1

    try:
        result = deliver(store, root, args.id)
    except (DeliveryBlocked, SpecError) as error:
        print(f"交付失败：{error}")
        return 1
    _print_delivery(result)
    return 0


def _print_preview(result: Preview) -> None:
    print(f"预演 {result.request_id}（{result.kind.value}）· {result.rows:,} 行 · 未写盘")
    for line in result.lines:
        print(f"  {line}")


def _print_delivery(result: DeliveryResult) -> None:
    print(f"已交付 {result.request_id}（{result.kind.value}） → {result.directory}")
    print(f"  {result.rows:,} 行 · {len(result.columns)} 列 · 口径 v{result.spec_version}")
    for item in result.files:
        digest = str(item["sha256"])[:16]
        print(f"  {item['name']}  {item['bytes']:,} 字节  sha256 {digest}…")
    health = result.health
    if health.status == "suspect":
        print(
            f"  [!] 行数突变：{health.baseline_rows:,} → {result.rows:,}"
            f"（{health.delta_pct:+.1f}%），已标疑，README 顶部有警告"
        )
    elif health.status == "first":
        print("  首次交付，无基线可比")
    elif health.note:
        print(f"  {health.note}")


def _request_run_due(args: argparse.Namespace, store: DatasetStore) -> int:
    """跑所有到期的需求。一个失败不挡别的，但整体退出码会反映出来。"""
    root = Path(args.requests_dir)
    if not root.is_dir():
        print(f"{root} 下还没有需求，无需交付。")
        return 0

    try:
        report = run_due(store, root)
    except DeliveryBlocked as error:
        # 底库本身不成立（current 缺失）：这不是某一条需求的问题，整体失败。
        print(f"交付失败：{error}")
        return 1

    for result in report.delivered:
        mark = "  [!] 已标疑" if result.health.status == "suspect" else ""
        print(f"  {result.request_id}  已交付 {result.rows:,} 行 → {result.directory.name}{mark}")
    for request_id, reason in report.skipped:
        print(f"  {request_id}  跳过：{reason}")
    for request_id, reason in report.failed:
        print(f"  [!] {request_id}  失败：{reason}")

    print(
        f"共 {len(report.delivered)} 交付 / {len(report.skipped)} 跳过 / {len(report.failed)} 失败"
    )
    return 1 if report.failed else 0
