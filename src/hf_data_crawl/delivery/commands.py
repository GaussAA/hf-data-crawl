"""`delivery` 切片的命令处理函数，参数解析与分派在 ops/cli。"""

from __future__ import annotations

import argparse
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ..platform.atomic import write_bytes
from ..platform.runs import Outcome
from ..platform.store import DatasetStore
from . import compare, specdiff
from .delivery import (
    PRUNED_NAME,
    DeliveryBlocked,
    DeliveryResult,
    Preview,
    deliver,
    deliveries_of,
    preview,
    prune,
    read_manifest,
    run_due,
    write_index,
)
from .header import HeaderError, header_change
from .method import METHOD_DIR, METHOD_DOC, METHOD_INDEX, advisories, problems
from .method import sync as sync_method
from .metrics import lookup
from .requirement import (
    DELIVERIES_DIR,
    HANDOFF_NAME,
    HISTORY_DIR,
    INPUT_DIR,
    MANIFEST_NAME,
    SPEC_NAME,
    STATUS_NAME,
    Cadence,
    Kind,
    Mode,
    Spec,
    SpecError,
    closed_note,
    compile_sql,
    directory_name,
    history_versions,
    is_sample,
    read_handoffs,
    read_status,
    request_dirs,
    resolve_dir,
    sample_note,
    skeleton,
    spec_digest,
    version_conflict,
)
from .requirement import load as load_spec
from .sheet import header_of


def run_export_wide(args: argparse.Namespace, store: DatasetStore) -> Outcome:
    """导出宽表。失败由 `_execute` 统一记进 runs/，不留半截文件。

    「可预期的失败」在这里就地转成 `Outcome(code=1)`，不打堆栈：空库与写错
    `--out` 都是输入状态问题，人能照提示修；堆栈只留给真正的意外。原先它们
    会穿透到 `_execute` 的 catch-all，用户看到的是一段 traceback 而不是
    「先跑 collect」或「输出必须在数据目录内」。
    """
    from ..query.gold import GoldNotReady
    from .wide import WideExportMismatch, WideExportPath, export_wide

    target = Path(args.out) if args.out else None
    try:
        summary = export_wide(store, target)
    except (GoldNotReady, WideExportMismatch, WideExportPath) as error:
        print(str(error))
        return Outcome({"error": str(error)}, code=1, reason=str(error))
    print(
        f"宽表已导出：{summary.path}（{summary.columns} 列 · "
        f"{summary.rows:,} 行 · {summary.bytes / 1e6:.1f}MB）"
    )
    print("  可重建产物：不参与状态更新，删掉后重跑本命令即可")
    return Outcome(summary)


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
    """交付目录的一行摘要。读不到就留空——台账缺一份不该让 show 整体失败。

    「缺」与「坏」分开说：前者说明目录不是一次交付，后者说明这份档案坏了，
    两种情形该做的处置不同。
    """
    body = read_manifest(directory)
    if body is None:
        note = "缺 manifest" if not (directory / MANIFEST_NAME).is_file() else "manifest 读不出来"
        return f"  （{note}）"
    rows = (body.get("result") or {}).get("rows")
    status = (body.get("health") or {}).get("status", "?")
    return f"  {rows} 行 · {status}"


def run_request(args: argparse.Namespace) -> int:
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
    if args.request_action == "index":
        return _request_index(root, args.id)
    if args.request_action == "usage":
        return _request_usage(root, args.term)
    if args.request_action == "prune":
        return _request_prune(root, args)
    if args.request_action == "close":
        return _request_close(root, args)
    if args.request_action == "reopen":
        return _request_reopen(root, args)
    if args.request_action == "handoff":
        return _request_handoff(root, args)
    if args.request_action == "sample":
        return _request_sample(root, args)
    return _request_show(root, args.id)


def _request_handoff(root: Path, args: argparse.Namespace) -> int:
    """记一笔「哪一版给了谁」。只追加，不改档案——外发是事实，不是口径。

    `HANDOFF.jsonl` 一行一次；坏行只影响那一行（`read_handoffs` 逐行解析）。
    为什么要记：档案能回答「谁提的、基于什么、交了什么」，回答不了「上周发的那版
    给谁了」——同一份需求发给多个消费方时，这张表就是唯一的答案。
    """
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1
    recipient = (args.to or "").strip()
    if not recipient:
        print("失败：--to 必填——外发记录没有收件人，等于没记。")
        return 1
    runs = deliveries_of(request_dir)
    if not runs:
        print(f"{spec.id} 还没有交付，没什么可外发的。先 `request run`。")
        return 1
    if args.delivery:
        chosen = [run for run in runs if run.name == args.delivery]
        if not chosen:
            names = "、".join(run.name for run in runs[-3:])
            print(f"失败：找不到交付 {args.delivery}（最近三条：{names}）。")
            return 1
        directory = chosen[0]
    else:
        directory = runs[-1]
    body = read_manifest(directory) or {}
    entry = {
        "at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "delivery": directory.name,
        "version": body.get("spec_version"),
        "to": recipient,
        "note": (args.note or "").strip(),
    }
    with (request_dir / HANDOFF_NAME).open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(entry, ensure_ascii=False) + "\n")
    version = f"v{entry['version']}" if entry["version"] is not None else "版本未知"
    print(f"{spec.id} 已记外发：{directory.name}（{version}）→ {recipient}")
    if entry["note"]:
        print(f"  备注：{entry['note']}")
    return 0


def _write_status(request_dir: Path, **fields: Any) -> dict[str, Any]:
    """更新 `STATUS.json`：合并现有键，再原子写。

    合并而不是覆盖：关闭状态与样例标记是两件独立的事实，后写的那次不该把前一次抹掉。
    """
    body = read_status(request_dir)
    body.update(fields)
    write_bytes(
        request_dir / STATUS_NAME,
        (json.dumps(body, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
    )
    return body


def _request_close(root: Path, args: argparse.Namespace) -> int:
    """关掉一份需求：不删任何东西，只是告诉交付链「这单不用再跑了」。

    状态写在 `requests/<id>/STATUS.json`，不进 spec——关闭是运营状态，不是口径；
    写进 spec 要么动口径指纹，要么重排人写的 YAML，两样都不值当。
    """
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1
    reason = (args.reason or "").strip()
    if not reason:
        print("失败：写一句关闭原因（--reason）——半年后只看到「已关闭」，没人知道为什么。")
        return 1
    _write_status(
        request_dir,
        status="closed",
        reason=reason,
        closed_at=datetime.now().astimezone().isoformat(timespec="seconds"),
    )
    print(f"{spec.id} 已关闭：{reason}")
    print("  台账与产物一份没动；`request run` / `run-due` 不再跑它。要恢复用 `request reopen`。")
    return 0


def _request_reopen(root: Path, args: argparse.Namespace) -> int:
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1
    if closed_note(request_dir) is None:
        print(f"{spec.id} 本来就是开着的，无需恢复。")
        return 0
    body = read_status(request_dir)
    body.pop("status", None)
    body.pop("reason", None)
    body.pop("closed_at", None)
    if body:
        # 样例标记要留着：恢复一份样例需求，不该把它变回真实需求。
        write_bytes(
            request_dir / STATUS_NAME,
            (json.dumps(body, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        )
    else:
        (request_dir / STATUS_NAME).unlink(missing_ok=True)
    print(f"{spec.id} 已恢复：`request run` / `run-due` 会重新把它算进来。")
    return 0


def _request_sample(root: Path, args: argparse.Namespace) -> int:
    """把一份需求标记成开发用样例（或撤销标记）。

    为什么要可识别：样例是用需求的形式写的测试夹具，台账与体检照常覆盖它们，
    但「改这个词条会影响谁」必须只算真实需求——否则影响面会被夹具淹没。
    """
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return 1
    if args.undo:
        body = read_status(request_dir)
        body.pop("sample", None)
        body.pop("sample_reason", None)
        body.pop("sample_marked_at", None)
        if body:
            write_bytes(
                request_dir / STATUS_NAME,
                (json.dumps(body, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
            )
        else:
            (request_dir / STATUS_NAME).unlink(missing_ok=True)
        print(f"{spec.id} 已取消样例标记：影响面统计会重新把它算进来。")
        return 0
    reason = (args.reason or "开发与验证用，不是真实需求").strip()
    _write_status(
        request_dir,
        sample=True,
        sample_reason=reason,
        sample_marked_at=datetime.now().astimezone().isoformat(timespec="seconds"),
    )
    print(f"{spec.id} 已标记为样例需求：{reason}")
    print("  台账与体检照常覆盖它；`request usage` 不再把它算进影响面。")
    return 0


def _request_prune(root: Path, args: argparse.Namespace) -> int:
    """清掉旧交付的产物字节；manifest、README 与它们的 sha256 一份不动。

    产物可以重算，台账不能重建——所以这里删的只有 `manifest.result.files` 点名的
    那些文件，删掉的每一样都写进 `deliveries/PRUNED.json`。
    """
    if args.keep < 1:
        print("失败：--keep 至少是 1——当前有效那条的产物永远留着。")
        return 1
    if args.id:
        try:
            targets = [resolve_dir(root, args.id)]
        except SpecError as error:
            print(f"失败：{error}")
            return 1
    else:
        targets = request_dirs(root)
    if not targets:
        print(f"{root} 下还没有需求。")
        return 1

    pruned_any = False
    for request_dir in targets:
        if not deliveries_of(request_dir):
            continue
        report = prune(request_dir, keep=args.keep, dry_run=args.dry_run)
        if not report.pruned:
            if not args.id:
                continue
            print(f"{report.request_id}：最近 {report.kept} 条之外没有可清理的产物。")
            continue
        pruned_any = True
        verb = "将清理" if args.dry_run else "已清理"
        print(f"{report.request_id}（保留最近 {report.kept} 条的产物）")
        for batch in report.pruned:
            names = "、".join(str(item["name"]) for item in batch.files)
            print(f"  {verb} {batch.name}  {batch.freed:,} 字节  {names}")
        if not args.dry_run:
            write_index(request_dir)
            print(
                f"  合计 {len(report.pruned)} 条 / {report.freed:,} 字节；"
                f"{DELIVERIES_DIR}/{PRUNED_NAME} 记了它们的 sha256，INDEX 标「已清理」"
            )
        else:
            print(f"  合计 {len(report.pruned)} 条 / {report.freed:,} 字节（--dry-run 未动磁盘）")
    if not pruned_any:
        print("没有可清理的产物：每条需求保留条数之外的产物都已不在。")
    return 0


def _request_usage(root: Path, term: str) -> int:
    """反查一个词条/库列被哪些需求用到。

    为什么要有：改一个词条的定义（或淘汰一个库列）时，最该先回答的是「谁会受影响」。
    口径散在各需求的 `spec.yaml` 里，没有这张反查表就只能靠 `grep`，而 `grep` 会把
    注释里提过一句的也算上，也不会告诉你那份需求最近交的是哪一版。
    """
    try:
        metric = lookup(term)
    except ValueError:
        metric = None
    field_name = metric.field if metric else term
    names = {term, field_name} | ({metric.term} if metric else set())
    if metric:
        print(f"{metric.term}（库列 `{metric.field}`）—— {metric.definition}")
    else:
        print(f"按库列名 `{term}` 反查（词表里没有这个叫法，只比列名与模板占位）")

    hits = 0
    requests = 0
    sample_hits = 0
    sample_requests = 0
    for request_dir in request_dirs(root):
        try:
            spec = load_spec(request_dir / SPEC_NAME)
        except SpecError as error:
            print(f"[!] {request_dir.name}：{error}")
            continue
        sites: list[str] = []
        if spec.kind is Kind.QUERY:
            for position, criterion in enumerate(spec.criteria, 1):
                if criterion.metric in names or _metric_field(criterion.metric) == field_name:
                    sites.append(
                        f"条件 {position}：{criterion.metric} {criterion.op} {criterion.value}"
                    )
            for key in spec.order_by:
                if key.metric in names or _metric_field(key.metric) == field_name:
                    sites.append(f"排序：{key.metric} {'降序' if key.descending else '升序'}")
            for name in spec.output.columns:
                if name in names:
                    sites.append(f"输出列：{name}")
        else:
            fill = spec.fill
            for rule in fill.columns if fill else ():
                if rule.field in names or rule.name in names:
                    sites.append(
                        f"列「{rule.name}」← {'field ' + str(rule.field) if rule.field else rule.kind}"
                    )
                elif rule.template and any(f"{{{name}}}" in rule.template for name in names):
                    sites.append(f"列「{rule.name}」← 模板 `{rule.template}`")
        if not sites:
            continue
        if is_sample(request_dir):
            # 样例是开发夹具：它们当然也引用这个词条，但「改定义会影响谁」的清单
            # 只该算真实需求。数量单独报一句，既不丢信息，也不污染影响面。
            sample_requests += 1
            sample_hits += len(sites)
            continue
        requests += 1
        hits += len(sites)
        runs = deliveries_of(request_dir)
        last = read_manifest(runs[-1]) if runs else None
        tail = (
            f"最近交付 {runs[-1].name}（v{last.get('spec_version')}）"
            if runs and last
            else "从未交付"
        )
        print(f"\n{spec.id}  {spec.title}  · v{spec.version} · {spec.kind.value} · {tail}")
        for line in sites:
            print(f"  {line}")

    sample_tail = (
        f"另有 {sample_requests} 份样例需求也引用了它（{sample_hits} 处，未计入影响面）。"
        if sample_requests
        else ""
    )
    if not hits:
        print("没有真实需求用到它——可以放心改定义。")
        if sample_tail:
            print(sample_tail)
        return 1
    print(f"\n共 {requests} 份真实需求、{hits} 处引用。改定义前，这些是必须复核的清单。")
    if sample_tail:
        print(sample_tail)
    return 0


def _metric_field(term: str) -> str | None:
    """词条名 → 库列名；认不出来返回 None（纯列名本来就没有词条）。"""
    try:
        return lookup(term).field
    except ValueError:
        return None


def _request_index(root: Path, request_id: str | None) -> int:
    """重写 `deliveries/INDEX.md`。不写数据的动作，不持数据目录的锁。

    为什么留独立入口：INDEX 平时随交付自动重写，但「补建历史 / AI 改完 AUDIT 后
    刷新一次」不能靠再交付一遍——那正是这次治理要断掉的循环。
    """
    if request_id:
        try:
            request_dir = resolve_dir(root, request_id)
        except SpecError as error:
            print(f"失败：{error}")
            return 1
        directories = [request_dir]
    else:
        directories = request_dirs(root)
    if not directories:
        print(f"{root} 下还没有需求。")
        return 1
    written = 0
    for request_dir in directories:
        if not deliveries_of(request_dir):
            continue
        path = write_index(request_dir)
        print(f"  {request_dir.name} → {path.relative_to(request_dir)}")
        written += 1
    if not written:
        print("没有已交付的需求，没有索引可写。")
        return 1
    print(f"已重写 {written} 份交付索引。")
    return 0


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
    # 只给一侧时原先落到 else 分支、静默改比最近两次：用户以为在看 A→B，实际看的
    # 是 C→D。要么两个都给，要么都不给。
    if bool(args.left) != bool(args.right):
        print("失败：两个交付号要么都给，要么都不给（不给就比最近两次）")
        return 1
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
    shown = specdiff.display_notes(old, new)
    if shown:
        print()
        print("  另（展示项：不进口径指纹、不升版本，但产物字节会变，仍要交付一次）：")
        for line in shown:
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
        parsed = header_of(given, sheet=sheet, header_row=header_row)
    except (HeaderError, OSError) as error:
        print(f"失败：读不出新表表头——{error}")
        return 1
    if fill.source.key_column not in parsed.columns:
        print(
            f"失败：新表第 {header_row} 行没有标识列 {fill.source.key_column!r}，接进来也没法用。"
        )
        print(f"  现有列：{'、'.join(parsed.names[:10])}{'…' if len(parsed.names) > 10 else ''}")
        return 1

    target = request_dir / INPUT_DIR / f"v{_next_input_version(request_dir)}" / given.name
    if target.exists():
        print(f"失败：{target} 已存在，不覆盖。")
        return 1
    target.parent.mkdir(parents=True, exist_ok=True)
    write_bytes(target, given.read_bytes())

    try:
        old_header = header_of(
            fill.source.path, sheet=fill.source.sheet, header_row=fill.source.header_row
        ).names
    except (HeaderError, OSError):
        old_header = ()
        print("  [!] 当前输入表读不出表头（文件或 sheet 有问题），只报新表结构")

    relative = target.relative_to(request_dir).as_posix()
    print(f"已收进 {relative}（{given.stat().st_size:,} 字节，原件未改动）")
    _print_header_change(old_header, parsed.names)

    missing = [item.name for item in fill.columns if item.name not in parsed.columns]
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
    # 软校验：写下来不拦交付，但要让人看见它缺着。
    for text in advisories(request_dir):
        print(f"  提示：{text}")
    return 0


def _request_new(args: argparse.Namespace, root: Path) -> int:
    """建需求：目录语义化，外部输入表当场复制进目录，之后与外部再无瓜葛。"""
    kind = Kind(args.kind)
    header_row = getattr(args, "header_row", None)
    if header_row is not None:
        if kind is not Kind.BACKFILL:
            print("失败：--header-row 只对 kind=backfill 有意义——查询型没有输入表。")
            return 1
        if header_row < 1:
            print("失败：--header-row 从 1 起算。")
            return 1
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
            write_bytes(snapshot, given.read_bytes())
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
                header_row=args.header_row,
            ),
            encoding="utf-8",
        )
        # 建完立刻按真规矩读一遍：不合法就整目录收走，好过等下次 run 才发现。
        load_spec(target)
        # 过程记录一并建好骨架：新需求从第一天起就有 method/ 这个坑位，
        # 不必等人想起来该记「怎么做的」——想不起来，是这类记录最常见的死法。
        sync_method(folder, request_id=args.id, title=args.title, kind=kind.value)
        if getattr(args, "sample", False):
            # 样例标记从建目录那一刻就写下：等它有了交付再补，那之前的台账就已经
            # 把夹具当真实需求算过一次影响面了。
            _write_status(
                folder,
                sample=True,
                sample_reason="开发与验证用，不是真实需求（--sample 创建）",
                sample_marked_at=datetime.now().astimezone().isoformat(timespec="seconds"),
            )
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
        note = closed_note(path.parent)
        if note is not None:
            print(f"  已关闭：{note}")
        sample = sample_note(path.parent)
        if sample is not None:
            print(f"  样例需求：{sample}（不计入影响面）")
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
    note = closed_note(path.parent)
    if note is not None:
        print(f"  状态：已关闭 —— {note}（`request reopen` 可恢复）")
    sample = sample_note(path.parent)
    if sample is not None:
        print(f"  样例需求：{sample}")
        print("    （台账与体检照常覆盖它；`request usage` 不计入影响面）")
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

    runs = deliveries_of(path.parent)
    delivered_versions = {(read_manifest(run) or {}).get("spec_version") for run in runs}
    versions = history_versions(path)
    if versions:
        print(f"历史版本：{'、'.join(f'v{version}' for version in versions)}")
        # 「快照在」不等于「交过」：失败尝试也会留下快照（崩溃窗口靠它兜底），
        # 两者的区别只有台账知道。混着列，读的人会以为每一版都真的交过。
        never = [version for version in versions if version not in delivered_versions]
        if never:
            print(
                f"  其中 {'、'.join(f'v{version}' for version in never)} 没有对应交付"
                "（失败尝试留下的快照，可删）"
            )
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
    if runs:
        # 台账从旧到新列，读的人还得自己挑最后一行；「当前有效」是 INDEX 的第一句话，
        # 这里也说出来——两份视图对同一件事只能有一个答案。
        print(f"  当前有效：{runs[-1].name}（`deliveries/INDEX.md` 的第一行同此）")
    for run in runs:
        print(f"  {run.name}{manifest_summary(run)}")

    handoffs = read_handoffs(path.parent)
    if handoffs:
        print(f"外发记录：{len(handoffs)} 次（{HANDOFF_NAME}）")
        for entry in handoffs[-3:]:
            version = f"v{entry.get('version')}" if entry.get("version") is not None else "v?"
            note = f" · {entry['note']}" if entry.get("note") else ""
            print(
                f"  {entry.get('at', '时间未知')}  {entry.get('delivery', '?')}（{version}）"
                f"→ {entry.get('to', '?')}{note}"
            )
    return 0


@dataclass(frozen=True)
class Attempt:
    """一次交付尝试的结局：退出码 + 进台账的那句原因。

    有它，`runs/` 里记的是「被哪道闸门拦住」，而不是「见上方说明」——终端早滚没了。
    """

    code: int = 0
    reason: str | None = None


def run_request_run(args: argparse.Namespace, store: DatasetStore) -> Attempt:
    """交付一次。一次性需求交付过就不再自动重复，加 --force 才重交。

    三条出口，对应三种「跑一次」：
    - `--dry-run`：只打印会出多少行（一行文件都不写）；
    - `--preview`：把真产物写进 `scratch/preview/`，可打开看，但不新增交付；
    - 默认：正式交付，会过代码闸门、护栏与重复闸门。

    返回 `Attempt` 而不只是退出码：调用方要把「被哪道闸门拦住」写进 `runs/`。
    """
    root = Path(args.requests_dir)
    # 两个开关说的是不同的「只看不写」，同时给出来时谁先谁后都是猜；直接让人选一个。
    if args.dry_run and args.preview:
        print("失败：--dry-run 与 --preview 选一个——前者只算不写，后者把产物落进草稿区。")
        return Attempt(1, "--dry-run 与 --preview 同时给出，语义冲突")
    try:
        request_dir = resolve_dir(root, args.id)
        spec = load_spec(request_dir / SPEC_NAME)
    except SpecError as error:
        print(f"失败：{error}")
        return Attempt(1, str(error))

    note = closed_note(request_dir)
    if note is not None:
        print(f"{spec.id} 已关闭：{note}")
        print("  交付链不再跑它；确实要出数据，先 `request reopen`。")
        return Attempt(1, f"需求已关闭：{note}")

    if args.dry_run:
        # 预演不写盘、也不占「once 已交付」的名额——它本来就是给人先看一眼的。
        try:
            looked = preview(store, root, args.id, keep_files=False)
        except (DeliveryBlocked, SpecError) as error:
            print(f"预演失败：{error}")
            return Attempt(1, f"预演失败：{error}")
        _print_preview(looked)
        return Attempt()

    if args.preview:
        try:
            looked = preview(store, root, args.id)
        except (DeliveryBlocked, SpecError) as error:
            print(f"预览失败：{error}")
            return Attempt(1, f"预览失败：{error}")
        _print_preview(looked)
        return Attempt()

    done = deliveries_of(request_dir)
    if spec.cadence is Cadence.ONCE and done and not args.force:
        print(f"{spec.id} 是一次性需求，已于 {done[-1].name} 交付过。")
        print("  确实要再交一份（比如对方要求重出），加 --force。")
        print("  但同底库、同内容的第二次会被重复闸门拦下——要出新东西，先让底库或口径真的变。")
        return Attempt(1, f"一次性需求已于 {done[-1].name} 交付过，未加 --force")

    try:
        result = deliver(store, root, args.id)
    except (DeliveryBlocked, SpecError) as error:
        print(f"交付失败：{error}")
        return Attempt(1, str(error))
    _print_delivery(result)
    return Attempt()


def _print_preview(result: Preview) -> None:
    label = "预览" if result.path else "预演"
    print(f"{label} {result.request_id}（{result.kind.value}）· {result.rows:,} 行")
    if result.path:
        print(f"  产物落在草稿区：{result.path}（可打开看；不进交付台账）")
        for item in result.files:
            print(f"    {item['name']}  {item['bytes']:,} 字节  sha256 {str(item['sha256'])[:16]}…")
    else:
        print("  未写盘")
    for note in result.notes:
        print(f"  [!] {note}")
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


def run_request_run_due(args: argparse.Namespace, store: DatasetStore) -> Attempt:
    """跑所有到期的需求。一个失败不挡别的，但整体退出码会反映出来。"""
    root = Path(args.requests_dir)
    if not root.is_dir():
        print(f"{root} 下还没有需求，无需交付。")
        return Attempt()

    try:
        report = run_due(store, root)
    except DeliveryBlocked as error:
        # 底库本身不成立（current 缺失）：这不是某一条需求的问题，整体失败。
        print(f"交付失败：{error}")
        return Attempt(1, str(error))

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
    if not report.failed:
        return Attempt()
    first = "；".join(f"{request_id}：{reason}" for request_id, reason in report.failed)
    return Attempt(1, f"{len(report.failed)} 条失败 —— {first}")
