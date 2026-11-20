"""交付：把一份需求跑成一份**能自证来源的档案**。

为什么不是「跑一条 SQL 存个文件」
--------------------------------
文件一旦离开这台机器，就只剩一份数据。三个月后对方问「这数从哪来的、怎么算的」，
没有档案就只能回来翻记录。所以每份交付固定三件：

- 产物本体（query 是 `data.parquet` / `data.csv`，backfill 是补全后的表格）
- `manifest.json`：机器读——底库指纹、口径、行数、列、sha256、健康标记
- `README.md`：人读——要什么、怎么算的、基于哪天的底库、几行

`manifest.source.current_md5` 是整份档案的锚点。底库每天都变，所以一份交付
只在「那一版底库」下成立；有这个 md5，任何一份历史交付都能回答它基于什么。

**不变与变**（这套体系的关键）
------------------------------
受理、定口径、按节奏跑、护栏、标疑、原子落盘、留档、可复现——对所有需求都一样，
全在本文件里走一条链。真正会变的只有「这次到底产出什么」，它被收进两处：

- `spec.kind`：形态的名字，只是一个枚举值；
- `EXECUTORS`：形态对应的执行体，产出统一的 `Payload`。

将来多一种交付形态（对账报表、按名单核验失效…），写一个执行体 + 注册一行即可，
不必再开一个目录、一套编号、一套命令。`Payload` 是执行体与档案层之间唯一的契约：
档案层只认「行数、列、产物清单、口径明细」，不认背后是 SQL 还是Excel。

三条硬规矩
----------
1. **0 行不交付**。空结果交出去没有意义，且通常意味着口径写错了。
2. **超上限不交付**。一条写错的需求不该把百万行倒给对方。
3. **判疑不阻断**。行数突变时照常交付，只在 manifest 与 README 顶部标记——
   「每周一给人一份」这种需求，当天交不出货比数字可疑更糟。但「这次不对劲」
   必须留在纸面上，不能靠人回忆上次是多少。

原子性：先写 `.tmp-*` 目录，三件齐了再整体改名。中途失败绝不留半份交付。

**自包含**：输入快照、口径、判读、历次交付**以及做法本身**（`method/`：步骤、
脚本原件、逐份指纹）全在需求目录内。产物绝不外写——一经外写，「哪一份才是正式的」
就说不清，而外部那份还会被随手改动。方法缺失时**不交付**：数据说不清来历，交出去
就永久补不回来了（见 `method.py`）。
"""

from __future__ import annotations

import hashlib
import json
import re
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ..platform.atomic import replace, write_bytes
from ..platform.store import DatasetStore
from ..query.gold import GoldCatalog, GoldNotReady

# 显式 re-export（`as` 同名）是给静态检查的信号：这两个模块导进来只为在 import 期
# 把执行体写进 `EXECUTORS`，本模块不取任何名字。写成 `from . import fill_exec` 时
# basedpyright strict 会报 reportUnusedImport——它看不见「导入即登记」这个副作用。
from . import fill_exec as fill_exec
from . import method
from . import query_exec as query_exec
from .contract import EXECUTORS, DeliveryBlocked, DeliveryUnchanged, Payload, RunContext
from .header import header_change
from .requirement import (
    DELIVERIES_DIR,
    HISTORY_DIR,
    MANIFEST_NAME,
    README_NAME,
    SPEC_NAME,
    Cadence,
    Kind,
    Spec,
    SpecError,
    closed_note,
    resolve_dir,
    sample_note,
    snapshot_history,
    spec_body,
    spec_body_digest,
    spec_core_digest,
    version_conflict,
)
from .requirement import load as load_spec
from .requirement import loads as load_spec_text
from .stamp import SourceMissing, SourceStamp, stamp

#: 交付目录名的时刻格式。到秒：同一分钟内跑两个需求是常态。
_STAMP_FORMAT = "%Y%m%dT%H%M%S"

#: 需求目录下的草稿区。预览产物落这里，**不进交付台账**：台账只收可交付的成品。
SCRATCH_DIR = "scratch"
PREVIEW_DIR = "preview"
PREVIEW_NAME = "PREVIEW.md"

#: 交付目录根下的两份索引文件。
#: `INDEX.md` 由引擎每次交付后重写（人只读）；`AUDIT.md` 是唯一的人工文件，
#: 记录那些引擎当场不知道、只能事后核出的事实（某条交付的代码与产物对不上）。
INDEX_NAME = "INDEX.md"
AUDIT_NAME = "AUDIT.md"

#: INDEX 里用来让 doctor 核对「索引是否过期」的机读标记。
_INDEX_MARK = "<!-- deliveries: {newest} · entries: {count} -->"


@dataclass(frozen=True)
class Health:
    """与上一次交付相比，这次的结果是否可疑。

    `status` 只有四值：`first` 首次（无基线）、`ok` 正常、`suspect` 波动超阈值、
    `unknown` 上次的 manifest 读不出来（不比较，但要说出来，不能假装正常）。
    """

    status: str
    baseline_rows: int | None = None
    delta_pct: float | None = None
    delta_abs: int | None = None
    #: 上一次交付的口径版本；与本版不同时不比较行数（改了口径必然大变）。
    supersedes: int | None = None
    note: str = ""
    #: 输入表头与上一份交付的差异。变了不是错，但必须留在纸面上——字段悄悄换了
    #: 是这类需求最容易踩的坑，而它不会让任何数字变红。
    header_note: str = ""


@dataclass(frozen=True)
class DeliveryResult:
    request_id: str
    directory: Path
    rows: int
    columns: tuple[str, ...]
    files: tuple[dict[str, Any], ...]
    health: Health
    spec_version: int
    kind: Kind


@dataclass(frozen=True)
class Preview:
    """预演结果：算一遍、把产物落在草稿区，不建交付目录、不留档案。

    与 `--dry-run` 的分工：`--dry-run` 只打印「会出多少行」，一行文件都不写；
    `--preview` 把真产物写进 `<需求目录>/scratch/preview/`，好让人**打开看一眼**。
    后者是这次治理加的：在此之前，「想看看这张表长什么样」的唯一办法是正式交付，
    于是排版问题只能靠多交一份成品来修——那正是交付目录里堆条目的直接原因。
    """

    request_id: str
    kind: Kind
    rows: int
    columns: tuple[str, ...]
    lines: list[str]
    #: 产物落在哪（草稿区绝对路径）。预演没写出任何文件时为 None。
    path: Path | None = None
    files: tuple[dict[str, Any], ...] = ()
    #: 正式交付会被拦下的原因（护栏、重复闸门）。预演不拦，但必须说出来——
    #: 否则人会以为「跑通了就能交」。
    notes: tuple[str, ...] = ()


def _stamp(store: DatasetStore) -> SourceStamp:
    """把「底库不在」翻译成本模块的失败类型，调用方看到的语义不变。"""
    try:
        return stamp(store)
    except SourceMissing as error:
        raise DeliveryBlocked(str(error)) from None


def _catalog(store: DatasetStore, catalog: GoldCatalog | None) -> GoldCatalog:
    if catalog is not None:
        return catalog
    try:
        return GoldCatalog(store)
    except GoldNotReady as error:
        raise DeliveryBlocked(str(error)) from error


def _pinned_inputs(request_dir: Path, spec: Spec) -> tuple[str, ...]:
    """这次交付实际用到的中间件相对路径（目前只有回填的判读表）。

    原先 `method.stamp` 写死 `overrides.json`：判读表放在子目录时档案一处都没
    钉住（「脚本 → 判读表 → 交付」那条链断在中间且不报错），而需求目录里恰好
    躺着一份没用上的同名文件时，又会把它记成这次交付的输入。两头都不对。

    这里的 `is_file` 与 `method.stamp` 里那次**不是重复劳动**：此处是**阻断**
    （受理时还在、现在没了，不能出土），那里是**跳过**（默认清单里的条目本来就
    可能不存在，跳过是合法形态）。合成一处就得二选一，必然错一头。
    """
    fill = spec.fill
    if fill is None or not fill.overrides_relative:
        return ()
    if not (request_dir / fill.overrides_relative).is_file():
        raise DeliveryBlocked(
            f"{spec.id}: 判读表 {fill.overrides_relative} 不在了（受理时还在）；补回后重跑"
        )
    return (fill.overrides_relative,)


def _method_stamp(request_dir: Path, spec: Spec) -> dict[str, Any]:
    """交付前闸门：过程记录不合规就不出货。

    与 `min_rows` 同级，拦的理由却不同——0 行是「这份数据不可信」，缺方法是
    「这份数据说不清来历」。后者当下不影响收数，可一旦交出去就永久补不回来：
    三个月后再想补写，当时的判据早已忘了，剩下的只是复述结论。

    翻译成 `DeliveryBlocked` 而非原样抛 `MethodError`：`run-due` 只认前者，
    一份缺方法的需求该记一条失败，不该让整批交付一起崩。
    """
    try:
        record = method.read(request_dir)
    except method.MethodError as error:
        raise DeliveryBlocked(
            f"{error}\n\n补齐后重跑；骨架用 `hf-data-crawl request method sync {spec.id}` 生成。"
        ) from None
    return method.stamp(request_dir, record, _pinned_inputs(request_dir, spec))


def _worktree_gate(request_id: str) -> dict[str, Any]:
    """脏代码不出版：档案记的 HEAD 必须真的能跑出这份产物。

    为什么拦而不是「记一笔脏」：档案的用处是「拿这个 commit 重跑，应当得到同一个
    sha256」。脏着跑出来的产物，这个承诺从写下的那一刻就是假的，而它看起来与真的
    一模一样——下次没人能从 manifest 里看出区别，只能靠重交一份来补。堵在出口上，
    这种事就不必发生。
    """
    state = method.worktree_state()
    if state["checked"] and not state["clean"]:
        sample = "、".join(f"`{name}`" for name in state["paths"][:5])
        more = f" 等 {len(state['paths'])} 处" if len(state["paths"]) > 5 else ""
        raise DeliveryBlocked(
            f"{request_id}: 决定产物的代码有未提交改动（{sample}{more}），"
            "交付档案会记一个复现不出的 commit。\n"
            "  先 `git commit` 再交付；只想先看产物长什么样，用 `request run <id> --preview`。"
        )
    return state


def _product_digests(files: Any) -> list[tuple[str, str]]:
    """产物指纹：只认「文件名 + sha256」，顺序无关。"""
    return sorted((str(item.get("name")), str(item.get("sha256"))) for item in (files or []))


def _refuse_unchanged(
    request_dir: Path, spec: Spec, stamp_value: SourceStamp, payload: Payload
) -> None:
    """同底库、同产物字节 → 不新增交付条目。

    判据是**两样都没变**：底库指纹没变说明原材料一样，产物字节没变说明这份表一个
    字符都不差。此时新增一条交付，除了给 spec 版本号留个摆设，没有任何信息增益——
    2026-11-17 REQ-0004 的 5 条里有 2 条就是这么来的。

    只在底库**也**没变时拒绝：底库变了而产物没变，是「在新底库上复核过，结果一致」，
    那是一条有效的观测，不该被当成重复。
    """
    last = _last_delivery(request_dir)
    if last is None:
        return
    body = read_manifest(last)
    if body is None:
        return
    previous_source = (body.get("source") or {}).get("current_md5")
    if previous_source != stamp_value.current_md5:
        return
    previous_files = (body.get("result") or {}).get("files")
    if _product_digests(previous_files) != _product_digests(payload.files):
        return
    raise DeliveryUnchanged(
        f"{spec.id}: 与上一条交付（{last.name}）底库相同、产物逐字节相同，"
        "这次运行没有新信息可记，未新增交付。\n"
        f"  要文件直接取 `{last}`；要出新内容，先让底库或口径真的变。"
    )


def _scratch_preview(request_dir: Path) -> Path:
    """预览产物目录：固定路径，每次重算前先清空——草稿只留最新一份。"""
    base = request_dir / SCRATCH_DIR / PREVIEW_DIR
    shutil.rmtree(base, ignore_errors=True)
    base.mkdir(parents=True, exist_ok=True)
    return base


def deliver(
    store: DatasetStore,
    requests_root: Path,
    request_id: str,
    *,
    catalog: GoldCatalog | None = None,
    source: SourceStamp | None = None,
    now: datetime | None = None,
) -> DeliveryResult:
    """跑一次交付。任何护栏不通过都抛 `DeliveryBlocked`，且不留任何目录。"""
    request_dir = resolve_dir(requests_root, request_id)
    spec_path = request_dir / SPEC_NAME
    # 关闭的需求不该从任何路径交付出去：`request run` 已经拦了一道，这里再兜一层，
    # 免得别的调用方（测试、脚本、将来的批量入口）绕过去。
    note = closed_note(request_dir)
    if note is not None:
        raise DeliveryBlocked(f"{request_id}: 已关闭（{note}）——要出数据先 `request reopen`")
    # 口径**只读一次**：校验、指纹、版本快照都从这份字节来。原先三处各读一次文件，
    # 中途有人改 spec，就会得到「校验的是旧版、快照的是新版」——档案从此说不清
    # 自己到底跑的是哪一版。
    try:
        raw_spec = spec_path.read_bytes()
    except OSError as error:
        raise DeliveryBlocked(f"{request_id}: 读不出 spec：{error}") from None
    spec = load_spec_text(raw_spec.decode("utf-8"), spec_path)
    # 代码干净与否先查：脏着跑出来的产物，manifest 记的 commit 复现不出来。
    # 放在方法闸门之前，是因为它的修复动作（commit）比补方法更快，早报早干活。
    worktree = _worktree_gate(spec.id)
    # 版本号不可复用：口径变了却还用 v1，档案会指着一份「当时说过、现在已经不是」
    # 的定义。这比缺方法更隐蔽——缺方法至少会报错，这一种只会让人拿着错前提核对。
    conflict = version_conflict(spec_path, spec.version, spec=spec)
    if conflict:
        raise DeliveryBlocked(f"{spec.id}: {conflict}")
    spec_sha = spec_body_digest(spec_body(spec))
    # 先验方法再算数：方法是「这份交付凭什么成立」的一部分，缺它连预留目录都不该做。
    method_stamp = _method_stamp(request_dir, spec)
    moment = now or datetime.now().astimezone()
    stamp_value = source or _stamp(store)
    runner = _catalog(store, catalog)

    # 先落口径快照，再换名出产物。
    #
    # 反过来（改名后快照）在那两步之间崩溃时会留下「产物已出土、history 没有这版」：
    # `version_conflict` 靠 `history/vN.yaml` 判断版本号是否用过，于是这版会静默
    # 隐身，下次复用同一版本号不会报冲突——历史被改写而无人知道。
    #
    # 快照先落，但**可处理的失败要收回去**（见下面的 `created_history`）：崩溃来
    # 不及收，只能靠「先落」兜底；而护栏、闸门、执行报错这些跑完就知道没发布的，
    # 留着只会白占一个版本号——改好口径还得再跳一版。取舍见 ADR-0004。
    history_path = request_dir / HISTORY_DIR / f"v{spec.version}.yaml"
    created_history = not history_path.exists()
    snapshot_history(spec_path, spec.version, content=raw_spec)

    directory = _reserve_dir(request_dir, moment)
    staging = directory.parent / f".tmp-{directory.name}"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    try:
        payload = _execute(
            RunContext(
                store=store,
                spec=spec,
                staging=staging,
                catalog=runner,
                source=stamp_value,
            )
        )
        _check_guard(spec, payload.rows)
        # 产物已经算出来了，重复闸门才有东西可比：同底库 + 同字节 = 没有新信息。
        _refuse_unchanged(request_dir, spec, stamp_value, payload)
        health = _health(request_dir, spec, payload.rows, payload.header)
        manifest = _manifest(
            spec=spec,
            moment=moment,
            stamp_value=stamp_value,
            payload=payload,
            health=health,
            previous=_last_delivery_name(request_dir, exclude=directory.name),
            method_stamp=method_stamp,
            spec_sha=spec_sha,
            worktree=worktree,
        )
        (staging / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (staging / README_NAME).write_text(
            _readme(spec, moment, stamp_value, payload, health, spec_sha), encoding="utf-8"
        )
        # 三件齐了才换名：中途失败最多留下一个 .tmp-* 目录，不会出现「有数据没说明」。
        replace(staging, directory)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        # 这次失败没有产出任何产物，就把它顺手落下的 vN 快照收回去：那版从没交付过，
        # 留着会在人改好口径后逼他再升一版（`version_conflict` 认快照、不认台账），
        # 白占一个版本号。只为「本次新建」的快照回滚——别人早就落下的不动。
        if created_history:
            history_path.unlink(missing_ok=True)
        raise

    # 索引随交付重写：它列的是台账本身，不重写就会在第二天开始说谎。
    #
    # 放在换名之后、且失败只告警：交付已经落盘，此时因为索引写不出来而报「交付失败」，
    # 只会让人再跑一次、然后被重复闸门拦住。索引缺失本来就是体检项，红得起来。
    try:
        write_index(request_dir)
    except OSError as error:
        print(f"[警告] 交付已落盘，但索引没写成（`doctor` 会报出来）：{error}")
    # 客已交完，草稿不再需要：留着只会让人以为「这里还有一份可能要交的东西」。
    shutil.rmtree(request_dir / SCRATCH_DIR / PREVIEW_DIR, ignore_errors=True)

    # 首次交付落一份口径快照——只留「真的用过」的版本，没用过的口径没有保留价值。
    # （快照已在函数开头、换名之前落盘，见那里的说明。）

    return DeliveryResult(
        request_id=spec.id,
        directory=directory,
        rows=payload.rows,
        columns=payload.columns,
        files=tuple(payload.files),
        health=health,
        spec_version=spec.version,
        kind=spec.kind,
    )


def preview(
    store: DatasetStore,
    requests_root: Path,
    request_id: str,
    *,
    catalog: GoldCatalog | None = None,
    source: SourceStamp | None = None,
    keep_files: bool = True,
) -> Preview:
    """算一遍；`keep_files` 时把真产物落进草稿区，否则一行文件都不写。

    两种「跑一次看看」共用本函数，区别只在落不落盘：
    - `--dry-run`（`keep_files=False`）只回答「会出多少行」，与治理前完全一致；
    - `--preview`（默认）把产物写进 `scratch/preview/`，好让人**打开看一眼**。
    后者是这次治理加的：此前「想看看这张表长什么样」只能靠正式交付，排版问题
    只好再交一份成品来修——那正是交付目录里堆条目的直接原因。
    """
    request_dir = resolve_dir(requests_root, request_id)
    spec = load_spec(request_dir / SPEC_NAME)
    stamp_value = source or _stamp(store)
    if keep_files:
        staging = _scratch_preview(request_dir)
    else:
        # 不落盘时给一个明确的哨兵路径：误写会立刻暴露在不存在目录上，
        # 而不是悄悄落到当前工作目录。
        staging = request_dir / DELIVERIES_DIR / ".preview"
    payload = _execute(
        RunContext(
            store=store,
            spec=spec,
            staging=staging,
            catalog=_catalog(store, catalog),
            source=stamp_value,
            dry_run=not keep_files,
        )
    )
    # 预演不拦，但要把「正式交付会被拦」说清楚：否则人会以为跑通了就能交。
    notes: list[str] = []
    # 草稿可能来自一份还没提交的改动：它长得像交付，但正式交时会被脏闸门拦下。
    # 不写这一句，人拿着草稿去对数字，最后发现「怎么交不出去」。
    state = method.worktree_state()
    if state.get("checked") and not state.get("clean"):
        paths = state.get("paths") or []
        notes.append(
            "这份草稿来自有未提交改动的代码"
            f"（{'、'.join(paths[:3])}{' 等' if len(paths) > 3 else ''}）：正式交付会被脏闸门拦下，"
            "产物也可能与提交后的不同。"
        )
    try:
        _check_guard(spec, payload.rows)
    except DeliveryBlocked as error:
        notes.append(str(error))
    if payload.files:
        # 重复闸门要比对产物字节，不落盘时没有字节可比——那就不报，不猜。
        try:
            _refuse_unchanged(request_dir, spec, stamp_value, payload)
        except DeliveryUnchanged as error:
            notes.append(str(error))
    if keep_files:
        _write_preview_note(request_dir, spec, stamp_value, payload, notes)
    return Preview(
        request_id=spec.id,
        kind=spec.kind,
        rows=payload.rows,
        columns=payload.columns,
        lines=_preview_lines(request_dir, payload.detail_lines),
        path=staging if keep_files else None,
        files=tuple(payload.files),
        notes=tuple(notes),
    )


def _write_preview_note(
    request_dir: Path,
    spec: Spec,
    stamp_value: SourceStamp,
    payload: Payload,
    notes: list[str],
) -> Path:
    """草稿区那一页说明：不落档案也要说清「这份草稿是哪一版口径、哪一版底库算的」。"""
    path = request_dir / SCRATCH_DIR / PREVIEW_DIR / PREVIEW_NAME
    lines = [
        f"# {spec.id} 预览（不是交付）",
        "",
        "> 本目录是草稿：可随时重算、会在下次成功交付时删除。",
        "> 它**不进交付台账**，也不受方法闸门与护栏阻断。",
        "",
        f"- 需求版本：v{spec.version}",
        f"- 底库：`{stamp_value.current_md5}` · {stamp_value.current_rows:,} 行",
        f"- 行数：{payload.rows:,} · 列数：{len(payload.columns)}",
    ]
    for entry in payload.files:
        lines.append(
            f"- `{entry['name']}`：{entry['bytes']:,} 字节，sha256 `{str(entry['sha256'])[:16]}…`"
        )
    if notes:
        lines += ["", "## 正式交付会被拦下的原因", ""]
        lines += [f"- {item}" for item in notes]
    else:
        lines += ["", "正式交付前仍要过：代码已提交、底库或产物与本条不同、方法合规。"]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _preview_lines(request_dir: Path, detail: list[str]) -> list[str]:
    """预演不拦，但要把「正式交付会被拦」写在最上面。

    预演的唯一用途就是「看看会不会 0 行」，此时也被方法闸门挡住等于自相矛盾；
    可若一声不吭地跑通，人就会以为可以交付了，然后在正式那一刻才撞墙。
    """
    found = method.problems(request_dir)
    if not found:
        return detail
    return [
        "> ⚠ 过程记录不合规，正式交付会被拦下：",
        *[f"> - {text}" for text in found],
        "",
        *detail,
    ]


def _execute(ctx: RunContext) -> Payload:
    """按 kind 分派，守住「有没有人登记过这种形态」。

    这里只捕 `OSError`（磁盘、权限这类与形态无关的失败）：各形态自己的失败语义由
    各自的执行体翻译成 `DeliveryBlocked`。档案层一旦开始 `except` 某个形态的异常
    类型，就又开始认识那个形态了——「只认 Payload」也就成了空话。

    翻译成 `DeliveryBlocked` 是给 `run-due` 用的：一份坏 spec 或打不开的输入表只该
    记一条失败，不该让整批交付一起崩。编程错误（TypeError 之类）不在此列，要照常
    抛出来。
    """
    executor = EXECUTORS.get(ctx.spec.kind)
    if executor is None:
        raise DeliveryBlocked(f"{ctx.spec.id}: 未注册的执行体 kind={ctx.spec.kind.value}")
    try:
        return executor(ctx)
    except OSError as error:
        raise DeliveryBlocked(f"{ctx.spec.id} 执行失败：{error}") from error


# 执行体在各自模块末尾把自己写进 `EXECUTORS`：下面的 import 就是「点名」与登记
# 两件事。档案层从此不必知道有哪几种形态，加一种也不用回来改这个文件。


# ---------------------------------------------------------------- 护栏


def _check_guard(spec: Spec, rows: int) -> None:
    if rows < spec.guard.min_rows:
        raise DeliveryBlocked(
            f"{spec.id} 只得到 {rows} 行，低于 min_rows={spec.guard.min_rows}。"
            "空结果交付出去没有意义，通常意味着口径写错了——先 `request show` 核对一遍。"
        )
    if spec.guard.max_rows is not None and rows > spec.guard.max_rows:
        raise DeliveryBlocked(
            f"{spec.id} 得到 {rows} 行，超过 max_rows={spec.guard.max_rows}。"
            "这么大的量交出去很可能不是对方要的；确认无误请调高 spec 里的 max_rows。"
        )


# ---------------------------------------------------------------- 与上次比较


def deliveries_of(request_dir: Path) -> list[Path]:
    """一次需求的交付目录，按名字升序——名字是时间戳，字典序即时间序。

    以 `.` 开头的是原子写留下的 `.tmp-*` 暂存目录，它**不是**一次交付：算进台账
    会让「once 已交付过」凭空成立，把真正需要的那份挡在门外。

    这条判据只能有一份。台账（本函数）、体检（`ops.doctor`）、`request list` 都靠它
    决定「哪些目录算一次交付」；三处各写一遍，迟早有一处把 `.tmp-` 算进去，而那种
    错不会报错——只会在半年后有人问「这份数据什么时候交的」时，答出一个没交成的日子。
    """
    base = request_dir / DELIVERIES_DIR
    if not base.is_dir():
        return []
    return sorted(
        path for path in base.iterdir() if path.is_dir() and not path.name.startswith(".")
    )


def _last_delivery(request_dir: Path) -> Path | None:
    done = deliveries_of(request_dir)
    return done[-1] if done else None


def _last_delivery_name(request_dir: Path, *, exclude: str) -> str | None:
    last = _last_delivery(request_dir)
    if last is None or last.name == exclude:
        return None
    return last.name


def read_manifest(directory: Path) -> dict[str, Any] | None:
    """读一次交付的 `manifest.json`；读不出来返回 None。

    三种失败（文件不在、JSON 坏了、顶层不是对象）一律折叠成 None——**怎么表达**
    留给调用方：`_health` 记 `unknown`、`manifest_summary` 印一行、`compare` 抛
    `CompareError`。折叠是因为「读」这件事本该只有一处，而表达方式本就各不相同。

    本函数收拢的其实是两样东西：文件名与解析容错。原先 `compare` 自己拼
    `"manifest.json"`，`commands` 又拼一次，谁改了文件名都不会通知另一方。
    """
    try:
        body = json.loads((directory / MANIFEST_NAME).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return body if isinstance(body, dict) else None


# ---------------------------------------------------------------- 交付索引


def audit_facts(request_dir: Path) -> dict[str, dict[str, Any]]:
    """读 `AUDIT.md` 里那段 ```json：引擎当场不知道、只能事后核出的事实。

    「某条交付的代码与产物对不上」这类判定，引擎在事后无法重算——它只知道交付时
    的 HEAD，不知道那天的 HEAD 是否包含实际跑着的改动。这种事只能人核一次、写下来，
    再由 INDEX 引用。**读不出来就当没有**（返回空），不因此拦交付：索引少一列可信度
    比交付被一份手写文件卡住要好。
    """
    path = request_dir / DELIVERIES_DIR / AUDIT_NAME
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return {}
    match = re.search(r"```json\s*\n(.*?)\n```", text, re.DOTALL)
    if match is None:
        return {}
    try:
        body = json.loads(match.group(1))
    except json.JSONDecodeError:
        return {}
    if not isinstance(body, dict):
        return {}
    return {str(key): value for key, value in body.items() if isinstance(value, dict)}


def _core_digest_of(request_dir: Path, body: dict[str, Any]) -> str | None:
    """从 `history/v<n>.yaml` 现算某一版的口径指纹；快照不在就返回 None。

    INDEX 要区分「口径变了」与「只是排过版」：档案里只记了完整指纹（含展示项），
    单看它分不出来。快照本来就在需求目录里，现算一次比在矩阵里加一列更省事。
    """
    version = body.get("spec_version")
    if not isinstance(version, int):
        return None
    path = request_dir / HISTORY_DIR / f"v{version}.yaml"
    if not path.is_file():
        return None
    try:
        return spec_core_digest(path, base_dir=request_dir)
    except SpecError:
        return None


def _nature(request_dir: Path, current: dict[str, Any], previous: dict[str, Any] | None) -> str:
    """这条交付相对上一条「变了什么」——只据档案与版本快照推断，不猜意图。

    判据次序：产物字节 → 列集 → 口径指纹（展示项除外）。合起来区分六件事：
    重钉、复核（底库变了产物没变）、结构变化、口径变化、排版变化、数据刷新。
    """
    if previous is None:
        return "首版"
    same_files = _product_digests((current.get("result") or {}).get("files")) == _product_digests(
        (previous.get("result") or {}).get("files")
    )
    same_source = (current.get("source") or {}).get("current_md5") == (
        previous.get("source") or {}
    ).get("current_md5")
    if same_files:
        return "重钉" if same_source else "复核（底库更新，产物未变）"
    now_columns = len(((current.get("result") or {}).get("columns")) or ())
    before_columns = len(((previous.get("result") or {}).get("columns")) or ())
    if now_columns != before_columns:
        return "结构扩展" if now_columns > before_columns else "结构收缩"
    now_core = _core_digest_of(request_dir, current)
    before_core = _core_digest_of(request_dir, previous)
    if now_core and before_core and now_core != before_core:
        return "口径变更"
    if current.get("spec_version") != previous.get("spec_version") or same_source:
        # 版本动了、口径指纹却相同：动的是展示项（旧规则下会顺带升一版），或只是重排。
        return "排版变更"
    return "数据刷新"


def _reproducible(body: dict[str, Any]) -> str:
    """这一条能不能按 manifest 记的 commit 复现。三态：是 / 否 / 未核。"""
    worktree = body.get("worktree")
    if isinstance(worktree, dict) and worktree.get("checked"):
        return "✓" if worktree.get("clean") else "✗ 代码未提交"
    return "?"


def index_lines(request_dir: Path) -> list[str]:
    """`deliveries/INDEX.md` 的正文。一条不删、不评，只把台账摊成一张能读的表。"""
    directories = deliveries_of(request_dir)
    audit = audit_facts(request_dir)
    # 目录名是 `REQ-0004-backfill-标题`：前两段才是需求号，`split('-', 1)[0]` 只会得到 `REQ`。
    request_id = "-".join(request_dir.name.split("-")[:2])
    header = [
        f"# {request_id} 交付索引",
        "",
        "> 由 `hf-data-crawl request index` 生成，**请勿手改**：下次交付会整份重写。",
        "> 交付目录是不可变台账——每条只增不删；「当前有效」只看最后一行。",
        "",
    ]
    sample = sample_note(request_dir)
    if sample is not None:
        # 样例需求常常交得比真实需求还整齐；不标出来，它就会被当成业务事实引用。
        header += [f"> **样例需求**：{sample}（影响面统计不计入它）", ""]
    if not directories:
        return [*header, "（尚未交付）", ""]
    lines = [
        *header,
        f"- 当前有效：`{directories[-1].name}`",
        f"- 交付条数：{len(directories)}",
        f"- 可复现性：`✓` 代码干净、`✗` 有未提交改动、`?` 该条档案里没有这项记录"
        f"（{AUDIT_NAME} 里的历史结论会覆盖 `?`）",
        "",
        "| 时刻 | 版本 | 行数 | 列数 | 性质 | 可复现 | 产物 | 产物 sha256 | 底库 md5 | 代码 commit |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    pruned = pruned_facts(request_dir)
    previous: dict[str, Any] | None = None
    for directory in directories:
        body = read_manifest(directory) or {}
        result = body.get("result") or {}
        files = result.get("files") or []
        digest = str(files[0].get("sha256", ""))[:8] if files else "-"
        source = str((body.get("source") or {}).get("current_md5", ""))[:8]
        commit = str(((body.get("method") or {}).get("env") or {}).get("commit", ""))[:8]
        known = audit.get(directory.name) or {}
        mark = (
            "✓（见 AUDIT）"
            if known.get("reproducible") is True
            else "✗（见 AUDIT）"
            if known.get("reproducible") is False
            else _reproducible(body)
        )
        # 产物那一列要分清三件事：还在、按留存策略清掉、没留存（早期手工删过）。
        # 后两者都「打不开」，但原因不同——混成同一个符号，读的人会以为清理漏了。
        if directory.name in pruned:
            product = "已清理"
        elif (
            all((directory / str(item.get("name"))).is_file() for item in files) if files else False
        ):
            product = "✓"
        else:
            product = "未留存"
        lines.append(
            f"| {directory.name} | v{body.get('spec_version', '?')} | {result.get('rows', '?')} "
            f"| {len(result.get('columns') or [])} | {_nature(request_dir, body, previous)} | {mark} "
            f"| {product} | {digest} | {source} | {commit} |"
        )
        previous = body
    lines += [
        "",
        _INDEX_MARK.format(newest=directories[-1].name, count=len(directories)),
        "",
    ]
    return lines


#: 产物留存清单：哪些交付的产物字节被清理了、它们的 sha256 是多少。
#: 交付目录是**不可变台账**，所以清理只动产物字节，绝不动 manifest / README；
#: 清掉的每一样都记在这里，档案仍能自证「那一份长什么样」。
PRUNED_NAME = "PRUNED.json"


@dataclass(frozen=True)
class PrunedBatch:
    """一条被清理的交付。`files` 里的 sha256 来自它自己的 manifest。"""

    name: str
    files: tuple[dict[str, Any], ...] = ()
    freed: int = 0


@dataclass(frozen=True)
class PruneReport:
    request_id: str
    kept: int
    pruned: tuple[PrunedBatch, ...] = ()
    #: 被跳过的目录（没有 manifest，或产物本来就缺）——不静默，列出来。
    skipped: tuple[str, ...] = ()

    @property
    def freed(self) -> int:
        return sum(item.freed for item in self.pruned)


def pruned_facts(request_dir: Path) -> dict[str, Any]:
    """读 `deliveries/PRUNED.json`；没有、坏了都当空——它只是附加账，不该拦住任何命令。"""
    path = request_dir / DELIVERIES_DIR / PRUNED_NAME
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    pruned = body.get("pruned")
    return pruned if isinstance(pruned, dict) else {}


def prune(
    request_dir: Path,
    *,
    keep: int = 1,
    moment: datetime | None = None,
    dry_run: bool = False,
) -> PruneReport:
    """清掉旧交付的产物字节，只留最近 `keep` 条；manifest 与 README 一份不动。

    为什么可以清：产物是「交付出去的那份文件」的重算副本，而 manifest 记着它的
    sha256、README 记着怎么复现。台账的价值在「当时交了什么」，不在于把每一版
    Excel 都永久留一份——有 daily 需求之后，那就是每月几十 MB 的无句点增长。

    为什么当前那条永不清：它是 `INDEX` 指向的「当前有效」，也是人最常打开的那份。
    """
    directories = deliveries_of(request_dir)
    keep = max(1, keep)
    targets = directories[:-keep]
    facts = pruned_facts(request_dir)
    stamp = (moment or datetime.now().astimezone()).isoformat(timespec="seconds")
    batches: list[PrunedBatch] = []
    skipped: list[str] = []
    for directory in targets:
        body = read_manifest(directory)
        if body is None:
            skipped.append(directory.name)
            continue
        files = (body.get("result") or {}).get("files") or []
        removed: list[dict[str, Any]] = []
        freed = 0
        for item in files:
            path = directory / str(item.get("name") or "")
            if not path.is_file():
                continue
            freed += path.stat().st_size
            removed.append(
                {
                    "name": path.name,
                    "bytes": path.stat().st_size,
                    "sha256": item.get("sha256") or _sha256(path),
                }
            )
        if not removed:
            continue
        batches.append(PrunedBatch(name=directory.name, files=tuple(removed), freed=freed))
        if dry_run:
            continue
        # 先记账、再删字节：反过来（先删后记）在两步之间崩溃，会留下「产物没了、
        # 也没人说是清理掉的」——那正是这套留存机制要防的「看起来坏了」。
        facts[directory.name] = {"pruned_at": stamp, "files": removed}
        write_bytes(
            request_dir / DELIVERIES_DIR / PRUNED_NAME,
            (
                json.dumps(
                    {"version": 1, "updated_at": stamp, "pruned": facts},
                    ensure_ascii=False,
                    indent=2,
                )
                + "\n"
            ).encode("utf-8"),
        )
        for item in removed:
            (directory / str(item["name"])).unlink(missing_ok=True)

    return PruneReport(
        request_id="-".join(request_dir.name.split("-")[:2]),
        kept=min(keep, len(directories)),
        pruned=tuple(batches),
        skipped=tuple(skipped),
    )


def _sha256(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1 << 20), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def write_index(request_dir: Path) -> Path:
    """把 `deliveries/INDEX.md` 整份重写。交付后与 `request index` 都走这里。

    原子写：半截索引会让 `doctor` 报「与台账对不上」，而它本来是唯一能指出
    「哪条算数」的东西——一份看起来正常、实际截断的索引比没有更坏。
    """
    path = request_dir / DELIVERIES_DIR / INDEX_NAME
    write_bytes(path, "\n".join(index_lines(request_dir)).encode("utf-8"))
    return path


def index_problem(request_dir: Path) -> str | None:
    """索引与台账对不上时返回一句人话（doctor 用）。对得上返回 None。

    判据是 INDEX 里那行机读标记，不是文件时间：`git checkout` 会把 mtime 全部刷新，
    按时间比会在每次 clone 之后误报一遍。
    """
    directories = deliveries_of(request_dir)
    if not directories:
        return None
    path = request_dir / DELIVERIES_DIR / INDEX_NAME
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return f"{request_dir.name}：有 {len(directories)} 条交付但没有 {INDEX_NAME}"
    match = re.search(r"<!-- deliveries: (\S+) · entries: (\d+) -->", text)
    if match is None:
        return f"{request_dir.name}：{INDEX_NAME} 里没有台账标记，说明它不是引擎生成的"
    if match.group(1) != directories[-1].name or int(match.group(2)) != len(directories):
        return (
            f"{request_dir.name}：{INDEX_NAME} 记的是 {match.group(2)} 条 / 最新 {match.group(1)}，"
            f"台账实际是 {len(directories)} 条 / 最新 {directories[-1].name}"
        )
    return None


def _health(request_dir: Path, spec: Spec, rows: int, header: tuple[str, ...] = ()) -> Health:
    """与上一次交付比：同一口径版本内才比行数；表头则只在有基线时比。

    改了口径（版本变了）必然行数大变，那时比较只会天天发假警报——这类误报最能
    训练人忽略告警，所以宁可显式跳过并记下 `supersedes`。

    表头单独比，**跨版本也比**：换一份字段不同的表，恰恰是改口径的常见原因。行数
    可以不比，但「这次的表和上次不一样」必须留在纸面上。
    """
    last = _last_delivery(request_dir)
    if last is None:
        return Health(status="first")

    body = read_manifest(last)
    if body is None:
        return Health(
            status="unknown", note=f"上一次交付（{last.name}）的 manifest 读不出来，本次未比较"
        )

    header_note = _header_note(body, header)

    baseline = (body.get("result") or {}).get("rows")
    if not isinstance(baseline, int):
        return Health(
            status="unknown",
            note=f"上一次交付（{last.name}）没有记行数，本次未比较",
            header_note=header_note,
        )

    previous_version = body.get("spec_version")
    if previous_version != spec.version:
        return Health(
            status="ok",
            baseline_rows=baseline,
            supersedes=previous_version if isinstance(previous_version, int) else None,
            note=f"口径已从 v{previous_version} 改到 v{spec.version}，行数变化不计入标疑",
            header_note=header_note,
        )

    delta_abs = rows - baseline
    delta_pct = (delta_abs / baseline * 100) if baseline else 0.0
    # 两个条件必须同时满足：只用百分比，小结果集会被误报（12 行变 15 行就是 +25%）。
    suspect = (
        abs(delta_pct) > spec.alert.row_delta_pct and abs(delta_abs) >= spec.alert.row_delta_abs
    )
    return Health(
        status="suspect" if suspect else "ok",
        baseline_rows=baseline,
        delta_pct=round(delta_pct, 2),
        delta_abs=delta_abs,
        header_note=header_note,
    )


def _header_note(body: dict[str, Any], header: tuple[str, ...]) -> str:
    """把表头差异说成人话。任一侧没有记录就明说「没比」，不假装一致。"""
    if not header:
        return ""
    recorded = ((body.get("fill") or {}).get("input") or {}).get("header") or {}
    columns = recorded.get("columns")
    if not isinstance(columns, list) or not columns:
        return "上一份档案没记输入表头，本次未比较（早期档案没有这一项）"
    old = tuple(str(name) for name in columns)
    if old == header:
        return ""
    change = header_change(old, header)
    parts: list[str] = []
    if change["added"]:
        parts.append("新增 " + "、".join(change["added"]))
    if change["removed"]:
        parts.append("少了 " + "、".join(change["removed"]))
    if change["reordered"]:
        parts.append("共同列次序变了")
    return "输入表头与上一份交付不同：" + "；".join(parts)


# ---------------------------------------------------------------- 落盘


def _reserve_dir(request_dir: Path, moment: datetime) -> Path:
    base = request_dir / DELIVERIES_DIR
    base.mkdir(parents=True, exist_ok=True)
    stem = moment.strftime(_STAMP_FORMAT)
    candidate = base / stem
    index = 2
    while candidate.exists():
        candidate = base / f"{stem}-{index}"
        index += 1
    return candidate


def _manifest(
    *,
    spec: Spec,
    moment: datetime,
    stamp_value: SourceStamp,
    payload: Payload,
    health: Health,
    previous: str | None,
    method_stamp: dict[str, Any],
    spec_sha: str,
    worktree: dict[str, Any],
) -> dict[str, Any]:
    manifest: dict[str, Any] = {
        "request_id": spec.id,
        "title": spec.title,
        "requester": spec.requester,
        "kind": spec.kind.value,
        "spec_version": spec.version,
        # 口径的语义指纹。只记版本号不够：版本号只是人写的一个数字，改了口径
        # 忘了加一，读档的人无从分辨。有了指纹，「这份档案对应哪一版口径」才可核对。
        "spec_sha256": spec_sha,
        "delivered_at": moment.isoformat(timespec="seconds"),
        "source": {
            "current_md5": stamp_value.current_md5,
            "current_rows": stamp_value.current_rows,
            "newest_snapshot_date": (
                stamp_value.newest_snapshot_date.isoformat()
                if stamp_value.newest_snapshot_date
                else None
            ),
        },
        "result": {
            "rows": payload.rows,
            "columns": list(payload.columns),
            "files": payload.files,
        },
        "health": {
            "status": health.status,
            "baseline_rows": health.baseline_rows,
            "delta_pct": health.delta_pct,
            "delta_abs": health.delta_abs,
            "supersedes": health.supersedes,
            "note": health.note,
            "header_note": health.header_note,
        },
        "previous": previous,
        # 过程自包含：这份结果由哪版方法、哪版脚本、哪版代码产出。缺了它，
        # 「基于哪一版底库」只钉住了原材料，钉不住做法。
        "method": method_stamp,
        # 代码是否干净：`method.env.commit` 记的是 HEAD，只有这一项能说明
        # 「产物是否真的出自那个提交」。INDEX 的可复现列就据此判。
        "worktree": worktree,
    }
    # 形态特有的口径进顶层，便于「读 manifest 就知道这次怎么算的」；公共键不许被
    # 覆盖——一旦覆盖，「这份交付基于哪版底库」就可能被某个执行体悄悄改写。
    overlap = manifest.keys() & payload.definition.keys()
    if overlap:
        raise DeliveryBlocked(f"{spec.id}: 执行体口径与档案公共字段冲突：{sorted(overlap)}")
    manifest.update(payload.definition)
    return manifest


def _readme(
    spec: Spec,
    moment: datetime,
    stamp_value: SourceStamp,
    payload: Payload,
    health: Health,
    spec_sha: str,
) -> str:
    lines: list[str] = [f"# {spec.id} {spec.title}", ""]
    if health.status == "suspect":
        lines += [
            f"> ⚠️ **本次结果与上次相比行数变动 {health.delta_pct:+.1f}%"
            f"（{health.baseline_rows} → {payload.rows}），已标疑。**"
            "交出去之前请先确认是底库正常波动，还是口径或上游出了问题。",
            "",
        ]
    elif health.status == "unknown":
        lines += [f"> 上一次交付的档案读不出来，本次未做行数比较（{health.note}）。", ""]
    if health.header_note:
        lines += [f"> 注意：{health.header_note}。行数没变不等于结果没变，请先核对这一批。", ""]

    lines += [f"- 提出人：{spec.requester}"]
    if spec.purpose:
        lines.append(f"- 用途：{spec.purpose}")
    lines += [
        f"- 交付时刻：{moment.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 需求形态：`{spec.kind.value}`",
        f"- 需求版本：v{spec.version}（口径指纹 `{spec_sha[:16]}…`）",
        f"- 交付节奏：{spec.cadence.value}",
        "",
        "## 口径",
        "",
        *payload.detail_lines,
        "",
        "## 基于哪一版底库",
        "",
        f"- `current.parquet` md5：`{stamp_value.current_md5}`",
        f"- 底库总行数：{stamp_value.current_rows:,}",
        f"- 最新观测日：{_day_text(stamp_value)}",
        "",
        "底库每天都在变，所以这份结果只在「这一版底库」下成立。要核对来源，",
        f"跑 `hf-data-crawl request show {spec.id}` 看交付台账。",
        "",
        "## 结果",
        "",
        f"- 行数：{payload.rows:,}",
        f"- 列数：{len(payload.columns)}",
    ]
    for entry in payload.files:
        lines.append(
            f"- `{entry['name']}`：{entry['bytes']:,} 字节，sha256 `{entry['sha256'][:16]}…`"
        )
    lines += [
        "",
        "## 复现",
        "",
        f"- 做法：`{method.METHOD_DIR}/{method.METHOD_DOC}`（步骤、判据、哪步机器哪步人判）",
        f"- 脚本：`{method.METHOD_DIR}/{method.SCRIPTS_DIR}/`，逐份 sha256 与所依赖的代码版本",
        "  见 `manifest.json` 的 `method` 段",
        "",
        "```bash",
        f"hf-data-crawl request run {spec.id} --force",
        "```",
        "",
        "同一版底库、同一份输入表下重跑，sha256 应当与上面一致。若不一致，",
        "说明底库或输入表已经变过。",
        "",
        f"只想先看看产物长什么样（不新增交付）：`hf-data-crawl request run {spec.id} --preview`。",
        "",
    ]
    return "\n".join(lines)


def _day_text(stamp_value: SourceStamp) -> str:
    return (
        stamp_value.newest_snapshot_date.isoformat() if stamp_value.newest_snapshot_date else "未知"
    )


# ---------------------------------------------------------------- 到期判断


#: 各节奏要求的最小间隔（日历日）。`daily` 用 1 天而非 24 小时：昨晚 23:50 交过，
#: 今早 07:00 该不该再交一次？按「每天一份」的语义，该——昨天那份属于昨天。
_CADENCE_DAYS: dict[Cadence, int] = {
    Cadence.DAILY: 1,
    Cadence.WEEKLY: 7,
    Cadence.MONTHLY: 30,
}


@dataclass(frozen=True)
class DueReport:
    delivered: list[DeliveryResult]
    skipped: list[tuple[str, str]]
    failed: list[tuple[str, str]]


def last_delivered_at(request_dir: Path) -> datetime | None:
    """上一次交付的时刻。读不出来就返回 None——调用方会当成「从未交付」。"""
    last = _last_delivery(request_dir)
    if last is None:
        return None
    body = read_manifest(last)
    if body is None:
        return None
    raw = body.get("delivered_at")
    if not isinstance(raw, str):
        return None
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def due_state(spec: Spec, request_dir: Path, now: datetime) -> tuple[bool, str]:
    """这份需求今天该不该跑，以及为什么。第二个返回值直接进日志。"""
    if spec.cadence is Cadence.MANUAL:
        return False, "manual：只等人跑"
    moment = last_delivered_at(request_dir)
    if moment is None:
        return True, "从未交付"
    if spec.cadence is Cadence.ONCE:
        return False, f"once 已交付过（{moment:%Y-%m-%d %H:%M}）"

    need = _CADENCE_DAYS[spec.cadence]
    elapsed = (now.date() - moment.date()).days
    if elapsed >= need:
        return True, f"距上次 {elapsed} 天，满 {need} 天"
    return False, f"距上次 {elapsed} 天，未满 {need} 天"


def run_due(
    store: DatasetStore,
    requests_root: Path,
    *,
    now: datetime | None = None,
    catalog: GoldCatalog | None = None,
    source: SourceStamp | None = None,
) -> DueReport:
    """把所有到期的需求各跑一次。

    一个需求失败不挡别的：坏 spec、0 行、执行出错都只记一条，继续下一个。
    但底库本身不成立（`current` 缺失）时整体失败——那不是某一条需求的问题，
    报十遍同样的错只会把真正的原因埋掉。
    """
    moment = now or datetime.now().astimezone()
    stamp_value = source or _stamp(store)
    runner = _catalog(store, catalog)

    delivered: list[DeliveryResult] = []
    skipped: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []

    for spec_path in sorted(requests_root.glob(f"*/{SPEC_NAME}")):
        # 目录名带语义后缀，交付时必须把「目录名」换成 spec 里的需求号——
        # `deliver` 靠编号去解析目录，传目录名会找不到家。
        try:
            spec = load_spec(spec_path)
        except SpecError as error:
            failed.append((spec_path.parent.name, str(error)))
            continue
        request_id = spec.id
        # 关闭是「这单不用再跑了」：跳过而不是失败——它不是故障，是运营决定。
        note = closed_note(spec_path.parent)
        if note is not None:
            skipped.append((spec.id, f"已关闭：{note}"))
            continue
        # 样例是开发夹具：它们可以手工 `request run`，但不该由定时链每天生产交付。
        sample = sample_note(spec_path.parent)
        if sample is not None:
            skipped.append((spec.id, f"样例需求：{sample}（不参与定时交付）"))
            continue

        due, reason = due_state(spec, spec_path.parent, moment)
        if not due:
            skipped.append((spec.id, reason))
            continue

        try:
            result = deliver(
                store,
                requests_root,
                request_id,
                catalog=runner,
                source=stamp_value,
                now=moment,
            )
        except DeliveryUnchanged as error:
            # 「无事发生」与「出事了」必须分开记：混成一种，日志里的红点会失去意义。
            skipped.append((request_id, str(error)))
            continue
        except (DeliveryBlocked, SpecError) as error:
            failed.append((request_id, str(error)))
            continue
        except (OSError, ValueError) as error:
            # 预期之外的失败也要落在「这一条」上：磁盘满、权限、口径里的取值转换
            # 写错，都会到这里。整批因为一条崩掉，其余需求就永远轮不到——批量交付
            # 的意义正是「坏的报出来，好的照常出」。
            failed.append((request_id, f"{type(error).__name__}: {error}"))
            continue
        delivered.append(result)

    return DueReport(delivered=delivered, skipped=skipped, failed=failed)
