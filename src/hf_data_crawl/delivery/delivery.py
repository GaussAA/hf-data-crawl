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

import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from ..platform.atomic import replace
from ..platform.store import DatasetStore
from ..query.gold import GoldCatalog, GoldNotReady
from . import fill_exec, method, query_exec  # noqa: F401
from .contract import EXECUTORS, DeliveryBlocked, Payload, RunContext
from .header import header_change
from .requirement import (
    DELIVERIES_DIR,
    MANIFEST_NAME,
    README_NAME,
    SPEC_NAME,
    Cadence,
    Kind,
    Spec,
    SpecError,
    resolve_dir,
    snapshot_history,
    spec_digest,
    version_conflict,
)
from .requirement import load as load_spec
from .stamp import SourceMissing, SourceStamp, stamp

#: 交付目录名的时刻格式。到秒：同一分钟内跑两个需求是常态。
_STAMP_FORMAT = "%Y%m%dT%H%M%S"


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
    """预演结果：只算不写，不建目录、不留档案，也不受护栏阻断。

    预演的用途恰恰是「看看会不会 0 行」，此时套护栏等于自相矛盾。
    """

    request_id: str
    kind: Kind
    rows: int
    columns: tuple[str, ...]
    lines: list[str]


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
    spec = load_spec(spec_path)
    # 版本号不可复用：口径变了却还用 v1，档案会指着一份「当时说过、现在已经不是」
    # 的定义。这比缺方法更隐蔽——缺方法至少会报错，这一种只会让人拿着错前提核对。
    conflict = version_conflict(spec_path, spec.version)
    if conflict:
        raise DeliveryBlocked(f"{spec.id}: {conflict}")
    spec_sha = spec_digest(spec_path)
    # 先验方法再算数：方法是「这份交付凭什么成立」的一部分，缺它连预留目录都不该做。
    method_stamp = _method_stamp(request_dir, spec)
    moment = now or datetime.now().astimezone()
    stamp_value = source or _stamp(store)
    runner = _catalog(store, catalog)

    # 先落口径快照，再换名出产物。
    #
    # 反过来（改名后快照）在那两步之间崩溃时会留下「产物已出土、history 没有这版」：
    # `version_conflict` 靠 `history/vN.yaml` 判断版本号是否用过，于是这版会静默
    # 隐身，下次复用同一版本号不会报冲突——历史被改写而无人知道。宁可多一条
    # 「从未交付」的快照记录（用户可以从 `history/` 里删掉），也不能少一条。
    snapshot_history(request_dir / SPEC_NAME, spec.version)

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
        raise

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
) -> Preview:
    """只算不写：看这次会出多少行、每列能填多少，不产生任何交付档案。"""
    request_dir = resolve_dir(requests_root, request_id)
    spec = load_spec(request_dir / SPEC_NAME)
    payload = _execute(
        RunContext(
            store=store,
            spec=spec,
            # 预演下执行体不得写盘；给一个明确的哨兵路径，误写也会立刻暴露在
            # 这个不存在的目录上，而不是悄悄落到当前工作目录。
            staging=request_dir / DELIVERIES_DIR / ".preview",
            catalog=_catalog(store, catalog),
            source=source or _stamp(store),
            dry_run=True,
        )
    )
    return Preview(
        request_id=spec.id,
        kind=spec.kind,
        rows=payload.rows,
        columns=payload.columns,
        lines=_preview_lines(request_dir, payload.detail_lines),
    )


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
        except (DeliveryBlocked, SpecError) as error:
            failed.append((request_id, str(error)))
            continue
        delivered.append(result)

    return DueReport(delivered=delivered, skipped=skipped, failed=failed)
