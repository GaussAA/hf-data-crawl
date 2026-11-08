"""交付：把一份需求跑成一份**能自证来源的档案**。

为什么不是「跑一条 SQL 存个文件」
--------------------------------
文件一旦离开这台机器，就只剩一份数据。三个月后对方问「这数从哪来的、怎么算的」，
没有档案就只能回来翻记录。所以每份交付固定三件：

- `data.parquet`（或 csv）：结果本体
- `manifest.json`：机器读——底库指纹、口径、行数、列、sha256、健康标记
- `README.md`：人读——要什么、怎么算的、基于哪天的底库、几行

`manifest.source.current_md5` 是整份档案的锚点。底库每天都变，所以一份交付
只在「那一版底库」下成立；有这个 md5，任何一份历史交付都能回答它基于什么。

三条硬规矩
----------
1. **0 行不交付**。空结果交出去没有意义，且通常意味着口径写错了。
2. **超上限不交付**。一条写错的需求不该把百万行倒给对方。
3. **判疑不阻断**。行数突变时照常交付，只在 manifest 与 README 顶部标记——
   「每周一给人一份」这种需求，当天交不出货比数字可疑更糟。但「这次不对劲」
   必须留在纸面上，不能靠人回忆上次是多少。

原子性：先写 `.tmp-*` 目录，三件齐了再整体改名。中途失败绝不留半份交付。
"""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import duckdb
import polars as pl

from .atomic import replace
from .gold import GoldCatalog, GoldNotReady
from .metrics import lookup
from .requirement import (
    DELIVERIES_DIR,
    MANIFEST_NAME,
    README_NAME,
    SPEC_NAME,
    Cadence,
    OutputFormat,
    Spec,
    SpecError,
    compile_sql,
    snapshot_history,
)
from .requirement import (
    load as load_spec,
)
from .store import DatasetStore

#: 交付目录名的时刻格式。到秒：同一分钟内跑两个需求是常态。
_STAMP_FORMAT = "%Y%m%dT%H%M%S"


class DeliveryBlocked(RuntimeError):
    """按护栏不该交付（0 行、超上限、底库缺失、查询失败）。一律不落盘。"""


@dataclass(frozen=True)
class SourceStamp:
    """底库指纹：这一份交付到底基于哪一版数据。"""

    current_md5: str
    current_rows: int
    newest_snapshot_date: date | None = None


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


@dataclass(frozen=True)
class DeliveryResult:
    request_id: str
    directory: Path
    rows: int
    columns: tuple[str, ...]
    files: tuple[dict[str, Any], ...]
    health: Health
    spec_version: int


def stamp(store: DatasetStore) -> SourceStamp:
    """算底库指纹。一份 run-due 只算一次，传给各个需求复用。"""
    path = store.path("current")
    if not path.exists():
        raise DeliveryBlocked("底库不存在，先跑 `hf-data-crawl collect`")
    digest = hashlib.md5(path.read_bytes()).hexdigest()
    frame = store.read("current")
    newest = frame["last_seen_at"].max() if frame.height else None
    return SourceStamp(
        current_md5=digest,
        current_rows=frame.height,
        newest_snapshot_date=newest if isinstance(newest, date) else None,
    )


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
    request_dir = requests_root / request_id
    spec = load_spec(request_dir / SPEC_NAME)
    moment = now or datetime.now().astimezone()
    stamp_value = source or stamp(store)
    frame = _query(store, spec, catalog)
    _check_guard(spec, frame)
    health = _health(request_dir, spec, frame.height)

    directory = _reserve_dir(request_dir, moment)
    staging = directory.parent / f".tmp-{directory.name}"
    shutil.rmtree(staging, ignore_errors=True)
    staging.mkdir(parents=True)
    try:
        written = _write_data(staging, spec, frame)
        manifest = _manifest(
            spec=spec,
            moment=moment,
            stamp_value=stamp_value,
            frame=frame,
            files=written,
            health=health,
            previous=_last_delivery_name(request_dir, exclude=directory.name),
        )
        (staging / MANIFEST_NAME).write_text(
            json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
        )
        (staging / README_NAME).write_text(
            _readme(spec, moment, stamp_value, frame, written, health), encoding="utf-8"
        )
        # 三件齐了才换名：中途失败最多留下一个 .tmp-* 目录，不会出现「有数据没说明」。
        replace(staging, directory)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    # 首次交付落一份口径快照——只留「真的用过」的版本，没用过的口径没有保留价值。
    snapshot_history(request_dir / SPEC_NAME, spec.version)

    return DeliveryResult(
        request_id=spec.id,
        directory=directory,
        rows=frame.height,
        columns=tuple(frame.columns),
        files=tuple(written),
        health=health,
        spec_version=spec.version,
    )


# ---------------------------------------------------------------- 执行


def _query(store: DatasetStore, spec: Spec, catalog: GoldCatalog | None) -> pl.DataFrame:
    sql = compile_sql(spec)
    try:
        runner = catalog or GoldCatalog(store)
    except GoldNotReady as error:
        raise DeliveryBlocked(str(error)) from error
    try:
        return runner.run(sql)
    except duckdb.Error as error:
        # 包一层：裸的 BinderException 看不出是哪条需求、哪条 SQL 出的问题。
        raise DeliveryBlocked(f"{spec.id} 查询失败：{error}\nSQL：\n{sql}") from None


def _check_guard(spec: Spec, frame: pl.DataFrame) -> None:
    rows = frame.height
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


def _last_delivery(request_dir: Path) -> Path | None:
    base = request_dir / DELIVERIES_DIR
    if not base.is_dir():
        return None
    done = sorted(
        path for path in base.iterdir() if path.is_dir() and not path.name.startswith(".")
    )
    return done[-1] if done else None


def _last_delivery_name(request_dir: Path, *, exclude: str) -> str | None:
    last = _last_delivery(request_dir)
    if last is None or last.name == exclude:
        return None
    return last.name


def _read_manifest(directory: Path) -> dict[str, Any] | None:
    path = directory / MANIFEST_NAME
    try:
        body = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return body if isinstance(body, dict) else None


def _health(request_dir: Path, spec: Spec, rows: int) -> Health:
    """与上一次交付比：同一口径版本内才比行数。

    改了口径（版本变了）必然行数大变，那时比较只会天天发假警报——这类误报最能
    训练人忽略告警，所以宁可显式跳过并记下 `supersedes`。
    """
    last = _last_delivery(request_dir)
    if last is None:
        return Health(status="first")

    body = _read_manifest(last)
    if body is None:
        return Health(
            status="unknown", note=f"上一次交付（{last.name}）的 manifest 读不出来，本次未比较"
        )

    baseline = (body.get("result") or {}).get("rows")
    if not isinstance(baseline, int):
        return Health(status="unknown", note=f"上一次交付（{last.name}）没有记行数，本次未比较")

    previous_version = body.get("spec_version")
    if previous_version != spec.version:
        return Health(
            status="ok",
            baseline_rows=baseline,
            supersedes=previous_version if isinstance(previous_version, int) else None,
            note=f"口径已从 v{previous_version} 改到 v{spec.version}，行数变化不计入标疑",
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
    )


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


def _write_data(directory: Path, spec: Spec, frame: pl.DataFrame) -> list[dict[str, Any]]:
    """写数据文件并算校验和。返回每个文件的元数据，按写入顺序。"""
    plans: list[tuple[str, str]] = []
    if spec.output.format is OutputFormat.CSV:
        plans.append(("data.csv", "csv"))
    elif spec.output.format is OutputFormat.BOTH:
        plans.extend([("data.parquet", "parquet"), ("data.csv", "csv")])
    else:
        plans.append(("data.parquet", "parquet"))

    written: list[dict[str, Any]] = []
    for name, kind in plans:
        target = directory / name
        if kind == "csv":
            _flat_for_csv(frame).write_csv(target)
        else:
            frame.write_parquet(target)
        payload = target.read_bytes()
        written.append(
            {
                "name": name,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
            }
        )
    return written


def _flat_for_csv(frame: pl.DataFrame) -> pl.DataFrame:
    """CSV 不支持嵌套类型，列表列先拍平成 `;` 连接的字符串。

    宽表里有 11 个列表列（`tags`、`languages`、`card_tags`…），不拍平的话
    `write_csv` 直接抛 `CSV format does not support nested data`，一份 csv 都交不出去。
    选择 `;` 而非 JSON 数组：CSV 是给人拿 Excel 打开的，`a;b` 比 `["a","b"]`
    少一层引号转义，肉眼也认得出来。空列表留空串。
    """
    exprs: list[pl.Expr] = []
    for name, dtype in frame.schema.items():
        if isinstance(dtype, pl.List):
            exprs.append(pl.col(name).list.join(";").alias(name))
        else:
            exprs.append(pl.col(name))
    return frame.select(exprs)


def _manifest(
    *,
    spec: Spec,
    moment: datetime,
    stamp_value: SourceStamp,
    frame: pl.DataFrame,
    files: list[dict[str, Any]],
    health: Health,
    previous: str | None,
) -> dict[str, Any]:
    criteria = [
        {
            "metric": item.metric,
            "field": lookup(item.metric).field,
            "op": item.op,
            "value": _jsonable(item.value),
            "definition": lookup(item.metric).definition,
        }
        for item in spec.criteria
    ]
    return {
        "request_id": spec.id,
        "title": spec.title,
        "requester": spec.requester,
        "spec_version": spec.version,
        "delivered_at": moment.isoformat(timespec="seconds"),
        "sql": compile_sql(spec),
        "criteria": criteria,
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
            "rows": frame.height,
            "columns": list(frame.columns),
            "files": files,
        },
        "health": {
            "status": health.status,
            "baseline_rows": health.baseline_rows,
            "delta_pct": health.delta_pct,
            "delta_abs": health.delta_abs,
            "supersedes": health.supersedes,
            "note": health.note,
        },
        "previous": previous,
    }


def _jsonable(value: Any) -> Any:
    if isinstance(value, date | datetime):
        return value.isoformat()
    return value


def _readme(
    spec: Spec,
    moment: datetime,
    stamp_value: SourceStamp,
    frame: pl.DataFrame,
    files: list[dict[str, Any]],
    health: Health,
) -> str:
    lines: list[str] = [f"# {spec.id} {spec.title}", ""]
    if health.status == "suspect":
        lines += [
            f"> ⚠️ **本次结果与上次相比行数变动 {health.delta_pct:+.1f}%"
            f"（{health.baseline_rows} → {frame.height}），已标疑。**"
            "交出去之前请先确认是底库正常波动，还是口径或上游出了问题。",
            "",
        ]
    elif health.status == "unknown":
        lines += [f"> 上一次交付的档案读不出来，本次未做行数比较（{health.note}）。", ""]

    lines += [
        f"- 提出人：{spec.requester}",
    ]
    if spec.purpose:
        lines.append(f"- 用途：{spec.purpose}")
    lines += [
        f"- 交付时刻：{moment.strftime('%Y-%m-%d %H:%M:%S')}",
        f"- 需求版本：v{spec.version}",
        f"- 交付节奏：{spec.cadence.value}",
        "",
        "## 口径",
        "",
    ]
    if spec.mode.value == "sql":
        lines += ["手写 SQL：", "", "```sql", (spec.sql or "").strip(), "```", ""]
    else:
        lines += ["| 词条 | 落成字段 | 条件 | 定义 |", "| --- | --- | --- | --- |"]
        for item in spec.criteria:
            metric = lookup(item.metric)
            lines.append(
                f"| {item.metric} | `{metric.field}` | {item.op} {item.value} | {metric.definition} |"
            )
        lines.append("")

    newest = (
        stamp_value.newest_snapshot_date.isoformat() if stamp_value.newest_snapshot_date else "未知"
    )
    lines += [
        "## 基于哪一版底库",
        "",
        f"- `current.parquet` md5：`{stamp_value.current_md5}`",
        f"- 底库总行数：{stamp_value.current_rows:,}",
        f"- 最新观测日：{newest}",
        "",
        "底库每天都在变，所以这份结果只在「这一版底库」下成立。要核对来源，",
        f"跑 `hf-data-crawl request show {spec.id}` 看交付台账。",
        "",
        "## 结果",
        "",
        f"- 行数：{frame.height:,}",
        f"- 列数：{len(frame.columns)}",
    ]
    for entry in files:
        lines.append(
            f"- `{entry['name']}`：{entry['bytes']:,} 字节，sha256 `{entry['sha256'][:16]}…`"
        )
    if spec.output.format is not OutputFormat.PARQUET:
        lines.append(
            "- CSV 里列表列（`tags`、`languages` 等）以 `;` 连接成一格；要保留原始结构请用 parquet"
        )
    lines += [
        "",
        "## 复现",
        "",
        "```bash",
        f"hf-data-crawl request run {spec.id} --force",
        "```",
        "",
        "同一版底库下重跑，sha256 应当与上面一致。若不一致，说明底库已经刷新过。",
        "",
    ]
    return "\n".join(lines)


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
    body = _read_manifest(last)
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

    一个需求失败不挡别的：坏 spec、0 行、查询出错都只记一条，继续下一个。
    但底库本身不成立（`current` 缺失）时整体失败——那不是某一条需求的问题，
    报十遍同样的错只会把真正的原因埋掉。
    """
    moment = now or datetime.now().astimezone()
    stamp_value = source or stamp(store)
    runner = catalog or GoldCatalog(store)

    delivered: list[DeliveryResult] = []
    skipped: list[tuple[str, str]] = []
    failed: list[tuple[str, str]] = []

    for spec_path in sorted(requests_root.glob(f"*/{SPEC_NAME}")):
        request_id = spec_path.parent.name
        try:
            spec = load_spec(spec_path)
        except SpecError as error:
            failed.append((request_id, str(error)))
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
        except (DeliveryBlocked, SpecError) as error:
            failed.append((request_id, str(error)))
            continue
        delivered.append(result)

    return DueReport(delivered=delivered, skipped=skipped, failed=failed)
