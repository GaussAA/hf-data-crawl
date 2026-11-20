"""体检与补跑：错过的任务要被看见，补跑不能让同一天重复采集。"""

from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from hf_data_crawl.acquire.hf.api import HfResponse
from hf_data_crawl.ops.cli import main
from hf_data_crawl.ops.doctor import (
    ALERT_NAME,
    CHECKS,
    LOCK_STATE,
    Finding,
    RunContext,
    _snapshot_dependent,
    diagnose,
    sync_alert,
)
from hf_data_crawl.ops.health import read as read_health
from hf_data_crawl.pipeline import lineage
from hf_data_crawl.pipeline.lineage import path_of as lineage_path
from hf_data_crawl.platform.lock import LockBusy
from hf_data_crawl.platform.retention import REPLAY_WINDOW_DAYS, manifest_path
from hf_data_crawl.platform.runs import Outcome, write_run
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import (
    CURRENT_SCHEMA,
    ENRICH_SCHEMA,
    README_SCHEMA,
    SIZE_SCHEMA,
)

NOW = datetime(2026, 11, 10, 12, 0, 0)


def make_run(
    store: DatasetStore, command: str, when: datetime, reason: str = "end", **extra: Any
) -> None:
    payload = {"snapshot_date": when.date().isoformat(), "stopped_reason": reason, **extra}
    path = write_run(store, command, when, Outcome(payload))
    assert path is not None, "台账写入不该失败"
    # write_run 的 finished_at 取真实当前时间；体检用它判新鲜度，夹具必须让它与假时钟一致。
    body = json.loads(path.read_text(encoding="utf-8"))
    body["finished_at"] = when.isoformat(timespec="microseconds")
    path.write_text(json.dumps(body, ensure_ascii=False), encoding="utf-8")


def make_raw_days(store: DatasetStore, days: list[date]) -> None:
    for day in days:
        directory = store.raw_dir / f"dt={day.isoformat()}"
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "part-1.json.gz").write_bytes(b"x")


def put_current(store: DatasetStore, seen: date, rows: int = 3) -> None:
    """造一张最小的 current：体检只看 `last_seen_at` 的行龄，其余列留空即可。"""
    data: dict[str, list[Any]] = {name: [None] * rows for name in CURRENT_SCHEMA}
    data["id"] = [f"acme/ds-{index}" for index in range(rows)]
    data["last_seen_at"] = [seen] * rows
    store.write("current", pl.DataFrame(data, schema=CURRENT_SCHEMA))


def put_current_shas(store: DatasetStore, seen: date, shas: Mapping[str, str | None]) -> None:
    """带 sha 的 current；卡片凭证检查要拿它与 `carddata.source_sha` 对照。"""
    data: dict[str, list[Any]] = {name: [None] * len(shas) for name in CURRENT_SCHEMA}
    data["id"] = list(shas)
    data["sha"] = list(shas.values())
    data["last_seen_at"] = [seen] * len(shas)
    store.write("current", pl.DataFrame(data, schema=CURRENT_SCHEMA))


def put_cards(store: DatasetStore, day: date, source_shas: Mapping[str, str | None]) -> None:
    """只造 id / source_sha / snapshot_date 三列，凭证检查只读它们。"""
    data: dict[str, list[Any]] = {name: [None] * len(source_shas) for name in ENRICH_SCHEMA}
    data["id"] = list(source_shas)
    data["source_sha"] = list(source_shas.values())
    data["snapshot_date"] = [day] * len(source_shas)
    store.write("carddata", pl.DataFrame(data, schema=ENRICH_SCHEMA))


def put_readme(store: DatasetStore, day: date, shas: Mapping[str, str | None]) -> None:
    """只造体检要读的四列：id / source_sha / status / snapshot_date。

    正文（`text`）留空：这一项判的是「多久没刷新」，不读内容；真读 2.8 GB 也不该
    发生在体检里。
    """
    data: dict[str, list[Any]] = {name: [None] * len(shas) for name in README_SCHEMA}
    data["id"] = list(shas)
    data["source_sha"] = list(shas.values())
    data["snapshot_date"] = [day] * len(shas)
    data["status"] = ["ok"] * len(shas)
    data["bytes"] = [0] * len(shas)
    store.write("readme", pl.DataFrame(data, schema=README_SCHEMA))


def put_ids(store: DatasetStore, name: str, day: date, ids: list[str]) -> None:
    """只写 id 列的分区。

    勾稽检查只扫 id（`silver` 两百多万行，读全列不值当），夹具照同一口径造：
    走 `store.write` 会按 schema 补齐全部列，反而把测试写成「造一张完整表」。
    """
    target = store.path(name, day)
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": ids}).write_parquet(target)


def put_export(store: DatasetStore, ids: list[str], *, empty: tuple[str, ...] = ()) -> None:
    """造一份符合默认交付契约的宽表导出；体检比对 id 差集、列集与「整列为空」。

    落点取 `delivery.export_path` 而不是自己拼 `root/exports`：夹具若自成一套，
    「体检与导出写的是同一个地方」这条就测不出来了——两边各错一半也会通过。

    各列按契约类型族填占位值，而不是一律 None：`contract_coverage` 把「整列为空」
    当故障信号，全 None 的夹具会把「夹具偷懒」报成「这一列没接上」。要测整列为空
    时用 `empty` 点名，一次只让该空的列空着。
    """
    from hf_data_crawl.delivery.fields import COLUMN_TYPES, DEFAULT_COLUMNS
    from hf_data_crawl.delivery.wide import export_path

    def placeholder(column: str) -> Any:
        family = COLUMN_TYPES.get(column)
        if family == "list":
            return ["x"]
        if family == "number":
            return 1
        if family == "time":
            return date(2026, 1, 1)
        return "x"

    target = export_path(store)
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {
            column: ids
            if column == "id"
            else ([None] * len(ids) if column in empty else [placeholder(column)] * len(ids))
            for column in DEFAULT_COLUMNS
        }
    ).write_parquet(target)


def put_artifact(store: DatasetStore, name: str, ids: list[str], day: date) -> None:
    """造一张富化表（单文件，按 id 覆盖）；体检只读 id 与 snapshot_date。"""
    target = store.path(name)
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": ids, "snapshot_date": [day] * len(ids)}).write_parquet(target)


def healthy(store: DatasetStore) -> None:
    """近 7 天是热区 Raw；更早的日期已由 compact 压进冷区基线并登记。

    逐日补齐派生凭证与两张流水分区：体检现在会勾稽 current / observations /
    silver，也会比对派生戳记。夹具若不造这些，「一切正常」就名不副实——
    它测的是「什么都没做」也不会报，而不是「做过一轮之后」不会报。
    """
    make_run(store, "collect", NOW - timedelta(hours=9), "watermark")
    # 对账改为每日，夹具也按日：留一天半余量，隔一天就该告警。
    make_run(store, "reconcile", NOW - timedelta(hours=9), "end")
    put_current(store, NOW.date())
    ids = [f"acme/ds-{index}" for index in range(3)]
    dates = [NOW.date() - timedelta(days=n) for n in range(1, 14)]
    cutoff = NOW.date() - timedelta(days=REPLAY_WINDOW_DAYS)
    hot = [day for day in dates if day >= cutoff]
    cold = [day for day in dates if day < cutoff]
    make_raw_days(store, hot)
    for day in hot:
        put_ids(store, "observations", day, ids)
        put_ids(store, "silver", day, ids)
        lineage.stamp(store, day, bronze_rows=0, silver_rows=0, observations=len(ids))
    put_export(store, ids)
    # README 是「抓过、且有人管」的常态：2026-11-19 起进 hf-refresh 的并行段
    # （`fetch-readme --changed-all`），这里造一轮已刷新的记录。
    put_readme(store, NOW.date(), {ident: None for ident in ids})
    make_run(store, "fetch-readme", NOW - timedelta(hours=20), "end")
    manifest = manifest_path(store)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        json.dumps({"compressed_dates": [day.isoformat() for day in cold]}),
        encoding="utf-8",
    )


def problems(store: DatasetStore) -> list[str]:
    return [item.text for item in diagnose(store, NOW) if item.problem]


def test_冷区分区未压缩要报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    old = NOW.date() - timedelta(days=40)
    make_raw_days(store, [old])

    found = [text for text in problems(store) if "冷区未压缩" in text]

    assert len(found) == 1 and old.isoformat() in found[0]


def test_冷区已登记不会误报断日(tmp_path: Path) -> None:
    """部分压缩现场：raw 只剩首尾两天，中间日期已压进冷区基线。

    若 `doctor` 不把 manifest 里的日期算作「有观测」，残留的早分区会把
    检查窗口拉长到 14 天，中间那些已压缩的日子就被误判成断日。
    """
    store = DatasetStore(tmp_path / "data")
    today = NOW.date()
    make_raw_days(store, [today - timedelta(days=1), today - timedelta(days=13)])
    cold = [today - timedelta(days=n) for n in range(2, 13)]
    manifest = manifest_path(store)
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        json.dumps({"compressed_dates": [day.isoformat() for day in cold]}),
        encoding="utf-8",
    )

    assert not any("断日" in text for text in problems(store))


def test_冷区档案损坏时体检不崩(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    manifest_path(store).write_text("{ 坏档案", encoding="utf-8")

    assert isinstance(problems(store), list)


def test_一切正常时没有问题(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)

    assert problems(store) == []


def test_采集太久没成功要报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    for path in (store.root / "runs").glob("*collect*"):
        path.unlink()
    make_run(store, "collect", NOW - timedelta(hours=60), "watermark")

    assert any("日增量采集" in text and "没有成功完成" in text for text in problems(store))


def test_有界试跑不算完成(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    for path in (store.root / "runs").glob("*collect*"):
        path.unlink()
    make_run(store, "collect", NOW - timedelta(hours=1), "max_pages")

    assert any("日增量采集" in text for text in problems(store))


def test_从未对账要报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    for path in (store.root / "runs").glob("*reconcile*"):
        path.unlink()

    assert any("全量对账：从未成功完成过" in text for text in problems(store))


def test_底库行龄过旧要点破对账没跑成(tmp_path: Path) -> None:
    """行龄是唯一直接量到「库里各值是哪天的」的检查；只看任务成没成会漏掉。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    put_current(store, NOW.date() - timedelta(days=30))

    found = [text for text in problems(store) if "底库新鲜度" in text]

    assert found and "P95" in found[0] and "全量对账" in found[0]


def test_底库为空要报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    (store.root / "state" / "current.parquet").unlink()

    assert any("current 为空" in text for text in problems(store))


def test_Raw断日要点名日期(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    missing = NOW.date() - timedelta(days=5)
    (store.raw_dir / f"dt={missing.isoformat()}" / "part-1.json.gz").unlink()
    (store.raw_dir / f"dt={missing.isoformat()}").rmdir()

    assert any("Raw 断日" in text and missing.isoformat() in text for text in problems(store))


def test_Raw分量骤降要点名日期(tmp_path: Path) -> None:
    """「采了一半」与「整天没采」是两件事：目录在、分块在，断日判据看不见。

    判据用分块数而不是观测行数——后者只追加指标真变过的行，上游平静的日子
    行数本来就少（11-14 只有 2,188 行而 Raw 完整），拿它判会天天误报。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    # healthy 每天只有 1 块；补到 5 块，只留下一天是 1 块。
    short_day = NOW.date() - timedelta(days=3)
    for offset in range(1, 7):
        day = NOW.date() - timedelta(days=offset)
        if day == short_day:
            continue
        directory = store.raw_dir / f"dt={day.isoformat()}"
        for index in range(2, 6):
            (directory / f"part-{index}.json.gz").write_bytes(b"x")

    found = [text for text in problems(store) if "Raw 分量" in text]

    assert len(found) == 1 and short_day.isoformat() in found[0]


def test_Raw分量样本不足四天时不判(tmp_path: Path) -> None:
    """只有一两天分块时，所谓「中位」就是它自己，拿它判会自说自话。

    留下的三天刻意做成 1 / 5 / 5 块：若去掉「样本不足四天」这道闸，第一天会被
    判成「明显少于中位」，本用例才会红。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    days = sorted(path for path in store.raw_dir.glob("dt=*") if any(path.glob("*.json.gz")))
    for index, path in enumerate(days[:3]):
        files = sorted(path.glob("*.json.gz"))
        target = 1 if index == 0 else 5
        for extra in files[target:]:
            extra.unlink()
        for number in range(len(files), target):
            (path / f"part-x{number}.json.gz").write_bytes(b"x")
    for path in days[3:]:
        for item in path.glob("*.json.gz"):
            item.unlink()
        path.rmdir()

    assert not any("明显少于" in text for text in problems(store))


def test_残留临时文件超过一小时才算(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    fresh = store.root / "state" / "fresh.parquet.tmp"
    stale = store.root / "state" / "stale.parquet.tmp"
    fresh.parent.mkdir(parents=True, exist_ok=True)
    fresh.write_bytes(b"x")
    stale.write_bytes(b"x")
    old = (NOW - timedelta(hours=3)).timestamp()
    os.utime(stale, (old, old))
    os.utime(fresh, (NOW.timestamp(), NOW.timestamp()))

    found = [text for text in problems(store) if "残留临时文件" in text]

    assert len(found) == 1 and "1 个" in found[0]


def test_近期失败运行要报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    write_run(store, "build", NOW - timedelta(days=1), Outcome({}, code=1, reason="LockBusy: x"))

    assert any("失败运行" in text and "LockBusy" in text for text in problems(store))


def test_失败后同一命令又成功了就不再报(tmp_path: Path) -> None:
    """断网后重跑成功，问题已解决；再报会让告警文件长期有效而失去信号价值。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    write_run(store, "build", NOW - timedelta(days=2), Outcome({}, code=1, reason="boom"))
    make_run(store, "build", NOW - timedelta(days=1), "end")

    assert not any("失败运行" in text for text in problems(store))


def test_单次交付被拦不算故障但批量交付失败要报(tmp_path: Path) -> None:
    """`request run` 是人当场发起的：闸门拒绝、没有新信息都会走到「失败」，人看着终端。

    把它算进七天红点，一次正常的拒绝就能让告警亮一周；而 `run-due` 是没人盯着的
    定时路径，坏了一定要报。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    write_run(
        store,
        "request-run",
        NOW - timedelta(days=1),
        Outcome({}, code=1, reason="代码有未提交改动"),
    )

    assert not any("失败运行" in text for text in problems(store))

    write_run(
        store,
        "request-run-due",
        NOW - timedelta(hours=2),
        Outcome({}, code=1, reason="有需求交付失败"),
    )

    assert any("失败运行" in text for text in problems(store))


def test_预览被限流停车不算故障但真崩了要报(tmp_path: Path) -> None:
    """`fetch-preview` 撞 429 是设计内的主动停车。

    `preview.py` 明确规定撞上 429 就整轮中止——CloudFront 按 IP 限流，各线程各自
    退避只会把封禁拖长（实测吞吐掉到 0.5 req/s）。下次重跑会接着抓，所以「主动
    刹车」不该点亮七天红点。但 reason 不含「被限流」时（真的崩了）照样要报。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    write_run(
        store,
        "fetch-preview",
        NOW - timedelta(hours=3),
        Outcome({}, code=1, reason="被限流（429）中止：已并入 1 行，其余未抓"),
    )

    assert not any("失败运行" in text for text in problems(store))

    write_run(
        store,
        "fetch-preview",
        NOW - timedelta(hours=2),
        Outcome({}, code=1, reason="KeyError: 'splits'"),
    )

    assert any("失败运行" in text for text in problems(store))


def test_别的命令成功不能覆盖这条命令的失败(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    write_run(store, "build", NOW - timedelta(days=2), Outcome({}, code=1, reason="boom"))
    make_run(store, "collect", NOW - timedelta(days=1), "watermark")

    assert any("失败运行" in text and "boom" in text for text in problems(store))


def test_失败原因为空时回退到payload里的error(tmp_path: Path) -> None:
    """旧版本只把原因写在 payload.error，reason 是 null，告警里不能显示 None。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    write_run(
        store,
        "reconcile",
        NOW - timedelta(hours=1),
        Outcome({"error": "ConnectError: eof"}, code=1, reason=None),
    )

    found = [text for text in problems(store) if "失败运行" in text]

    assert found and "ConnectError: eof" in found[0] and "None" not in found[0]


def test_告警文件有问题时写恢复后删(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    sync_alert(store, diagnose(store, NOW), NOW)
    assert (store.root / ALERT_NAME).exists()

    healthy(store)
    sync_alert(store, diagnose(store, NOW), NOW)

    assert not (store.root / ALERT_NAME).exists()


def test_doctor命令有问题时退出码为1且写告警(tmp_path: Path) -> None:
    root = tmp_path / "data"

    code = main(["--data-dir", str(root), "doctor"])

    assert code == 1
    assert (root / ALERT_NAME).exists()


# ---------------------------------------------------------------- 补跑去重


class OnePage:
    def __init__(self) -> None:
        self.calls = 0

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls += 1
        body = [
            {
                "id": "acme/ds-1",
                "author": "acme",
                "sha": "s",
                "lastModified": "2026-11-01T00:00:00.000Z",
                "createdAt": "2025-01-01T00:00:00.000Z",
                "downloads": 1,
                "downloadsAllTime": 2,
                "likes": 0,
                "tags": [],
            }
        ]
        return HfResponse(status=200, headers={}, body=json.dumps(body).encode())


def test_skip_if_done当天已完整采集就不再联网(tmp_path: Path) -> None:
    root = tmp_path / "data"
    transport = OnePage()
    argv = ["--data-dir", str(root), "collect", "--date", "2026-11-04", "--skip-if-done"]

    assert main(argv, transport=transport) == 0
    first = transport.calls
    assert first > 0

    assert main(argv, transport=transport) == 0
    assert transport.calls == first


def test_不带skip_if_done时照常再采一次(tmp_path: Path) -> None:
    root = tmp_path / "data"
    transport = OnePage()
    argv = ["--data-dir", str(root), "collect", "--date", "2026-11-04"]

    main(argv, transport=transport)
    first = transport.calls
    main(argv, transport=transport)

    assert transport.calls > first


def test_skip_if_done只认同一快照日(tmp_path: Path) -> None:
    root = tmp_path / "data"
    transport = OnePage()
    main(
        ["--data-dir", str(root), "collect", "--date", "2026-11-03", "--skip-if-done"],
        transport=transport,
    )
    first = transport.calls

    main(
        ["--data-dir", str(root), "collect", "--date", "2026-11-04", "--skip-if-done"],
        transport=transport,
    )

    assert transport.calls > first


# ---------------------------------------------------------------- Raw 治理


def test_Raw规模超阈值要报(tmp_path: Path, monkeypatch: Any) -> None:
    import hf_data_crawl.ops.doctor as doctor

    store = DatasetStore(tmp_path / "data")
    healthy(store)
    monkeypatch.setattr(doctor, "RAW_MAX_FILES", 2)
    monkeypatch.setattr(doctor, "RAW_MAX_BYTES", 10)

    assert any("Raw 规模超阈值" in text for text in problems(store))


def test_超出保留窗口仍是原始分块要报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    old = NOW.date() - timedelta(days=40)
    directory = store.raw_dir / f"dt={old.isoformat()}"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "part-1.json.gz").write_bytes(b"x")

    found = [text for text in problems(store) if "Raw 归档积压" in text]

    assert len(found) == 1 and old.isoformat() in found[0]


def test_已归档分片不算积压(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    old = NOW.date() - timedelta(days=40)
    directory = store.raw_dir / f"dt={old.isoformat()}"
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "part-archive-000001-abcd1234.json.gz").write_bytes(b"x")

    assert not any("Raw 归档积压" in text for text in problems(store))


def test_backup超期未清理要报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    stale = store.root / "backup" / "raw" / "dt=2026-09-01"
    stale.mkdir(parents=True, exist_ok=True)
    when = (NOW - timedelta(days=10)).timestamp()
    os.utime(stale, (when, when))

    assert any("backup/raw" in text and "未清理" in text for text in problems(store))


def test_backup刚归档不报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    fresh = store.root / "backup" / "raw" / "dt=2026-11-09"
    fresh.mkdir(parents=True, exist_ok=True)
    os.utime(fresh, (NOW.timestamp(), NOW.timestamp()))

    assert not any("backup/raw" in text for text in problems(store))


def test_派生滞后要点名日期并给出命令(tmp_path: Path) -> None:
    """Raw 被重写后派生不会自己更新——这是本项目此前唯一无法被观测的坏状态。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    day = NOW.date() - timedelta(days=2)
    (store.raw_dir / f"dt={day.isoformat()}" / "part-2.json.gz").write_bytes(b"y")

    found = [text for text in problems(store) if "派生滞后" in text]

    assert len(found) == 1
    assert day.isoformat() in found[0]
    assert "build --stale" in found[0]


def test_派生无凭证也要报(tmp_path: Path) -> None:
    """没有戳记与戳记过期在执行上是同一件事：都得重派一次才有据可查。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    lineage_path(store).unlink()

    assert any("派生无凭证" in text for text in problems(store))


def test_层间勾稽发现当前态缺观测来源(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    for target in store.root.glob("state/observations/dt=*/obs.parquet"):
        pl.DataFrame({"id": ["acme/ds-0", "acme/ds-1"]}).write_parquet(target)

    found = [text for text in problems(store) if "层间勾稽" in text]

    assert found and "没有观测来源" in found[0]


def test_层间勾稽发现当前态落后于Raw(tmp_path: Path) -> None:
    """采进来了却没有任何一行进入当前态，典型成因是派生跑的是别的日期。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    put_current(store, NOW.date() - timedelta(days=3))

    found = [text for text in problems(store) if "层间勾稽" in text]

    assert found and "当前态最新只到" in found[0]


def test_宽表导出落后于底库要报(tmp_path: Path) -> None:
    """导出物只在跑 export-wide 时刷新。

    2026-11-08 实测：对账 04:11 把底库刷到 1,123,235 行，导出仍是前夜 02:33 的
    1,123,090 行，差 145 行且没有任何提示——拿它做分析会得出错结论。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    put_export(store, ["acme/ds-0", "acme/ds-1"])

    found = [text for text in problems(store) if "产物新鲜度" in text]

    assert found and "缺 1 行" in found[0] and "export-wide" in found[0]


def test_宽表导出缺失要报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    (store.root / "exports" / "datasets.parquet").unlink()

    assert any("导出不存在" in text for text in problems(store))


def test_导出物整列为空要报(tmp_path: Path) -> None:
    """列名、行数、类型都对，仍可能整列没有值。

    2026-11-04 `private` 整列静默变空是同一个形态，发生在 bronze；导出物每天被覆盖、
    不报错、查询照常返回，是它最容易藏住的地方。
    """
    store = DatasetStore(tmp_path / "data")
    ids = [f"acme/ds-{index}" for index in range(3)]
    put_current(store, NOW.date(), rows=3)
    put_export(store, ids, empty=("downloads",))

    finding = next(item for item in diagnose(store, NOW) if item.text.startswith("交付面取值"))

    assert finding.problem
    assert "downloads" in finding.text


def test_来源表未采集时整列空只提示不报红(tmp_path: Path) -> None:
    """`size` 没抓时四个 `server_*` 必然全空，这是计划内，不是故障。

    判红的话从装好到抓完 size 之间天天红，告警很快就没人看了——与「产物新鲜度」
    只把未生成的富化表记成提示是同一条理由。
    """
    store = DatasetStore(tmp_path / "data")
    ids = [f"acme/ds-{index}" for index in range(3)]
    put_current(store, NOW.date(), rows=3)
    put_export(
        store,
        ids,
        empty=(
            "server_num_rows",
            "server_num_columns",
            "server_config_count",
            "server_num_bytes_parquet",
        ),
    )

    finding = next(item for item in diagnose(store, NOW) if item.text.startswith("交付面取值"))

    assert not finding.problem
    assert "size 未采集" in finding.text


def test_来源表有数据而导出仍全空要报(tmp_path: Path) -> None:
    """同样是整列为空，来源表有没有数据决定它是「还没抓」还是「没接上」。"""
    store = DatasetStore(tmp_path / "data")
    ids = [f"acme/ds-{index}" for index in range(3)]
    put_current(store, NOW.date(), rows=3)
    put_export(store, ids, empty=("server_num_rows",))
    data: dict[str, list[Any]] = {name: [None] for name in SIZE_SCHEMA}
    data["id"] = [ids[0]]
    store.write("size", pl.DataFrame(data, schema=SIZE_SCHEMA))

    finding = next(item for item in diagnose(store, NOW) if item.text.startswith("交付面取值"))

    assert finding.problem
    assert "有数据却全空" in finding.text


def test_宽表导出列集不符契约要报(tmp_path: Path) -> None:
    """文件被手改回旧版全列时，体检要发现「默认交付口径与底库漂移」。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    target = store.root / "exports" / "datasets.parquet"
    pl.DataFrame({"id": ["acme/ds-0", "acme/ds-1", "acme/ds-2"]}).write_parquet(target)

    assert any("列集与默认交付契约不一致" in text for text in problems(store))


def test_富化表停更超过七天要报(tmp_path: Path) -> None:
    """覆盖不全属设计（按下载量取头部），整张表停更才是链路断了。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    put_artifact(store, "carddata", ["acme/ds-0"], NOW.date() - timedelta(days=30))

    found = [text for text in problems(store) if "产物新鲜度" in text]

    assert found and "30 天没更新" in found[0]


def test_富化表覆盖不全不算问题(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    put_artifact(store, "carddata", ["acme/ds-0"], NOW.date() - timedelta(days=1))

    assert not any("产物新鲜度" in text for text in problems(store))


def test_卡片凭证发现活跃库整趟未抓(tmp_path: Path) -> None:
    """活跃库是日增量真正扫过的那批，也正是卡片趟该覆盖的那批。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    shas = {f"acme/ds-{index}": f"new-{index}" for index in range(10)}
    put_current_shas(store, NOW.date(), shas)
    put_cards(store, NOW.date(), {key: f"old-{index}" for index, key in enumerate(shas)})

    found = [text for text in problems(store) if "卡片凭证" in text]

    assert found and "10/10" in found[0] and "collect --cards" in found[0]


def test_卡片凭证全部对齐时正常(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    shas = {f"acme/ds-{index}": f"same-{index}" for index in range(4)}
    put_current_shas(store, NOW.date(), shas)
    put_cards(store, NOW.date(), shas)

    found = [text for text in problems(store) if "卡片凭证" in text]

    assert not found  # 一致时不成问题项


def test_无凭证单独计数而非算作落后(tmp_path: Path) -> None:
    """加列之前的行 source_sha 为空——那是「没记账」，不是「记错了」。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    put_current_shas(store, NOW.date(), {"acme/ds-0": "x"})
    put_cards(store, NOW.date(), {"acme/ds-0": None})

    found = [text for text in problems(store) if "卡片凭证" in text]

    assert found and "全无 source_sha" in found[0]


def test_长尾库不计入卡片凭证的落后比例(tmp_path: Path) -> None:
    """几百万年不动的库，卡片本就该保持原样，算进来比例永远难看。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    data: dict[str, list[Any]] = {name: [None, None] for name in CURRENT_SCHEMA}
    data["id"] = ["acme/fresh", "acme/stale"]
    data["sha"] = ["a", "b"]
    data["last_seen_at"] = [NOW.date(), NOW.date() - timedelta(days=400)]
    store.write("current", pl.DataFrame(data, schema=CURRENT_SCHEMA))
    put_cards(store, NOW.date(), {"acme/fresh": "a", "acme/stale": "zzz"})

    found = [text for text in problems(store) if "卡片凭证" in text]

    # acme/stale 不在活跃集里，它的「对不上」不参与比例计算。
    assert not found


# ---------------------------------------------------------------- 快照锁


def test_非快照模式降级跨表检查并报告锁状态(tmp_path: Path) -> None:
    """拿不到锁时跨表结论只作提示；快照模式下不重复抢锁。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    # 只破坏跨表的层间勾稽：current 多出 observations / silver 没有的 id。
    put_current(store, NOW.date(), rows=5)

    strict = diagnose(store, NOW)
    assert any(item.problem and "层间勾稽" in item.text for item in strict)
    assert not any("写锁" in item.text for item in strict)

    loose = diagnose(store, NOW, snapshot=False)
    downgraded = [item for item in loose if "层间勾稽" in item.text]
    # 降级不抹判断：`problem` 仍为 True，只是这次读数不算数。抹成 False 会让
    # 「本来是红、因降级变黄」这件事从数据里消失，只剩字符串开头一个前缀可查。
    assert downgraded and all(item.problem and item.degraded for item in downgraded)
    assert all(item.text.startswith("[非快照]") for item in downgraded)
    assert any("写锁" in item.text for item in loose)


def test_非快照告警首行标注(tmp_path: Path) -> None:
    """非快照的告警文件要自证口径，否则会被当成一次完整体检的结论。"""
    store = DatasetStore(tmp_path / "data")

    sync_alert(store, [Finding(True, "Raw 断日：2026-11-06")], NOW, snapshot=False)

    first = (store.root / ALERT_NAME).read_text(encoding="utf-8").splitlines()[0]
    assert first.startswith("[非快照]")


def test_doctor拿不到锁时降级为非快照(tmp_path: Path, monkeypatch: Any) -> None:
    """锁被占用不阻塞排障：照常体检，跨表问题不参与退出码与告警文件。"""
    root = tmp_path / "data"

    def busy(*_args: Any, **_kwargs: Any) -> Any:
        raise LockBusy("数据目录正被另一个写命令占用：collect")

    def fake_diagnose(*_args: Any, snapshot: bool = True, **_kwargs: Any) -> list[Finding]:
        return [
            Finding(
                snapshot,
                "层间勾稽：x" if snapshot else "[非快照] 层间勾稽：x",
                snapshot_dependent=True,
            )
        ]

    monkeypatch.setattr("hf_data_crawl.ops.commands.exclusive", busy)
    monkeypatch.setattr("hf_data_crawl.ops.commands.diagnose", fake_diagnose)

    code = main(["--data-dir", str(root), "doctor", "--lock-wait", "0"])

    assert code == 0
    assert not (root / ALERT_NAME).exists()
    assert read_health(DatasetStore(root))[-1].snapshot is False


def test_已交付需求缺过程记录要在体检里报出来(tmp_path: Path) -> None:
    """方法没留在目录里，那份数据就永远说不清来历；体检只判已交付的，不催未交付的。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    root = tmp_path / "req"
    delivered = root / "REQ-0001-query-高热度"
    (delivered / "deliveries" / "20260101T000000").mkdir(parents=True)
    (delivered / "spec.yaml").write_text("id: REQ-0001\n", encoding="utf-8")
    # 还没交过的需求不报：方法正在写，天天红只会训练人忽略告警。
    pending = root / "REQ-0002-query-待办"
    pending.mkdir(parents=True)
    (pending / "spec.yaml").write_text("id: REQ-0002\n", encoding="utf-8")

    texts = [item.text for item in diagnose(store, NOW, requests_root=root) if item.problem]

    hits = [text for text in texts if "需求自包含" in text]
    assert len(hits) == 1
    assert "REQ-0001-query-高热度" in hits[0]
    assert "REQ-0002" not in hits[0]


def test_口径改了没升版本要在体检里报出来(tmp_path: Path) -> None:
    """档案指着 v1、说的却不是 v1 当初那件事——这种错不报错，只能靠体检盯。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    root = tmp_path / "req"
    folder = root / "REQ-0001-query-高热度"
    (folder / "deliveries" / "20260101T000000").mkdir(parents=True)
    (folder / "history").mkdir()
    old = """\
id: REQ-0001
title: 高热度
requester: 大帅
version: 1
criteria:
  - {metric: 下载量, op: '>', value: 1000}
"""
    (folder / "history" / "v1.yaml").write_text(old, encoding="utf-8")
    (folder / "spec.yaml").write_text(old.replace("1000", "800"), encoding="utf-8")

    texts = [item.text for item in diagnose(store, NOW, requests_root=root) if item.problem]

    hits = [text for text in texts if "口径版本" in text]
    assert len(hits) == 1
    assert "版本号没跟着升" in hits[0]


def test_历史快照被删要在体检里报出来(tmp_path: Path) -> None:
    """`version_conflict` 只认 `history/vN.yaml`：它被删掉，版本号闸门对那一版就失效了。

    删一个文件不会报任何错，档案照样长得完整——只有体检能发现这条自证链断了。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    root = tmp_path / "req"
    folder = root / "REQ-0001-query-高热度"
    delivery = folder / "deliveries" / "20260101T000000"
    delivery.mkdir(parents=True)
    (folder / "spec.yaml").write_text(
        "id: REQ-0001\ntitle: 高热度\nrequester: 大帅\nversion: 1\n"
        "criteria:\n  - {metric: 下载量, op: '>', value: 1000}\n",
        encoding="utf-8",
    )
    (delivery / "manifest.json").write_text(
        json.dumps({"spec_version": 1, "result": {"files": []}}), encoding="utf-8"
    )

    texts = [item.text for item in diagnose(store, NOW, requests_root=root) if item.problem]

    hits = [text for text in texts if "交付锚点" in text]
    assert len(hits) == 1
    assert "历史快照不见了" in hits[0]


def test_输入快照被改过要在体检里报出来(tmp_path: Path) -> None:
    """`input/` 只读是规矩，不是保证：改过之后产物就对不上档案里那行 sha256。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    root = tmp_path / "req"
    folder = root / "REQ-0001-backfill-清单"
    delivery = folder / "deliveries" / "20260101T000000"
    (folder / "input").mkdir(parents=True)
    delivery.mkdir(parents=True)
    (folder / "history").mkdir()
    (folder / "spec.yaml").write_text(
        "id: REQ-0001\ntitle: 清单\nrequester: 大帅\nversion: 1\n", encoding="utf-8"
    )
    (folder / "history" / "v1.yaml").write_text(
        (folder / "spec.yaml").read_text(encoding="utf-8"), encoding="utf-8"
    )
    snapshot = folder / "input" / "清单.xlsx"
    snapshot.write_bytes(b"original")
    digest = hashlib.sha256(b"original").hexdigest()
    (delivery / "manifest.json").write_text(
        json.dumps(
            {
                "spec_version": 1,
                "result": {"files": []},
                "fill": {"input": {"path": "input/清单.xlsx", "sha256": digest}},
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    snapshot.write_bytes(b"tampered")

    texts = [item.text for item in diagnose(store, NOW, requests_root=root) if item.problem]

    hits = [text for text in texts if "交付锚点" in text]
    assert len(hits) == 1
    assert "与档案指纹不符" in hits[0]


def _seed_delivery(root: Path, name: str, *, sha: str = "s1") -> Path:
    """造一条最小交付：目录 + manifest + 一个产物文件。"""
    folder = root / "REQ-0001-backfill-清单"
    directory = folder / "deliveries" / name
    directory.mkdir(parents=True, exist_ok=True)
    (folder / "spec.yaml").write_text(
        "id: REQ-0001\ntitle: 清单\nrequester: 大帅\nversion: 1\n", encoding="utf-8"
    )
    (directory / "manifest.json").write_text(
        json.dumps(
            {
                "spec_version": 1,
                "result": {
                    "rows": 1,
                    "columns": ["id"],
                    "files": [{"name": "d.xlsx", "sha256": sha}],
                },
            }
        ),
        encoding="utf-8",
    )
    (directory / "d.xlsx").write_bytes(b"payload")
    return directory


def test_说清理了却还在要报(tmp_path: Path) -> None:
    """清理中断或手工删了一半，账实就不符了；重跑 `request prune` 能修，但得先看见。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    root = tmp_path / "req"
    older = _seed_delivery(root, "20261101T000000")
    _seed_delivery(root, "20261102T000000")
    (older.parent / "PRUNED.json").write_text(
        json.dumps(
            {
                "pruned": {
                    "20261101T000000": {
                        "pruned_at": "2026-11-17T00:00:00+08:00",
                        "files": [{"name": "d.xlsx", "bytes": 7, "sha256": "s1"}],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    texts = [item.text for item in diagnose(store, NOW, requests_root=root) if item.problem]

    hits = [text for text in texts if "产物留存" in text]
    assert len(hits) == 1
    assert "说清理了却还在" in hits[0]


def test_当前有效那条被清掉要报(tmp_path: Path) -> None:
    """INDEX 指着「当前有效」，产物却按留存策略没了——人一打开就是空目录。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    root = tmp_path / "req"
    current = _seed_delivery(root, "20261102T000000")
    (current / "d.xlsx").unlink()
    (current.parent / "PRUNED.json").write_text(
        json.dumps(
            {
                "pruned": {
                    "20261102T000000": {
                        "pruned_at": "2026-11-17T00:00:00+08:00",
                        "files": [{"name": "d.xlsx", "bytes": 7, "sha256": "s1"}],
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    texts = [item.text for item in diagnose(store, NOW, requests_root=root) if item.problem]

    hits = [text for text in texts if "产物留存" in text]
    assert len(hits) == 1
    assert "产物被清理了" in hits[0]


def test_产物没留存不算故障(tmp_path: Path) -> None:
    """产物是可重算的副本：早期交付用手工删过，重算即可，不该让体检长期发红。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    root = tmp_path / "req"
    directory = _seed_delivery(root, "20261101T000000")
    (directory / "d.xlsx").unlink()

    findings = [item for item in diagnose(store, NOW, requests_root=root)]
    hits = [item for item in findings if "产物留存" in item.text]

    assert len(hits) == 1
    assert hits[0].problem is False
    assert "未留存" in hits[0].text


def test_探测成片失败要报出来(tmp_path: Path) -> None:
    """探测失败落成 unknown，处置是「先别动」——状态原地不动，界面上看不出异常。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    make_run(
        store,
        "reconcile",
        NOW,
        probe_probed=1000,
        probe_failed=400,
        probe_deferred=0,
    )

    texts = [item.text for item in diagnose(store, NOW) if item.problem]

    hits = [text for text in texts if "探测复核" in text]
    assert len(hits) == 1
    assert "失败 400" in hits[0] and "40%" in hits[0]


def test_探测少量失败只记不报_顺延要露出来(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    make_run(
        store,
        "reconcile",
        NOW,
        probe_probed=2000,
        probe_failed=10,
        probe_deferred=4248,
    )

    finding = next(item for item in diagnose(store, NOW) if "探测复核" in item.text)

    assert finding.problem is False
    assert "失败 10" in finding.text
    assert "顺延 4,248" in finding.text


def test_旧记录没有探测计数时不据此下结论(tmp_path: Path) -> None:
    """升级前的对账记录没有 probe_probed；拿它当「探测 0 个、失败 0 个」会误判成健康。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)

    finding = next(item for item in diagnose(store, NOW) if "探测复核" in item.text)

    assert finding.problem is False
    assert "尚无带计数" in finding.text


def drop_raw_day(store: DatasetStore, day: date) -> None:
    """删掉某天的 Raw 分区，制造断日。"""
    folder = store.raw_dir / f"dt={day.isoformat()}"
    (folder / "part-1.json.gz").unlink()
    folder.rmdir()


def test_已知断日只印不判_不再天天红(tmp_path: Path) -> None:
    """不补的缺口若天天判红，ALERT.txt 会长期挂同一条，告警就废了。"""
    from hf_data_crawl.ops.doctor import KNOWN_RAW_GAPS

    store = DatasetStore(tmp_path / "data")
    healthy(store)
    known = next(day for day in KNOWN_RAW_GAPS if day < NOW.date())
    drop_raw_day(store, known)

    finding = next(item for item in diagnose(store, NOW) if "断日" in item.text)

    assert finding.problem is False
    assert "已知缺口" in finding.text
    assert known.isoformat() in finding.text


def test_已知缺口之外的新断日照常判红(tmp_path: Path) -> None:
    """白名单只赦免登记过的日子，新掉的必须照报——否则它就是一张免罪符。"""
    from hf_data_crawl.ops.doctor import KNOWN_RAW_GAPS

    store = DatasetStore(tmp_path / "data")
    healthy(store)
    known = next(day for day in KNOWN_RAW_GAPS if day < NOW.date())
    fresh = known - timedelta(days=1)
    drop_raw_day(store, known)
    drop_raw_day(store, fresh)

    texts = [item.text for item in diagnose(store, NOW) if item.problem]

    hits = [text for text in texts if "断日" in text]
    assert len(hits) == 1
    assert fresh.isoformat() in hits[0]
    assert known.isoformat() not in hits[0]


# ---------------------------------------------------------------- 字段级新鲜度


def test_字段新鲜度以最近成功对账日为基准而非日历日(tmp_path: Path) -> None:
    """链条 03:10 才跑：凌晨 01:40 查一次，不能因为「今天还没扫」就判全红。

    基准取最近一次成功对账的快照日（这里是 11-09），不是 11-10 这个日历日。
    四类字段各报各的数：规模「有行」与「有值」必须分开。
    """
    field_now = datetime(2026, 11, 10, 1, 40)
    field_sweep = datetime(2026, 11, 9, 23, 50)
    store = DatasetStore(tmp_path / "data")
    put_current(store, field_sweep.date(), rows=2)
    sizes: dict[str, list[Any]] = {name: [None, None] for name in SIZE_SCHEMA}
    sizes["id"] = ["acme/ds-0", "acme/ds-1"]
    sizes["snapshot_date"] = [field_sweep.date()] * 2
    sizes["status"] = ["ok", "unavailable"]
    sizes["num_rows"] = [100, None]
    store.write("size", pl.DataFrame(sizes, schema=SIZE_SCHEMA))
    make_run(store, "reconcile", field_sweep)

    item = next(f for f in diagnose(store, field_now) if f.text.startswith("字段新鲜度"))

    assert not item.problem, item.text
    assert "列表字段最近一轮（2026-11-09）覆盖 100.0%" in item.text
    assert "规模 有行 100% / 有值 50%" in item.text


def test_全站扫描没扫完时字段新鲜度判红(tmp_path: Path) -> None:
    """列表字段占比是「这轮扫完没有」的直接证据，低于九成即不合格。

    库老了但仍是 active，说明它已连续多轮没被列表扫到——那正是扫描被截断或
    中途失败的样子，而不是「它本来就没变」。
    """
    field_now = datetime(2026, 11, 10, 1, 40)
    field_sweep = datetime(2026, 11, 9, 23, 50)
    store = DatasetStore(tmp_path / "data")
    put_current(store, field_sweep.date() - timedelta(days=2))
    make_run(store, "reconcile", field_sweep)

    item = next(f for f in diagnose(store, field_now) if f.text.startswith("字段新鲜度"))

    assert item.problem
    assert "没扫完" in item.text


# ---------------------------------------------------------------- 失败覆盖的时间口径


def ctx_of(runs: list[dict], root: Path, now: datetime = NOW) -> RunContext:
    """只造检查函数真正用到的那部分上下文，不落盘。

    直接调单项检查的测试用它，比 `diagnose` 快也更好定位：一条判据坏掉时，
    不必先被另外二十条搅一遍。
    """
    return RunContext(
        store=DatasetStore(root / "data"),
        now=now,
        runs=runs,
        current=pl.DataFrame(),
        current_ids=frozenset(),
    )


def test_后来成功覆盖失败按时间点而非字符串(tmp_path: Path) -> None:
    """成功 10:00:00Z 早于失败 10:00:00.5Z，不能算「已恢复」。

    旧实现把两条 ISO 字符串直接比字典序，`Z`(90) 大于 `.`(46)，于是更早的
    成功反而「大于」更晚的失败，失败被静默判为已覆盖——告警从此不再出现。
    """
    from hf_data_crawl.ops.doctor import _recent_failures

    runs = [
        {
            "command": "collect",
            "status": "failed",
            "started_at": "2026-11-10T10:00:00.500000Z",
            "reason": "断网",
        },
        {
            "command": "collect",
            "status": "success",
            "started_at": "2026-11-10T10:00:00Z",
        },
    ]

    finding = _recent_failures(ctx_of(runs, tmp_path))

    assert finding.problem
    assert "1 次未恢复" in finding.text


def test_带时区的时间戳不会让体检崩溃(tmp_path: Path) -> None:
    """runs/ 写的是本地 naive 时间；外部注入的 Z 后缀不能让减法抛 TypeError。"""
    from hf_data_crawl.ops.doctor import _recent_failures

    runs = [
        {"command": "collect", "status": "failed", "started_at": "2026-11-10T10:00:00Z"},
        {"command": "collect", "status": "success", "started_at": "2026-11-10T11:00:00Z"},
    ]

    finding = _recent_failures(ctx_of(runs, tmp_path))

    assert not finding.problem


def write_snapshot(store: DatasetStore, body: str) -> None:
    from hf_data_crawl.pipeline.rebuild import snapshot_path

    path = snapshot_path(store)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


def test_无残留状态快照时体检正常(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)

    finding = next(item for item in diagnose(store, NOW) if "状态快照" in item.text)

    assert finding.problem is False
    assert "无残留" in finding.text


def test_残留状态快照要报出来(tmp_path: Path) -> None:
    """快照只在重建进行中出现；留着会被下次重建当成「上次被打断」。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    write_snapshot(store, json.dumps({"acme/ds-1": "renamed"}))

    finding = next(item for item in diagnose(store, NOW) if "状态快照" in item.text)

    assert finding.problem
    assert "残留 1 个" in finding.text
    assert "先备份" in finding.text


def test_损坏的状态快照要报出静默回退风险(tmp_path: Path) -> None:
    """retain 把损坏归一为 None；不报，非在架状态丢了也没人知道。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    write_snapshot(store, "{半截")

    finding = next(item for item in diagnose(store, NOW) if "状态快照" in item.text)

    assert finding.problem
    assert "读不出来" in finding.text
    assert "静默回退" in finding.text
    assert "别直接删" in finding.text


def test_探测连续失败要单独报告(tmp_path: Path) -> None:
    """与单轮失败占比分开报：成片限流与个别顽固是两件事。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    make_run(store, "reconcile", NOW, probe_probed=1000, probe_failed=5, probe_degraded=2)

    finding = next(item for item in diagnose(store, NOW) if "探测顽固失败" in item.text)

    assert finding.problem
    assert "2 个库连续 3 轮" in finding.text
    assert "probe_degraded" in finding.text


def test_旧对账记录没有degraded字段不误报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    make_run(store, "reconcile", NOW, probe_probed=1000, probe_failed=400)

    finding = next(item for item in diagnose(store, NOW) if "探测顽固失败" in item.text)

    assert finding.problem is False


def test_告警文件走原子写(tmp_path: Path, monkeypatch: Any) -> None:
    """`path.write_text` 会先截断再写，两个 doctor 并发时读者可能看到半截告警。"""
    from hf_data_crawl.ops import doctor

    store = DatasetStore(tmp_path / "data")
    calls: list[Path] = []
    original = doctor.write_text

    def spy(path: Path, content: str) -> None:
        calls.append(path)
        original(path, content)

    monkeypatch.setattr(doctor, "write_text", spy)

    sync_alert(store, [Finding(True, "Raw 断日：2026-11-06")], NOW)

    assert calls == [store.root / ALERT_NAME]


def test_告警写失败不阻断体检流水(tmp_path: Path, monkeypatch: Any) -> None:
    """告警与流水是独立产物；前者写不下不该让后者也一行都不留。"""
    from hf_data_crawl.ops import commands

    def broken(*_args: Any, **_kwargs: Any) -> None:
        raise OSError("磁盘只读")

    monkeypatch.setattr(commands, "sync_alert", broken)
    store = DatasetStore(tmp_path / "data")

    commands._doctor(store, snapshot=True)

    assert len(read_health(store)) == 1


# ---------------------------------------------------------------- 同一口径只有一份


def test_卡片覆盖在两个检查项里报同一个数(tmp_path: Path) -> None:
    """同一份 `carddata`，两个检查项不能各印一个数字。

    2026-11-15 实测：同一份体检里「产物新鲜度」印 68%、「字段新鲜度」印 68.3%。
    查下来分子其实相等（交集与 `n_unique` 差 0 个 id），是**格式**各写各的：
    一处 `.0f`、一处 `.1%`。数字不同不只难看——读者会以为其中一个坏了，
    查完发现两个都对，从此不再信这两个数。
    """
    store = DatasetStore(tmp_path / "data")
    ids = [f"acme/ds-{index}" for index in range(10)]
    put_current(store, NOW.date(), rows=10)
    put_export(store, ids)
    put_artifact(store, "carddata", ids[:3], NOW.date())
    make_run(store, "reconcile", NOW)

    findings = diagnose(store, NOW)
    artifact = next(item for item in findings if item.text.startswith("产物新鲜度"))
    field = next(item for item in findings if item.text.startswith("字段新鲜度"))

    assert "卡片覆盖 30%" in artifact.text
    assert "卡片覆盖 30%" in field.text


def test_体检读导出物走_delivery_定义的位置(tmp_path: Path, monkeypatch: Any) -> None:
    """导出位置只能有一个定义，体检必须跟着它走。

    体检原先自己拼 `root/exports/datasets.parquet`。哪天有人改了
    `wide.DEFAULT_RELATIVE`，体检就会报「宽表导出不存在，跑 export-wide」，
    而重跑只会写到新位置——照这条提示做，永远修不好它指的病。
    """
    from hf_data_crawl.delivery import wide

    monkeypatch.setattr(wide, "DEFAULT_RELATIVE", Path("exports") / "renamed.parquet")

    store = DatasetStore(tmp_path / "data")
    ids = [f"acme/ds-{index}" for index in range(3)]
    put_current(store, NOW.date(), rows=3)
    put_export(store, ids)

    artifact = next(item for item in diagnose(store, NOW) if item.text.startswith("产物新鲜度"))

    assert not artifact.problem
    assert "导出与底库一致" in artifact.text


# ---------------------------------------------------------------- 结论的身份


def test_每条体检结论都带检查项标识(tmp_path: Path) -> None:
    """`code` 是告警去重与连续计数的唯一依据，缺一个就静默失效。

    空 code 的结论照样进 ALERT.txt 与流水，看起来一切正常，只是永远数不到它
    连续红了几轮。所以在编排处守住：不能有空的，也不能两项共用一个。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)

    findings = diagnose(store, NOW)
    codes = [item.code for item in findings]

    assert all(codes), f"有结论没贴 code：{[i.text for i in findings if not i.code]}"
    assert len(set(codes)) == len(codes), "code 重复：连续计数会把两项混成一项"
    assert {"collect_freshness", "doc_drift", "field_freshness", "raw_gaps", "storage"} <= set(
        codes
    )


def test_告警标注连续天数(tmp_path: Path) -> None:
    """「已经红到第几天」是决定要不要现在动手的依据，得写进告警。

    按 `code` 数而不是按文案子串：上一轮写「Raw 断日：11-05」、这一轮写
    「Raw 断日：11-06」，子串匹配会得出「换了件事」，按 code 才知道是同一项。
    """
    from hf_data_crawl.ops import health

    store = DatasetStore(tmp_path / "data")
    health.record(store, [Finding(True, "Raw 断日：2026-11-05", code="raw_gaps")], NOW)

    sync_alert(
        store,
        [Finding(True, "Raw 断日：2026-11-06", code="raw_gaps")],
        NOW + timedelta(days=1),
    )

    body = (store.root / ALERT_NAME).read_text(encoding="utf-8")
    assert "已连续 2 天" in body


def test_同一天多次体检只算一天(tmp_path: Path) -> None:
    """一天跑三次 `doctor` 不等于红了三天。

    2026-11-15 当日为验改动跑了六次体检。若按次数数，告警会写「连续 6 轮」，
    读起来像红了六天——那个数会比实际严重五倍，而它是用来催人动手的。
    """
    from hf_data_crawl.ops import health

    store = DatasetStore(tmp_path / "data")
    for hours in (0, 3, 6):
        health.record(
            store,
            [Finding(True, "Raw 断日：2026-11-05", code="raw_gaps")],
            NOW + timedelta(hours=hours),
        )

    sync_alert(
        store,
        [Finding(True, "Raw 断日：2026-11-05", code="raw_gaps")],
        NOW + timedelta(hours=9),
    )

    body = (store.root / ALERT_NAME).read_text(encoding="utf-8")
    assert "已连续" not in body


def _ctx(store: DatasetStore, now: datetime = NOW) -> RunContext:
    """最小上下文：锁与对账判据只用到 store / now / runs。"""
    return RunContext(
        store=store, now=now, runs=[], current=pl.DataFrame(), current_ids=frozenset()
    )


def test_写锁占用过久要报(tmp_path: Path) -> None:
    """一个挂死数小时的进程不能只在终端露一行。

    `_lock_state` 原先两个分支都返回绿，于是「谁占着」永远进不了 `ALERT.txt`：
    2026-11-13 实测过一次卡住的对账持锁，只有恰好看见终端的人知道。
    """
    from hf_data_crawl.ops.doctor import LOCK_ALERT_AFTER_MINUTES, _lock_state

    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)
    since = NOW - timedelta(minutes=LOCK_ALERT_AFTER_MINUTES + 1)
    (store.root / ".lock").write_bytes(
        b"\0"
        + json.dumps({"command": "reconcile", "pid": 4242, "since": since.isoformat()}).encode()
    )

    finding = _lock_state(_ctx(store))

    assert finding.problem
    assert "4242" in finding.text


def test_写锁刚占用不算问题(tmp_path: Path) -> None:
    """正常的一轮对账要跑半小时，刚占一会儿不该报警。"""
    from hf_data_crawl.ops.doctor import _lock_state

    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)
    since = NOW - timedelta(minutes=5)
    (store.root / ".lock").write_bytes(
        b"\0" + json.dumps({"command": "collect", "pid": 7, "since": since.isoformat()}).encode()
    )

    assert not _lock_state(_ctx(store)).problem


def test_有界对账不算完整全量() -> None:
    """`reconcile --max-pages N` 也是 status=success，但只扫了前 N 页。

    拿它当「最近一次全量对账」，`_field_freshness` 的覆盖基准会被半轮顶替：
    要么假红「那轮没扫完」，要么把此前的真全量轮挡在基准之外。
    """
    from hf_data_crawl.ops.doctor import _last_sweep_date

    bounded = {
        "command": "reconcile",
        "status": "success",
        "payload": {"snapshot_date": "2026-11-10", "stopped_reason": "max_pages"},
    }
    full = {
        "command": "reconcile",
        "status": "success",
        "payload": {"snapshot_date": "2026-11-09", "stopped_reason": "end"},
    }

    assert _last_sweep_date([bounded]) is None
    assert _last_sweep_date([full, bounded]) == date(2026, 11, 9)


def test_观测骤降要报(tmp_path: Path) -> None:
    """重放先清分区再写回：这条不报，删掉的历史观测不留任何痕迹。"""
    store = DatasetStore(tmp_path / "data")
    make_run(store, "build", NOW, shrinks=[["2026-11-09", 782026, 205545]])

    texts = problems(store)

    assert any("观测骤降" in text and "2026-11-09" in text for text in texts)


def test_没有骤降记录时不报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    make_run(store, "build", NOW)

    assert not any("观测骤降" in text for text in problems(store))


def test_存储报出备份稳态与观测年化(tmp_path: Path) -> None:
    """「会不会只增不减」得有个数字，而不是等磁盘满了才发现。

    backup 的稳态由「保留天数 × 每天搬进多少」决定；观测流水是唯一无界增长的
    一层，按最近一天年化。两句都是粗估，但比逐个 du 强。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)

    finding = next(item for item in diagnose(store, NOW) if item.text.startswith("存储"))

    assert "backup 稳态约" in finding.text
    assert "观测流水每年约" in finding.text


def test_停滞的抓取分片要报(tmp_path: Path) -> None:
    """`fetch-sizes` / `fetch-readme` 的分片不进任何表，也不在 `*.tmp` 的扫描范围。

    进程被杀、机器重启，分片就留在 `.staging/` 里，没人知道有半成品。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    shard = store.root / ".staging" / "sizes" / "2026-11-17-abc-0000.parquet"
    shard.parent.mkdir(parents=True)
    shard.write_bytes(b"x")
    old = (NOW - timedelta(days=3)).timestamp()
    os.utime(shard, (old, old))

    finding = next(item for item in diagnose(store, NOW) if item.text.startswith("抓取暂存"))

    assert finding.problem
    assert "sizes" in finding.text
    assert "3 天前" in finding.text


def test_正在跑的抓取分片不报(tmp_path: Path) -> None:
    """最新分片还在动，说明这一批还在跑——报红只会训练人忽略告警。"""
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    shard = store.root / ".staging" / "readme" / "2026-11-17-def-0000.parquet"
    shard.parent.mkdir(parents=True)
    shard.write_bytes(b"x")

    finding = next(item for item in diagnose(store, NOW) if item.text.startswith("抓取暂存"))

    assert not finding.problem


def test_体检项登记表的码唯一且非空() -> None:
    """码是历史记录的主键：重复了会把两项检查算成一项，空了则断代。"""
    codes = [check.code for check in (*CHECKS, LOCK_STATE)]

    assert all(codes), "每条检查都要有码"
    assert len(codes) == len(set(codes)), "码不能重复"


def test_降级不会丢掉已经贴上的码() -> None:
    """`_snapshot_dependent` 曾经重新构造 `Finding`，把 code 与 degraded 一起丢掉。

    今天靠「先包快照依赖、后贴码」的顺序遮着；顺序一改就是静默丢字段，台账里那条
    记录的码变空，而连续天数正是按码数的——归零看起来和「已经修好了」一模一样。
    """
    marked = Finding(True, "raw 断日", code="raw_gaps")

    downgraded = _snapshot_dependent(marked)

    assert downgraded.snapshot_dependent is True
    assert downgraded.code == "raw_gaps"


# ------------------------------------------------------------ README 全文保鲜


def test_README太久没刷新要报(tmp_path: Path) -> None:
    """README 与侧栏字段不进任何一条链，没人刷新就会悄悄变旧。

    2026-11-18 之前体检二十七项里**没有一项**看它：2.8 GB 正文即使三周没动，
    所有读数照样全绿——因为没有一处读它。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    old = NOW.date() - timedelta(days=40)
    put_readme(store, old, {"acme/ds-0": None})
    for path in (store.root / "runs").glob("*fetch-readme*"):
        path.unlink()

    found = [text for text in problems(store) if "README 全文" in text]

    assert found, "README 陈旧却没人报"
    assert "没有刷新" in found[0] and "fetch-readme" in found[0]
    assert "陈旧" in found[0] and "无记录" in found[0], "两个计数要一并给出，否则不知道该重取多少"


def test_README刚跑过就不报即使一条没写(tmp_path: Path) -> None:
    """`--changed` 找不到陈旧会提前返回、不写行；只查数据会把它误报成「没刷新」。

    判据因此取「表里最大 snapshot_date」与「最近一次成功运行」两者较大者。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    put_readme(store, NOW.date() - timedelta(days=60), {"acme/ds-0": None})
    for path in (store.root / "runs").glob("*fetch-readme*"):
        path.unlink()
    make_run(store, "fetch-readme", NOW - timedelta(hours=2), "end")

    assert not any("README 全文" in text for text in problems(store))


def test_README从未抓过不算故障(tmp_path: Path) -> None:
    """富化产物缺表不是故障：全新安装、或刻意不抓，都该是绿的。

    红只留给「抓过、但没人管了」。判据与 `card_provenance` 同一取向：
    缺表说「无从对照」，不判 problem。
    """
    store = DatasetStore(tmp_path / "data")
    healthy(store)
    store.path("readme").unlink()

    assert not any("README 全文" in text for text in problems(store))


def test_文档口径漂移能抓出弃用词(tmp_path: Path) -> None:
    """「讲现在」的文档里出现已弃用的任务名，体检要报出来。

    判定刻意窄：只认一份固定的弃用词表，不猜「这句话是否过时」。
    """
    from hf_data_crawl.ops.doctor import _doc_drift

    (tmp_path / "README.md").write_text("例行由 hf-daily 每日跑一次。\n", encoding="utf-8")

    finding = _doc_drift(ctx_of([], tmp_path))

    assert finding.problem
    assert "hf-daily" in finding.text


def test_讲历史的那一行不算漂移(tmp_path: Path) -> None:
    """MONITORING 里「旧的六个任务已删除」是现状说明，不能报成漂移。"""
    from hf_data_crawl.ops.doctor import _doc_drift

    (tmp_path / "README.md").write_text(
        "旧的六个任务（hf-daily 等）已删除并并入本链。\n", encoding="utf-8"
    )

    finding = _doc_drift(ctx_of([], tmp_path))

    assert not finding.problem
