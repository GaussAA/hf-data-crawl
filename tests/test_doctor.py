"""体检与补跑：错过的任务要被看见，补跑不能让同一天重复采集。"""

from __future__ import annotations

import json
import os
from collections.abc import Mapping
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from hf_data_crawl.cli import main
from hf_data_crawl.doctor import ALERT_NAME, Finding, diagnose, sync_alert
from hf_data_crawl.health import read as read_health
from hf_data_crawl.hf.api import HfResponse
from hf_data_crawl.pipeline import lineage
from hf_data_crawl.pipeline.lineage import path_of as lineage_path
from hf_data_crawl.platform.lock import LockBusy
from hf_data_crawl.platform.retention import REPLAY_WINDOW_DAYS, manifest_path
from hf_data_crawl.platform.runs import Outcome, write_run
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import CURRENT_SCHEMA, ENRICH_SCHEMA

NOW = datetime(2026, 11, 10, 12, 0, 0)


def make_run(
    store: DatasetStore, command: str, when: datetime, reason: str = "end", **extra: Any
) -> None:
    payload = {"snapshot_date": when.date().isoformat(), "stopped_reason": reason, **extra}
    path = write_run(store, command, when, Outcome(payload))
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


def put_ids(store: DatasetStore, name: str, day: date, ids: list[str]) -> None:
    """只写 id 列的分区。

    勾稽检查只扫 id（`silver` 两百多万行，读全列不值当），夹具照同一口径造：
    走 `store.write` 会按 schema 补齐全部列，反而把测试写成「造一张完整表」。
    """
    target = store.path(name, day)
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"id": ids}).write_parquet(target)


def put_export(store: DatasetStore, ids: list[str]) -> None:
    """造一份符合默认交付契约的宽表导出；体检比对 id 差集与列集。"""
    from hf_data_crawl.fields import DEFAULT_COLUMNS

    target = store.root / "exports" / "datasets.parquet"
    target.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(
        {column: ids if column == "id" else [None] * len(ids) for column in DEFAULT_COLUMNS}
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
    make_run(store, "collect", NOW - timedelta(hours=60), "watermark")  # 更旧的不影响
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


def test_底库行龄新鲜时不报(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    healthy(store)

    assert not any("底库新鲜度" in text for text in problems(store))


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
    import hf_data_crawl.doctor as doctor

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
    assert downgraded and all(not item.problem for item in downgraded)
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

    monkeypatch.setattr("hf_data_crawl.cli.exclusive", busy)
    monkeypatch.setattr("hf_data_crawl.cli.diagnose", fake_diagnose)

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
    from hf_data_crawl.doctor import KNOWN_RAW_GAPS

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
    from hf_data_crawl.doctor import KNOWN_RAW_GAPS

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
