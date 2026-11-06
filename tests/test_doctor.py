"""体检与补跑：错过的任务要被看见，补跑不能让同一天重复采集。"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from hf_data_crawl.api import HfResponse
from hf_data_crawl.cli import main
from hf_data_crawl.doctor import ALERT_NAME, diagnose, sync_alert
from hf_data_crawl.runs import Outcome, write_run
from hf_data_crawl.store import DatasetStore

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


def healthy(store: DatasetStore) -> None:
    make_run(store, "collect", NOW - timedelta(hours=9), "watermark")
    make_run(store, "reconcile", NOW - timedelta(days=3), "end")
    make_raw_days(store, [NOW.date() - timedelta(days=n) for n in range(1, 14)])


def problems(store: DatasetStore) -> list[str]:
    return [item.text for item in diagnose(store, NOW) if item.problem]


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
        NOW - timedelta(days=1),
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
