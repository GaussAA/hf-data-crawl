"""S8 · 运行摘要与失败留痕的行为测试。

验收点：成功与失败都留痕、失败必须带可读原因、连续两轮可对比。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from hf_data_crawl.ops.cli import main
from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.runs import Outcome, read_runs, write_run
from hf_data_crawl.platform.store import DatasetStore

DAY1 = date(2026, 11, 1)


def record(index: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": 100,
        "downloadsAllTime": 200,
        "likes": 1,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en"],
    }


def seed(store: DatasetStore) -> None:
    raw.write(store, snapshot_date=DAY1, page_index=1, records=[record(1)], meta={})
    derive_day(store, DAY1)


def test_成功运行写入可读摘要(tmp_path: Path) -> None:
    root = tmp_path / "data"
    seed(DatasetStore(root))

    code = main(["--data-dir", str(root), "build"])

    runs = read_runs(DatasetStore(root))
    assert code == 0
    assert len(runs) == 1
    assert runs[0]["command"] == "build"
    assert runs[0]["status"] == "success"
    assert runs[0]["payload"]["current_rows"] == 1


def test_失败运行留下可读原因(tmp_path: Path) -> None:
    root = tmp_path / "data"
    broken = root / "raw" / f"dt={DAY1.isoformat()}" / "part-000001-deadbeef.json.gz"
    broken.parent.mkdir(parents=True)
    broken.write_bytes(b"not a gzip payload")

    code = main(["--data-dir", str(root), "build"])

    runs = read_runs(DatasetStore(root))
    assert code == 1
    assert runs[0]["status"] == "failed"
    assert "error" in runs[0]["payload"]
    assert runs[0]["payload"]["error"]


def test_连续两轮摘要可对比(tmp_path: Path) -> None:
    root = tmp_path / "data"
    seed(DatasetStore(root))

    main(["--data-dir", str(root), "build"])
    main(["--data-dir", str(root), "build"])

    runs = read_runs(DatasetStore(root))
    assert len(runs) == 2
    assert runs[0]["finished_at"] <= runs[1]["started_at"]
    assert runs[0]["payload"]["current_rows"] == runs[1]["payload"]["current_rows"]


def test_损坏的摘要文件不阻断其余读取(tmp_path: Path) -> None:
    root = tmp_path / "data"
    store = DatasetStore(root)
    seed(store)
    main(["--data-dir", str(root), "build"])
    (root / "runs" / "broken.json").write_text("{半截", encoding="utf-8")

    runs = read_runs(store)

    assert len(runs) == 1


def test_同一秒内两轮的顺序按内容而非文件名(tmp_path: Path) -> None:
    """撞名文件 `-build-2.json` 的字典序排在 `-build.json` 之前。

    若按文件名排序，后一轮会被读到前面，「连续两轮对比」于是读到颠倒的一对，
    而且不报错——只是 finished_at <= started_at 这条检查突然不成立。
    """
    store = DatasetStore(tmp_path / "data")
    runs_dir = store.root / "runs"
    runs_dir.mkdir(parents=True)
    (runs_dir / "20261105T001500-build.json").write_text(
        json.dumps(
            {
                "command": "build",
                "status": "success",
                "started_at": "2026-11-05T00:15:00.000000",
                "finished_at": "2026-11-05T00:15:09.000000",
                "payload": {},
            }
        ),
        encoding="utf-8",
    )
    (runs_dir / "20261105T001500-build-2.json").write_text(
        json.dumps(
            {
                "command": "build",
                "status": "success",
                "started_at": "2026-11-05T00:15:20.000000",
                "finished_at": "2026-11-05T00:15:29.000000",
                "payload": {},
            }
        ),
        encoding="utf-8",
    )

    runs = read_runs(store)

    assert [item["started_at"] for item in runs] == [
        "2026-11-05T00:15:00.000000",
        "2026-11-05T00:15:20.000000",
    ]


def test_业务失败记为failed而非success(tmp_path: Path) -> None:
    """退出码 1 而记录 success，等于没有一处能回答「这轮成功了吗」。

    2026-11-05 前：`_execute` 对正常返回一律写 success，于是 report 在无数据时
    GoldNotReady、退出码 1，runs/ 里却是一份成功的记录。
    """
    root = tmp_path / "data"

    code = main(["--data-dir", str(root), "report"])

    runs = read_runs(DatasetStore(root))
    assert code == 1
    assert runs[0]["status"] == "failed"
    assert runs[0]["reason"], "失败必须带可读原因"


def test_无数据时补齐规模也记为失败(tmp_path: Path) -> None:
    root = tmp_path / "data"

    code = main(["--data-dir", str(root), "fetch-sizes"])

    runs = read_runs(DatasetStore(root))
    assert code == 1
    assert runs[0]["status"] == "failed"


def test_运行摘要超过上限时只保留最近若干条(tmp_path: Path, monkeypatch: Any) -> None:
    """上限由 RUNS_KEEP 控制，按内容里的开始时刻保留最新，而不是按文件名。"""
    from datetime import datetime

    from hf_data_crawl.platform.runs import Outcome, write_run

    monkeypatch.setattr("hf_data_crawl.platform.runs.RUNS_KEEP", 2)
    store = DatasetStore(tmp_path / "data")
    for hour in (1, 2, 3, 4):
        write_run(store, "build", datetime(2026, 11, 5, hour, 0, 0), Outcome(payload=hour))

    assert [run["payload"] for run in read_runs(store)] == [3, 4]
    assert len(list((store.root / "runs").glob("*.json"))) == 2


def test_摘要中日期与路径被序列化为字符串(tmp_path: Path) -> None:
    root = tmp_path / "data"
    seed(DatasetStore(root))
    main(["--data-dir", str(root), "build"])

    payload = read_runs(DatasetStore(root))[0]["payload"]

    assert isinstance(payload["dates"], list)
    assert isinstance(payload["dates"][0], str)


def test_同秒并发写台账不互相覆盖(tmp_path: Path) -> None:
    """两个写命令同秒启动：`path.exists()` 去重不原子，后写会静默盖掉先写。"""
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from datetime import datetime

    from hf_data_crawl.platform.runs import Outcome, write_run

    store = DatasetStore(tmp_path / "data")
    same_second = datetime(2026, 11, 5, 0, 15, 0)
    barrier = threading.Barrier(2)

    def writer() -> None:
        barrier.wait(timeout=5)
        write_run(store, "build", same_second, Outcome({"who": threading.get_ident()}))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(writer) for _ in range(2)]
        for future in futures:
            future.result()

    assert len(read_runs(store)) == 2
    assert len(list((store.root / "runs").glob("*.json"))) == 2


def test_台账写不进去也不让命令失败(tmp_path: Path) -> None:
    """数据已经落盘，不能因为记录写失败就让一次成功的命令以异常栈收场。

    原先 `write_run` 在 `_execute` 的 try 之外：`runs/` 不可写、磁盘满或杀软
    瞬时占用新文件时，异常直接穿透——命令成功了却报失败、退出码 1，而台账里
    查不到这次运行，`--skip-if-done` 与体检的完成判据随后全部看空。
    """
    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)
    (store.root / "runs").write_text("占位：让 mkdir 失败", encoding="utf-8")

    assert write_run(store, "build", datetime(2026, 11, 5), Outcome()) is None


def test_台账里的绝对路径被脱敏(tmp_path: Path) -> None:
    """异常消息带完整路径时，台账不该原样写进去。

    `cli.py` 里「台账保持机读，不夹带本机路径」那句原先只在注释里，而
    `FileNotFoundError` 这类异常消息照旧把 `C:\\WorkSpace\\...` 写进了 runs/。
    """
    store = DatasetStore(tmp_path / "data")
    store.root.mkdir(parents=True)
    leaked = f"FileNotFoundError: {store.root.resolve()}/state/x.parquet"
    outcome = Outcome(payload={}, code=1, reason=leaked)

    path = write_run(store, "build", datetime(2026, 11, 16, 12, 0), outcome)

    assert path is not None
    reason = json.loads(path.read_text(encoding="utf-8"))["reason"]
    assert str(store.root.resolve()) not in reason
    assert "<data>/state/x.parquet" in reason


# ---------------------------------------------------------------- 查询面（时间归一与完成态）


def test_带时区的时间戳与本地表示可互比() -> None:
    """`15:08Z` 与 UTC+8 的 `23:08` 是同一时刻，必须相等。

    原先归一成 UTC：UTC+8 机器上 `15:08Z` 变成 `15:08`，与同一条本地记录差 8 小时
    ——两个时刻一并排就判反。写入端 `datetime.now()` 写的是本地无时区时间，归一
    目标必须与它对齐。
    """
    from hf_data_crawl.platform.runs import run_time

    local = datetime(2026, 11, 21, 12, 0)
    aware = local.astimezone(UTC)

    assert run_time(local) == run_time(aware.isoformat())


def test_坏时间戳返回空值而不是抛错() -> None:
    """台账里可能有半截写入的 JSON 与旧格式；读取方不能因此崩。"""
    from hf_data_crawl.platform.runs import run_time

    assert run_time("soon") is None
    assert run_time(None) is None
    assert run_time("") is None
    assert run_time(123) is None


def test_last_success按完成态筛掉半轮() -> None:
    """`reconcile --max-pages 100` 是 success，但不是完整会话。"""
    from hf_data_crawl.platform.runs import FULL_SCAN, last_success

    runs = [
        {
            "command": "reconcile",
            "status": "success",
            "finished_at": "2026-11-21T01:00:00",
            "payload": {"stopped_reason": "max_pages"},
        },
        {
            "command": "reconcile",
            "status": "success",
            "finished_at": "2026-11-20T01:00:00",
            "payload": {"stopped_reason": "end"},
        },
    ]

    assert last_success(runs, "reconcile", done_reasons=FULL_SCAN) == datetime(2026, 11, 20, 1, 0)


def test_last_success不传完成态时成功即完整() -> None:
    """`build` / `export` 没有「扫到哪」的概念，`stopped_reason` 缺席不算缺陷。"""
    from hf_data_crawl.platform.runs import last_success

    runs = [
        {"command": "build", "status": "success", "finished_at": "2026-11-21T02:00:00"},
        {"command": "build", "status": "failed", "finished_at": "2026-11-21T03:00:00"},
    ]

    assert last_success(runs, "build") == datetime(2026, 11, 21, 2, 0)


def test_last_success取最晚而不是第一条() -> None:
    """同一命令一天跑过多轮时，最晚那次才是当前数据的来源。"""
    from hf_data_crawl.platform.runs import last_success

    runs = [
        {
            "command": "collect",
            "status": "success",
            "finished_at": "2026-11-21T00:32:00",
            "payload": {"stopped_reason": "end"},
        },
        {
            "command": "collect",
            "status": "success",
            "finished_at": "2026-11-21T23:08:00",
            "payload": {"stopped_reason": "end"},
        },
    ]

    assert last_success(runs, "collect") == datetime(2026, 11, 21, 23, 8)
