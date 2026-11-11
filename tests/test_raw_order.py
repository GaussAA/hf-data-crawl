"""Raw 的写入序。

一条不变量：**谁胜出，由 Raw 自身记录，不由文件系统决定**。

2026-11-04 两次事故都出在这里：
1. 按文件名字典序排序，摘要值是随机哈希，旧批次压住了新批次；
2. 改用 mtime 后修好了顺序，却把「事实来源」放到了 Raw 之外——复制一份
   raw/ 目录（备份恢复、换机迁移、解压归档），同一批数据就能推出不同的
   Bronze，而且不报错。

而 `rebuild` 宣称「Raw 是唯一事实来源、结果可复现」。这两句话必须同时为真，
否则 rebuild 就只是「碰巧能重建一次」。

2026-11-07 收口：新分块写 `written_at`；历史分区经 `archive` 把真实写入序
冻结进 `groups`；两者皆无的旧分块按文件名确定性定序，不再读 mtime。
"""

from __future__ import annotations

import gzip
import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from hf_data_crawl import bronze, raw
from hf_data_crawl.store import DatasetStore

DAY = date(2026, 11, 4)


def record(index: int, *, main_size: int | None = None) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "sha": f"sha-{index}",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "downloads": 100,
        "downloadsAllTime": 200,
        "likes": 1,
        "tags": ["language:en"],
    }
    if main_size is not None:
        payload["mainSize"] = main_size
    return payload


def payloads(store: DatasetStore) -> list[dict[str, Any]]:
    partition = store.raw_dir / f"dt={DAY.isoformat()}"
    found = []
    for path in sorted(partition.glob("*.json.gz")):
        found.append(json.loads(gzip.decompress(path.read_bytes()).decode("utf-8")))
    return found


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    return DatasetStore(tmp_path / "data")


# ---------------------------------------------------------------- 落盘


def test_写入时刻落进raw的meta(store: DatasetStore) -> None:
    """序必须在数据里，才能在复制、迁移、归档之后依然成立。"""
    raw.write(store, snapshot_date=DAY, page_index=1, records=[record(1)], meta={})

    meta = payloads(store)[0]["meta"]

    assert "written_at" in meta
    assert isinstance(meta["written_at"], str) and meta["written_at"]


def test_写入时刻可注入以便确定性测试(store: DatasetStore) -> None:
    """不注入就只能靠真实时间，测试会随时钟抖动。"""
    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[record(1)],
        meta={},
        written_at="2026-11-04T10:00:00+00:00",
    )

    assert payloads(store)[0]["meta"]["written_at"] == "2026-11-04T10:00:00+00:00"


# ---------------------------------------------------------------- 排序


def test_胜出者由写入时刻决定而不是文件名(store: DatasetStore) -> None:
    """摘要值是随机哈希，字典序谁先谁后纯属偶然。"""
    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[record(1, main_size=111)],
        meta={},
        written_at="2026-11-04T12:00:00.000000+00:00",
    )
    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[record(1, main_size=222)],
        meta={},
        written_at="2026-11-04T09:00:00.000000+00:00",
    )

    bronze.build(store, DAY)

    # 09:00 那批虽是后写的，但时刻更早，故 12:00 的 111 胜出。
    assert store.read("bronze", DAY).row(0, named=True)["main_size"] == 111


def test_胜出者由写入时刻决定而不是文件修改时间(store: DatasetStore) -> None:
    """核心断言：把 mtime 全打乱，结果必须不变。

    这条测试在旧实现上必失败——旧实现只看 mtime。
    """
    import os
    import time

    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[record(1, main_size=111)],
        meta={},
        written_at="2026-11-04T12:00:00.000000+00:00",
    )
    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[record(1, main_size=222)],
        meta={},
        written_at="2026-11-04T09:00:00.000000+00:00",
    )

    # 把「时刻较晚」的那份改成文件系统上最早，旧实现会被骗到。
    partition = store.raw_dir / f"dt={DAY.isoformat()}"
    for path in partition.glob("*.json.gz"):
        data = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
        stamp = data["meta"]["written_at"]
        when = time.time() + (100 if stamp.startswith("2026-11-04T09") else -100)
        os.utime(path, (when, when))

    bronze.build(store, DAY)

    assert store.read("bronze", DAY).row(0, named=True)["main_size"] == 111


# ---------------------------------------------------------------- 可复现


def test_复制raw目录后重建结果完全一致(tmp_path: Path) -> None:
    """rebuild 的承诺：Raw 是唯一事实来源。

    把整个 raw 复制到另一个数据根（并故意打乱 mtime），重建出的 Bronze 必须
    逐字节等价。这是「复制一份就得出不同结果」那类事故的回归护栏。
    """
    import os
    import time

    source = DatasetStore(tmp_path / "a")
    for index, stamp in enumerate(
        [
            "2026-11-04T09:00:00.000000+00:00",
            "2026-11-04T10:00:00.000000+00:00",
            "2026-11-04T11:00:00.000000+00:00",
        ],
        1,
    ):
        raw.write(
            source,
            snapshot_date=DAY,
            page_index=index,
            records=[record(index, main_size=index * 100)],
            meta={},
            written_at=stamp,
        )
        raw.write(
            source,
            snapshot_date=DAY,
            page_index=index,
            records=[record(index, main_size=index * 999)],
            meta={},
            written_at=stamp.replace("2026-11-04", "2026-11-05"),
        )

    bronze.build(source, DAY)
    expected = source.read("bronze", DAY)

    target = DatasetStore(tmp_path / "b")
    shutil.copytree(source.raw_dir, target.raw_dir)
    # 复制会保留 mtime，故意全部打乱，模拟解压归档/换机迁移后的状态。
    now = time.time()
    for offset, path in enumerate(sorted(target.raw_dir.rglob("*.json.gz"))):
        os.utime(path, (now + offset, now + offset))

    bronze.build(target, DAY)

    assert target.read("bronze", DAY).equals(expected)


# ---------------------------------------------------------------- 向后兼容


def test_旧raw没有写入时刻时按文件名确定性定序(tmp_path: Path) -> None:
    """历史分块没有这个字段，定序不得依赖文件系统 mtime。

    故意把 mtime 设成与文件名字典序相反：若实现还在读 mtime，111 会胜出。
    未归档的旧分块应先跑 `archive --date`，把真实写入序冻结进 groups。
    """
    import os

    store = DatasetStore(tmp_path / "legacy")
    partition = store.raw_dir / f"dt={DAY.isoformat()}"
    partition.mkdir(parents=True)

    for name, main_size, order in [
        ("part-000001-aaaa.json.gz", 111, 10_000),
        ("part-000001-bbbb.json.gz", 222, 10),
    ]:
        body = json.dumps(
            {
                "meta": {"snapshot_date": DAY.isoformat(), "page_index": 1},
                "records": [record(1, main_size=main_size)],
            },
            ensure_ascii=False,
            sort_keys=True,
        ).encode("utf-8")
        path = partition / name
        with gzip.open(path, "wb") as handle:
            handle.write(body)
        os.utime(path, (order, order))

    bronze.build(store, DAY)

    assert store.read("bronze", DAY).row(0, named=True)["main_size"] == 222


def test_新分块永远压过没有时刻的旧分块(store: DatasetStore) -> None:
    """迁移期的关键保证：新写的带时刻，必须赢过历史。"""
    import os
    import time

    partition = store.raw_dir / f"dt={DAY.isoformat()}"
    partition.mkdir(parents=True, exist_ok=True)
    body = json.dumps(
        {
            "meta": {"snapshot_date": DAY.isoformat(), "page_index": 1},
            "records": [record(1, main_size=111)],
        },
        ensure_ascii=False,
        sort_keys=True,
    ).encode("utf-8")
    legacy = partition / "part-000001-aaaa.json.gz"
    with gzip.open(legacy, "wb") as handle:
        handle.write(body)
    # 旧文件故意设成「最晚」，若实现被 mtime 骗到就会输错。
    future = time.time() + 10_000
    os.utime(legacy, (future, future))

    raw.write(
        store,
        snapshot_date=DAY,
        page_index=1,
        records=[record(1, main_size=222)],
        meta={},
        written_at="2026-11-04T09:00:00.000000+00:00",
    )

    bronze.build(store, DAY)

    assert store.read("bronze", DAY).row(0, named=True)["main_size"] == 222


# ---------------------------------------------------------------- 归档冻结


def test_归档分片自带排序键不受文件mtime影响(tmp_path: Path) -> None:
    """archive 把真实写入序冻结进 groups；此后 Raw 自包含，迁移不再改结果。"""
    import os
    import time

    store = DatasetStore(tmp_path / "archived")
    partition = store.raw_dir / f"dt={DAY.isoformat()}"
    partition.mkdir(parents=True)
    payload = {
        "meta": {"label": "part", "archived": True, "record_count": 2},
        "groups": [
            {
                "key": [1, "2026-11-04T09:00:00.000000+00:00", "old.json.gz"],
                "file": "old.json.gz",
                "count": 1,
            },
            {
                "key": [1, "2026-11-04T12:00:00.000000+00:00", "new.json.gz"],
                "file": "new.json.gz",
                "count": 1,
            },
        ],
        "records": [record(1, main_size=111), record(1, main_size=222)],
    }
    path = partition / "part-archive-000001-abcd1234.json.gz"
    with gzip.open(path, "wb") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8"))
    future = time.time() + 10_000
    os.utime(path, (future, future))

    bronze.build(store, DAY)

    assert store.read("bronze", DAY).row(0, named=True)["main_size"] == 222
