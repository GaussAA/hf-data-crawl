"""S6 · 从 Raw 重建派生层的行为测试。

核心不变式：重建结果必须与「逐日正常跑」逐字节等价，否则重建就等于造了一份新数据。
"""

from __future__ import annotations

import hashlib
import json
import shutil
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.ops.cli import main
from hf_data_crawl.pipeline import raw, state
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.pipeline.lineage import path_of as lineage_path
from hf_data_crawl.pipeline.rebuild import (
    discover_raw_dates,
    divergent_since,
    rebuild_from_raw,
    replay_suffix,
)
from hf_data_crawl.platform.retention import REPLAY_WINDOW_DAYS
from hf_data_crawl.platform.store import DatasetStore

DAY1 = date(2026, 11, 1)
DAY2 = date(2026, 11, 2)


def record(index: int, downloads: int) -> dict[str, Any]:
    return {
        "id": f"acme/ds-{index}",
        "author": "acme",
        "lastModified": "2026-11-01T00:00:00.000Z",
        "createdAt": "2025-01-01T00:00:00.000Z",
        "sha": f"sha-{index}",
        "downloads": downloads,
        "downloadsAllTime": downloads * 2,
        "likes": 1,
        "gated": False,
        "private": False,
        "description": "d",
        "tags": ["language:en", "license:mit", "task_categories:text-classification"],
    }


def seed(store: DatasetStore, snapshot: date, records: list[dict[str, Any]]) -> None:
    raw.write(store, snapshot_date=snapshot, page_index=1, records=records, meta={})
    derive_day(store, snapshot)


@pytest.fixture()
def store(tmp_path: Path) -> DatasetStore:
    instance = DatasetStore(tmp_path / "data")
    seed(instance, DAY1, [record(1, 100), record(2, 50)])
    seed(instance, DAY2, [record(1, 180), record(2, 50)])
    return instance


def test_发现的日期按升序排列(store: DatasetStore) -> None:
    assert discover_raw_dates(store) == [DAY1, DAY2]


def test_归档日参与重放且与全量结果一致(store: DatasetStore) -> None:
    """仅归档分片的日子也算可重放；局部重放与全量重放逐行等价。"""
    archive_day = date(2026, 11, 3)
    partition = raw.partition_dir(store, archive_day)
    partition.mkdir(parents=True)
    payload = {"meta": {}, "records": [record(1, 300), record(2, 50)]}
    # 文件名交给 raw.archive_name 生成：后缀随 raw 层走，内容的压缩方式也随它走，
    # 手写字面量迟早与其中一头脱节（这里就脱过一次）。
    (partition / raw.archive_name("part", 1, "abcd1234")).write_bytes(
        raw.pack(json.dumps(payload).encode("utf-8"))
    )

    assert discover_raw_dates(store) == [DAY1, DAY2, archive_day]

    local = rebuild_from_raw(store, since=archive_day)
    local_current = state.read_current(store)
    local_observations = store.read_all("observations")

    full = rebuild_from_raw(store, window_days=None)

    assert local.dates == [archive_day]
    assert full.dates == [DAY1, DAY2, archive_day]
    assert state.read_current(store).equals(local_current)
    assert store.read_all("observations").equals(local_observations)


def test_重建后的current与原结果完全一致(store: DatasetStore) -> None:
    expected_current = state.read_current(store)
    expected_obs = store.read_all("observations")

    summary = rebuild_from_raw(store)

    assert summary.dates == [DAY1, DAY2]
    assert state.read_current(store).equals(expected_current)
    assert store.read_all("observations").equals(expected_obs)


def test_派生层被删光也能重建(store: DatasetStore) -> None:
    import shutil

    # 分区表的路径是 <表目录>/dt=<日期>/<文件>，往上两级就是整张表的目录。
    for name in ("bronze", "silver"):
        shutil.rmtree(store.path(name, DAY1).parents[1])
    shutil.rmtree(store.state_dir)

    summary = rebuild_from_raw(store)

    assert summary.current_rows == 2
    # Silver 是属性变更流水：两天属性没变，只有首日的两行。
    assert store.read_all("silver").height == 2
    # 观测只有 ds-1 在 DAY2 变了，因此两条
    assert store.read_all("observations").height == 3


def test_无raw时返回空结果且不抛错(tmp_path: Path) -> None:
    summary = rebuild_from_raw(DatasetStore(tmp_path / "empty"))

    assert summary.dates == []
    assert summary.current_rows == 0


def test_不规范的raw目录名被忽略(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    (store.raw_dir / "dt=garbage").mkdir(parents=True)

    assert discover_raw_dates(store) == []


def test_重建不清除规模表(store: DatasetStore) -> None:
    """规模表来自 datasets-server，raw 里没有对应数据，删掉就白丢一轮抓取。

    2026-11-04 实测踩过：build 把 enrich 整个删掉，两万条规模数据（约 13 分钟）
    无声消失，页面上的「规模覆盖」直接归零。
    """
    import polars as pl

    from hf_data_crawl.acquire.sizes import SIZE_SCHEMA

    store.upsert(
        "size",
        pl.DataFrame(
            [
                {
                    "id": "acme/ds-1",
                    "snapshot_date": DAY2,
                    "status": "ok",
                    "num_rows": 42,
                    "estimated_num_rows": None,
                    "num_bytes_original": 10,
                    "num_bytes_parquet": 9,
                    "num_bytes_memory": 8,
                    "config_count": 1,
                    "num_columns_max": 3,
                    "configs": None,
                }
            ],
            schema=SIZE_SCHEMA,
        ),
    )

    rebuild_from_raw(store)

    kept = store.read("size")
    assert kept.height == 1
    assert kept.row(0, named=True)["num_rows"] == 42


def test_重建会重算卡片表(store: DatasetStore) -> None:
    """卡片表可由 raw 重算，规则一改就该整体重来，旧的必须被清掉。"""
    raw.write(
        store,
        snapshot_date=DAY2,
        page_index=1,
        records=[{"id": "acme/ds-1", "cardData": {"pretty_name": "正式名"}}],
        meta={},
        label="card",
    )

    rebuild_from_raw(store)

    assert store.read("carddata").height == 1
    assert store.read("carddata").row(0, named=True)["pretty_name"] == "正式名"


def test_重建保留对账探测出的失效状态(store: DatasetStore) -> None:
    """失效状态来自单库探测，不在 raw 里，重建不得抹掉。

    2026-11-05 实测踩到：463 条 restricted / renamed 在一次 build 后全部变回
    active，页面上「已失效」直接归零，且不报任何错。
    """
    state.mark_status(store, {"acme/ds-2": "restricted"})

    rebuild_from_raw(store)

    assert status_of(store, "acme/ds-2") == "restricted"
    assert status_of(store, "acme/ds-1") == "active"


def test_重建后新增数据仍为在架(store: DatasetStore) -> None:
    """留存失效状态不等于把所有库钉死在旧状态。"""
    state.mark_status(store, {"acme/ds-1": "renamed"})

    rebuild_from_raw(store)

    assert status_of(store, "acme/ds-1") == "renamed"
    assert state.read_current(store).filter(pl.col("status") == "active").height == 1


def status_of(store: DatasetStore, dataset_id: str) -> str:
    frame = state.read_current(store).filter(pl.col("id") == dataset_id)
    return frame.row(0, named=True)["status"]


def test_上次重建中断时仍能从快照恢复状态(store: DatasetStore) -> None:
    """rebuild 不是事务性的：清除 state 与写回状态之间被打断，判断就丢了。

    2026-11-05 实测：一次 build 被 SIGTERM 杀掉，恰好死在两者之间，
    463 条 restricted / renamed 全部归零。快照让下一次 build 还能接上。
    """
    import shutil

    from hf_data_crawl.pipeline.rebuild import retain

    state.mark_status(store, {"acme/ds-2": "restricted"})
    retain(store)  # 重建的第一步：快照落盘
    # 模拟中断现场：快照已落盘，state 已被清空，状态还没写回
    shutil.rmtree(store.state_dir)

    rebuild_from_raw(store)

    assert status_of(store, "acme/ds-2") == "restricted"


def test_重建成功后快照被删除(store: DatasetStore) -> None:
    """快照只表示「重建进行中」；留着会被下一次重建当成「上次被打断」。"""
    state.mark_status(store, {"acme/ds-1": "renamed"})

    rebuild_from_raw(store)

    assert not (store.root / "meta" / "failed-status.json").exists()
    assert status_of(store, "acme/ds-1") == "renamed"


def test_重建后已恢复的库不会被旧快照改回去(store: DatasetStore) -> None:
    state.mark_status(store, {"acme/ds-2": "restricted"})
    rebuild_from_raw(store)
    state.mark_status(store, {"acme/ds-2": "active"})  # 之后对账把它恢复了

    rebuild_from_raw(store)

    assert status_of(store, "acme/ds-2") == "active"


# ---------------------------------------------------------------- 热区/冷区


def fingerprint(store: DatasetStore) -> dict[str, str]:
    """三张权威层表的逐行指纹；List 列不能写 CSV，故用规范化 repr。"""
    out: dict[str, str] = {}
    for name, frame in (
        ("observations", store.read_all("observations")),
        ("silver", store.read_all("silver")),
        ("current", state.read_current(store)),
    ):
        columns = sorted(frame.columns)
        keys = [column for column in ("id", "snapshot_date") if column in frame.columns]
        payload = repr(frame.select(columns).sort(keys).to_dict(as_series=False))
        out[name] = hashlib.sha256(payload.encode()).hexdigest()[:16]
    return out


def seed_long(store: DatasetStore) -> None:
    """造超过重放窗口的数据，冷区与热区都含属性变更与新增。"""
    for number in range(1, REPLAY_WINDOW_DAYS + 4):
        day = date(2026, 11, number)
        first = record(1, 100 + number * 10)
        first["sha"] = f"sha-{number // 3}"
        records = [first, record(2, 50)]
        if number >= REPLAY_WINDOW_DAYS:
            records.append(record(3, 7))
        raw.write(
            store,
            snapshot_date=day,
            page_index=1,
            records=records,
            meta={},
            written_at=f"2026-11-{number:02d}T00:00:00.000000+00:00",
        )
        derive_day(store, day)


def test_局部重放与全量重放及逐日结果三向一致(tmp_path: Path) -> None:
    """核心不变式：冷区权威层保留后，只重放热区不得改变任何一条记录。"""
    store = DatasetStore(tmp_path / "data")
    seed_long(store)
    baseline = fingerprint(store)

    full = rebuild_from_raw(store, window_days=None)

    assert full.mode == "full"
    assert len(full.dates) == REPLAY_WINDOW_DAYS + 3
    assert fingerprint(store) == baseline

    partial = rebuild_from_raw(store)

    assert partial.mode == "window"
    assert len(partial.dates) == REPLAY_WINDOW_DAYS
    assert fingerprint(store) == baseline


def test_权威层缺失时自动回退全量重放(tmp_path: Path) -> None:
    """冷区权威层已损坏时，局部重放会静默丢历史，必须退回全量。"""
    store = DatasetStore(tmp_path / "data")
    seed_long(store)
    baseline = fingerprint(store)
    shutil.rmtree(store.state_dir)

    summary = rebuild_from_raw(store)

    assert summary.mode == "recovery"
    assert len(summary.dates) == REPLAY_WINDOW_DAYS + 3
    assert fingerprint(store) == baseline


def test_非法窗口立即报错(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="window_days"):
        rebuild_from_raw(DatasetStore(tmp_path / "data"), window_days=0)


# ---------------------------------------------------------------- 定点重放
#
# Raw 被重写（归档重分片、补采追加、手工修数据）之后，派生层不会自己更新。
# 定点重放是修法：调用方只给起始日，后缀由 rebuild 自己算——曾经它收一个现成的
# 日期列表，于是「漏了中间几天」在类型上写得出来，而后果是 current 静默回退。


def test_重放范围必须后缀闭合(store: DatasetStore) -> None:
    assert replay_suffix(store, DAY2) == [DAY2]
    assert replay_suffix(store, DAY1) == [DAY1, DAY2]


def test_定点重放自己补到最新一天(store: DatasetStore) -> None:
    """给一个起始日，就要一直放到最新 Raw 日——这是调用方唯一能表达的东西。

    曾经调用方传的是日期列表，`days=[DAY1]` 也收：结果 DAY1 之后那天的属性变更
    被吃掉、`current` 退回 DAY1，两个数字都变小而没有任何报错。
    """
    summary = rebuild_from_raw(store, since=DAY1)

    assert summary.mode == "target"
    assert summary.dates == [DAY1, DAY2]


def test_只重放最新一天不改动结果(store: DatasetStore) -> None:
    """最新日收尾时，重放结果与逐日推进逐字节一致——这是定点重放合法的最小形。"""
    expected_current = state.read_current(store)
    expected_obs = store.read_all("observations")

    summary = rebuild_from_raw(store, since=DAY2)

    assert summary.mode == "target"
    assert summary.dates == [DAY2]
    assert state.read_current(store).equals(expected_current)
    assert store.read_all("observations").equals(expected_obs)


def test_定点重放只清自己的分区(tmp_path: Path) -> None:
    """冷区分区与更早日的热区分区都不动：它们是权威层，清掉重建不回来。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, 100)])
    seed(store, DAY2, [record(1, 180)])

    rebuild_from_raw(store, since=DAY2)

    assert store.path("silver", DAY1).exists()


def test_起始日之后没有_Raw_时什么都不做(store: DatasetStore) -> None:
    expected = state.read_current(store)

    summary = rebuild_from_raw(store, since=date(2026, 11, 3))

    assert summary.dates == []
    assert state.read_current(store).equals(expected)


def test_无戳记时定格到最早一天(tmp_path: Path) -> None:
    """机制上线首轮没有凭证，必须从头建立基线——只补最新一天会留下无凭证的旧日。"""
    store = DatasetStore(tmp_path / "data")
    raw.write(store, snapshot_date=DAY1, page_index=1, records=[record(1, 100)], meta={})
    raw.write(store, snapshot_date=DAY2, page_index=1, records=[record(1, 180)], meta={})
    lineage_path(store).unlink(missing_ok=True)

    assert divergent_since(store) == DAY1


def test_全部对齐时不定格(store: DatasetStore) -> None:
    assert divergent_since(store) is None


def test_cli定点重放修好被重写的Raw(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, 100), record(2, 50)])
    seed(store, DAY2, [record(1, 180), record(2, 50)])
    # 补一块新分块，但没有触发派生：Raw 新、派生旧。
    raw.write(store, snapshot_date=DAY2, page_index=2, records=[record(3, 10)], meta={})
    assert divergent_since(store) == DAY2

    code = main(["--data-dir", str(store.root), "build", "--stale"])

    assert code == 0
    assert divergent_since(store) is None
    assert "acme/ds-3" in set(state.read_current(store)["id"].to_list())


def test_cli全量与定点互斥(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, 100)])

    code = main(["--data-dir", str(store.root), "build", "--full", "--stale"])

    assert code == 1
    assert [item for item in (store.root / "runs").glob("*build*")]


# ------------------------------------------------------- 全量重建的状态与凭证留存


def test_全量重建逐id保留全部非在架状态(tmp_path: Path) -> None:
    """`--full` 会清掉 current 再按天重放；失效状态来自探测、不在 Raw 里。

    与局部重放不同的分支：全量模式清空全部 RAW/STATE 表并作废 state/ 会话文件，
    留存协议必须照样把三种非在架状态逐 id 还原，且未标记过的库仍为在架。
    """
    store = DatasetStore(tmp_path / "data")
    records = [record(index, index * 10) for index in range(1, 5)]
    seed(store, DAY1, records)
    seed(store, DAY2, records)
    before = {
        "acme/ds-1": "disappeared",
        "acme/ds-2": "restricted",
        "acme/ds-3": "renamed",
    }
    state.mark_status(store, before)

    summary = rebuild_from_raw(store, window_days=None)

    assert summary.mode == "full"
    current = state.read_current(store)
    after = dict(zip(current["id"].to_list(), current["status"].to_list(), strict=True))
    assert {dataset_id: after[dataset_id] for dataset_id in before} == before
    assert after["acme/ds-4"] == "active"


def test_全量重建只清会话不清派生凭证(store: DatasetStore) -> None:
    """`state/*.json` 通配不该连 `lineage.json` 一起删；冷区戳记只此一份。

    冷区日期的 Raw 已移出主目录、不参与重放，它们的派生凭证不会被重放过程
    重新写入——删了就是永久丢失，且 `divergent_since` 读不到也不会报错。
    """
    from hf_data_crawl.pipeline import lineage

    cold_day = date(2026, 10, 31)
    stamps = lineage.read(store)
    stamps[cold_day] = lineage.DayStamp(
        source="cold-baseline",
        derived_at="2026-10-31T00:00:00+00:00",
        bronze_rows=0,
        silver_rows=0,
        observations=0,
    )
    lineage._write(store, stamps)
    checkpoints = ("collect-checkpoint.json", "reconcile-checkpoint.json")
    for name in checkpoints:
        (store.state_dir / name).write_text("{}", encoding="utf-8")

    rebuild_from_raw(store, window_days=None)

    assert cold_day in lineage.read(store)  # 该留的留
    for name in checkpoints:
        assert not (store.state_dir / name).exists()  # 该清的照样清


# ---------------------------------------------------------------- 冷区与 --full
# `--full` 的名字承诺「按全部 Raw 重放」，而 `compact` 每日把满 7 天的分块移去
# `data/backup/raw/`。冷区日期的 silver / observations 一旦被清空就再也补不回来，
# 故入口先校验；recovery 回退不能拒，但必须点名救不回来的日期。


def _compacted(store: DatasetStore, day: date) -> None:
    """按真实路径压缩某日：分块移入 backup/raw，日期进冷区清单。"""
    from hf_data_crawl.ops.compact import compact

    assert compact(store, [day]) is not None


def test_全量重建在冷区缺口时被拒绝(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """拒绝必须发生在清空之前，否则「拒绝」只是事后报丧。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, 100), record(2, 50), record(3, 7)])
    seed(store, DAY2, [record(1, 180), record(2, 50)])
    _compacted(store, DAY1)
    before = state.read_current(store).height
    before_obs = store.read_all("observations").height

    code = main(["--data-dir", str(store.root), "build", "--full"])

    out = capsys.readouterr().out
    assert code == 1
    assert "压入冷区" in out
    assert "--allow-cold-loss" in out
    # 一行未动：current 与观测分区都还是压缩前的样子
    assert state.read_current(store).height == before
    assert store.read_all("observations").height == before_obs


def test_逃生阀放行并写明损失(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """明知故犯也得留字据：放行后仍要印出「冷区 N 天未参与重放」。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, 100), record(2, 50), record(3, 7)])
    seed(store, DAY2, [record(1, 180), record(2, 50)])
    _compacted(store, DAY1)

    code = main(["--data-dir", str(store.root), "build", "--full", "--allow-cold-loss"])

    out = capsys.readouterr().out
    assert code == 0
    assert "已按 --allow-cold-loss 放行" in out
    assert "冷区 1 天未参与重放" in out
    assert state.read_current(store).height == 2  # ds-3 只在 11-01 出现过


def test_recovery回退点名冷区缺口(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """权威层坏了只能按仅存 Raw 抢救，但得让人知道抢救回了多少。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, 100), record(2, 50)])
    seed(store, DAY2, [record(1, 180), record(2, 50)])
    _compacted(store, DAY1)
    store.path("observations", DAY2).unlink()
    store.path("current").unlink()

    summary = rebuild_from_raw(store, window_days=REPLAY_WINDOW_DAYS)

    out = capsys.readouterr().out
    assert summary.mode == "recovery"
    assert summary.cold_gaps == 1
    assert "冷区 1 天没有 Raw 可重放" in out
    assert "无法恢复" in out


def test_状态写回丢弃时点名(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """`mark_status` 只改 current 里已有的行；丢掉的 id 必须被喊出来。

    直调 API 而非 CLI，是因为 CLI 已在入口拒绝——这条守的是「即使放行也不沉默」。
    """
    from hf_data_crawl.pipeline.rebuild import retain

    store = DatasetStore(tmp_path / "data")
    seed(store, DAY1, [record(1, 100), record(2, 50), record(3, 7)])
    seed(store, DAY2, [record(1, 180), record(2, 50)])
    state.mark_status(store, {"acme/ds-3": "disappeared"})
    retain(store)  # 快照落盘：ds-3 → disappeared
    _compacted(store, DAY1)

    summary = rebuild_from_raw(store, window_days=None)

    out = capsys.readouterr().out
    assert summary.cold_gaps == 1
    assert "状态写回丢了 1 个 id" in out
    assert "acme/ds-3" in out
