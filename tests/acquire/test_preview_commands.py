"""`fetch-preview` 命令层的编排：候选怎么选、哪些跳过、何时中止。

`test_preview.py` 覆盖的是「一次响应怎么分类」；这里覆盖的是「一轮跑哪些库、
跑完怎么并入」。两者的错误代价不同：分类错会让某个库重试到天亮，候选选错则是
把配额撒在没有 viewer 的库上（14.25% 的库根本没有），或者漏掉真正该刷的那批。

不起网络：`PreviewFetcher` 换成替身，只验编排。
"""

from __future__ import annotations

from argparse import Namespace
from datetime import date
from pathlib import Path

import polars as pl
import pytest

from hf_data_crawl.acquire import commands
from hf_data_crawl.acquire.preview import PreviewSummary
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import table

SNAPSHOT = date(2026, 11, 19)


def args(**overrides: object) -> Namespace:
    base: dict[str, object] = {
        "ids": None,
        "top": 100,
        "rows": 5,
        "config": None,
        "split": None,
        "rate": 10.0,
        "throttle_wait": 0.0,
        "workers": 1,
        "flush_every": 200,
        "date": SNAPSHOT.isoformat(),
    }
    base.update(overrides)
    return Namespace(**base)


def seed(store: DatasetStore, name: str, rows: list[dict[str, object]]) -> None:
    """按表的真实 schema 补空列：`upsert` 要求整列对齐，少一列就报 ColumnNotFound。"""
    store.upsert(name, pl.DataFrame(rows, schema=dict(table(name).schema)))


class FakeFetcher:
    """替身：记下候选，返回给定的摘要与帧。"""

    def __init__(self, summary: PreviewSummary, frame: pl.DataFrame) -> None:
        self.summary = summary
        self.frame = frame
        self.seen: list[str] = []

    def run(
        self,
        snapshot: date,
        candidates: list[str],
        *,
        flush: object = None,
        flush_every: int = 200,
    ) -> tuple[PreviewSummary, pl.DataFrame]:
        self.seen = list(candidates)
        self.flush = flush
        self.flush_every = flush_every
        return self.summary, self.frame


def patch_fetcher(
    monkeypatch: pytest.MonkeyPatch,
    *,
    summary: PreviewSummary | None = None,
    frame: pl.DataFrame | None = None,
) -> FakeFetcher:
    fake = FakeFetcher(
        summary or PreviewSummary(fetched=1, elapsed_seconds=0.5),
        frame if frame is not None else pl.DataFrame([], schema=dict(table("preview").schema)),
    )
    monkeypatch.setattr(commands, "PreviewFetcher", lambda **_kwargs: fake)
    monkeypatch.setattr(commands, "_open_transport", lambda _transport: None)
    return fake


# ---------------------------------------------------------------- 候选选择


def test_没有采集数据时拒绝并指出先跑什么(tmp_path: Path) -> None:
    store = DatasetStore(tmp_path / "data")

    outcome = commands.run_fetch_preview(args(), store, None)

    assert outcome.code == 1
    assert outcome.reason is not None and "尚无采集数据" in outcome.reason


def test_size表没有ok记录时提示先跑规模(tmp_path: Path, capsys) -> None:
    """没有 viewer 的库连请求都不该发；筛不出候选时要说清是上游没跑，不是没数据。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, "current", [{"id": "a/x", "downloads": 10}])
    seed(store, "size", [{"id": "a/x", "status": "error"}])

    outcome = commands.run_fetch_preview(args(), store, None)

    assert outcome.code == 0
    assert outcome.payload == {"fetched": 0}
    assert "先跑 `fetch-sizes`" in capsys.readouterr().out


def test_已定局的库不再发请求(tmp_path: Path, monkeypatch, capsys) -> None:
    """`ok` 与 `unavailable` 都是终局。不跳过的话，没有 viewer 的库每天都被问一遍。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, "current", [{"id": "a/x", "downloads": 20}, {"id": "a/y", "downloads": 10}])
    seed(store, "size", [{"id": "a/x", "status": "ok"}, {"id": "a/y", "status": "ok"}])
    seed(store, "preview", [{"id": "a/x", "status": "ok", "snapshot_date": SNAPSHOT}])
    fake = patch_fetcher(monkeypatch)

    commands.run_fetch_preview(args(), store, None)

    assert fake.seen == ["a/y"]
    assert "跳过 1 个已定局的库" in capsys.readouterr().out


def test_全部定局时一次请求都不发(tmp_path: Path, monkeypatch, capsys) -> None:
    store = DatasetStore(tmp_path / "data")
    seed(store, "current", [{"id": "a/x", "downloads": 20}])
    seed(store, "size", [{"id": "a/x", "status": "ok"}])
    seed(store, "preview", [{"id": "a/x", "status": "unavailable", "snapshot_date": SNAPSHOT}])
    fake = patch_fetcher(monkeypatch)

    outcome = commands.run_fetch_preview(args(), store, None)

    assert fake.seen == []
    assert outcome.payload == {"fetched": 0}
    assert "无需抓取" in capsys.readouterr().out


def test_指定ids时只抓点名的库(tmp_path: Path, monkeypatch) -> None:
    store = DatasetStore(tmp_path / "data")
    seed(store, "current", [{"id": "a/x", "downloads": 20}, {"id": "a/y", "downloads": 10}])
    seed(store, "size", [{"id": "a/x", "status": "ok"}, {"id": "a/y", "status": "ok"}])
    fake = patch_fetcher(monkeypatch)

    commands.run_fetch_preview(args(ids="a/y"), store, None)

    assert fake.seen == ["a/y"]


def test_指定的ids都不存在时拒绝(tmp_path: Path) -> None:
    """「跑完了 0 行、状态 success」比报错更容易被当成正常结果。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, "current", [{"id": "a/x", "downloads": 20}])

    outcome = commands.run_fetch_preview(args(ids="b/nope"), store, None)

    assert outcome.code == 1
    assert outcome.reason is not None and "都不存在" in outcome.reason


# ---------------------------------------------------------------- 并入与中止


def test_抓到的行并入preview表(tmp_path: Path, monkeypatch) -> None:
    """抓取在锁外，只有并入这一小段独占：不并入等于白抓。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, "current", [{"id": "a/x", "downloads": 20}])
    seed(store, "size", [{"id": "a/x", "status": "ok"}])
    got = pl.DataFrame(
        [{"id": "a/x", "status": "ok", "snapshot_date": SNAPSHOT, "total_rows": 3}],
        schema=dict(table("preview").schema),
    )
    patch_fetcher(monkeypatch, summary=PreviewSummary(fetched=1), frame=got)

    outcome = commands.run_fetch_preview(args(), store, None)

    assert outcome.code == 0
    assert store.read("preview").height == 1


def test_被限流中止时退出码非零且给出降速建议(tmp_path: Path, monkeypatch, capsys) -> None:
    """中止不等于失败：已抓到的行仍然并入，但这一轮没跑完，退出码要反映出来。"""
    store = DatasetStore(tmp_path / "data")
    seed(store, "current", [{"id": "a/x", "downloads": 20}])
    seed(store, "size", [{"id": "a/x", "status": "ok"}])
    got = pl.DataFrame(
        [{"id": "a/x", "status": "ok", "snapshot_date": SNAPSHOT}],
        schema=dict(table("preview").schema),
    )
    patch_fetcher(monkeypatch, summary=PreviewSummary(fetched=1, aborted=True), frame=got)

    outcome = commands.run_fetch_preview(args(rate=20.0), store, None)

    assert outcome.code == 1
    assert outcome.reason is not None and "被限流" in outcome.reason
    out = capsys.readouterr().out
    assert "降低 --rate" in out and "20.0" in out
    assert store.read("preview").height == 1, "已抓到的行不该因为中止被丢掉"


def test_top算的是本轮请求数而不是底库前N名(tmp_path: Path, monkeypatch) -> None:
    """`--top N` 是这一轮最多发多少个请求，不是「底库前 N 名」。

    此前截的是底库前 N 名：已定局的库也占名额，覆盖率过半之后，
    `--top 10000` 可能只发出几百个请求，而命令看上去「跑完了」。
    """
    store = DatasetStore(tmp_path / "data")
    seed(
        store,
        "current",
        [{"id": f"a/{index}", "downloads": 100 - index} for index in range(5)],
    )
    seed(store, "size", [{"id": f"a/{index}", "status": "ok"} for index in range(5)])
    seed(
        store,
        "preview",
        [
            {"id": "a/0", "status": "ok", "snapshot_date": SNAPSHOT},
            {"id": "a/1", "status": "ok", "snapshot_date": SNAPSHOT},
        ],
    )
    fake = patch_fetcher(monkeypatch)

    commands.run_fetch_preview(args(top=3), store, None)

    assert fake.seen == ["a/2", "a/3", "a/4"], "名额该给还没定局的库"


def test_top为零时抓全量而不是一条都不抓(tmp_path: Path, monkeypatch) -> None:
    """`--top 0` = 全量：与 fetch-files / fetch-readme / fetch-sizes 同一个约定。

    预览原先直接切 `[: args.top]`，`0` 会切出空列表——命令看起来「跑完了」，
    其实一条没抓，而且不报错。
    """
    store = DatasetStore(tmp_path / "data")
    seed(
        store,
        "current",
        [{"id": f"a/{index}", "downloads": 100 - index} for index in range(5)],
    )
    seed(store, "size", [{"id": f"a/{index}", "status": "ok"} for index in range(5)])
    fake = patch_fetcher(monkeypatch)

    commands.run_fetch_preview(args(top=0), store, None)

    assert len(fake.seen) == 5


def test_top非零时仍按下载量截断(tmp_path: Path, monkeypatch) -> None:
    """`--top N` 照旧只抓前 N 名——别把「0 表示全量」写成「<=0 都全量」的通融。"""
    store = DatasetStore(tmp_path / "data")
    seed(
        store,
        "current",
        [{"id": f"a/{index}", "downloads": 100 - index} for index in range(5)],
    )
    seed(store, "size", [{"id": f"a/{index}", "status": "ok"} for index in range(5)])
    fake = patch_fetcher(monkeypatch)

    commands.run_fetch_preview(args(top=3), store, None)

    assert fake.seen == ["a/0", "a/1", "a/2"]
