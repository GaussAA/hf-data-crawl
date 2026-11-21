"""README 全文表的契约测试。

钉住四件事：

1. 四种状态不能混——`ok` / `absent`（仓库没有）/ `denied`（读不到）/
   `error`（这次失败），把后三种都读成「空描述」是这张表最容易骗人的地方；
2. 重试与限流——429 听 `Retry-After`，临时失败不落库、下次自然重来；
3. 不重复烧请求——已存 ok/absent 跳过，`refresh` 与 `retry_denied` 才重抓；
4. 全文不覆盖旧值、也不进默认宽表——一次瞬时失败不该抹掉几 KB 正文。

其余（`NETWORK` 表身份、视图可查）是「加一张表不该弄坏别处」的护栏。
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.acquire.batch import staged_shards
from hf_data_crawl.acquire.commands import run_parse_readme
from hf_data_crawl.acquire.hf.api import HfResponse, TransportError
from hf_data_crawl.acquire.merge import keep_ok_rows
from hf_data_crawl.acquire.readme import (
    README_MERGE,
    README_STAGING_DIR,
    HfReadmeApi,
    RateLimiter,
    ReadmeFetcher,
)
from hf_data_crawl.ops.cli import SELF_LOCKED_COMMANDS, WRITE_COMMANDS, main
from hf_data_crawl.pipeline import raw
from hf_data_crawl.pipeline.derive import derive_day
from hf_data_crawl.platform.runs import read_runs
from hf_data_crawl.platform.store import DatasetStore
from hf_data_crawl.platform.tables import README_SCHEMA, Origin, table
from hf_data_crawl.query.gold import GoldCatalog, wide_column_names

DAY = date(2026, 11, 7)
BODY = "# 数据集甲\n\n正文第一段。\n".encode()


@dataclass(frozen=True)
class RateLimited:
    """带 `retry-after` 响应头的 429。"""

    retry_after: float


class FakeReadmeTransport:
    """按 dataset id 回放预设响应，并记录调用。

    `tuple` 表示按调用次序回放的序列；`bytes` 是 200 正文；`int` 是状态码；
    `Exception` 直接抛出（模拟连接失败）。未预设的 id 一律 404。
    """

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[str] = []
        self.urls: list[str] = []
        self.headers: list[dict[str, str]] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        dataset_id = url.split("/datasets/", 1)[-1].removesuffix("/raw/main/README.md")
        self.calls.append(dataset_id)
        self.urls.append(url)
        self.headers.append(dict(headers))
        value = self.responses.get(dataset_id, 404)
        if isinstance(value, tuple):
            value = value[min(self.calls.count(dataset_id) - 1, len(value) - 1)]
        if isinstance(value, Exception):
            raise value
        if isinstance(value, RateLimited):
            return HfResponse(status=429, headers={"retry-after": str(value.retry_after)}, body=b"")
        if isinstance(value, int):
            return HfResponse(status=value, headers={}, body=b"")
        return HfResponse(status=200, headers={}, body=value)


def api(
    transport: FakeReadmeTransport,
    *,
    max_retries: int = 2,
    slept: list[float] | None = None,
    rate: float = 0,
) -> HfReadmeApi:
    """测试默认不限速：真实配额由 `RateLimiter` 的单测覆盖，这里只测抓取逻辑。"""
    return HfReadmeApi(
        transport=transport,
        max_retries=max_retries,
        sleeper=(slept.append if slept is not None else (lambda _: None)),
        rate=rate,
    )


def fetcher(transport: FakeReadmeTransport, *, workers: int = 2) -> ReadmeFetcher:
    return ReadmeFetcher(api=api(transport), workers=workers)


def seed(store: DatasetStore, count: int = 3) -> None:
    records = [
        {
            "id": f"acme/ds-{index}",
            "author": "acme",
            "lastModified": "2026-11-01T00:00:00.000Z",
            "createdAt": "2025-01-01T00:00:00.000Z",
            "sha": f"sha-{index}",
            "downloads": index * 100,
            "downloadsAllTime": index * 1000,
            "likes": index,
            "gated": False,
            "private": False,
            "description": "d",
            "tags": ["license:mit"],
        }
        for index in range(1, count + 1)
    ]
    raw.write(store, snapshot_date=DAY, page_index=1, records=records, meta={})
    derive_day(store, DAY)


@pytest.fixture()
def store_with_data(tmp_path: Path) -> DatasetStore:
    store = DatasetStore(tmp_path / "data")
    seed(store)
    return store


# ---------------------------------------------------------------- 抓取口径


def test_抓到全文并落ok行() -> None:
    transport = FakeReadmeTransport({"acme/x": BODY})

    result = api(transport).fetch("acme/x", DAY, "sha-x")

    assert result.status == "ok"
    row = result.row
    assert row["text"] == BODY.decode()
    assert row["bytes"] == len(BODY)
    assert row["source_sha"] == "sha-x"
    assert row["url"] == "https://huggingface.co/datasets/acme/x"
    assert set(row) == set(README_SCHEMA)
    assert result.payload_bytes == len(BODY)


def test_请求走raw文件通道() -> None:
    transport = FakeReadmeTransport({"acme/x": BODY})

    api(transport).fetch("acme/x", DAY)

    assert transport.urls == ["https://huggingface.co/datasets/acme/x/raw/main/README.md"]


@pytest.mark.parametrize(
    ("status", "expected"),
    [(404, "absent"), (401, "denied"), (403, "denied")],
)
def test_没有README与读不到分开记账(status: int, expected: str) -> None:
    transport = FakeReadmeTransport({"acme/x": status})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == expected
    assert result.row["status"] == expected
    assert result.row["text"] is None
    assert result.row["bytes"] is None


def test_非utf8正文替换而不是丢行() -> None:
    transport = FakeReadmeTransport({"acme/x": b"\xff\xfe# \xe6\xad\xa3\xe6\x96\x87"})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok", "能读到正文就落库，替换字符比整行丢失诚实"
    assert "\ufffd" in result.row["text"]
    assert "正文" in result.row["text"]


def test_限流按retry_after退避后成功() -> None:
    transport = FakeReadmeTransport({"acme/x": (RateLimited(7.5), BODY)})
    slept: list[float] = []

    result = api(transport, slept=slept).fetch("acme/x", DAY)

    assert result.status == "ok"
    assert slept == [7.5]


def test_重试耗尽落临时失败() -> None:
    transport = FakeReadmeTransport({"acme/x": (503, 503, 503)})
    slept: list[float] = []

    result = api(transport, max_retries=2, slept=slept).fetch("acme/x", DAY)

    assert result.status == "error"
    assert transport.calls.count("acme/x") == 3
    assert len(slept) == 2, "两次重试各退避一次，第三次失败直接收工"


def test_连接失败也走重试() -> None:
    transport = FakeReadmeTransport({"acme/x": (TransportError("boom"), BODY)})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok"


def test_限速器按间隔放行() -> None:
    """配额是「5000 次 / 5 分钟」，靠撞 429 后退避会反复撞线；匀速才跑得完。"""
    now = [0.0]
    sleeps: list[float] = []
    limiter = RateLimiter(10, clock=lambda: now[0], sleep=sleeps.append)

    limiter.acquire()
    now[0] = 0.05
    limiter.acquire()

    assert sleeps == [pytest.approx(0.05)]


def test_限速为零时不等待() -> None:
    sleeps: list[float] = []
    limiter = RateLimiter(0, clock=lambda: 0.0, sleep=sleeps.append)

    limiter.acquire()
    limiter.acquire()

    assert sleeps == []
    assert not limiter.enabled


def test_每次请求都先买票() -> None:
    transport = FakeReadmeTransport({"acme/x": BODY})
    readme_api = HfReadmeApi(transport=transport, rate=0, sleeper=lambda _: None)

    class Spy:
        calls = 0

        def acquire(self) -> None:
            Spy.calls += 1

    readme_api._limiter = Spy()  # type: ignore[assignment]  # noqa: SLF001 - 只想确认调用次序，不重建限速器
    readme_api.fetch("acme/x", DAY)

    assert Spy.calls == 1


# ---------------------------------------------------------------- 重复请求


def existing_frame(*rows: dict[str, Any]) -> pl.DataFrame:
    return pl.DataFrame(list(rows), schema=README_SCHEMA)


def test_已存ok与absent跳过_denied也要开关() -> None:
    transport = FakeReadmeTransport({})
    existing = existing_frame(
        {"id": "acme/a", "status": "ok"},
        {"id": "acme/b", "status": "absent"},
        {"id": "acme/c", "status": "denied"},
    )

    fetcher(transport).run(DAY, dataset_ids=["acme/a", "acme/b", "acme/c"], existing=existing)

    assert transport.calls == [], "三种状态都算已问过，默认不该再烧请求"


def test_重试开关让denied重抓() -> None:
    transport = FakeReadmeTransport({"acme/c": BODY})
    existing = existing_frame({"id": "acme/c", "status": "denied"})

    summary, frame = fetcher(transport).run(
        DAY, dataset_ids=["acme/c"], existing=existing, retry_denied=True
    )

    assert transport.calls == ["acme/c"]
    assert summary.fetched == 1
    assert frame["status"].to_list() == ["ok"]


def test_refresh里的id一律重抓() -> None:
    transport = FakeReadmeTransport({"acme/a": BODY})
    existing = existing_frame({"id": "acme/a", "status": "ok", "source_sha": "old"})

    summary, _ = fetcher(transport).run(
        DAY, dataset_ids=["acme/a"], existing=existing, refresh={"acme/a"}
    )

    assert transport.calls == ["acme/a"]
    assert summary.fetched == 1


# ---------------------------------------------------------------- 陈旧与覆盖


def test_临时失败不覆盖已拿到的全文() -> None:
    previous = existing_frame({"id": "acme/a", "status": "ok", "text": "正文"})
    fresh = existing_frame(
        {"id": "acme/a", "status": "error"},
        {"id": "acme/b", "status": "absent"},
    )

    kept = keep_ok_rows(previous, fresh, degraded=README_MERGE.degraded)

    assert kept["id"].to_list() == ["acme/b"], "error 行被挡下，absent 是明确结论可以覆盖"


def test_没有旧行时照常写入() -> None:
    fresh = existing_frame({"id": "acme/a", "status": "error"})

    assert keep_ok_rows(existing_frame(), fresh, degraded=README_MERGE.degraded).height == 1


def test_流式抓取不把全文留在内存() -> None:
    transport = FakeReadmeTransport({f"acme/r{index}": BODY for index in range(5)})
    flushed: list[pl.DataFrame] = []

    summary, frame = fetcher(transport).run(
        DAY,
        dataset_ids=[f"acme/r{index}" for index in range(5)],
        flush=flushed.append,
        flush_every=2,
    )

    assert frame.height == 0, "有 flush 时结果只落盘，不在内存里攒"
    assert sum(item.height for item in flushed) == 5, "尾巴不足一批也要写出去"
    assert summary.fetched == 5
    assert summary.downloaded_bytes == 5 * len(BODY)
    assert summary.table_rows == 5, "流式模式下也要如实记本轮写了多少行"


# ---------------------------------------------------------------- 端到端


def test_readme表是网络表且重建保留() -> None:
    assert table("readme").origin is Origin.NETWORK
    assert table("readme").path == "enrich/readme.parquet"


def test_fetcher_fetch_readme属于写命令且自排锁() -> None:
    assert "fetch-readme" in WRITE_COMMANDS
    assert "fetch-readme" in SELF_LOCKED_COMMANDS


def test_parse_readme也自排锁() -> None:
    """`parse-readme` 必须走自排锁，否则 `_execute` 与内部 `exclusive` 自锁死锁。

    2026-11-18 实测：忘了登记，78 万条解析二十多分钟后以
    `LockBusy: ... 被另一个写命令占用：parse-readme（自己）` 收场——锁是被自己
    持有的，命令却一直在等它。解析全量正文要二十几分钟，全程持锁还会挡住采集。
    """
    assert "parse-readme" in WRITE_COMMANDS
    assert "parse-readme" in SELF_LOCKED_COMMANDS


def test_命令行抓取并入表并留下运行摘要(store_with_data: DatasetStore) -> None:
    transport = FakeReadmeTransport({"acme/ds-3": BODY, "acme/ds-2": 404})

    code = main(
        ["--data-dir", str(store_with_data.root), "fetch-readme", "--top", "2"],
        transport=transport,
    )

    assert code == 0
    frame = store_with_data.read("readme")
    assert frame.height == 2
    statuses = dict(zip(frame["id"].to_list(), frame["status"].to_list()))
    assert statuses == {"acme/ds-3": "ok", "acme/ds-2": "absent"}
    assert staged_shards(store_with_data, README_STAGING_DIR) == [], "合并后不该留下分片"
    runs = [item for item in read_runs(store_with_data) if item.get("command") == "fetch-readme"]
    assert runs and runs[-1]["status"] == "success"


def test_命令行只抓指定ids(store_with_data: DatasetStore) -> None:
    transport = FakeReadmeTransport({"acme/ds-1": BODY})

    code = main(
        [
            "--data-dir",
            str(store_with_data.root),
            "fetch-readme",
            "--ids",
            "acme/ds-1,acme/ds-3",
        ],
        transport=transport,
    )

    assert code == 0
    assert set(transport.calls) == {"acme/ds-1", "acme/ds-3"}
    assert set(store_with_data.read("readme")["id"].to_list()) == {"acme/ds-1", "acme/ds-3"}


def test_命令行changed会带上还没有结论的库(store_with_data: DatasetStore) -> None:
    """修复前 `--changed` 只看已有记录，readme 表为空时整轮零请求。"""
    transport = FakeReadmeTransport({"acme/ds-1": BODY})

    code = main(
        ["--data-dir", str(store_with_data.root), "fetch-readme", "--changed", "5"],
        transport=transport,
    )

    assert code == 0
    # seed 造了 3 个库、readme 表初始为空，三个都算缺口。
    assert set(transport.calls) == {"acme/ds-1", "acme/ds-2", "acme/ds-3"}
    assert store_with_data.read("readme").height == 3


def test_命令行重试denied要真的发请求(store_with_data: DatasetStore) -> None:
    """`denied` 是结论，不算缺口；不显式点进候选，`--retry-denied` 就是空转。"""
    store_with_data.upsert(
        "readme",
        pl.DataFrame(
            [
                {
                    "id": "acme/ds-1",
                    "url": "https://huggingface.co/datasets/acme/ds-1",
                    "snapshot_date": DAY,
                    "source_sha": "sha-1",
                    "status": "denied",
                    "bytes": 0,
                    "text": None,
                }
            ],
            schema=README_SCHEMA,
        ),
    )
    transport = FakeReadmeTransport({"acme/ds-1": BODY})

    code = main(
        [
            "--data-dir",
            str(store_with_data.root),
            "fetch-readme",
            "--changed",
            "5",
            "--retry-denied",
        ],
        transport=transport,
    )

    assert code == 0
    assert "acme/ds-1" in transport.calls, "denied 被点名重取，而不是按结论跳过"


def test_只剩暂存分片时也要合并(store_with_data: DatasetStore) -> None:
    """上一轮抓完却没合并（撞锁、被杀）：本轮没有候选，但分片必须并进表。

    否则它们永远滞留——分片里的行已经算「有结论」，缺口是 0，每轮都提前返回。
    """
    staging = store_with_data.root / ".staging" / "readme"
    staging.mkdir(parents=True)
    pl.DataFrame(
        [
            {
                "id": f"acme/ds-{index}",
                "url": f"https://huggingface.co/datasets/acme/ds-{index}",
                "snapshot_date": DAY,
                "source_sha": f"sha-{index}",
                "status": "ok",
                "bytes": 5,
                "text": "正文",
            }
            for index in (1, 2, 3)
        ],
        schema=README_SCHEMA,
    ).write_parquet(staging / "shard-0000.parquet")

    code = main(
        ["--data-dir", str(store_with_data.root), "fetch-readme", "--changed", "5"],
        transport=FakeReadmeTransport({}),
    )

    assert code == 0
    assert store_with_data.read("readme").height == 3
    assert staged_shards(store_with_data, README_STAGING_DIR) == []


def test_全文视图可查但不进默认宽表(store_with_data: DatasetStore) -> None:
    store_with_data.upsert(
        "readme",
        pl.DataFrame(
            [
                {
                    "id": "acme/ds-1",
                    "url": "https://huggingface.co/datasets/acme/ds-1",
                    "snapshot_date": DAY,
                    "source_sha": "sha-1",
                    "status": "ok",
                    "bytes": 10,
                    "text": "全文",
                }
            ],
            schema=README_SCHEMA,
        ),
    )

    catalog = GoldCatalog(store_with_data)
    try:
        rows = catalog.run("SELECT id, text FROM readme ORDER BY id")
    finally:
        catalog.close()

    assert rows["text"].to_list() == ["全文"]
    assert "readme_text" not in wide_column_names(), "全文不进宽表：每行数 KB，会把查询拖重"


# ------------------------------------------------- parse-readme：逐批读正文


def _readme_row(dataset_id: str, status: str, text: str | None, sha: str) -> dict[str, Any]:
    return {
        "id": dataset_id,
        "url": f"https://huggingface.co/datasets/{dataset_id}",
        "snapshot_date": DAY,
        "source_sha": sha,
        "status": status,
        "bytes": len(text.encode()) if text else 0,
        "text": text,
    }


def test_解析只处理ok行且抽出侧栏字段(store_with_data: DatasetStore) -> None:
    """`absent` / `denied` 没有正文可抽，不该在派生表里留行。"""
    store = store_with_data
    body = "- **Homepage:** https://example.test/paper\n- **Paper:** [A Paper](https://arxiv.org/abs/1)\n"
    store.write(
        "readme",
        pl.DataFrame(
            [
                _readme_row("acme/ds-1", "ok", body, "sha-1"),
                _readme_row("acme/ds-2", "absent", None, "sha-2"),
                _readme_row("acme/ds-3", "denied", None, "sha-3"),
            ],
            schema=README_SCHEMA,
        ),
    )

    outcome = run_parse_readme(argparse.Namespace(), store)

    assert outcome.payload["parsed"] == 1
    fields = store.read("readme_fields")
    assert fields.height == 1
    assert fields.row(0, named=True)["id"] == "acme/ds-1"
    assert fields.row(0, named=True)["homepage"] == "https://example.test/paper"


def test_解析不再整表读入正文(
    store_with_data: DatasetStore, monkeypatch: pytest.MonkeyPatch
) -> None:
    """整表读入是 OOM 的根源：`readme` 只允许按列读，正文靠逐批读。

    2026-11-18 实测：`parse-readme` 一上来 `store.read("readme")`，78 万条正文
    占 3.6 GB，机器只剩 1.9 GB 时被迫停。这条钉住「别再整表读」——
    改回去这里立刻红。
    """
    store = store_with_data
    store.write(
        "readme",
        pl.DataFrame(
            [_readme_row("acme/ds-1", "ok", "# 标题\n正文\n", "sha-1")], schema=README_SCHEMA
        ),
    )

    called: list[tuple[str, tuple[str, ...] | None]] = []
    original = DatasetStore.read

    def spy(
        self: DatasetStore,
        name: str,
        dt: date | None = None,
        *,
        columns: Any = None,
    ) -> pl.DataFrame:
        called.append((name, tuple(columns) if columns is not None else None))
        return original(self, name, dt, columns=columns)

    monkeypatch.setattr(DatasetStore, "read", spy)

    run_parse_readme(argparse.Namespace(), store)

    readme_calls = [columns for name, columns in called if name == "readme"]
    assert readme_calls, "应当按列读过 readme"
    assert all(columns is not None for columns in readme_calls), "readme 不得整表读入"


def test_解析跳过同id同sha的行(store_with_data: DatasetStore) -> None:
    """保鲜每周会重取几千条；若每次都重抽 78 万条，那 340 秒纯属白烧。

    判据是 `(id, source_sha)`：同 id 同 sha 说明正文没变，抽出来的字段必然相同。
    """
    store = store_with_data
    store.write(
        "readme",
        pl.DataFrame(
            [
                _readme_row("acme/ds-1", "ok", "- **Homepage:** https://a.test\n", "sha-1"),
                _readme_row("acme/ds-2", "ok", "- **Homepage:** https://b.test\n", "sha-2"),
            ],
            schema=README_SCHEMA,
        ),
    )

    first = run_parse_readme(argparse.Namespace(), store)
    second = run_parse_readme(argparse.Namespace(), store)

    assert first.payload["parsed"] == 2
    assert second.payload["parsed"] == 0, "同 id 同 sha 的行不该重抽"
    assert second.payload["skipped"] == 2
    # 跳过不等于丢字段：表里仍是两条，值不变。
    fields = store.read("readme_fields")
    assert fields.height == 2
    assert set(fields["id"].to_list()) == {"acme/ds-1", "acme/ds-2"}


def test_解析在sha变化后重抽该行(store_with_data: DatasetStore) -> None:
    """正文换了版本，旧字段就不再是事实——静默留着比抽错更难查。"""
    store = store_with_data
    store.write(
        "readme",
        pl.DataFrame(
            [_readme_row("acme/ds-1", "ok", "- **Homepage:** https://old.test\n", "sha-old")],
            schema=README_SCHEMA,
        ),
    )
    run_parse_readme(argparse.Namespace(), store)

    store.write(
        "readme",
        pl.DataFrame(
            [_readme_row("acme/ds-1", "ok", "- **Homepage:** https://new.test\n", "sha-new")],
            schema=README_SCHEMA,
        ),
    )
    outcome = run_parse_readme(argparse.Namespace(), store)

    assert outcome.payload["parsed"] == 1
    row = store.read("readme_fields").row(0, named=True)
    assert row["homepage"] == "https://new.test"
    assert row["source_sha"] == "sha-new"
