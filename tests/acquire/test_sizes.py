"""规模补齐的行为测试。

这个接口的失败分两类，处理方式必须不同：
- 500/401/501 是「这个数据集没有规模数据」，重试一万次也不会变；
- 429/502/503 是「现在不行」，退避后应当重试。
把两者混为一谈，要么白烧 30% 的请求预算，要么把可救的记录永久丢掉。
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.acquire.hf.api import HfResponse, TransportError
from hf_data_crawl.acquire.sizes import (
    SIZE_SCHEMA,
    HfSizeApi,
    SizeFetcher,
    SizeSummary,
    protect_ok_rows,
    stale_ids,
    tail,
)
from hf_data_crawl.platform.tables import TIMESTAMP

DAY = date(2026, 11, 4)


def _size_row(
    dataset_id: str,
    status: str,
    num_rows: int | None,
    *,
    snapshot: date = DAY,
    fetched_at: datetime | None = None,
) -> dict[str, Any]:
    """一行规模记录（未用到的列留空，与真实写入形状一致）。"""
    return {
        "id": dataset_id,
        "url": f"https://huggingface.co/datasets/{dataset_id}",
        "snapshot_date": snapshot,
        "fetched_at": fetched_at,
        "status": status,
        "num_rows": num_rows,
        "estimated_num_rows": None,
        "num_bytes_original": None,
        "num_bytes_parquet": None,
        "num_bytes_memory": None,
        "config_count": None,
        "num_columns_max": None,
        "configs": None,
    }


def _current_row(dataset_id: str, last_modified: datetime) -> dict[str, Any]:
    """`current` 里陈旧判据要用到的两列。"""
    return {"id": dataset_id, "last_modified": last_modified}


def test_同日再次改动的库会被判成陈旧() -> None:
    """库在被抓取的同一天又改一次：只比日期会永久漏判。

    两侧日期相等时 `last_modified.date() > snapshot_date` 恒不成立；而该行只有
    判据成立才会被重抓，也就不会产生新的 snapshot_date，于是它永久停在旧值，
    连 doctor 的「规模待重取」也看不见（判据与 fetch-sizes --changed 同源）。
    """
    stale = stale_ids(
        pl.DataFrame(
            [_current_row("a/x", datetime(2026, 11, 4, 18, 0, tzinfo=UTC))],
            schema={"id": pl.Utf8, "last_modified": TIMESTAMP},
        ),
        pl.DataFrame(
            [_size_row("a/x", "ok", 100, fetched_at=datetime(2026, 11, 4, 10, 0, tzinfo=UTC))],
            schema=SIZE_SCHEMA,
        ),
    )

    assert stale.height == 1


def test_抓取之后没再改动的不算陈旧() -> None:
    """收紧判据不能把「本来就是最新」的行也拖进来——那会天天白烧配额。"""
    stale = stale_ids(
        pl.DataFrame(
            [_current_row("a/x", datetime(2026, 11, 4, 9, 0, tzinfo=UTC))],
            schema={"id": pl.Utf8, "last_modified": TIMESTAMP},
        ),
        pl.DataFrame(
            [_size_row("a/x", "ok", 100, fetched_at=datetime(2026, 11, 4, 10, 0, tzinfo=UTC))],
            schema=SIZE_SCHEMA,
        ),
    )

    assert stale.height == 0


def test_不可用不覆盖已拿到的数字() -> None:
    """一次瞬时 500 不能把旧行的 num_rows 抹成 null。

    `unavailable` 是「这个数据集没有规模数据」的结论，但 `upsert` 的替换键是整行：
    那条全 null 的 unavailable 行会把旧值一起抹掉，此后 `_resolved_ids` 把该 id
    长期跳过（只认「有没有行」），只有周任务 `--retry-unavailable` 可能救回。
    而一次瞬时 500 恰好落在 `UNAVAILABLE_STATUS` 里。
    """
    previous = pl.DataFrame(
        [
            _size_row("a/ok", "ok", 100),
            _size_row("b/was-bad", "unavailable", None),
            _size_row("c/never", "unavailable", None),
        ],
        schema=SIZE_SCHEMA,
    )
    fresh = pl.DataFrame(
        [
            _size_row("a/ok", "unavailable", None),  # 瞬时失败
            _size_row("b/was-bad", "ok", 200),  # 这次拿到了
            _size_row("c/never", "unavailable", None),  # 仍然没有
        ],
        schema=SIZE_SCHEMA,
    )

    kept = protect_ok_rows(previous, fresh)

    assert kept.filter(pl.col("id") == "a/ok").height == 0, "已拿到数字的 id 不该被降级行覆盖"
    assert kept.filter(pl.col("id") == "b/was-bad")["num_rows"].item() == 200
    assert kept.filter(pl.col("id") == "c/never").height == 1, "从未拿到过的照常写 unavailable"


def test_重试不可用拿到数据时仍然覆盖() -> None:
    """保护的是「ok 被降级」，不是「unavailable 不许变 ok」。"""
    previous = pl.DataFrame([_size_row("a/x", "unavailable", None)], schema=SIZE_SCHEMA)
    fresh = pl.DataFrame([_size_row("a/x", "ok", 7)], schema=SIZE_SCHEMA)

    assert protect_ok_rows(previous, fresh)["num_rows"].item() == 7


def test_没有旧表时原样返回() -> None:
    fresh = pl.DataFrame([_size_row("a/x", "unavailable", None)], schema=SIZE_SCHEMA)

    assert protect_ok_rows(pl.DataFrame(schema=SIZE_SCHEMA), fresh).height == 1


class FakeSizeTransport:
    """按 dataset id 回放预设响应，并记录调用次数。"""

    def __init__(self, responses: dict[str, Any]) -> None:
        self.responses = responses
        self.calls: list[str] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        dataset_id = params["dataset"]
        self.calls.append(dataset_id)
        value = self.responses.get(dataset_id, 404)
        if isinstance(value, list):
            # 列表表示「前几次这样、之后最后一次」，用于测重试。
            index = min(self.calls.count(dataset_id) - 1, len(value) - 1)
            value = value[index]
        if isinstance(value, Exception):
            raise value
        if isinstance(value, int):
            return HfResponse(status=value, headers={}, body=b"{}")
        return HfResponse(
            status=200, headers={}, body=json.dumps(value, ensure_ascii=False).encode()
        )


def payload(num_rows: int, configs: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "size": {
            "dataset": {
                "dataset": "acme/x",
                "num_bytes_original_files": 1000,
                "num_bytes_parquet_files": 900,
                "num_bytes_memory": 5000,
                "num_rows": num_rows,
                "estimated_num_rows": None,
            },
            "configs": configs
            if configs is not None
            else [
                {"config": "default", "num_rows": num_rows, "num_columns": 3},
            ],
        }
    }


def api(transport: FakeSizeTransport) -> HfSizeApi:
    return HfSizeApi(transport=transport, max_retries=2, sleeper=lambda _: None)


# ---------------------------------------------------------------- 解析


def test_成功响应解析出规模与列数() -> None:
    transport = FakeSizeTransport({"acme/x": payload(52002)})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok"
    assert result.row["num_rows"] == 52002
    assert result.row["num_bytes_memory"] == 5000
    assert result.row["num_bytes_original"] == 1000
    assert result.row["config_count"] == 1
    assert result.row["num_columns_max"] == 3


def test_多config取最大列数() -> None:
    transport = FakeSizeTransport(
        {
            "acme/x": payload(
                10,
                [
                    {"config": "a", "num_rows": 5, "num_columns": 2},
                    {"config": "b", "num_rows": 5, "num_columns": 17},
                ],
            )
        }
    )

    row = api(transport).fetch("acme/x", DAY).row

    assert row["config_count"] == 2
    assert row["num_columns_max"] == 17
    assert json.loads(row["configs"])[1]["config"] == "b"


def test_无config时列数为空而不是零() -> None:
    transport = FakeSizeTransport({"acme/x": payload(1, [])})

    row = api(transport).fetch("acme/x", DAY).row

    assert row["config_count"] is None
    assert row["num_columns_max"] is None


def test_畸形响应体记为临时失败() -> None:
    transport = FakeSizeTransport({"acme/x": {"size": "这不是对象"}})

    assert api(transport).fetch("acme/x", DAY).status == "error"


# ---------------------------------------------------------------- 失败分类


@pytest.mark.parametrize("status", [401, 403, 404, 500, 501])
def test_永久缺失不重试(status: int) -> None:
    """重试永久缺失等于白烧预算。

    500/501 在这里是实测结论，不是疏忽：HF 的 `/size` 用它们表示「不支持」，
    抽样 15 个这样的库全部 0.3–0.9 秒返回、无一恢复。挪进 `RETRYABLE_STATUS`
    会让每个库空转 5 次重试 + 退避约 31 秒（1,269 个就是 3.6 小时）。
    """
    transport = FakeSizeTransport({"acme/x": status})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "unavailable"
    assert len(transport.calls) == 1


@pytest.mark.parametrize("status", [429, 502, 503, 504])
def test_限流与网关抖动会重试(status: int) -> None:
    """这些才是真正的瞬时故障。"""
    transport = FakeSizeTransport({"acme/x": [status, payload(7)]})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok"
    assert len(transport.calls) == 2


def test_重试耗尽后记为临时失败() -> None:
    transport = FakeSizeTransport({"acme/x": 503})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "error"
    assert len(transport.calls) == 3  # 首次 + 2 次重试


def test_传输异常也会退避重试() -> None:
    transport = FakeSizeTransport({"acme/x": [TransportError("断流"), payload(9)]})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "ok"
    assert len(transport.calls) == 2


# ---------------------------------------------------------------- 批量


def run_batch(
    transport: FakeSizeTransport, ids: list[str], **kwargs: Any
) -> tuple[SizeSummary, pl.DataFrame]:
    fetcher = SizeFetcher(api=api(transport), workers=4)
    return fetcher.run(DAY, dataset_ids=ids, top=len(ids), **kwargs)


def test_批量并发不丢结果() -> None:
    ids = [f"acme/ds-{index}" for index in range(20)]
    transport = FakeSizeTransport(
        {dataset_id: payload(index) for index, dataset_id in enumerate(ids)}
    )

    summary, frame = run_batch(transport, ids)

    assert summary.requested == 20
    assert summary.fetched == 20
    assert frame.height == 20
    assert sorted(frame["id"].to_list()) == sorted(ids)


def test_已存在的id默认跳过() -> None:
    existing = pl.DataFrame(
        {"id": ["acme/ds-0"], "status": ["ok"]}, schema={"id": pl.Utf8, "status": pl.Utf8}
    ).pipe(lambda frame: frame.with_columns(pl.lit(None, dtype=pl.Date).alias("snapshot_date")))
    transport = FakeSizeTransport({"acme/ds-0": payload(1), "acme/ds-1": payload(2)})

    summary, frame = run_batch(transport, ["acme/ds-0", "acme/ds-1"], existing=existing)

    assert summary.skipped == 1
    assert transport.calls == ["acme/ds-1"]
    assert frame.height == 1


def test_指定的库即使已存在也会重取() -> None:
    """`refresh` 是「问过之后它变了」的唯一表达方式：跳过判据只看有没有行。

    规模是内容的函数。库改了而规模行还在，跳过判据会一直把它当成「已处理」，
    于是行数永远停在改之前——这不是慢半拍，是永久错。
    """
    existing = pl.DataFrame(
        {
            "id": ["acme/ds-0"],
            "status": ["ok"],
            "snapshot_date": [date(2026, 11, 1)],
        }
    )
    transport = FakeSizeTransport({"acme/ds-0": payload(9)})

    summary, frame = run_batch(transport, ["acme/ds-0"], existing=existing, refresh={"acme/ds-0"})

    assert summary.skipped == 0
    assert transport.calls == ["acme/ds-0"]
    assert frame.height == 1


def test_陈旧判据只看内容晚于抓取时刻() -> None:
    """抓取之后没再改动就不算陈旧；只比日期会让「同日二次变更」永久漏判。

    这里给 `acme/same` 一个晚于 `last_modified` 的 `fetched_at`，表示「那次改动
    已经包含在本次抓取里」；`acme/changed` 留空 `fetched_at`（加列之前的旧行），
    判据回退成「该日零点」，所以仍然判陈旧——升级后第一轮会把它补抓一次。
    """
    current = pl.DataFrame(
        {
            "id": ["acme/same", "acme/changed", "acme/new", "acme/no_size"],
            # 底库也有 `status`（active/disappeared）。两张表同名列时，过滤若取到
            # 这一列，判据恒不成立——夹具带它，才钉得住这个坑。
            "status": ["active"] * 4,
            "last_modified": [
                datetime(2026, 11, 3, 8, tzinfo=UTC),
                datetime(2026, 11, 5, 9, tzinfo=UTC),
                datetime(2026, 11, 9, 9, tzinfo=UTC),
                datetime(2026, 11, 6, 9, tzinfo=UTC),
            ],
        },
        schema={"id": pl.Utf8, "status": pl.Utf8, "last_modified": TIMESTAMP},
    )
    existing = pl.DataFrame(
        {
            "id": ["acme/same", "acme/changed", "acme/no_size"],
            "snapshot_date": [date(2026, 11, 3)] * 3,
            "fetched_at": [datetime(2026, 11, 3, 10, tzinfo=UTC), None, None],
            "status": ["ok", "ok", "unavailable"],
        },
        schema={
            "id": pl.Utf8,
            "snapshot_date": pl.Date,
            "fetched_at": TIMESTAMP,
            "status": pl.Utf8,
        },
    )

    stale = stale_ids(current, existing)

    # 新库没有规模行，本就不在这条判据里——它走常规缺口补抓；
    # unavailable 的改了 README 也变不出行数，逐日重烧只是白费预算。
    assert stale["id"].to_list() == ["acme/changed"]


def test_重试开关只跳过已成功的() -> None:
    existing = pl.DataFrame(
        {
            "id": ["acme/ok", "acme/gone"],
            "status": ["ok", "unavailable"],
            "snapshot_date": [DAY, DAY],
        }
    )
    transport = FakeSizeTransport({"acme/ok": payload(1), "acme/gone": payload(2)})

    summary, _ = run_batch(
        transport, ["acme/ok", "acme/gone"], existing=existing, retry_unavailable=True
    )

    assert summary.skipped == 1
    assert transport.calls == ["acme/gone"]


def test_不可用的也落库以免下轮重烧() -> None:
    transport = FakeSizeTransport({"acme/a": 404, "acme/b": payload(3)})

    summary, frame = run_batch(transport, ["acme/a", "acme/b"])

    assert summary.unavailable == 1
    assert frame.height == 2
    assert set(frame["status"].to_list()) == {"ok", "unavailable"}


def test_临时失败不落库以便下次重试() -> None:
    transport = FakeSizeTransport({"acme/a": 503, "acme/b": payload(3)})

    summary, frame = run_batch(transport, ["acme/a", "acme/b"])

    assert summary.failed == 1
    assert frame.height == 1
    assert frame["id"].to_list() == ["acme/b"]


def test_结果表符合schema() -> None:
    transport = FakeSizeTransport({"acme/a": payload(3)})

    _, frame = run_batch(transport, ["acme/a"])

    assert set(frame.columns) == set(SIZE_SCHEMA)


def test_进度回调在终态必有一次() -> None:
    """停在 97% 的进度条比没有进度条更糟。"""
    seen: list[tuple[int, int]] = []
    transport = FakeSizeTransport({"acme/a": payload(1), "acme/b": payload(2)})
    fetcher = SizeFetcher(api=api(transport), workers=2, progress=lambda d, t: seen.append((d, t)))

    fetcher.run(DAY, dataset_ids=["acme/a", "acme/b"], top=2)

    assert seen[-1] == (2, 2)


def test_空候选时不报错() -> None:
    summary, frame = run_batch(FakeSizeTransport({}), [])

    assert summary.requested == 0
    assert frame.height == 0


@pytest.mark.parametrize(
    "count,total,expected", [(0, 10, 0), (5, 10, 50), (10, 10, 100), (3, 0, 0)]
)
def test_进度百分比边界(count: int, total: int, expected: int) -> None:
    assert tail(count, total) == expected


def test_分批落库让中断不只损失全部成果() -> None:
    """十万条跑一小时，只在末尾写一次等于把一小时押在最后一个字节上。"""
    ids = [f"acme/ds-{index}" for index in range(12)]
    transport = FakeSizeTransport({i: payload(index) for index, i in enumerate(ids)})
    batches: list[pl.DataFrame] = []

    summary, frame = SizeFetcher(api=api(transport), workers=4).run(
        DAY, dataset_ids=ids, top=12, flush=batches.append, flush_every=5
    )

    assert len(batches) == 2, "12 条按 5 条一批应交付 2 批，余下 2 条随最终结果返回"
    assert all(batch.height == 5 for batch in batches)
    assert frame.height == 12
    assert summary.fetched == 12


def test_不传flush时行为不变() -> None:
    ids = [f"acme/ds-{index}" for index in range(3)]
    transport = FakeSizeTransport({i: payload(1) for i in ids})

    _, frame = SizeFetcher(api=api(transport), workers=2).run(DAY, dataset_ids=ids, top=3)

    assert frame.height == 3


def test_top为0表示全量而不是写死行数() -> None:
    """周任务用 `--top 0`：取 current 实际行数，底库涨了也不会漏掉长尾。"""
    from hf_data_crawl.acquire.commands import resolve_size_top

    assert resolve_size_top(0, 1_137_878, set()) == 1_137_878
    assert resolve_size_top(20_000, 1_137_878, set()) == 20_000
    # 指定 --changed 时这批一个都不能少，--top 不参与截断。
    assert resolve_size_top(20_000, 8_000, {"acme/a"}) == 8_000
    # 空候选也不能把 0 传给切片（[:0] 会得到空列表）。
    assert resolve_size_top(0, 0, set()) == 1
