"""规模补齐的行为测试。

这个接口的失败分两类，处理方式必须不同：
- 500/401/501 是「这个数据集没有规模数据」，重试一万次也不会变；
- 429/502/503 是「现在不行」，退避后应当重试。
把两者混为一谈，要么白烧 30% 的请求预算，要么把可救的记录永久丢掉。
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import polars as pl
import pytest

from hf_data_crawl.hf.api import HfResponse, TransportError
from hf_data_crawl.sizes import (
    SIZE_SCHEMA,
    HfSizeApi,
    SizeFetcher,
    SizeSummary,
    tail,
)

DAY = date(2026, 11, 4)


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
    """重试永久缺失等于白烧预算——30% 的候选都是这一类。"""
    transport = FakeSizeTransport({"acme/x": status})

    result = api(transport).fetch("acme/x", DAY)

    assert result.status == "unavailable"
    assert len(transport.calls) == 1


@pytest.mark.parametrize("status", [429, 502, 503, 504])
def test_限流与网关抖动会重试(status: int) -> None:
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
    transport = FakeSizeTransport({"acme/a": 500, "acme/b": payload(3)})

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
