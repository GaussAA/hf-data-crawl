"""样例抓取：viewer 的响应分类、单元格截断、成帧。

不打真网：`Transport` 是个 Protocol，换一个按脚本回话的替身即可。要验的是
「哪种状态算永久缺失、哪种算临时失败」——分类错了，重试策略就跟着错。
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any

import pytest

from hf_data_crawl.acquire.hf.api import HfResponse, TransportError
from hf_data_crawl.acquire.preview import (
    MAX_CELL_CHARS,
    TRUNCATION_MARK,
    HfPreviewApi,
    PreviewFetcher,
    Throttled,
)
from hf_data_crawl.platform.tables import PREVIEW_SCHEMA

DAY = date(2026, 11, 17)


class ScriptedTransport:
    """按脚本回话：给 `HfResponse` 就返回，给异常就抛。"""

    def __init__(self, *responses: Any) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls.append(dict(params))
        item = self._responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _ok(*rows: dict[str, Any], total: int = 500) -> HfResponse:
    body = {
        "features": [{"name": "text", "type": {"dtype": "string", "_type": "Value"}}],
        "rows": [{"row_idx": index, "row": row} for index, row in enumerate(rows)],
        "num_rows_total": total,
        "truncated_cells": [],
    }
    return HfResponse(status=200, headers={}, body=json.dumps(body).encode())


def _error(status: int) -> HfResponse:
    return HfResponse(status=status, headers={}, body=b'{"error":"Not found."}')


def _api(transport: ScriptedTransport, **kwargs: Any) -> HfPreviewApi:
    """默认 `config="default"`：这些用例验的是 `/rows` 的处理。

    生产默认已是 `auto`（先问 `/splits` 要真实 config 名），但那会多发一个请求，
    让 `ScriptedTransport` 的脚本错位。config 解析本身由
    `test_自动解析_config_与_split` 单独覆盖。
    """
    kwargs.setdefault("config", "default")
    return HfPreviewApi(transport, rate=0, sleeper=lambda _seconds: None, **kwargs)


def test_取回三行并记下_config_与_split() -> None:
    transport = ScriptedTransport(_ok({"text": "甲"}, {"text": "乙"}, {"text": "丙"}, total=530))
    result = _api(transport).fetch("a/b", DAY)

    assert result.status == "ok"
    row = result.row
    assert row["config"] == "default"
    assert row["split"] == "train"
    assert row["rows_total"] == 530
    assert row["row_count"] == 3
    assert row["truncated"] is False
    assert [item["row"]["text"] for item in json.loads(row["rows"])] == ["甲", "乙", "丙"]
    # 参数必须带上，少一个 `/rows` 就是 422。
    assert transport.calls[0] == {
        "dataset": "a/b",
        "config": "default",
        "split": "train",
        "offset": 0,
        "length": 3,
    }


@pytest.mark.parametrize("status", [401, 404])
def test_没有viewer或受限时记成不可用且不重试(status: int) -> None:
    """401 与 404 都是永久缺失：重试只会把同一个答案再问一遍。"""
    transport = ScriptedTransport(_error(status))
    result = _api(transport).fetch("a/b", DAY)

    assert result.status == "unavailable"
    assert result.row["status"] == "unavailable"
    assert len(transport.calls) == 1


def test_临时失败会退避重试到成功() -> None:
    transport = ScriptedTransport(_error(503), TransportError("boom"), _ok({"text": "甲"}))
    result = _api(transport).fetch("a/b", DAY)

    assert result.status == "ok"
    assert len(transport.calls) == 3


def test_一直临时失败就不落库() -> None:
    """`error` 不落库：可救的别钉成失败，下一轮自然重试。"""
    transport = ScriptedTransport(*[TransportError("boom")] * 4)
    result = _api(transport, max_retries=3).fetch("a/b", DAY)

    assert result.status == "error"
    assert result.row == {}


def test_超长单元格被截断并标出来() -> None:
    long_text = "长" * (MAX_CELL_CHARS + 50)
    transport = ScriptedTransport(_ok({"text": long_text}))
    result = _api(transport).fetch("a/b", DAY)

    stored = json.loads(result.row["rows"])[0]["row"]["text"]
    assert len(stored) == MAX_CELL_CHARS + len(TRUNCATION_MARK)
    assert stored.endswith(TRUNCATION_MARK)
    assert result.row["truncated"] is True


def test_列表列压成字符串再截断() -> None:
    """留着结构，每行大小就不可控；`features` 已经记了类型，值压缩不丢信息。"""
    transport = ScriptedTransport(_ok({"tags": ["标"] * MAX_CELL_CHARS}))
    result = _api(transport).fetch("a/b", DAY)

    stored = json.loads(result.row["rows"])[0]["row"]["tags"]
    assert isinstance(stored, str)
    assert stored.endswith(TRUNCATION_MARK)
    assert result.row["truncated"] is True


def test_短值与数字原样保留() -> None:
    transport = ScriptedTransport(_ok({"text": "短", "label": 1, "ok": True, "none": None}))
    result = _api(transport).fetch("a/b", DAY)

    row = json.loads(result.row["rows"])[0]["row"]
    assert (row["text"], row["label"], row["ok"], row["none"]) == ("短", 1, True, None)


def test_响应体不是_json_时算临时失败() -> None:
    transport = ScriptedTransport(HfResponse(status=200, headers={}, body=b"<html>"))
    result = _api(transport).fetch("a/b", DAY)

    assert result.status == "error"


def test_成帧按登记表的列序与类型() -> None:
    body = json.dumps(
        {
            "features": [],
            "rows": [{"row_idx": 0, "row": {"text": "甲"}}],
            "num_rows_total": 500,
        }
    )
    transport = ScriptedTransport(HfResponse(status=200, headers={}, body=body.encode()))
    fetcher = PreviewFetcher(_api(transport), workers=2)
    summary, frame = fetcher.run(DAY, ["a/b"])

    assert summary.fetched == 1
    assert frame.columns == list(PREVIEW_SCHEMA)
    assert frame.height == 1
    assert frame["truncated"].to_list() == [False]


def test_没有viewer也落一行不可用记下来() -> None:
    """`unavailable` 要落库：不可救的别每轮重烧，下轮据此跳过。

    这条与 `test_一直临时失败就不落库` 是一对：可救的不钉死、不可救的不重复问。
    """
    transport = ScriptedTransport(_error(404))
    fetcher = PreviewFetcher(_api(transport), workers=2)
    summary, frame = fetcher.run(DAY, ["a/b"])

    assert summary.fetched == 0
    assert summary.unavailable == 1
    assert frame.columns == list(PREVIEW_SCHEMA)
    assert frame.height == 1
    assert frame["status"].to_list() == ["unavailable"]
    assert frame["rows"].to_list() == [None]


def test_被限流时整轮中止并保留已取到的行() -> None:
    """429 按 IP 封：继续打只会把封禁拖长，所以整轮停。

    已取到的行仍然有效（并入后下次跑只用补剩下的），但这一轮不算跑完——摘要里
    `aborted` 为真，调用方据此返回非零退出码。
    """
    transport = ScriptedTransport(
        _ok({"text": "甲"}),
        _ok({"text": "乙"}),
        _error(429),
        _ok({"text": "丙"}),
        _ok({"text": "丁"}),
    )
    fetcher = PreviewFetcher(_api(transport), workers=1)
    summary, frame = fetcher.run(DAY, ["a/1", "a/2", "a/3", "a/4", "a/5"])

    assert summary.aborted is True
    assert summary.fetched >= 2
    assert frame.height == summary.fetched


def test_限流不重试而是立刻抛出() -> None:
    """`/rows` 的 429 是 CloudFront 按 IP 限流，逐条退避只会把封禁拖长。

    这条钉着「只打一次」：若哪天有人把 429 放回 `RETRYABLE_STATUS`，请求数与
    封禁时长都会翻几倍，而其它测试都不会红。
    """
    transport = ScriptedTransport(_error(429))

    with pytest.raises(Throttled):
        _api(transport).fetch("a/b", DAY)

    assert len(transport.calls) == 1


# ------------------------------------------------------------ config 自动解析


def _splits_body(
    splits: list[dict[str, Any]], pending: list[dict[str, Any]] | None = None
) -> bytes:
    return json.dumps({"splits": splits, "pending": pending or [], "failed": []}).encode()


def test_自动解析_config_与_split() -> None:
    """`config=auto` 先问 `/splits` 要真实 config 名，再拿它去问 `/rows`。

    多数库的 config 不叫 `default`（`openai/gsm8k` 是 `main`、`allenai/c4` 是
    `en`），而传错名字收到的是 429/500 而不是「config 不存在」——所以只能先问。
    """
    splits = HfResponse(
        status=200,
        headers={},
        body=_splits_body(
            [
                {"dataset": "a/b", "config": "main", "split": "test"},
                {"dataset": "a/b", "config": "main", "split": "train"},
            ]
        ),
    )
    transport = ScriptedTransport(splits, _ok({"text": "甲"}))
    result = _api(transport, config="auto").fetch("a/b", DAY)

    assert result.status == "ok"
    assert result.row is not None
    assert (result.row["config"], result.row["split"]) == ("main", "train")
    # 一次 `/splits`、一次 `/rows`
    assert len(transport.calls) == 2
    assert transport.calls[1]["config"] == "main"


def test_splits_撞限流也要整轮中止() -> None:
    """`/splits` 的 429 不能静默记成 error。

    原先这里返回 `error`，于是这一头吞掉限流、另一头继续打，实测连吞 26 个才由
    `/rows` 发现（2026-11-18），缓存窗口被白白烧掉。
    """
    transport = ScriptedTransport(_error(429))

    with pytest.raises(Throttled):
        _api(transport, config="auto").fetch("a/b", DAY)


def test_没有_splits_也没有_pending_记永久缺失() -> None:
    splits = HfResponse(status=200, headers={}, body=_splits_body([]))
    transport = ScriptedTransport(splits)
    result = _api(transport, config="auto").fetch("a/b", DAY)

    assert result.status == "unavailable"


def test_viewer_还在生成记临时失败以便重试() -> None:
    """`splits: []` 配 `pending` 非空 = 正在生成，与「没有 viewer」是两件事。

    实测 `tasl-lab/uniocc` 就是这个状态；若记成永久缺失，等它生成好了也不会再问。
    """
    splits = HfResponse(
        status=200,
        headers={},
        body=_splits_body([], [{"dataset": "a/b", "config": "default"}]),
    )
    transport = ScriptedTransport(splits)
    result = _api(transport, config="auto").fetch("a/b", DAY)

    assert result.status == "error"
