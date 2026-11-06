"""S1 · HF 列表客户端的 transport seam 测试。

只验外部行为：给定假 transport 返回的页，客户端产出什么 Page、发出什么请求。
"""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from hf_data_crawl.api import HfDatasetApi, HfResponse, TransportError
from hf_data_crawl.checkpoint import MemoryCheckpoint


class FakeTransport:
    """按游标回放预设响应的假 transport，并记录每次请求参数。

    行为对齐真实接口：无 cursor 返回首页；有 cursor 则返回该游标指向的下一页。
    这样「中断续跑」才能被真正验到，而不是靠调用次数侥幸对齐。
    """

    def __init__(self, pages: list[list[dict[str, Any]]], links: list[str | None]) -> None:
        self._pages = pages
        self._links = links
        self.calls: list[dict[str, Any]] = []
        self._cursor_to_index: dict[str, int] = {}
        for index, link in enumerate(links):
            if link is None:
                continue
            cursor = parse_qs(urlparse(link).query)["cursor"][0]
            self._cursor_to_index[cursor] = index + 1

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls.append(dict(params))
        cursor = params.get("cursor")
        if cursor is None:
            index = 0
        else:
            index = self._cursor_to_index[str(cursor)]
        page = self._pages[index]
        response_headers = {"RateLimit": '"api";r=400;t=180'}
        link = self._links[index]
        if link is not None:
            response_headers["Link"] = f'<{link}>; rel="next"'
        return HfResponse(
            status=200,
            headers=response_headers,
            body=json.dumps(page).encode("utf-8"),
        )


def record(dataset_id: str, last_modified: str = "2026-11-01T00:00:00.000Z") -> dict[str, Any]:
    return {"id": dataset_id, "lastModified": last_modified}


def test_不指定排序时拒绝请求() -> None:
    transport = FakeTransport([[record("a/x")]], [None])
    api = HfDatasetApi(transport=transport)

    with pytest.raises(ValueError, match="sort"):
        list(api.scan(sort=""))

    assert transport.calls == []


def test_limit超过上限被收敛为一千() -> None:
    transport = FakeTransport([[record("a/x")]], [None])
    api = HfDatasetApi(transport=transport)

    list(api.scan(sort="createdAt", limit=5000))

    assert transport.calls[0]["limit"] == "1000"


def test_三页遍历在末页无Link时终止() -> None:
    base = "https://huggingface.co/api/datasets"
    pages = [[record("a/1")], [record("a/2")], [record("a/3")]]
    transport = FakeTransport(
        pages,
        [f"{base}?cursor=c2", f"{base}?cursor=c3", None],
    )
    api = HfDatasetApi(transport=transport)

    collected = [r["id"] for page in api.scan(sort="createdAt") for r in page.records]

    assert collected == ["a/1", "a/2", "a/3"]
    assert len(transport.calls) == 3
    assert "cursor" not in transport.calls[0]


def test_游标从Link头提取并解码后回传() -> None:
    base = "https://huggingface.co/api/datasets"
    transport = FakeTransport(
        [[record("a/1")], [record("a/2")]],
        [f"{base}?cursor=eyJ4IjoiYT0ifQ%3D%3D", None],
    )
    api = HfDatasetApi(transport=transport)

    list(api.scan(sort="createdAt"))

    assert transport.calls[1]["cursor"] == "eyJ4IjoiYT0ifQ=="


def test_expand字段以重复键发送() -> None:
    transport = FakeTransport([[record("a/x")]], [None])
    api = HfDatasetApi(transport=transport)

    list(api.scan(sort="createdAt", expand=("downloads", "downloadsAllTime")))

    assert transport.calls[0]["expand[]"] == ["downloads", "downloadsAllTime"]


def test_expand含白名单外字段时立即报错() -> None:
    transport = FakeTransport([[record("a/x")]], [None])
    api = HfDatasetApi(transport=transport)

    with pytest.raises(ValueError, match="usedStorage"):
        list(api.scan(sort="createdAt", expand=("downloads", "usedStorage")))

    assert transport.calls == []


def test_缺id或lastModified的记录被跳过并计数() -> None:
    bad = [
        {"lastModified": "2026-11-01T00:00:00.000Z"},
        {"id": "a/nolmod"},
        {"id": "", "lastModified": "x"},
    ]
    transport = FakeTransport([[record("a/ok"), *bad]], [None])
    api = HfDatasetApi(transport=transport)

    pages = list(api.scan(sort="createdAt"))

    assert [r["id"] for r in pages[0].records] == ["a/ok"]
    assert pages[0].skipped == 3


class ScriptedTransport:
    """按脚本回放状态码的假 transport，用于验退避与配额等待。"""

    def __init__(self, script: list[tuple[int, dict[str, str], list[dict[str, Any]]]]) -> None:
        self._script = script
        self.calls: list[dict[str, str]] = []

    def get(self, url: str, params: dict[str, str], headers: dict[str, str]) -> HfResponse:
        index = len(self.calls)
        self.calls.append(dict(params))
        status, extra_headers, page = self._script[min(index, len(self._script) - 1)]
        response_headers = {"RateLimit": '"api";r=400;t=180', **extra_headers}
        return HfResponse(status=status, headers=response_headers, body=json.dumps(page).encode())


def test_429退避后重试成功() -> None:
    transport = ScriptedTransport(
        [
            (429, {"RateLimit": '"api";r=0;t=7'}, []),
            (429, {"RateLimit": '"api";r=0;t=7'}, []),
            (200, {}, [record("a/ok")]),
        ]
    )
    sleeps: list[float] = []
    api = HfDatasetApi(transport=transport, sleeper=sleeps.append)

    pages = list(api.scan(sort="createdAt"))

    assert [r["id"] for r in pages[0].records] == ["a/ok"]
    assert len(transport.calls) == 3
    assert len(sleeps) == 2 and all(s >= 7 for s in sleeps)


def test_配额将尽时先等待再取下一页() -> None:
    transport = FakeTransport(
        [[record("a/1")], [record("a/2")]],
        ["https://huggingface.co/api/datasets?cursor=c2", None],
    )
    original_get = transport.get

    def low_quota_get(url: str, params: dict[str, str], headers: dict[str, str]) -> HfResponse:
        response = original_get(url, params, headers)
        if len(transport.calls) == 1:
            return HfResponse(
                status=response.status,
                headers={**response.headers, "RateLimit": '"api";r=3;t=42'},
                body=response.body,
            )
        return response

    transport.get = low_quota_get  # type: ignore[method-assign]
    sleeps: list[float] = []
    api = HfDatasetApi(transport=transport, sleeper=sleeps.append)

    list(api.scan(sort="createdAt"))

    assert sleeps == [42]


def test_中断后从checkpoint续跑不重复入库() -> None:
    base = "https://huggingface.co/api/datasets"
    pages = [[record("a/1")], [record("a/2")], [record("a/3")]]
    links = [f"{base}?cursor=c2", f"{base}?cursor=c3", None]

    full = [
        r["id"]
        for page in HfDatasetApi(transport=FakeTransport(pages, links)).scan(sort="createdAt")
        for r in page.records
    ]

    checkpoint = MemoryCheckpoint()
    first_half = [
        r["id"]
        for page in HfDatasetApi(transport=FakeTransport(pages, links)).scan(
            sort="createdAt", max_pages=2, checkpoint=checkpoint
        )
        for r in page.records
    ]
    resumed = [
        r["id"]
        for page in HfDatasetApi(transport=FakeTransport(pages, links)).scan(
            sort="createdAt", checkpoint=checkpoint
        )
        for r in page.records
    ]

    assert full == ["a/1", "a/2", "a/3"]
    assert first_half + resumed == full


def test_连接中断后重试并最终成功() -> None:
    """代理 SSL 断流并不返回状态码，只抛连接异常——这条路径必须也能重试。"""

    class FlakyTransport:
        def __init__(self) -> None:
            self.calls = 0

        def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
            self.calls += 1
            if self.calls <= 2:
                raise TransportError("ConnectError: [SSL: UNEXPECTED_EOF_WHILE_READING]")
            return HfResponse(
                status=200,
                headers={"RateLimit": '"api";r=470;t=180'},
                body=json.dumps([record("a/ok")]).encode(),
            )

    sleeps: list[float] = []
    api = HfDatasetApi(transport=FlakyTransport(), sleeper=sleeps.append)

    pages = list(api.scan(sort="createdAt"))

    assert [r["id"] for r in pages[0].records] == ["a/ok"]
    assert len(sleeps) == 2


def test_连接持续中断超过重试次数即抛出() -> None:
    class DeadTransport:
        def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
            raise TransportError("ConnectError: connection reset")

    api = HfDatasetApi(transport=DeadTransport(), sleeper=lambda _: None, max_retries=3)

    with pytest.raises(TransportError, match="connection reset"):
        list(api.scan(sort="createdAt"))


def test_响应头大小写不敏感() -> None:
    """真实接口经 HTTP/2 会把头名规范成小写，按大写查找会静默取空。"""

    class LowerHeaderTransport:
        def __init__(self) -> None:
            self.calls: list[dict[str, Any]] = []

        def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
            self.calls.append(dict(params))
            if "cursor" not in params:
                return HfResponse(
                    status=200,
                    headers={
                        "link": '<https://huggingface.co/api/datasets?cursor=c2>; rel="next"',
                        "ratelimit": '"api";r=5;t=11',
                    },
                    body=json.dumps([record("a/1")]).encode(),
                )
            return HfResponse(
                status=200,
                headers={"ratelimit": '"api";r=300;t=100'},
                body=json.dumps([record("a/2")]).encode(),
            )

    sleeps: list[float] = []
    api = HfDatasetApi(transport=LowerHeaderTransport(), sleeper=sleeps.append)

    pages = list(api.scan(sort="createdAt"))

    assert [r["id"] for page in pages for r in page.records] == ["a/1", "a/2"]
    assert pages[0].rate_remaining == 5
    assert sleeps == [11]


# ---------------------------------------------------------------- 单库探测


class DetailTransport:
    """只回应单库详情：状态码或一个 JSON 体。"""

    def __init__(self, status: int, body: Any = None) -> None:
        self.status = status
        self.body = body

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        payload = json.dumps(self.body).encode() if self.body is not None else b"{}"
        return HfResponse(status=self.status, headers={}, body=payload)


def test_详情返回自身id判为present而非renamed() -> None:
    """库还在、id 没变时，详情接口返回的就是它自己的 id。

    此前一律记成 resolved_id，存活的库会被标成 renamed。
    """
    api = HfDatasetApi(transport=DetailTransport(200, {"id": "acme/b"}))

    probe = api.probe("acme/b")

    assert probe.resolved_id is None
    assert probe.verdict == "present"


def test_详情返回新id判为renamed() -> None:
    api = HfDatasetApi(transport=DetailTransport(200, {"id": "acme/b-v2"}))

    probe = api.probe("acme/b")

    assert probe.resolved_id == "acme/b-v2"
    assert probe.verdict == "renamed"


@pytest.mark.parametrize(
    ("status", "verdict"),
    [(404, "deleted"), (401, "restricted"), (403, "restricted"), (500, "unknown")],
)
def test_探测状态码翻译成业务结论(status: int, verdict: str) -> None:
    api = HfDatasetApi(transport=DetailTransport(status))

    assert api.probe("acme/b").verdict == verdict
