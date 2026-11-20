"""S1 · HF 列表客户端的 transport seam 测试。

只验外部行为：给定假 transport 返回的页，客户端产出什么 Page、发出什么请求。
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from hf_data_crawl.acquire.hf.api import (
    MAX_RETRY_SLEEP,
    HfDatasetApi,
    HfResponse,
    TransportError,
    _parse_rate_limit,
)
from hf_data_crawl.acquire.hf.checkpoint import FileCheckpoint


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


def test_重复的expand字段会被去重且保序() -> None:
    """基座与专属项偶尔重叠；同名字段发两遍只是白增请求长度。"""
    transport = FakeTransport([[record("a/x")]], [None])
    api = HfDatasetApi(transport=transport)

    list(api.scan(sort="createdAt", expand=("downloads", "likes", "downloads")))

    assert transport.calls[0]["expand[]"] == ["downloads", "likes"]


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


def test_扫描撞满窗口的429会等满再继续而不是耗尽重试() -> None:
    """全站扫 1140 页、配额约 1000/5 分钟，撞 429 是常态，不是故障。

    2026-11-20 首跑：`t=300` 被 30 秒封顶截断，5 轮重试共 150 秒等不满窗口，
    于是「重试已耗尽（HTTP 429）」，collect 判负、下游全部跳过。
    改用 honor_window 后，一次退避就跨过窗口，重试次数给 1 次也够。
    """
    transport = ScriptedTransport(
        [
            (429, {"RateLimit": '"api";r=0;t=300'}, []),
            (200, {}, [record("a/ok")]),
        ]
    )
    sleeps: list[float] = []
    api = HfDatasetApi(transport=transport, sleeper=sleeps.append, max_retries=1)

    pages = list(api.scan(sort="lastModified"))

    assert [r["id"] for r in pages[0].records] == ["a/ok"]
    assert sleeps == [301.0]


def test_重试耗尽时错误消息带最后一次状态码() -> None:
    """`持续限流或不可用` 分不清 429 与 5xx，而两者的处置完全不同。

    等配额与等上游恢复是两条路。2026-11-16 首次 collect 被打停时，台账里只有
    这句话，只能靠手动重放同一请求才确认是 429（配额被并发任务摊薄，不是上游坏）。
    """
    transport = ScriptedTransport([(503, {}, []) for _ in range(5)])
    api = HfDatasetApi(transport=transport, sleeper=lambda _: None, max_retries=2)

    with pytest.raises(RuntimeError, match="HTTP 503"):
        list(api.scan(sort="createdAt"))


def test_RateLimit头格式抖动不中断扫描() -> None:
    """`t=41.5`（实测出现过的小数秒）与空值 `r=` 曾让 `int()` 抛 ValueError。

    一个响应头的格式抖动不该比配额本身更有杀伤力——它会整轮扫描作废。
    """
    assert _parse_rate_limit({"RateLimit": '"api";r=400;t=180'}) == (400, 180)
    assert _parse_rate_limit({"RateLimit": '"api";r=;t=41.5'}) == (None, 41)
    assert _parse_rate_limit({"RateLimit": '"api";r=abc'}) == (None, None)
    assert _parse_rate_limit({}) == (None, None)


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

    # 3 个请求还剩 42 秒 ⇒ 每请求 14 秒。睡 42 秒等整窗重开也行，但那样剩下的
    # 3 个配额全废；匀速能把它们用满，且不会在第 4 个请求上撞 429。
    assert sleeps == [14.0]


def test_配额偏紧时按剩余匀速而不是先把配额打光() -> None:
    """`r=30;t=180` ⇒ 这一页之后每请求还能占 6 秒。

    老逻辑要等剩余掉到 10 以下才停，意味着每轮扫描都得先撞一次 429；匀速把这
    6 秒摊在每一页上，中途不撞墙。配额宽裕时（`r=400;t=180` ⇒ 0.45 秒，比网络
    延迟还短）则不睡——那时瓶颈在往返，不在配额。
    """

    def paced(rate_header: str) -> list[float]:
        class _T:
            calls = 0

            def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
                type(self).calls += 1
                link = "" if self.calls >= 3 else f'<{url}?cursor=c{self.calls}>; rel="next"'
                return HfResponse(
                    status=200,
                    headers={"RateLimit": rate_header, "Link": link}
                    if link
                    else {"RateLimit": rate_header},
                    body=json.dumps([record(f"a/{self.calls}")]).encode(),
                )

        sleeps: list[float] = []
        list(HfDatasetApi(transport=_T(), sleeper=sleeps.append).scan(sort="lastModified"))
        return sleeps

    assert paced('"api";r=30;t=180') == [6.0, 6.0]
    assert paced('"api";r=400;t=180') == []


def test_中断后从checkpoint续跑不重复入库(tmp_path: Path) -> None:
    base = "https://huggingface.co/api/datasets"
    pages = [[record("a/1")], [record("a/2")], [record("a/3")]]
    links = [f"{base}?cursor=c2", f"{base}?cursor=c3", None]

    full = [
        r["id"]
        for page in HfDatasetApi(transport=FakeTransport(pages, links)).scan(sort="createdAt")
        for r in page.records
    ]

    checkpoint = FileCheckpoint(tmp_path / "checkpoint.json")
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


def test_扫描能熬过短时的连接中断而探测不行() -> None:
    """连接中断没有答复可依据，只能靠退避熬；扫描多给 3 次（共约 3 分钟）。

    2026-11-20 00:31 代理抖了一下，`collect` 五次重试共 31 秒没熬过去，当天整链
    判负、下游全跳过。探测不享受这份宽限：它持写锁，熬久了会拖死整条链。
    """

    class _Blip:
        def __init__(self, fails: int) -> None:
            self.left = fails
            self.calls = 0

        def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
            self.calls += 1
            if self.left > 0:
                self.left -= 1
                raise TransportError("SSL: UNEXPECTED_EOF_WHILE_READING")
            return HfResponse(
                status=200,
                headers={"RateLimit": '"api";r=470;t=180'},
                body=json.dumps([record("a/ok")]).encode(),
            )

    scanning = _Blip(7)
    pages = list(HfDatasetApi(transport=scanning, sleeper=lambda _: None).scan(sort="lastModified"))
    assert [r["id"] for r in pages[0].records] == ["a/ok"]
    assert scanning.calls == 8

    probing = _Blip(7)
    assert (
        HfDatasetApi(transport=probing, sleeper=lambda _: None).probe("a/ok").verdict == "unknown"
    )
    assert probing.calls == 6  # 首次 + 5 次重试，不多熬


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
    # 顺带验到限流头也走小写读取：`r=5;t=11` ⇒ 每请求 2.2 秒（旧逻辑会睡整窗 11）。
    assert sleeps == [2.2]


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
    # `sleeper` 显式换成 no-op：500 是可重试状态，真等退避会让这条用例跑半分钟。
    api = HfDatasetApi(transport=DetailTransport(status), sleeper=lambda _: None, max_retries=2)

    assert api.probe("acme/b").verdict == verdict


class FlakyProbeTransport:
    """先按预设状态码连败几次，之后才给正常详情。"""

    def __init__(self, statuses: list[int], body: Any) -> None:
        self._statuses = list(statuses)
        self._body = body
        self.calls = 0

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        self.calls += 1
        status = self._statuses.pop(0) if self._statuses else 200
        body = json.dumps(self._body).encode() if status == 200 else b"{}"
        return HfResponse(status=status, headers={}, body=body)


def test_探测遇限流会重试而不是直接判无结论() -> None:
    """限流下的「先别动」会退化成「永远不动」：状态停在旧值，没有任何机制回头查。"""
    transport = FlakyProbeTransport([429, 429], {"id": "acme/b"})
    api = HfDatasetApi(transport=transport, sleeper=lambda _: None)

    probe = api.probe("acme/b")

    assert probe.verdict == "present"
    assert transport.calls == 3


def test_探测重试耗尽才算无结论() -> None:
    transport = FlakyProbeTransport([429] * 20, None)
    api = HfDatasetApi(transport=transport, sleeper=lambda _: None, max_retries=2)

    probe = api.probe("acme/b")

    assert probe.verdict == "unknown"
    assert transport.calls == 3  # 首次 + 两次重试，之后不再徒劳地打接口


def test_退避不睡满整个限流窗口() -> None:
    """`t=300` 指的是「窗口还剩 300 秒」，不是「该睡 300 秒」。

    照单全睡时，一次探测退避 5 轮就是 25 分钟；2000 个探测足以把进程挂上数小时，
    还攥着写锁（2026-11-13 实测堵死日常链 2 小时 50 分）。
    """

    class _Noop:
        def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
            raise AssertionError("只验退避计算，不该发请求")

    api = HfDatasetApi(transport=_Noop())
    response = HfResponse(status=429, headers={"RateLimit": '"api";r=0;t=300'}, body=b"{}")

    assert api._retry_delay(response, 0) == MAX_RETRY_SLEEP
    # 但扫描路径必须睡满窗口：`t=300` 截成 30 秒，5 轮重试共 150 秒等不满一个
    # 窗口，整轮判负——2026-11-20 刷新链首跑就是这么丢掉一整天的。
    assert api._retry_delay(response, 0, honor_window=True) == 301.0
    # 上限兜底：窗口再长也不睡过 MAX_RATE_LIMIT_SLEEP（600 秒）。
    assert (
        api._retry_delay(
            HfResponse(429, {"RateLimit": '"api";r=0;t=9000'}, b"{}"), 0, honor_window=True
        )
        == 600.0
    )
    # 没有限流头时仍走指数退避，并保持原有 60 秒上限。
    plain = HfResponse(status=503, headers={}, body=b"{}")
    assert api._retry_delay(plain, 10) == 60.0
