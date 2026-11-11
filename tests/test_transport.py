"""生产传输 `HttpxTransport`：失败归一为 `TransportError`，且连接可复用。"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from hf_data_crawl.hf import transport as transport_module
from hf_data_crawl.hf.api import TransportError
from hf_data_crawl.hf.transport import HttpxTransport

#: 构造 Client 不联网；显式给代理，避免 httpx 去读本机环境变量里的 NO_PROXY。
LOCAL_PROXY = "http://127.0.0.1:1"


def test_代理地址写坏时归一为TransportError() -> None:
    """InvalidURL 不是 httpx.HTTPError 的子类；漏掉它会绕过重试与失败留痕。

    本机 `NO_PROXY` 含 `[::1]` 时就会这样抛错，不需要联网即可复现。
    """
    transport = HttpxTransport(proxy="http://127.0.0.1:bad-port")

    with pytest.raises(TransportError):
        transport.get("https://huggingface.co/api/datasets", {}, {})


def test_构造本身不创建连接即使代理写坏() -> None:
    HttpxTransport(proxy="http://127.0.0.1:bad-port")


@pytest.fixture()
def counted_clients(monkeypatch: pytest.MonkeyPatch) -> list[httpx.Client]:
    """把 httpx.Client 换成走内存应答的真实 Client，并记下创建了几个。"""
    created: list[httpx.Client] = []
    real_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    def factory(**kwargs: Any) -> httpx.Client:
        kwargs.pop("proxy", None)
        client = real_client(transport=httpx.MockTransport(handler), **kwargs)
        created.append(client)
        return client

    monkeypatch.setattr(transport_module.httpx, "Client", factory)
    return created


def test_多次请求共用同一个Client(counted_clients: list[httpx.Client]) -> None:
    transport = HttpxTransport(proxy=LOCAL_PROXY)

    for _ in range(3):
        response = transport.get("https://example.test/x", {}, {})
        assert response.status == 200

    assert len(counted_clients) == 1


def test_close之后再请求会重新创建(counted_clients: list[httpx.Client]) -> None:
    transport = HttpxTransport(proxy=LOCAL_PROXY)
    transport.get("https://example.test/x", {}, {})

    transport.close()
    transport.get("https://example.test/x", {}, {})

    assert len(counted_clients) == 2
    assert counted_clients[0].is_closed


def test_多次请求复用同一个Client() -> None:
    """每次新建 Client 就无法复用连接；复用靠实例持有同一个对象。"""
    transport = HttpxTransport(proxy=LOCAL_PROXY)

    first = transport._get_client()
    second = transport._get_client()
    transport.close()

    assert first is second


def test_close后重新获取会建新Client() -> None:
    transport = HttpxTransport(proxy=LOCAL_PROXY)
    first = transport._get_client()
    transport.close()

    second = transport._get_client()
    transport.close()

    assert first is not second
    assert first.is_closed
