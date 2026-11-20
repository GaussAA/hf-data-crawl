"""生产传输 `HttpxTransport`：失败归一为 `TransportError`，且连接可复用。"""

from __future__ import annotations

from typing import Any

import httpx
import pytest

from hf_data_crawl.acquire.hf import transport as transport_module
from hf_data_crawl.acquire.hf.api import TransportError
from hf_data_crawl.acquire.hf.transport import HttpxTransport

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


def test_没设_keepalive_时不要把_limits_传成_None() -> None:
    """`httpx.Client(limits=None)` 在建连接池时就炸：`NoneType` 没有 `max_connections`。

    这条钉住「参数没给就别传」——httpx 不给 `limits` 时用自己的 `DEFAULT_LIMITS`。
    2026-11-18 的真实回归：给长跑加 `max_keepalive` 时顺带把 `limits` 无条件传了，
    于是**所有没设 keepalive 的传输**在建连接时抛 AttributeError，`collect`、
    `fetch-sizes`、`fetch-files`、`fetch-preview` 全部不可用；只有 `fetch-readme`
    （唯一传了 `max_keepalive=0` 的那个）照常。测试全用假传输，所以一条都没红。
    """
    client = HttpxTransport(proxy=LOCAL_PROXY)._get_client()

    assert client is not None


def test_关掉空闲连接复用也能建客户端() -> None:
    client = HttpxTransport(proxy=LOCAL_PROXY, max_keepalive=0)._get_client()

    assert client is not None


def test_一次失败就丢掉连接池_重试不会撞进坏池(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """代理掉线留下的坏池必须扔掉，否则重试会永久挂住。

    2026-11-18 实测 `fetch-readme`：代理一掉，12 个线程同时断连；重试撞进同一个
    池子后全部停在 `PoolRequest.wait_for_connection()`——httpcore 1.0.9 调用它时
    不传 timeout，连接又被记成「已借出」不归还，于是永久等下去。现场只有心跳、
    没有错误行，CPU 与 TCP 连接双静止，与「跑得慢」无从分辨。

    `close()` 会顺带唤醒池里等待的请求，所以换池不只是防下一次，还能解半死状态。
    """
    created: list[httpx.Client] = []
    real_client = httpx.Client
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("代理断了", request=request)
        return httpx.Response(200, json={"ok": True})

    def factory(**kwargs: Any) -> httpx.Client:
        kwargs.pop("proxy", None)
        client = real_client(transport=httpx.MockTransport(handler), **kwargs)
        created.append(client)
        return client

    monkeypatch.setattr(transport_module.httpx, "Client", factory)
    transport = HttpxTransport(proxy=LOCAL_PROXY)

    with pytest.raises(TransportError):
        transport.get("https://example.test/x", {}, {})

    assert len(created) == 1
    assert created[0].is_closed, "失败后旧池必须关掉，留着就等于留着坏状态"

    # 第二次请求必须在一张新池上跑，且能成功——这就是「重试有意义」的判据。
    response = transport.get("https://example.test/x", {}, {})
    assert response.status == 200
    assert len(created) == 2
    transport.close()


def test_关闭失败不盖住原始错误(monkeypatch: pytest.MonkeyPatch) -> None:
    """`close()` 自身抛错时，调用方看到的仍应是那条连接错误。"""
    real_client = httpx.Client

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("代理断了", request=request)

    class ExplodingClient(httpx.Client):
        def close(self) -> None:
            raise OSError("关不掉")

    def factory(**kwargs: Any) -> httpx.Client:
        kwargs.pop("proxy", None)
        return ExplodingClient(transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(transport_module.httpx, "Client", factory)
    transport = HttpxTransport(proxy=LOCAL_PROXY)

    with pytest.raises(TransportError, match="ConnectError"):
        transport.get("https://example.test/x", {}, {})

    assert real_client is not None


def test_别的线程换池后这次请求归一为传输错误而不是整轮崩掉() -> None:
    """并发下 `_discard_client()` 会关掉别人手里的 Client，再发请求 httpx 抛 RuntimeError。

    那是「池子在换」而不是程序错：必须归一成 `TransportError` 去走重试。2026-11-20
    实测：`fetch-files` 跑到 1,400 条时被这条运行时异常掀翻整轮，30 分钟的活只剩暂存分片。
    """
    transport = HttpxTransport(proxy=LOCAL_PROXY)
    closed = transport._get_client()
    closed.close()  # 模拟另一个线程刚刚换池

    with pytest.raises(TransportError):
        transport.get("https://huggingface.co/api/datasets", {}, {})

    # 换池之后拿到的是新 Client，不再是那个关掉的。
    assert transport._get_client() is not closed


def test_其它_runtime_error_不许被吞掉(monkeypatch) -> None:
    """只接「client has been closed」这一种：别的 RuntimeError 要原样抛，别当网络抖动。"""
    transport = HttpxTransport(proxy=LOCAL_PROXY)

    class Boom:
        def get(self, *_args, **_kwargs):
            raise RuntimeError("真的代码 bug")

    monkeypatch.setattr(transport, "_get_client", lambda: Boom())

    with pytest.raises(RuntimeError, match="真的代码 bug"):
        transport.get("https://huggingface.co/api/datasets", {}, {})
