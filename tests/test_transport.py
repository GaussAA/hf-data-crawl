"""生产传输 `HttpxTransport` 的失败归一：上层只应看到 `TransportError`。"""

from __future__ import annotations

import pytest

from hf_data_crawl.api import TransportError
from hf_data_crawl.transport import HttpxTransport


def test_代理地址写坏时归一为TransportError() -> None:
    """InvalidURL 不是 httpx.HTTPError 的子类；漏掉它会绕过重试与失败留痕。

    本机 `NO_PROXY` 含 `[::1]` 时就会这样抛错，不需要联网即可复现。
    """
    transport = HttpxTransport(proxy="http://127.0.0.1:bad-port")

    with pytest.raises(TransportError):
        transport.get("https://huggingface.co/api/datasets", {}, {})
