"""transport 的生产实现：httpx。

放在这里而不是 api.py，是为了让 api.py 保持零网络依赖、可纯内存测试。
"""

from __future__ import annotations

import os
import threading
from typing import Any

import httpx

from .api import HfResponse, TransportError


class HttpxTransport:
    """真实 HTTP 传输。代理与令牌一律来自环境变量，绝不硬编码。

    整个实例共用一个 `httpx.Client`，连接可复用。`fetch-sizes` 用 16 个线程共享
    同一个传输，httpx 的 Client 线程安全；懒创建，所以构造本身不碰网络，也不会因
    代理写坏而在构造时抛错。
    """

    def __init__(
        self,
        *,
        proxy: str | None = None,
        token: str | None = None,
        timeout: float = 120.0,
    ) -> None:
        self._proxy = proxy if proxy is not None else os.environ.get("HF_PROXY")
        self._token = token if token is not None else os.environ.get("HF_TOKEN")
        self._timeout = timeout
        self._client: httpx.Client | None = None
        self._lock = threading.Lock()

    def _get_client(self) -> httpx.Client:
        with self._lock:
            if self._client is None:
                self._client = httpx.Client(
                    proxy=self._proxy, timeout=self._timeout, follow_redirects=True
                )
            return self._client

    def close(self) -> None:
        with self._lock:
            client, self._client = self._client, None
        if client is not None:
            client.close()

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        request_headers = dict(headers)
        if self._token:
            request_headers["Authorization"] = f"Bearer {self._token}"
        try:
            # 创建 Client 也在 try 里：代理或 NO_PROXY 写坏时 InvalidURL 在这里抛。
            response = self._get_client().get(url, params=params, headers=request_headers)
        except (httpx.HTTPError, httpx.InvalidURL) as error:
            # 统一成 TransportError，让上层只依赖一种失败类型，且无需 import httpx。
            # InvalidURL 不是 HTTPError 的子类（实测 MRO：InvalidURL → Exception），
            # 漏掉它会绕过重试与失败留痕。
            raise TransportError(f"{type(error).__name__}: {error}") from error
        return HfResponse(
            status=response.status_code,
            headers={k: v for k, v in response.headers.items()},
            body=response.content,
        )
