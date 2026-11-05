"""transport 的生产实现：httpx。

放在这里而不是 api.py，是为了让 api.py 保持零网络依赖、可纯内存测试。
"""

from __future__ import annotations

import os
from typing import Any

import httpx

from .api import HfResponse, TransportError


class HttpxTransport:
    """真实 HTTP 传输。代理与令牌一律来自环境变量，绝不硬编码。"""

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

    def get(self, url: str, params: dict[str, Any], headers: dict[str, str]) -> HfResponse:
        request_headers = dict(headers)
        if self._token:
            request_headers["Authorization"] = f"Bearer {self._token}"
        try:
            with httpx.Client(
                proxy=self._proxy, timeout=self._timeout, follow_redirects=True
            ) as client:
                response = client.get(url, params=params, headers=request_headers)
        except httpx.HTTPError as error:
            # 统一成 TransportError，让上层只依赖一种失败类型，且无需 import httpx。
            raise TransportError(f"{type(error).__name__}: {error}") from error
        return HfResponse(
            status=response.status_code,
            headers={k: v for k, v in response.headers.items()},
            body=response.content,
        )
