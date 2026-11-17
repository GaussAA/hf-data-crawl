"""transport 的生产实现：httpx。

放在这里而不是 api.py，是为了让 api.py 保持零网络依赖、可纯内存测试。
"""

from __future__ import annotations

import contextlib
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
        max_keepalive: int | None = None,
    ) -> None:
        self._proxy = proxy if proxy is not None else os.environ.get("HF_PROXY")
        self._token = token if token is not None else os.environ.get("HF_TOKEN")
        self._timeout = timeout
        #: 连接池里最多保留几条空闲连接。`None` 用 httpx 默认值（20 条）。
        #:
        #: 为什么要能关：走本地代理跑十几小时的批量抓取时，空闲连接会被代理侧
        #: 单方面丢掉而 httpx 不知道，下一次复用就卡在 TLS 握手上直到超时
        #: （2026-11-18 实测：10 个连接同时变坏后，吞吐从 14/s 掉到 0.33/s，
        #: 且进程重启即恢复）。这类链路给 0，每条请求各开各的连接。
        self._max_keepalive = max_keepalive
        self._client: httpx.Client | None = None
        self._lock = threading.Lock()

    def _get_client(self) -> httpx.Client:
        with self._lock:
            if self._client is None:
                # `httpx.Client(limits=None)` 会在建连接池时炸（`NoneType` 没有
                # `max_connections`）：它的默认值是 `DEFAULT_LIMITS`，不是 None。
                # 所以「没设置就别传」，而不是传个 None 进去。
                options: dict[str, Any] = {
                    "proxy": self._proxy,
                    "timeout": self._timeout,
                    "follow_redirects": True,
                }
                if self._max_keepalive is not None:
                    options["limits"] = httpx.Limits(max_keepalive_connections=self._max_keepalive)
                self._client = httpx.Client(**options)
            return self._client

    def _discard_client(self) -> None:
        """丢掉当前 Client 及其连接池。

        代理掉线时同一刻有十几个请求一起出错（2026-11-18 实测 `fetch-readme`：
        12 个线程同时收到断连）。httpcore 的池子在这种批量失败之后不可再信：
        连接被记成「已借出」却不归还，而 `PoolRequest.wait_for_connection()` 的
        `timeout` 默认 `None`、httpcore 1.0.9 调用它时也不传——于是后续重试全部
        永久停在这一行。现场症状是 **CPU 与 TCP 连接双静止**，日志只有心跳，
        与「跑得慢」无从分辨。

        换池的代价只是一次握手，比整条链卡死便宜得多。`close()` 同时会唤醒
        池里等待的请求（httpcore 给它们置异常），所以顺带能把半死状态解开。
        """
        with self._lock:
            client, self._client = self._client, None
        if client is not None:
            # 关闭失败不该盖住调用方真正的失败原因（原始异常更有诊断价值）。
            with contextlib.suppress(Exception):
                client.close()

    def close(self) -> None:
        self._discard_client()

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
            #
            # **先换池再抛**：这次的失败可能已经弄脏了连接池，若不换，上层的重试
            # 会撞进同一个坏池并永久挂住——那正是 2026-11-18 两次卡死的形状。
            self._discard_client()
            raise TransportError(f"{type(error).__name__}: {error}") from error
        return HfResponse(
            status=response.status_code,
            headers={k: v for k, v in response.headers.items()},
            body=response.content,
        )
