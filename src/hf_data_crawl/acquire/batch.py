"""按 id 批量补采的公共骨架：并发、边跑边落库、进度。

`size` 与 `files` 是同构的两条链路：按下载量取头部 → 逐库打外部接口 →
累积一张 `Origin.NETWORK` 表。这段循环此前只在 `sizes.py` 里有一份，再写一遍
就会出现两处各自漂移的节奏：一处加了 flush，另一处忘了，于是长任务中断时
丢掉一小时成果；一处进度终态必发，另一处停在 96%。

抽到此处后，两条链路只声明「怎么请求一条、结果长什么样」，调度只有一份。
`FetchResult` 用 Protocol 而非基类：两个模块的 dataclass 按结构满足即可，
不必为了共用调度而继承同一个类型。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from typing import Any, Protocol

import polars as pl

from ..platform.tables import PolarsType

#: 心跳间隔：这么久一条都没完成，就回调一次 `heartbeat`。
#:
#: 2026-11-16 实测：一次 `fetch-sizes` 在日志里停了 1 小时 35 分，一个字节没多写。
#: 事后核对进程 8 秒内读写计数与 CPU 时间**完全不变**、无 TCP 连接——它是真卡住了，
#: 但从日志看与「老老实实跑得慢」一模一样。没有心跳，人就只能靠比对文件时间来猜。
#:
#: 30 秒这个数是为了「一眼分开慢与死」：正常的 16 并发每几十毫秒就有一条完成，
#: 30 秒一条都没有，只可能是请求挂住了。
STALL_HEARTBEAT_SECONDS = 30.0


class FetchResult(Protocol):
    """单库补采结果的最小契约：`SizeResult` / `FileResult` 都按结构满足。

    成员声明成只读 property：两个结果都是 frozen dataclass，若在这里写成
    普通变量，mypy 会认为协议要求「可写」，结构兼容判定便失败。
    """

    @property
    def status(self) -> str: ...

    @property
    def row(self) -> dict[str, Any]: ...


def collect_rows(
    *,
    pending: Sequence[str],
    fetch: Callable[[str], FetchResult],
    schema: dict[str, PolarsType],
    workers: int = 16,
    flush: Callable[[pl.DataFrame], object] | None = None,
    flush_every: int = 5_000,
    progress: Callable[[int, int], None] | None = None,
    heartbeat: Callable[[int, int], None] | None = None,
) -> tuple[list[FetchResult], pl.DataFrame]:
    """并发跑完 `pending`，返回 (逐条结果, 汇总表)。

    有两点容易写错：

    - **边跑边落库**：给了 `flush` 就每攒满一批交出去一次。十万条要跑一小时，
      只在末尾写一次等于把一小时成果押在最后一个字节上。
    - **终态必发进度**：只在整除时回调会停在 99%，看着像卡死；
      `completed == len(pending)` 时无条件再发一次。零任务时不回调，
      否则 `total=0` 会让调用方除零。

    第三条是 2026-11-16 补的：**卡住要说出来**。原先用 `as_completed` 一路干等，
    请求挂住时既不报错也不出字，日志停在那里与「跑得慢」毫无分别。现在改成带超时
    的 `wait`：`STALL_HEARTBEAT_SECONDS` 内一条都没完成就回调 `heartbeat`，把那
    段时间显式说出来。
    """
    results: list[FetchResult] = []
    rows: list[dict[str, Any]] = []
    buffer: list[dict[str, Any]] = []
    total = len(pending)

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        waiting = {pool.submit(fetch, dataset_id) for dataset_id in pending}
        while waiting:
            finished, waiting = wait(
                waiting, timeout=STALL_HEARTBEAT_SECONDS, return_when=FIRST_COMPLETED
            )
            if not finished:
                # 一条都没完成：可能只是慢（大表解析），也可能是请求挂住了。
                # 叫一声，让日志能区分这两件事——它没法替人判断，但不该沉默。
                if heartbeat is not None:
                    heartbeat(len(results), total)
                continue
            for future in finished:
                result = future.result()
                results.append(result)
                if result.row:
                    rows.append(result.row)
                    buffer.append(result.row)

                if flush is not None and len(buffer) >= max(1, flush_every):
                    flush(pl.DataFrame(buffer, schema=schema))
                    buffer.clear()

                completed = len(results)
                if progress is not None and (completed % 100 == 0 or completed == total):
                    progress(completed, total)

    frame = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return results, frame
