"""按 id 批量补采的公共骨架：并发、边跑边落库、暂存、进度。

`size` / `files` / `readme` 是同构的几条链路：按下载量取头部 → 逐库打外部
接口 → 累积一张 `Origin.NETWORK` 表。这段循环此前只在 `sizes.py` 里有一份，
再写一遍就会出现两处各自漂移的节奏：一处加了 flush，另一处忘了，于是长任务
中断时丢掉一小时成果；一处进度终态必发，另一处停在 96%。

暂存分片也在这里：全量抓取要跑十几小时（README 全量约 17 小时），整段持锁会
挡住当天的采集（2026-11-07 实际事故），故长耗时部分只写锁外的原子分片，
收尾再用短锁并入正式表。各条链路各占 `.staging/<名字>/`，互不干扰。

抽到此处后，各链路只声明「怎么请求一条、结果长什么样」，调度与暂存只有一份。
`FetchResult` 用 Protocol 而非基类：各模块的 dataclass 按结构满足即可，
不必为了共用调度而继承同一个类型。
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol
from uuid import uuid4

import polars as pl

from ..platform.atomic import write_parquet
from ..platform.tables import PolarsType

if TYPE_CHECKING:
    from ..platform.store import DatasetStore

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


#: `collect_rows` 具体结果类型（`SizeResult` / `FileResult` / `ReadmeResult`）。
#:
#: 必须泛型化而不是统一写成 `FetchResult`：`Callable` 的参数是**逆变**的，
#: 一个 `Callable[[ReadmeResult], None]` 不能赋给 `Callable[[FetchResult], None]`
#: ——mypy 会判 `readme.py` 里的 `on_result=tally` 不兼容。约束到 `FetchResult`
#: 上既保住「必须有 status/row」的要求，又让每个调用方拿到自己的类型。


def collect_rows[R: FetchResult](
    *,
    pending: Sequence[str],
    fetch: Callable[[str], R],
    schema: dict[str, PolarsType],
    workers: int = 16,
    flush: Callable[[pl.DataFrame], object] | None = None,
    flush_every: int = 5_000,
    progress: Callable[[int, int], None] | None = None,
    heartbeat: Callable[[int, int], None] | None = None,
    retain: bool = True,
    on_result: Callable[[R], None] | None = None,
) -> tuple[list[R], pl.DataFrame]:
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

    `retain=False` 是为正文这类「一条几 KB、全量上百万条」的链路准备的：结果由
    `flush` 直接写盘、不在内存里攒（否则全量 README 会在收尾时占掉几个 GB），
    统计改由 `on_result` 逐条累加；此时返回的汇总表为空，尾巴那一小批也会在
    返回前写完。用 `retain=False` 必须同时给 `flush`——不给就真的把数据丢了。
    """
    if not retain and flush is None:
        raise ValueError("retain=False 必须同时给 flush，否则结果无处可去")

    results: list[R] = []
    rows: list[dict[str, Any]] = []
    buffer: list[dict[str, Any]] = []
    total = len(pending)
    completed = 0

    with ThreadPoolExecutor(max_workers=max(1, workers)) as pool:
        # 滑动窗口，而不是把 `pending` 一次性全部 submit。
        #
        # `--top 0`（全量）时 pending 有 111 万个 id：一次性提交会创建 111 万个
        # Future，`wait()` 每次都要遍历整个集合，CPU 被吃满而请求根本发不出去
        # （2026-11-17 实测：运行 9 分钟 0 条完成、CPU 99.8%、staging 无新增）。
        # 6 万条时还能忍，百万级直接卡死。窗口取「并发数的 8 倍」并封顶 4096：
        # 留足并发，又不让集合膨胀到百万级。
        window = min(max(max(1, workers) * 8, 64), 4096)
        cursor = 0
        waiting: set[Any] = set()

        def fill() -> None:
            nonlocal cursor
            while len(waiting) < window and cursor < total:
                waiting.add(pool.submit(fetch, pending[cursor]))
                cursor += 1

        fill()
        while waiting:
            finished, waiting = wait(
                waiting, timeout=STALL_HEARTBEAT_SECONDS, return_when=FIRST_COMPLETED
            )
            if not finished:
                # 一条都没完成：可能只是慢（大表解析），也可能是请求挂住了。
                # 叫一声，让日志能区分这两件事——它没法替人判断，但不该沉默。
                if heartbeat is not None:
                    heartbeat(completed, total)
                continue
            for future in finished:
                result = future.result()
                completed += 1
                if on_result is not None:
                    on_result(result)
                if retain:
                    results.append(result)
                    if result.row:
                        rows.append(result.row)
                if result.row:
                    buffer.append(result.row)

                if flush is not None and len(buffer) >= max(1, flush_every):
                    flush(pl.DataFrame(buffer, schema=schema))
                    buffer.clear()

                if progress is not None and (completed % 100 == 0 or completed == total):
                    progress(completed, total)
            fill()

    # 流式模式下尾巴不足一批，也必须写出去——否则最后几十条永远停在内存里。
    if not retain and buffer and flush is not None:
        flush(pl.DataFrame(buffer, schema=schema))
        buffer.clear()

    frame = pl.DataFrame(rows, schema=schema) if rows else pl.DataFrame(schema=schema)
    return results, frame


# ------------------------------------------------------------ 暂存分片


#: 暂存根目录（相对数据根）。各条长耗时抓取各占一个子目录，互不干扰。
STAGING_ROOT = ".staging"


def staging_root(store: DatasetStore, subdir: str) -> Path:
    """某条链路的暂存分片目录。"""
    return store.root / subdir


def staged_shards(store: DatasetStore, subdir: str) -> list[Path]:
    """现有暂存分片，按文件名排序。

    同一轮内序号递增，文件名序即写入序；跨轮顺序无关紧要——各轮的 id 集合
    已经互斥（本轮开跑时就把已有分片记作「已处理」）。
    """
    root = staging_root(store, subdir)
    if not root.exists():
        return []
    return sorted(root.glob("*.parquet"))


def read_staged(
    store: DatasetStore, subdir: str, schema: dict[str, PolarsType]
) -> tuple[pl.DataFrame, list[Path]]:
    """读出上次中断留下的暂存分片，返回 (结果表, 分片路径)。

    分片写盘是原子的，读到的要么是完整的一批、要么不存在；不必做半截容错。
    但并发跑的另一个进程可能在列目录之后、读文件之前把某个分片删掉（它合并完
    自己的那份），丢掉一个分片不算错误——那一批已经由它并入正式表了。
    """
    paths = staged_shards(store, subdir)
    frames: list[pl.DataFrame] = []
    for path in paths:
        try:
            frames.append(pl.read_parquet(path))
        except FileNotFoundError:
            continue
    if not frames:
        return pl.DataFrame(schema=schema), paths
    return pl.concat(frames, how="vertical_relaxed"), paths


class StagingWriter:
    """抓取期间的追加式暂存：一批一个分片，原子写入，不碰正式表。

    为什么要它：长耗时抓取（`fetch-sizes` 三小时、`fetch-readme` 十几小时）
    若整段持锁，当天的 `collect` 会直接因拿不到锁而失败（2026-11-07 实际事故）。
    把写入改成「锁外写分片、收尾短锁合并」之后，长耗时部分不再占锁；中断时
    已抓到的批次也还在分片里，下一轮据此续抓，而不是从头再烧一遍请求。
    """

    def __init__(self, store: DatasetStore, snapshot_date: date, subdir: str) -> None:
        self._root = staging_root(store, subdir)
        self._prefix = snapshot_date.isoformat()
        # 用随机串而不是 pid 做分片标识：pid 会被复用，复用之后这一轮的分片会
        # 直接覆盖上一轮遗留的同名分片（原子替换不会报错，只是数据悄悄没了）。
        self._run = uuid4().hex[:12]
        self._seq = 0
        #: 已写进分片的行数。尾部不足 `flush_every` 的那批由调用方补写。
        self.flushed = 0

    def write(self, frame: pl.DataFrame) -> Path | None:
        """原子写一个分片；空批返回 None。"""
        if frame.height == 0:
            return None
        self._root.mkdir(parents=True, exist_ok=True)
        target = self._root / f"{self._prefix}-{self._run}-{self._seq:04d}.parquet"
        write_parquet(frame, target)
        self._seq += 1
        self.flushed += frame.height
        return target


def clear_staged(store: DatasetStore, paths: Sequence[Path] | None, subdir: str) -> None:
    """删除已并入正式表的暂存分片；不给路径就清空该链路的暂存目录。

    只删「这一次读到的路径」而不是整目录：并发跑的另一轮可能刚写进新分片，
    那是它的成果，不能替它丢掉。
    """
    targets = staged_shards(store, subdir) if paths is None else list(paths)
    for path in targets:
        path.unlink(missing_ok=True)
    root = staging_root(store, subdir)
    if root.exists() and not any(root.iterdir()):
        root.rmdir()
