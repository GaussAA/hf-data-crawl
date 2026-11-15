"""执行体与档案层之间的契约：交付那一刻双方都认的几样东西。

为什么单独一个模块
------------------
`Payload` / `RunContext` / `EXECUTORS` 原先都住在 `delivery.py`。于是任何一种形态
的执行体（查询、回填，将来还有别的）想被档案层看见，就得往档案层那个文件里塞两百
行——`EXECUTORS` 注释里「新形态只在这里加一行」是一句办不到的话，因为它还有半句
没说：另外两百行也得写进同一个文件。

把契约单独拿出来，方向才顺：

    执行体模块（query_exec / fill_exec） → 本模块 → 档案层（delivery）

执行体不必认识「档案怎么落盘、README 怎么写」，档案层也不必认识「这次跑的是 SQL
还是 Excel」——它只认 `Payload`。两边都只依赖这里。

与 `ops/finding.py` 是同一个手法：概念独立成模块，环就解开了，且不再需要靠
`TYPE_CHECKING` 把环藏起来（架构测试把延迟导入也算环，理由很对）。
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ..platform.store import DatasetStore
from ..query.gold import GoldCatalog
from .requirement import Kind, Spec
from .stamp import SourceStamp


class DeliveryBlocked(RuntimeError):
    """按护栏不该交付（0 行、超上限、底库缺失、执行失败）。一律不落盘。"""


@dataclass(frozen=True)
class Payload:
    """执行体的产出，也是**执行体与档案层之间唯一的契约**。

    档案与 README 只认这个结构，不认「这次跑的是 SQL 还是回填」。它们交付的东西
    不一样，可「交付这件事」是一样的——这正是两类需求能合成一套体系的原因。
    """

    rows: int
    columns: tuple[str, ...]
    #: 产物元数据：name / bytes / sha256。预演时为空。
    files: list[dict[str, Any]] = field(default_factory=list)
    #: 写进 manifest 顶层的形态特有口径（query 的 sql/criteria、backfill 的 fill）。
    definition: dict[str, Any] = field(default_factory=dict)
    #: 写进 README「口径」段的若干行（markdown）。
    detail_lines: list[str] = field(default_factory=list)
    #: 输入表的列名（回填形态有；查询形态没有）。档案据此比对「这次的表和上次一样吗」。
    header: tuple[str, ...] = ()


@dataclass(frozen=True)
class RunContext:
    """执行体拿到的全部上下文。执行体不自己开 catalog、不算指纹、不定目录。"""

    store: DatasetStore
    spec: Spec
    staging: Path
    catalog: GoldCatalog
    source: SourceStamp
    dry_run: bool = False


#: 形态 → 执行体。新形态自己在本文件之外登记，档案层一行都不用改。
Executor = Callable[[RunContext], Payload]
EXECUTORS: dict[Kind, Executor] = {}
