# pyright: reportUnsupportedDunderAll=false
"""Pipeline 切片公开接口：数据流水线（raw → bronze → silver → current）。

这里登记的是**跨切片契约**，粒度是「子模块」而非「符号」：本切片的公开面本来
就是这几个模块，`pipeline.rebuild` 这样的名字本身就是一句可审的声明——「我依赖
重建这一整块」，而不必逐符号背书。

为什么不逐符号：早期试过用惰性 `__getattr__` 把 15 个符号提到顶层，结果是
basedpyright 对 `__all__` 报 15 条 `reportUnsupportedDunderAll`（静态看不见），而环
检测同时把 `__init__ → derive` 与 `derive → __init__` 连成一条环。两条都是真信号：
符号级门面在这个切片上换来的只是「写法好看」，代价却是静态可分析性。

本切片内部大量 `from . import state` 这类写法，__init__ 因此**不 import 任何子
模块**——否则初始化时序会反过来影响内部模块能否取到名字。空 `__all__` 曾是这个
原因的结果，现在改为把子模块名登记在此，既不触加载，又让外部依赖有据可查。

旧注释说 `check_and_recover` 会引入循环依赖；实测它经 `rebuild` 拉进 `derive`，
并未成环，只是急切加载下会改变初始化时序。登记制不加载，该顾虑不再存在。

上面那条 `reportUnsupportedDunderAll=false` 只为本文件而设：`__all__` 里写的是
子模块名，basedpyright 要求这些名字先在本模块出现，而急切 `from . import raw` 会与
内部模块的 `from . import …` 连成环（环检测会如实报出来）。名字的真实性由
`tests/test_architecture.py::test_门面登记的名字都真实存在` 逐条核对，不靠类型检查。
"""

__all__ = [
    # 原始层：批次文件、目录布局、日状态
    "raw",
    # 派生链：逐日派生、重建、血缘
    "bronze",
    "silver",
    "state",
    "derive",
    "rebuild",
    "lineage",
    "marker",
    "recovery",
    # 卡片与 README 字段
    "card",
    "readme_fields",
    # 命令入口（组合根调用）
    "commands",
]
