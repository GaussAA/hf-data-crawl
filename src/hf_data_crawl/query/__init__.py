"""Query 切片公开接口：Gold 视图与榜单。

这里登记的是**跨切片契约**：别的切片真的 import 了它，才写进来。当前全部
外部依赖来自 `delivery`——档案层要读 Gold、要算覆盖率、要生成报告。

榜单（BOARDS）供组合根注册子命令，`GoldCatalog` 是档案层唯一的数据读取入口；
`filled_count` / `column_coverage` / `share_text` 是填充率的三个口径，三者必须
一起改，散在各处就会算出互相矛盾的数字。
"""

from .gold import (
    BOARDS,
    GoldCatalog,
    GoldNotReady,
    column_coverage,
    filled_count,
    share_text,
    wide_column_names,
)

__all__ = [
    "BOARDS",
    # Gold 读取入口与未就绪信号
    "GoldCatalog",
    "GoldNotReady",
    # 填充率口径：三个一起看才自洽
    "filled_count",
    "column_coverage",
    "share_text",
    # 宽表列名
    "wide_column_names",
]
