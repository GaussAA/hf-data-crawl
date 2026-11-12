"""测试公用的小工具。

只放「多个测试文件都要、又不属于产品代码」的东西。放在这里而不是 conftest，
是因为它们是被显式调用的函数，不是 pytest 夹具——藏在夹具里，读测试的人得先
去翻另一份文件才知道数据是怎么造出来的。
"""

from __future__ import annotations

from pathlib import Path

from hf_data_crawl.delivery import method


def write_method(
    request_dir: Path, *, request_id: str = "REQ-0001", title: str = "测试需求", kind: str = "query"
) -> None:
    """给测试需求写一份合规的过程记录。

    走真实入口 `method.sync` 生成骨架，再把「待填」标记抹掉——测试要验的是
    「有方法时交付照常」，不是「骨架长什么样」；骨架本身由 test_method 专测。
    """
    method.sync(request_dir, request_id=request_id, title=title, kind=kind)
    doc = request_dir / method.METHOD_DIR / method.METHOD_DOC
    doc.write_text(doc.read_text(encoding="utf-8").replace(method.DRAFT_MARK, ""), encoding="utf-8")
