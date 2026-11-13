"""Pipeline 切片公开接口：数据流水线（raw → bronze → silver → current）。

注意: check_and_recover 不在此暴露,因为它会引入循环依赖。
CLI 直接从 pipeline.recovery 导入。
"""

__all__: list[str] = []
