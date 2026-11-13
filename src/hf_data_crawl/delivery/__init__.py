"""Delivery 切片公开接口：需求管理与交付。

提供:
- 需求规约类型 (Cadence, Kind)
"""

from .requirement import Cadence, Kind

__all__ = [
    "Cadence",
    "Kind",
]
