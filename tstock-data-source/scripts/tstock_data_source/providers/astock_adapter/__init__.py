"""
astock_adapter 模块
提供上游 a-stock-data 动态载入能力
"""

from .loader import (
    load_astock_namespace,
    get_astock_function,
    call_astock,
)

__all__ = [
    "load_astock_namespace",
    "get_astock_function",
    "call_astock",
]
