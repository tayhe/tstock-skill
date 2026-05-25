"""A股统一数据源包。"""

from tstock_data_source.snapshot import fetch_stock_snapshot, completeness_score
from tstock_data_source.batch import fetch_batch, get_scope_codes
from tstock_data_source.valuation import get_valuation_stable

__all__ = [
    "fetch_stock_snapshot",
    "completeness_score",
    "fetch_batch",
    "get_scope_codes",
    "get_valuation_stable",
]
