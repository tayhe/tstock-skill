"""
Unit tests for a-stock-data dynamic adapter and business functions.
"""

import sys
from pathlib import Path
import unittest
import numpy as np
import pandas as pd

# Add repo root and tstock-data-source/scripts to sys.path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tstock-data-source" / "scripts"))

from tstock_data_source.providers.astock_adapter import (
    load_astock_namespace,
    get_astock_function,
    call_astock,
)
from tstock_data_source.providers.astock import (
    fetch_realtime_quote,
    get_basic_from_astock,
    get_market_quote_from_astock,
    fetch_concept_blocks,
    fetch_holder_num_change,
    fetch_dividend_history,
    fetch_margin_trading,
    fetch_chip_distribution,
)


class TestAStockAdapter(unittest.TestCase):

    def test_loader_namespace(self):
        ns = load_astock_namespace()
        self.assertIsInstance(ns, dict)
        self.assertGreater(len(ns), 50)
        self.assertIn("EM_SESSION", ns)
        self.assertIn("em_get", ns)
        self.assertIn("tencent_quote", ns)
        self.assertIn("norm_ticker", ns)
        self.assertIn("chip_distribution", ns)

    def test_norm_ticker(self):
        self.assertEqual(call_astock("norm_ticker", "SH600519"), "600519")
        self.assertEqual(call_astock("norm_ticker", "000001.SZ"), "000001")
        self.assertEqual(call_astock("norm_ticker", "bj920001"), "920001")

    def test_realtime_quote_single(self):
        q = fetch_realtime_quote("600519")
        self.assertEqual(q.get("code"), "600519")
        self.assertEqual(q.get("name"), "贵州茅台")
        self.assertGreater(q.get("price"), 0)
        self.assertGreater(q.get("mcap_yi"), 1000)
        self.assertGreater(q.get("pe_ttm"), 0)
        self.assertEqual(q.get("source"), "astock.tencent")

    def test_realtime_quote_batch(self):
        quotes = fetch_realtime_quote(["600519", "000001"])
        self.assertIn("600519", quotes)
        self.assertIn("000001", quotes)
        self.assertEqual(quotes["600519"]["name"], "贵州茅台")
        self.assertEqual(quotes["000001"]["name"], "平安银行")

    def test_get_basic_from_astock(self):
        basic = get_basic_from_astock("600519")
        self.assertEqual(basic.get("code"), "600519")
        self.assertEqual(basic.get("name"), "贵州茅台")
        self.assertGreater(basic.get("market_cap"), 1e11)
        self.assertGreater(basic.get("pe_ttm"), 0)
        self.assertEqual(basic.get("source"), "astock")

    def test_get_market_quote_from_astock(self):
        mkt = get_market_quote_from_astock("600519")
        self.assertGreater(mkt.get("latest_price"), 0)
        self.assertIsNotNone(mkt.get("price_change_pct"))
        self.assertGreater(mkt.get("turnover"), 0)
        self.assertEqual(mkt.get("source"), "astock.tencent")

    def test_fetch_concept_blocks(self):
        cb = fetch_concept_blocks("600519")
        self.assertEqual(cb.get("code"), "600519")
        self.assertIsInstance(cb.get("concept_tags"), list)
        self.assertGreater(len(cb.get("concept_tags")), 0)
        self.assertIn("白酒", "".join(cb.get("concept_tags")))

    def test_fetch_microstructure(self):
        # 股东户数
        holders = fetch_holder_num_change("600519", page_size=2)
        self.assertIsInstance(holders, list)
        if holders:
            self.assertIn("holder_num", holders[0])

        # 分红历史
        divs = fetch_dividend_history("600519", page_size=2)
        self.assertIsInstance(divs, list)
        if divs:
            self.assertIn("bonus_rmb", divs[0])

        # 融资融券
        margins = fetch_margin_trading("600519", page_size=2)
        self.assertIsInstance(margins, list)
        if margins:
            self.assertIn("rzye", margins[0])

    def test_fetch_chip_distribution(self):
        dates = pd.date_range("2026-01-01", periods=60).strftime("%Y-%m-%d")
        df = pd.DataFrame({
            "date": dates,
            "high": np.linspace(102, 115, 60),
            "low": np.linspace(99, 109, 60),
            "close": np.linspace(101, 112, 60),
            "turn": np.full(60, 1.5),
        })
        res = fetch_chip_distribution(df)
        self.assertIn("profit_ratio", res)
        self.assertIn("avg_cost", res)
        self.assertIn("concentration_90", res)
        self.assertGreaterEqual(res["profit_ratio"], 0.0)
        self.assertLessEqual(res["profit_ratio"], 1.0)
        self.assertGreater(res["avg_cost"], 90.0)


if __name__ == "__main__":
    unittest.main()
