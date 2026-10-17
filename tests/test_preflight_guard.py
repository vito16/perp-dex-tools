import unittest
from decimal import Decimal
from helpers.preflight_guard import PreFlightLiquidityGuard, PreFlightCheckResult


class TestPreFlightLiquidityGuard(unittest.TestCase):

    def setUp(self):
        self.guard = PreFlightLiquidityGuard(
            min_depth_ratio=Decimal('1.5'),
            max_slippage_bps=Decimal('15'),  # 0.15%
            adaptive_sizing=True,
            min_order_size=Decimal('0.01'),
        )

    def test_sufficient_depth_low_slippage_passes(self):
        # Asks: 100 @ 1000.0, 100 @ 1000.5, 100 @ 1001.0
        asks = [
            (Decimal('1000.0'), Decimal('100')),
            (Decimal('1000.5'), Decimal('100')),
            (Decimal('1001.0'), Decimal('100')),
        ]
        res = self.guard.check_liquidity(
            orderbook_levels=asks,
            side='buy',
            desired_quantity=Decimal('50')
        )
        self.assertTrue(res.passed)
        self.assertEqual(res.safe_quantity, Decimal('50'))
        self.assertEqual(res.expected_vwap, Decimal('1000.0'))
        self.assertEqual(res.slippage_bps, Decimal('0'))

    def test_excessive_slippage_triggers_adaptive_or_reject(self):
        # Asks: 1 @ 1000.0, 10 @ 1050.0 (5% slippage on second level)
        asks = [
            (Decimal('1000.0'), Decimal('1.0')),
            (Decimal('1050.0'), Decimal('10.0')),
        ]
        # Target 5 units -> will need 1 unit @ 1000 and 4 units @ 1050 -> massive slippage
        # With adaptive_sizing=False:
        strict_guard = PreFlightLiquidityGuard(
            min_depth_ratio=Decimal('1.5'),
            max_slippage_bps=Decimal('10'),
            adaptive_sizing=False,
            min_order_size=Decimal('0.01'),
        )
        res = strict_guard.check_liquidity(
            orderbook_levels=asks,
            side='buy',
            desired_quantity=Decimal('5.0')
        )
        self.assertFalse(res.passed)
        self.assertIn("Pre-flight check failed", res.reason)

        # With adaptive_sizing=True:
        res_adaptive = self.guard.check_liquidity(
            orderbook_levels=asks,
            side='buy',
            desired_quantity=Decimal('5.0')
        )
        self.assertTrue(res_adaptive.passed)
        # Should adaptively size down to top_size / min_depth_ratio = 1.0 / 1.5 = ~0.666...
        self.assertLess(res_adaptive.safe_quantity, Decimal('5.0'))
        self.assertGreaterEqual(res_adaptive.safe_quantity, self.guard.min_order_size)

    def test_thin_orderbook_insufficient_depth(self):
        asks = [
            (Decimal('1000.0'), Decimal('0.005')),
        ]
        res = self.guard.check_liquidity(
            orderbook_levels=asks,
            side='buy',
            desired_quantity=Decimal('1.0')
        )
        # Total available is 0.005, which is < min_order_size 0.01
        self.assertFalse(res.passed)

    def test_empty_levels(self):
        res = self.guard.check_liquidity(
            orderbook_levels=[],
            side='sell',
            desired_quantity=Decimal('1.0')
        )
        self.assertFalse(res.passed)

    def test_sell_side_depth_and_vwap(self):
        bids = [
            (Decimal('2000.0'), Decimal('20')),
            (Decimal('1999.0'), Decimal('20')),
        ]
        res = self.guard.check_liquidity(
            orderbook_levels=bids,
            side='sell',
            desired_quantity=Decimal('10')
        )
        self.assertTrue(res.passed)
        self.assertEqual(res.safe_quantity, Decimal('10'))
        self.assertEqual(res.expected_vwap, Decimal('2000.0'))


if __name__ == '__main__':
    unittest.main()
