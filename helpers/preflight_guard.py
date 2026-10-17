"""
Pre-Flight Liquidity & Slippage Guard for DEX/CEX arbitrage and hedging.

Provides pre-trade orderbook depth validation, simulated VWAP impact calculation,
and adaptive order sizing to prevent single-leg naked exposure before placing orders.
"""

from decimal import Decimal
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass


@dataclass
class PreFlightCheckResult:
    """Result of a pre-flight liquidity and slippage check."""
    passed: bool
    safe_quantity: Decimal
    expected_vwap: Decimal
    slippage_bps: Decimal
    available_depth: Decimal
    reason: str


class PreFlightLiquidityGuard:
    """
    Pre-Flight Liquidity & Slippage Guard.

    Validates that:
    1. The taker/hedge leg has sufficient order book depth (at least min_depth_ratio * quantity).
    2. Simulated VWAP slippage across depth levels does not exceed max_slippage_bps.
    3. If the book cannot absorb the full quantity, it can optionally calculate the maximum
       safe quantity that fits within the slippage limit (adaptive sizing).
    """

    def __init__(
        self,
        min_depth_ratio: Decimal = Decimal('1.5'),
        max_slippage_bps: Decimal = Decimal('15'),  # 15 bps = 0.15%
        adaptive_sizing: bool = True,
        min_order_size: Decimal = Decimal('0.001'),
    ):
        self.min_depth_ratio = min_depth_ratio
        self.max_slippage_bps = max_slippage_bps
        self.adaptive_sizing = adaptive_sizing
        self.min_order_size = min_order_size

    def check_liquidity(
        self,
        orderbook_levels: List[Tuple[Decimal, Decimal]],  # [(price, size), ...]
        side: str,  # 'buy' or 'sell' (the taker side)
        desired_quantity: Decimal,
        reference_best_price: Optional[Decimal] = None
    ) -> PreFlightCheckResult:
        """
        Verify liquidity for an impending taker order.
        
        Args:
            orderbook_levels: Sorted list of (price, size).
                             For buy: asks sorted ascending by price.
                             For sell: bids sorted descending by price.
            side: 'buy' (taker buying from asks) or 'sell' (taker selling to bids)
            desired_quantity: Desired order quantity to fill.
            reference_best_price: Optional best price override. If None, uses top level price.
        """
        if not orderbook_levels or desired_quantity <= Decimal('0'):
            return PreFlightCheckResult(
                passed=False,
                safe_quantity=Decimal('0'),
                expected_vwap=Decimal('0'),
                slippage_bps=Decimal('0'),
                available_depth=Decimal('0'),
                reason="Empty orderbook levels or invalid desired quantity"
            )

        best_price = reference_best_price or orderbook_levels[0][0]
        if best_price <= Decimal('0'):
            return PreFlightCheckResult(
                passed=False,
                safe_quantity=Decimal('0'),
                expected_vwap=Decimal('0'),
                slippage_bps=Decimal('0'),
                available_depth=Decimal('0'),
                reason=f"Invalid best price: {best_price}"
            )

        # Calculate cumulative depth and simulated VWAP for desired_quantity
        required_qty = desired_quantity * self.min_depth_ratio
        cum_qty = Decimal('0')
        fill_for_target = Decimal('0')
        notional_for_target = Decimal('0')

        for price, size in orderbook_levels:
            if size <= Decimal('0'):
                continue
            
            # Accumulate for target quantity VWAP
            if fill_for_target < desired_quantity:
                needed = desired_quantity - fill_for_target
                fill = min(size, needed)
                fill_for_target += fill
                notional_for_target += fill * price

            # Accumulate total available depth
            cum_qty += size

        # Check if desired_quantity can be filled
        if fill_for_target < desired_quantity:
            if not self.adaptive_sizing:
                return PreFlightCheckResult(
                    passed=False,
                    safe_quantity=Decimal('0'),
                    expected_vwap=Decimal('0'),
                    slippage_bps=Decimal('9999'),
                    available_depth=cum_qty,
                    reason=f"Insufficient book depth: available {fill_for_target} < target {desired_quantity}"
                )
            if fill_for_target < self.min_order_size:
                return PreFlightCheckResult(
                    passed=False,
                    safe_quantity=Decimal('0'),
                    expected_vwap=Decimal('0'),
                    slippage_bps=Decimal('9999'),
                    available_depth=cum_qty,
                    reason=f"Available depth {fill_for_target} is below min_order_size {self.min_order_size}"
                )

        target_to_eval = min(desired_quantity, fill_for_target)
        target_vwap = notional_for_target / target_to_eval if target_to_eval > 0 else best_price
        
        # Calculate slippage in basis points (bps)
        # For buy: higher vwap is adverse (slippage > 0)
        # For sell: lower vwap is adverse (slippage > 0)
        if side.lower() == 'buy':
            slippage = (target_vwap - best_price) / best_price
        else:
            slippage = (best_price - target_vwap) / best_price
        
        slippage_bps = max(Decimal('0'), slippage * Decimal('10000'))
        has_depth_buffer = cum_qty >= required_qty

        # Evaluate pass
        if slippage_bps <= self.max_slippage_bps and has_depth_buffer:
            return PreFlightCheckResult(
                passed=True,
                safe_quantity=target_to_eval,
                expected_vwap=target_vwap,
                slippage_bps=slippage_bps,
                available_depth=cum_qty,
                reason="Pre-flight check passed"
            )

        # If slippage or depth failed, but adaptive sizing is enabled:
        if self.adaptive_sizing:
            safe_q = self._find_max_safe_quantity(orderbook_levels, side, best_price)
            if safe_q >= self.min_order_size:
                return PreFlightCheckResult(
                    passed=True,
                    safe_quantity=safe_q,
                    expected_vwap=best_price,
                    slippage_bps=Decimal('0'),
                    available_depth=cum_qty,
                    reason=f"Adaptive down-scale: reduced from {desired_quantity} to {safe_q} to satisfy slippage & depth buffer"
                )

        reason = (
            f"Pre-flight check failed: slippage {slippage_bps:.2f} bps "
            f"(max {self.max_slippage_bps} bps), depth {cum_qty} (required {required_qty})"
        )
        return PreFlightCheckResult(
            passed=False,
            safe_quantity=Decimal('0'),
            expected_vwap=target_vwap,
            slippage_bps=slippage_bps,
            available_depth=cum_qty,
            reason=reason
        )

    def _find_max_safe_quantity(
        self,
        orderbook_levels: List[Tuple[Decimal, Decimal]],
        side: str,
        best_price: Decimal
    ) -> Decimal:
        """Find the maximum quantity that can be filled within max_slippage_bps and depth buffer."""
        top_size = orderbook_levels[0][1]
        safe_by_top = top_size / self.min_depth_ratio
        return safe_by_top
