"""
Hyperliquid exchange client implementation.
"""

import os
import asyncio
import traceback
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from eth_account import Account
from hyperliquid.info import Info
from hyperliquid.exchange import Exchange
from hyperliquid.utils import constants

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger


class HyperliquidClient(BaseExchangeClient):
    """Hyperliquid exchange client implementation."""

    def __init__(self, config: Dict[str, Any]):
        """Initialize Hyperliquid client."""
        super().__init__(config)

        # Hyperliquid credentials from environment
        self.secret_key = os.getenv('HYPERLIQUID_API_SECRET_KEY')
        self.account_address = os.getenv('HYPERLIQUID_API_ADDRESS')
        self.use_testnet = os.getenv('HYPERLIQUID_TESTNET', 'false').lower() == 'true'

        if not self.secret_key or not self.account_address:
            raise ValueError("HYPERLIQUID_API_SECRET_KEY and HYPERLIQUID_API_ADDRESS must be set")

        # Set API URL based on environment
        self.base_url = constants.TESTNET_API_URL if self.use_testnet else constants.MAINNET_API_URL

        # Initialize wallet from private key
        self.wallet = Account.from_key(self.secret_key)

        # Detect builder perps dex from ticker (e.g., "xyz:GOLD" -> perp_dexs=["xyz"])
        perp_dexs = None
        if ":" in self.config.ticker:
            dex_name = self.config.ticker.split(":")[0].lower()
            perp_dexs = [dex_name]

        # Initialize Info client (REST API, no WebSocket yet)
        self.info = Info(self.base_url, skip_ws=True, perp_dexs=perp_dexs)

        # Initialize Exchange client for trading (with perp_dexs for builder perps)
        self.exchange = Exchange(
            self.wallet, 
            self.base_url, 
            account_address=self.account_address,
            perp_dexs=perp_dexs
        )

        # Initialize logger
        self.logger = TradingLogger(exchange="hyperliquid", ticker=self.config.ticker, log_to_console=False)

        # Order update handler
        self._order_update_handler = None

        # WebSocket state
        self._ws_info: Optional[Info] = None
        self._ws_task: Optional[asyncio.Task] = None
        self._ws_stop = asyncio.Event()

        # Asset metadata cache
        self._asset_index: Optional[int] = None
        self._sz_decimals: Optional[int] = None

    def _validate_config(self) -> None:
        """Validate Hyperliquid configuration."""
        required_env_vars = ['HYPERLIQUID_API_SECRET_KEY', 'HYPERLIQUID_API_ADDRESS']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    # ---------------------------
    # Connection / WebSocket
    # ---------------------------

    async def connect(self) -> None:
        """Connect WebSocket for order updates."""
        try:
            # Create a separate Info client with WebSocket enabled
            self._ws_info = Info(self.base_url, skip_ws=False)
            self.logger.log("[WS] connected", "INFO")
            await asyncio.sleep(0.5)  # Give connection time to establish
        except Exception as e:
            self.logger.log(f"[WS] connection error: {e}", "ERROR")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Hyperliquid."""
        try:
            self._ws_stop.set()
            if self._ws_info:
                # SDK handles cleanup internally
                self._ws_info = None
            self.logger.log("[WS] disconnected", "INFO")
        except Exception as e:
            self.logger.log(f"Error during disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "hyperliquid"

    # ---------------------------
    # WebSocket Order Updates
    # ---------------------------

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

        def order_update_callback(message):
            """Handle order updates from WebSocket."""
            try:
                if not message:
                    return

                channel = message.get("channel", "")
                if channel != "orderUpdates":
                    return

                data = message.get("data", [])
                for order in data:
                    order_id = str(order.get("oid", ""))
                    status = order.get("status", "").upper()
                    coin = order.get("coin", "")
                    side = order.get("side", "").lower()
                    sz = order.get("sz", "0")
                    limit_px = order.get("limitPx", "0")
                    filled = order.get("filled", "0")

                    # Filter by contract
                    if coin != self.config.ticker:
                        continue

                    # Determine order type (OPEN or CLOSE)
                    if side == self.config.close_order_side:
                        order_type = "CLOSE"
                    else:
                        order_type = "OPEN"

                    # Map Hyperliquid status to standard status
                    status_map = {
                        "OPEN": "OPEN",
                        "FILLED": "FILLED",
                        "CANCELED": "CANCELED",
                        "REJECTED": "CANCELED",
                    }
                    mapped_status = status_map.get(status, status)

                    # Check for partial fill
                    if mapped_status == "OPEN" and Decimal(filled) > 0:
                        mapped_status = "PARTIALLY_FILLED"

                    if mapped_status in ['OPEN', 'PARTIALLY_FILLED', 'FILLED', 'CANCELED']:
                        if self._order_update_handler:
                            self._order_update_handler({
                                'order_id': order_id,
                                'side': side,
                                'order_type': order_type,
                                'status': mapped_status,
                                'size': sz,
                                'price': limit_px,
                                'contract_id': coin,
                                'filled_size': filled
                            })

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Subscribe to order updates
        if self._ws_info:
            self._ws_info.subscribe(
                {"type": "orderUpdates", "user": self.account_address},
                order_update_callback
            )
            self.logger.log("[WS] subscribed to orderUpdates", "INFO")

    # REST API Helpers
    # ---------------------------

    def _get_l2_book(self, coin: str) -> dict:
        """Get L2 book directly via API (bypasses SDK name_to_coin mapping).
        
        This is needed for builder perps (xyz:GOLD) which SDK doesn't map.
        """
        import requests
        try:
            response = requests.post(
                f"{self.base_url}/info",
                json={"type": "l2Book", "coin": coin},
                timeout=10
            )
            if response.status_code != 200:
                self.logger.log(f"L2 book API error: {response.status_code}", "ERROR")
                return {"levels": [[], []]}
            data = response.json()
            if data is None:
                self.logger.log(f"L2 book returned None for {coin}", "ERROR")
                return {"levels": [[], []]}
            return data
        except Exception as e:
            self.logger.log(f"L2 book request failed: {e}", "ERROR")
            return {"levels": [[], []]}

    @query_retry(default_return=(Decimal(0), Decimal(0)))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices."""
        # For builder perps (xyz:GOLD, @182), use direct API call
        if ":" in contract_id or contract_id.startswith("@"):
            book = self._get_l2_book(contract_id)
        else:
            book = self.info.l2_snapshot(contract_id)

        levels = book.get("levels", [[], []])
        bids, asks = levels

        best_bid = Decimal(bids[0]["px"]) if bids else Decimal(0)
        best_ask = Decimal(asks[0]["px"]) if asks else Decimal(0)

        return best_bid, best_ask

    async def get_order_price(self, direction: str) -> Decimal:
        """Get order price based on direction."""
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.ticker)

        if best_bid <= 0 or best_ask <= 0:
            self.logger.log("Invalid bid/ask prices", "ERROR")
            raise ValueError("Invalid bid/ask prices")

        if direction == 'buy':
            order_price = best_ask - self.config.tick_size
        else:
            order_price = best_bid + self.config.tick_size

        return self.round_to_tick(order_price)

    # ---------------------------
    # Order Placement
    # ---------------------------

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Hyperliquid using ALO (Post Only)."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            try:
                best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

                if best_bid <= 0 or best_ask <= 0:
                    return OrderResult(success=False, error_message='Invalid bid/ask prices')

                if direction == 'buy':
                    order_price = best_ask - self.config.tick_size
                    is_buy = True
                else:
                    order_price = best_bid + self.config.tick_size
                    is_buy = False

                order_price = float(self.round_to_tick(order_price))

                # Place ALO (Add Liquidity Only / Post Only) order
                result = self.exchange.order(
                    contract_id,
                    is_buy,
                    float(quantity),
                    order_price,
                    {"limit": {"tif": "Alo"}}
                )

                if result.get("status") != "ok":
                    error_msg = str(result)
                    return OrderResult(success=False, error_message=error_msg)

                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if not statuses:
                    return OrderResult(success=False, error_message="No status in response")

                status = statuses[0]

                # Check for error (e.g., crossed market)
                if "error" in status:
                    error_msg = status["error"]
                    if "crossed" in error_msg.lower() or "minimum" in error_msg.lower() or "post only" in error_msg.lower():
                        retry_count += 1
                        await asyncio.sleep(0.05)
                        continue
                    return OrderResult(success=False, error_message=error_msg)

                # Success - order is resting
                if "resting" in status:
                    oid = status["resting"]["oid"]
                    return OrderResult(
                        success=True,
                        order_id=str(oid),
                        side=direction,
                        size=quantity,
                        price=Decimal(str(order_price)),
                        status='OPEN'
                    )

                # Order was filled immediately
                if "filled" in status:
                    oid = status["filled"]["oid"]
                    return OrderResult(
                        success=True,
                        order_id=str(oid),
                        side=direction,
                        size=quantity,
                        price=Decimal(str(order_price)),
                        status='FILLED'
                    )

                return OrderResult(success=False, error_message=f"Unexpected status: {status}")

            except Exception as e:
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)
                    continue
                return OrderResult(success=False, error_message=str(e))

        return OrderResult(success=False, error_message='Max retries exceeded')

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Hyperliquid using ALO (Post Only)."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            try:
                best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

                if best_bid <= 0 or best_ask <= 0:
                    return OrderResult(success=False, error_message='Invalid bid/ask prices')

                is_buy = side.lower() == 'buy'
                adjusted_price = price

                # Ensure post-only by adjusting price if needed
                if side.lower() == 'sell':
                    if price <= best_bid:
                        adjusted_price = best_bid + self.config.tick_size
                elif side.lower() == 'buy':
                    if price >= best_ask:
                        adjusted_price = best_ask - self.config.tick_size

                adjusted_price = float(self.round_to_tick(adjusted_price))

                result = self.exchange.order(
                    contract_id,
                    is_buy,
                    float(quantity),
                    adjusted_price,
                    {"limit": {"tif": "Alo"}}
                )

                if result.get("status") != "ok":
                    return OrderResult(success=False, error_message=str(result))

                statuses = result.get("response", {}).get("data", {}).get("statuses", [])
                if not statuses:
                    return OrderResult(success=False, error_message="No status in response")

                status = statuses[0]

                if "error" in status:
                    error_msg = status["error"]
                    if "crossed" in error_msg.lower() or "post only" in error_msg.lower():
                        retry_count += 1
                        await asyncio.sleep(0.05)
                        continue
                    return OrderResult(success=False, error_message=error_msg)

                if "resting" in status:
                    oid = status["resting"]["oid"]
                    return OrderResult(
                        success=True,
                        order_id=str(oid),
                        side=side,
                        size=quantity,
                        price=Decimal(str(adjusted_price)),
                        status='OPEN'
                    )

                if "filled" in status:
                    oid = status["filled"]["oid"]
                    return OrderResult(
                        success=True,
                        order_id=str(oid),
                        side=side,
                        size=quantity,
                        price=Decimal(str(adjusted_price)),
                        status='FILLED'
                    )

                return OrderResult(success=False, error_message=f"Unexpected status: {status}")

            except Exception as e:
                if retry_count < max_retries - 1:
                    retry_count += 1
                    await asyncio.sleep(0.1)
                    continue
                return OrderResult(success=False, error_message=str(e))

        return OrderResult(success=False, error_message='Max retries exceeded for close order')

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order."""
        try:
            result = self.exchange.cancel(self.config.ticker, int(order_id))

            if result.get("status") == "ok":
                return OrderResult(success=True)

            return OrderResult(success=False, error_message=str(result))

        except Exception as e:
            return OrderResult(success=False, error_message=str(e))

    @query_retry()
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information."""
        order_status = self.info.query_order_by_oid(self.account_address, int(order_id))

        if not order_status:
            return None

        # API returns: {status: "order", order: {order: {...}, status: "filled/open/...", statusTimestamp: ...}}
        order_wrapper = order_status.get("order", {})
        if not order_wrapper:
            return None

        order = order_wrapper.get("order", {})
        status = order_wrapper.get("status", "").upper()

        if not order:
            return None

        side_raw = order.get("side", "")
        side = "buy" if side_raw == "B" else "sell" if side_raw == "A" else side_raw.lower()
        sz = Decimal(order.get("origSz", order.get("sz", "0")))
        limit_px = Decimal(order.get("limitPx", "0"))

        # Calculate filled from origSz - sz (sz is remaining)
        orig_sz = Decimal(order.get("origSz", "0"))
        remaining_sz = Decimal(order.get("sz", "0"))
        filled = orig_sz - remaining_sz

        # Map status
        if status == "OPEN" and filled > 0:
            status = "PARTIALLY_FILLED"

        return OrderInfo(
            order_id=order_id,
            side=side,
            size=orig_sz,
            price=limit_px,
            status=status,
            filled_size=filled,
            remaining_size=remaining_sz
        )

    def _get_open_orders(self, dex: str = None) -> list:
        """Get open orders, with optional dex parameter for builder perps."""
        import requests
        payload = {"type": "openOrders", "user": self.account_address}
        if dex:
            payload["dex"] = dex
        response = requests.post(f"{self.base_url}/info", json=payload, timeout=10)
        return response.json()

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract."""
        # For builder perps, need to pass dex parameter
        dex = None
        if ":" in contract_id:
            dex = contract_id.split(":")[0].lower()
            orders = self._get_open_orders(dex)
        else:
            orders = self.info.open_orders(self.account_address)

        if not orders:
            return []

        result = []
        for order in orders:
            coin = order.get("coin", "")
            if coin != contract_id:
                continue

            oid = str(order.get("oid", ""))
            side_raw = order.get("side", "")
            # API returns "A" for sell (ask), "B" for buy (bid)
            side = "sell" if side_raw == "A" else "buy" if side_raw == "B" else side_raw.lower()
            sz = Decimal(order.get("sz", "0"))
            limit_px = Decimal(order.get("limitPx", "0"))

            result.append(OrderInfo(
                order_id=oid,
                side=side,
                size=sz,
                price=limit_px,
                status="OPEN",
                filled_size=Decimal(0),
                remaining_size=sz
            ))

        return result

    def _get_user_state(self, dex: str = None) -> dict:
        """Get user state, with optional dex parameter for builder perps."""
        import requests
        payload = {"type": "clearinghouseState", "user": self.account_address}
        if dex:
            payload["dex"] = dex
        response = requests.post(f"{self.base_url}/info", json=payload, timeout=10)
        return response.json()

    @query_retry(default_return=Decimal(0))
    async def get_account_positions(self) -> Decimal:
        """Get account positions for current contract."""
        ticker = self.config.ticker

        # For builder perps, need to pass dex parameter
        dex = None
        if ":" in ticker:
            dex = ticker.split(":")[0].lower()
            state = self._get_user_state(dex)
        else:
            state = self.info.user_state(self.account_address)

        if not state:
            return Decimal(0)

        positions = state.get("assetPositions", [])
        for pos in positions:
            position = pos.get("position", {})
            coin = position.get("coin", "")
            if coin == ticker:
                szi = position.get("szi", "0")
                return abs(Decimal(szi))

        return Decimal(0)

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract attributes (contract_id, tick_size)."""
        ticker = self.config.ticker

        if not ticker:
            raise ValueError("Ticker is empty")

        # Builder perps format: {dex}:{coin} (e.g., xyz:GOLD, xyz:SILVER)
        # or HIP-3 @ prefix (e.g., @182)
        if ":" in ticker or ticker.startswith("@"):
            # Normalize: dex part must be lowercase, coin part stays as-is
            # e.g., XYZ:GOLD -> xyz:GOLD
            if ":" in ticker:
                parts = ticker.split(":", 1)
                ticker = f"{parts[0].lower()}:{parts[1]}"
                self.config.ticker = ticker  # Update config with normalized ticker

            # For builder perps, use direct API call (SDK doesn't support these)
            book = self._get_l2_book(ticker)
            levels = book.get("levels", [[], []])

            if not levels[0]:
                raise ValueError(f"Market {ticker} not found or has no liquidity")

            # Calculate mid price from BBO
            bid = float(levels[0][0]["px"])
            ask = float(levels[1][0]["px"]) if levels[1] else bid
            mid_price = (bid + ask) / 2

            # Determine tick size based on price
            if mid_price > 10000:
                tick_size = Decimal("1")
            elif mid_price > 1000:
                tick_size = Decimal("0.1")
            elif mid_price > 100:
                tick_size = Decimal("0.01")
            elif mid_price > 1:
                tick_size = Decimal("0.001")
            else:
                tick_size = Decimal("0.0001")

            self.config.contract_id = ticker
            self.config.tick_size = tick_size
            self.logger.log(f"Builder perp: {ticker}, mid={mid_price:.2f}, tick={tick_size}", "INFO")

            return ticker, tick_size

        # Standard perpetuals - find in meta().universe
        meta = self.info.meta()
        universe = meta.get("universe", [])

        for i, asset in enumerate(universe):
            if asset.get("name") == ticker:
                self._asset_index = i
                self._sz_decimals = asset.get("szDecimals", 0)

                all_mids = self.info.all_mids()
                mid_price = float(all_mids.get(ticker, 0))

                if mid_price > 10000:
                    tick_size = Decimal("1")
                elif mid_price > 100:
                    tick_size = Decimal("0.1")
                elif mid_price > 1:
                    tick_size = Decimal("0.01")
                else:
                    tick_size = Decimal("0.0001")

                self.config.contract_id = ticker
                self.config.tick_size = tick_size

                return ticker, tick_size

        raise ValueError(f"Asset {ticker} not found in Hyperliquid universe")
