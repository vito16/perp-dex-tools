"""
Binance exchange client implementation.
"""

import os
import asyncio
import json
import sys
from decimal import Decimal
from typing import Dict, Any, List, Optional, Tuple
from binance.um_futures import UMFutures
from binance.websocket.um_futures.websocket_client import UMFuturesWebsocketClient

from .base import BaseExchangeClient, OrderResult, OrderInfo, query_retry
from helpers.logger import TradingLogger


class BinanceWebSocketManager:
    """WebSocket manager for Binance order updates."""

    def __init__(self, api_key: str, api_secret: str, symbol: str, order_update_callback):
        self.api_key = api_key
        self.api_secret = api_secret
        self.symbol = symbol
        self.order_update_callback = order_update_callback
        self.ws_client = None
        self.running = False
        self.logger = None
        self.listen_key = None
        self.client = UMFutures(key=api_key, secret=api_secret)
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _get_listen_key(self) -> str:
        """Get or refresh the listen key."""
        if not self.listen_key:
            self.listen_key = self.client.new_listen_key()["listenKey"]
        return self.listen_key

    def _renew_listen_key(self):
        """Renew the listen key."""
        try:
            self.client.renew_listen_key(listenKey=self.listen_key)
        except Exception:
            # If renewal fails, get a new key
            self.listen_key = None
            self._get_listen_key()

    async def connect(self):
        """Connect to Binance WebSocket."""
        try:
            self.logger.log(f"Connecting to Binance WebSocket", "INFO")

            self._loop = asyncio.get_running_loop()
            if not self._loop:
                raise ValueError("BinanceWebSocketManager Error: no loop")
            
            # Get listen key
            listen_key = self._get_listen_key()
            
            # Create WebSocket client
            self.ws_client = UMFuturesWebsocketClient(on_message=self._handle_message)
            self.ws_client.user_data(listen_key=listen_key)
            self.running = True
            
            if self.logger:
                self.logger.log(f"Connected to Binance WebSocket for {self.symbol}", "INFO")
            
            # Start listen key renewal task
            asyncio.create_task(self._renew_listen_key_loop())
            
        except Exception as e:
            if self.logger:
                self.logger.log(f"WebSocket connection error: {e}", "ERROR")
            raise

    async def _renew_listen_key_loop(self):
        """Background task to renew listen key every 30 minutes."""
        while self.running:
            try:
                await asyncio.sleep(30 * 60)  # 30 minutes
                self._renew_listen_key()
                if self.logger:
                    self.logger.log("Renewed Binance listen key", "INFO")
            except Exception as e:
                if self.logger:
                    self.logger.log(f"Error renewing listen key: {e}", "ERROR")

    def _handle_message(self, _, raw_msg: str):
        """Handle incoming WebSocket messages."""
        try:
            msg = json.loads(raw_msg)
            
            # Check if it's an order update event
            if 'e' not in msg or msg.get('e') != 'ORDER_TRADE_UPDATE':
                return
            
            order = msg.get('o', {})
            
            # Only process orders for our symbol
            if order.get('s') != self.symbol:
                return
            
            # Process the order update - use thread-safe method
            try:
                asyncio.run_coroutine_threadsafe(self._handle_order_update(order), self._loop)
            except RuntimeError:
                # If no event loop, process synchronously
                # This is a fallback - ideally we should always have a loop
                if self.logger:
                    self.logger.log("No event loop available for order update", "WARNING")
            
        except json.JSONDecodeError as e:
            if self.logger:
                self.logger.log(f"Failed to parse WebSocket message: {e}", "ERROR")
        except Exception as e:
            if self.logger:
                self.logger.log(f"Error handling WebSocket message: {e}", "ERROR")

    async def _handle_order_update(self, order_data: Dict[str, Any]):
        """Handle order update messages."""
        try:
            order_id = order_data.get('i', '')
            symbol = order_data.get('s', '')
            side = order_data.get('S', '').lower()  # BUY or SELL
            status = order_data.get('X', '')  # Order status
            quantity = order_data.get('q', '0')
            price = order_data.get('p', '0')
            filled_quantity = order_data.get('z', '0')
            avg_price = order_data.get('ap', '0')
            
            # Determine if this is a close order based on reduceOnly flag
            is_reduce_only = order_data.get('R', False)
            order_type = "CLOSE" if is_reduce_only else "OPEN"
            
            # Map Binance status to our standard status
            status_map = {
                'NEW': 'OPEN',
                'PARTIALLY_FILLED': 'PARTIALLY_FILLED',
                'FILLED': 'FILLED',
                'CANCELED': 'CANCELED',
                'EXPIRED': 'CANCELED'
            }
            
            mapped_status = status_map.get(status, status)
            
            # Call the order update callback if it exists
            if self.order_update_callback:
                await self.order_update_callback({
                    'order_id': order_id,
                    'side': side,
                    'order_type': order_type,
                    'status': mapped_status,
                    'size': quantity,
                    'price': avg_price if float(avg_price) > 0 else price,
                    'contract_id': symbol,
                    'filled_size': filled_quantity
                })
            
            if mapped_status in ['FILLED']:
                self.logger.log_transaction(order_id, side, quantity, price, status, 'binance')
                
        except Exception as e:
            if self.logger:
                self.logger.log(f"Error handling order update: {e}", "ERROR")

    async def disconnect(self):
        """Disconnect from WebSocket."""
        self.running = False
        if self.ws_client:
            self.ws_client.stop()
            if self.logger:
                self.logger.log("WebSocket disconnected", "INFO")

    def set_logger(self, logger):
        """Set the logger instance."""
        self.logger = logger


class BinanceClient(BaseExchangeClient):
    """Binance exchange client implementation."""

    def __init__(self, config: Dict[str, Any], logger: Optional[TradingLogger]):
        """Initialize Binance client."""
        super().__init__(config)

        # Binance credentials from environment
        self.api_key = os.getenv('BINANCE_API_KEY')
        self.api_secret = os.getenv('BINANCE_API_SECRET')

        if not self.api_key or not self.api_secret:
            raise ValueError("BINANCE_API_KEY and BINANCE_API_SECRET must be set in environment variables")

        # Initialize Binance client
        self.client = UMFutures(key=self.api_key, secret=self.api_secret)
        self._order_update_handler = None
        self.logger = logger or TradingLogger(exchange="binance", ticker=self.config.ticker, log_to_console=False)

    def _validate_config(self) -> None:
        """Validate Binance configuration."""
        required_env_vars = ['BINANCE_API_KEY', 'BINANCE_API_SECRET']
        missing_vars = [var for var in required_env_vars if not os.getenv(var)]
        if missing_vars:
            raise ValueError(f"Missing required environment variables: {missing_vars}")

    async def connect(self) -> None:
        """Connect to Binance WebSocket."""
        # Initialize WebSocket manager
        self.ws_manager = BinanceWebSocketManager(
            api_key=self.api_key,
            api_secret=self.api_secret,
            symbol=self.config.contract_id,
            order_update_callback=self._handle_websocket_order_update
        )
        
        # Pass config to WebSocket manager
        self.ws_manager.config = self.config

        # Initialize logger
        self.ws_manager.set_logger(self.logger)

        try:
            # Start WebSocket connection
            await self.ws_manager.connect()
            # Wait a moment for connection to establish
            await asyncio.sleep(2)
        except Exception as e:
            self.logger.log(f"Error connecting to Binance WebSocket: {e}", "ERROR")
            raise

    async def disconnect(self) -> None:
        """Disconnect from Binance."""
        try:
            if hasattr(self, 'ws_manager') and self.ws_manager:
                await self.ws_manager.disconnect()
        except Exception as e:
            self.logger.log(f"Error during Binance disconnect: {e}", "ERROR")

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return "binance"

    def setup_order_update_handler(self, handler) -> None:
        """Setup order update handler for WebSocket."""
        self._order_update_handler = handler

    async def _handle_websocket_order_update(self, order_data: Dict[str, Any]):
        """Handle order updates from WebSocket."""
        try:
            if self._order_update_handler:
                self._order_update_handler(order_data)
        except Exception as e:
            self.logger.log(f"Error handling WebSocket order update: {e}", "ERROR")

    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        """Fetch best bid and offer prices."""
        # Get order book ticker
        ticker = self.client.book_ticker(symbol=contract_id)
        
        best_bid = Decimal(ticker.get('bidPrice', '0'))
        best_ask = Decimal(ticker.get('askPrice', '0'))
        
        return best_bid, best_ask
    
    @query_retry(default_return=(0, 0))
    async def fetch_bbo_prices_quanities(self, contract_id: str) -> Tuple[Decimal, Decimal, Decimal, Decimal]:
        """Fetch best bid and offer prices."""
        # Get order book ticker
        ticker = self.client.book_ticker(symbol=contract_id)
        
        best_bid = Decimal(ticker.get('bidPrice', '0'))
        best_bid_qty = Decimal(ticker.get('bidQty', '0'))
        best_ask = Decimal(ticker.get('askPrice', '0'))
        best_ask_qty = Decimal(ticker.get('askQty', '0'))
        
        return best_bid, best_ask, best_bid_qty, best_ask_qty

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        """Place an open order with Binance with retry logic for POST_ONLY rejections."""
        max_retries = 1
        retry_count = 0

        while retry_count < max_retries:
            retry_count += 1

            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message='Invalid bid/ask prices')

            if direction == 'buy':
                # For buy orders, place at or below best ask
                order_price = best_ask - self.config.tick_size
                side = 'BUY'
            else:
                # For sell orders, place at or above best bid
                order_price = best_bid + self.config.tick_size
                side = 'SELL'

            try:
                # Place the order using Binance API (post-only)
                order_result = self.client.new_order(
                    symbol=contract_id,
                    side=side,
                    positionSide='BOTH',
                    type='LIMIT',
                    quantity=str(quantity),
                    price=str(self.round_to_tick(order_price)),
                    timeInForce='GTX'  # Good Till Crossing (Post Only)
                )

                if not order_result:
                    return OrderResult(success=False, error_message='Failed to place order')

                # Extract order ID from response
                order_id = order_result.get('orderId')
                if not order_id:
                    self.logger.log(f"[OPEN] No order ID in response: {order_result}", "ERROR")
                    return OrderResult(success=False, error_message='No order ID in response')

                # Order successfully placed
                return OrderResult(
                    success=True,
                    order_id=str(order_id),
                    side=side.lower(),
                    size=quantity,
                    price=order_price,
                    status='NEW'
                )

            except Exception as e:
                error_msg = str(e)
                if 'would immediately match and trade' in error_msg.lower():
                    self.logger.log(f"[OPEN] Order rejected (would match): {error_msg}", "WARNING")
                    continue
                else:
                    self.logger.log(f"[OPEN] Error placing order: {error_msg}", "ERROR")
                    return OrderResult(success=False, error_message=error_msg)

        return OrderResult(success=False, error_message='Max retries exceeded')

    async def place_market_order(self, contract_id: str, quantity: Decimal, direction: str, reduce_only: bool = False) -> OrderResult:
        """Place a market order with Binance."""
        try:
            # Validate direction
            if direction.lower() == 'buy':
                side = 'BUY'
            elif direction.lower() == 'sell':
                side = 'SELL'
            else:
                raise Exception(f"[OPEN] Invalid direction: {direction}")

            result = self.client.new_order(
                symbol=contract_id,
                side=side,
                positionSide='BOTH',
                type='MARKET',
                quantity=str(quantity),
                reduceOnly=reduce_only,
            )

            order_id = str(result.get('orderId'))
            order_status = result.get('status', '').upper()

            if order_status != 'FILLED':
                self.logger.log(f"Market order failed with status: {order_status}", "ERROR")
                sys.exit(1)
            else:
                avg_price = Decimal(result.get('avgPrice', '0'))
                return OrderResult(
                    success=True,
                    order_id=order_id,
                    side=direction.lower(),
                    size=quantity,
                    price=avg_price,
                    status='FILLED'
                )

        except Exception as e:
            self.logger.log(f"Error placing market order: {e}", "ERROR")
            return OrderResult(success=False, error_message=str(e))

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        """Place a close order with Binance with retry logic."""
        max_retries = 15
        retry_count = 0

        while retry_count < max_retries:
            retry_count += 1
            
            # Get current market prices
            best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

            if best_bid <= 0 or best_ask <= 0:
                return OrderResult(success=False, error_message='No bid/ask data available')

            # Adjust order price based on market conditions
            adjusted_price = price
            if side.lower() == 'sell':
                binance_side = 'SELL'
                # For sell orders, ensure price is above best bid
                if price <= best_bid:
                    adjusted_price = best_bid + self.config.tick_size
            elif side.lower() == 'buy':
                binance_side = 'BUY'
                # For buy orders, ensure price is below best ask
                if price >= best_ask:
                    adjusted_price = best_ask - self.config.tick_size

            adjusted_price = self.round_to_tick(adjusted_price)

            try:
                # Place the order using Binance API
                order_result = self.client.new_order(
                    symbol=contract_id,
                    side=binance_side,
                    positionSide='BOTH',
                    type='LIMIT',
                    quantity=str(quantity),
                    price=str(adjusted_price),
                    reduceOnly='true',
                    timeInForce='GTX'  # Post Only
                )

                if not order_result:
                    return OrderResult(success=False, error_message='Failed to place order')

                # Extract order ID from response
                order_id = order_result.get('orderId')
                if not order_id:
                    self.logger.log(f"[CLOSE] No order ID in response: {order_result}", "ERROR")
                    return OrderResult(success=False, error_message='No order ID in response')

                # Order successfully placed
                return OrderResult(
                    success=True,
                    order_id=str(order_id),
                    side=side.lower(),
                    size=quantity,
                    price=adjusted_price,
                    status='NEW'
                )

            except Exception as e:
                error_msg = str(e)
                if 'would immediately match and trade' in error_msg.lower():
                    self.logger.log(f"[CLOSE] Order rejected (would match): {error_msg}", "WARNING")
                    continue
                else:
                    self.logger.log(f"[CLOSE] Error placing order: {error_msg}", "ERROR")
                    return OrderResult(success=False, error_message=error_msg)

        return OrderResult(success=False, error_message='Max retries exceeded for close order')

    async def cancel_order(self, order_id: str) -> OrderResult:
        """Cancel an order with Binance."""
        try:
            cancel_result = self.client.cancel_order(
                symbol=self.config.contract_id,
                orderId=order_id
            )

            if not cancel_result:
                return OrderResult(success=False, error_message='Failed to cancel order')

            filled_size = Decimal(cancel_result.get('executedQty', 0))
            
            return OrderResult(success=True, filled_size=filled_size)

        except Exception as e:
            error_msg = str(e)
            self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {error_msg}", "ERROR")
            return OrderResult(success=False, error_message=error_msg)

    @query_retry()
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        """Get order information from Binance."""
        try:
            order_result = self.client.query_order(
                symbol=self.config.contract_id,
                orderId=order_id
            )

            if not order_result:
                return None

            # Map Binance side to our format
            side = order_result.get('side', '').lower()
            
            return OrderInfo(
                order_id=str(order_result.get('orderId', '')),
                side=side,
                size=Decimal(order_result.get('origQty', 0)),
                price=Decimal(order_result.get('price', 0)),
                status=order_result.get('status', ''),
                filled_size=Decimal(order_result.get('executedQty', 0)),
                remaining_size=Decimal(order_result.get('origQty', 0)) - Decimal(order_result.get('executedQty', 0))
            )

        except Exception as e:
            self.logger.log(f"Error getting order info: {e}", "ERROR")
            return None

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        """Get active orders for a contract."""
        try:
            active_orders = self.client.get_orders(symbol=contract_id)

            if not active_orders:
                return []

            orders = []
            for order in active_orders:
                if isinstance(order, dict):
                    side = order.get('side', '').lower()
                    orders.append(OrderInfo(
                        order_id=str(order.get('orderId', '')),
                        side=side,
                        size=Decimal(order.get('origQty', 0)),
                        price=Decimal(order.get('price', 0)),
                        status=order.get('status', ''),
                        filled_size=Decimal(order.get('executedQty', 0)),
                        remaining_size=Decimal(order.get('origQty', 0)) - Decimal(order.get('executedQty', 0))
                    ))

            return orders

        except Exception as e:
            self.logger.log(f"Error getting active orders: {e}", "ERROR")
            return []

    @query_retry(default_return=0)
    async def get_account_positions(self) -> Decimal:
        """Get account positions."""
        try:
            positions_data = self.client.get_position_risk()
            position_amt = 0
            
            for position in positions_data:
                if position.get('symbol', '') == self.config.contract_id:
                    position_amt = abs(Decimal(position.get('positionAmt', 0)))
                    break
                    
            return position_amt

        except Exception as e:
            self.logger.log(f"Error getting account positions: {e}", "ERROR")
            return Decimal(0)
    
    @query_retry(default_return=0)
    async def get_account_positions_with_sign(self) -> Decimal:
        """Get account positions."""
        try:
            positions_data = self.client.get_position_risk()
            position_amt = 0
            
            for position in positions_data:
                if position.get('symbol', '') == self.config.contract_id:
                    position_amt = Decimal(position.get('positionAmt', 0))
                    break
                    
            return position_amt

        except Exception as e:
            self.logger.log(f"Error getting account positions: {e}", "ERROR")
            return Decimal(0)

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        """Get contract ID and tick size for a ticker."""
        ticker = self.config.ticker
        
        if len(ticker) == 0:
            self.logger.log("Ticker is empty", "ERROR")
            raise ValueError("Ticker is empty")

        # Get exchange info
        exchange_info = self.client.exchange_info()
        
        # Find the symbol in the exchange info
        for symbol_info in exchange_info.get('symbols', []):
            symbol = symbol_info.get('symbol', '')
            base_asset = symbol_info.get('baseAsset', '')
            quote_asset = symbol_info.get('quoteAsset', '')
            
            # Match ticker with USDC quote
            if base_asset == ticker and quote_asset == 'USDC':
                self.config.contract_id = symbol
                
                # Get filters
                filters = symbol_info.get('filters', [])
                for f in filters:
                    if f.get('filterType') == 'PRICE_FILTER':
                        self.config.tick_size = Decimal(f.get('tickSize', 0))
                    elif f.get('filterType') == 'LOT_SIZE':
                        min_quantity = Decimal(f.get('minQty', 0))
                        
                        if self.config.quantity < min_quantity:
                            self.logger.log(
                                f"Order quantity is less than min quantity: {self.config.quantity} < {min_quantity}",
                                "ERROR"
                            )
                            raise ValueError(f"Order quantity is less than min quantity")
                
                break

        if self.config.contract_id == '':
            self.logger.log("Failed to get contract ID for ticker", "ERROR")
            raise ValueError("Failed to get contract ID for ticker")

        if self.config.tick_size == 0:
            self.logger.log("Failed to get tick size for ticker", "ERROR")
            raise ValueError("Failed to get tick size for ticker")

        return self.config.contract_id, self.config.tick_size