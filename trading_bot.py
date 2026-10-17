"""
Modular Trading Bot - Supports multiple exchanges
"""

import os
import time
import asyncio
import traceback
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from exchanges import ExchangeFactory
from helpers import TradingLogger
from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot


@dataclass
class TradingConfig:
    """Configuration class for trading parameters."""
    ticker: str
    contract_id: str
    quantity: Decimal
    take_profit: Decimal
    tick_size: Decimal
    direction: str
    max_orders: int
    wait_time: int
    exchange: str
    grid_step: Decimal
    stop_price: Decimal
    pause_price: Decimal
    boost_mode: bool
    dry_run: bool = False

    @property
    def close_order_side(self) -> str:
        """Get the close order side based on bot direction."""
        return 'buy' if self.direction == "sell" else 'sell'


@dataclass
class OrderMonitor:
    """Thread-safe order monitoring state."""
    order_id: Optional[str] = None
    filled: bool = False
    filled_price: Optional[Decimal] = None
    filled_qty: Decimal = 0.0

    def reset(self):
        """Reset the monitor state."""
        self.order_id = None
        self.filled = False
        self.filled_price = None
        self.filled_qty = 0.0


class TradingBot:
    """Modular Trading Bot - Main trading logic supporting multiple exchanges."""

    def __init__(self, config: TradingConfig):
        self.config = config
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True)

        # Create exchange client
        try:
            self.exchange_client = ExchangeFactory.create_exchange(
                config.exchange,
                config
            )
        except ValueError as e:
            raise ValueError(f"Failed to create exchange client: {e}")

        # Trading state
        self.active_close_orders = []
        self.last_close_orders = 0
        self.last_open_order_time = 0
        self.last_log_time = 0
        self.last_telegram_status_time = 0
        self.current_order_status = None
        self.order_filled_event = asyncio.Event()
        self.order_canceled_event = asyncio.Event()
        self.shutdown_requested = False
        self.loop = None

        # Register order callback
        self._setup_websocket_handlers()

    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Disconnect from exchange
            await self.exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")

    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Check if this is for our contract
                if message.get('contract_id') != self.config.contract_id:
                    return

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                filled_size = Decimal(message.get('filled_size'))
                if order_type == "OPEN":
                    self.current_order_status = status

                if status == 'FILLED':
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event.set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.order_filled_event.set()

                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                    self.logger.log_transaction(order_id, side, message.get('size'), message.get('price'), status)
                elif status == "CANCELED":
                    if order_type == "OPEN":
                        self.order_filled_amount = filled_size
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_canceled_event.set)
                        else:
                            self.order_canceled_event.set()

                        if self.order_filled_amount > 0:
                            self.logger.log_transaction(order_id, side, self.order_filled_amount, message.get('price'), status)

                    # PATCH
                    if self.config.exchange == "extended":
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{Decimal(message.get('size')) - filled_size} @ {message.get('price')}", "INFO")
                    else:
                        self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                        f"{message.get('size')} @ {message.get('price')}", "INFO")
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{filled_size} @ {message.get('price')}", "INFO")
                else:
                    self.logger.log(f"[{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.exchange_client.setup_order_update_handler(order_update_handler)

    def _calculate_wait_time(self) -> Decimal:
        """Calculate wait time between orders."""
        cool_down_time = self.config.wait_time

        if len(self.active_close_orders) < self.last_close_orders:
            self.last_close_orders = len(self.active_close_orders)
            return 0

        self.last_close_orders = len(self.active_close_orders)
        if len(self.active_close_orders) >= self.config.max_orders:
            return 1

        if len(self.active_close_orders) / self.config.max_orders >= 2/3:
            cool_down_time = 2 * self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/3:
            cool_down_time = self.config.wait_time
        elif len(self.active_close_orders) / self.config.max_orders >= 1/6:
            cool_down_time = self.config.wait_time / 2
        else:
            cool_down_time = self.config.wait_time / 4

        # if the program detects active_close_orders during startup, it is necessary to consider cooldown_time
        if self.last_open_order_time == 0 and len(self.active_close_orders) > 0:
            self.last_open_order_time = time.time()

        if time.time() - self.last_open_order_time > cool_down_time:
            return 0
        else:
            return 1

    async def _has_open_capacity(self) -> bool:
        """Prevent new entries from exceeding max-orders exposure.

        This only blocks new opening orders. Existing positions and
        reduce-only closing orders are not cancelled or modified.
        """
        live_position = abs(
            await self.exchange_client.get_account_positions()
        )

        active_orders = await self.exchange_client.get_active_orders(
            self.config.contract_id
        )

        opening_side = self.config.direction.lower()

        pending_open_quantity = sum(
            abs(order.size)
            for order in active_orders
            if order.side.lower() == opening_side
            and order.status in ("OPEN", "PARTIALLY_FILLED")
        )

        max_exposure = (
            self.config.quantity * self.config.max_orders
        )

        projected_exposure = (
            live_position
            + pending_open_quantity
            + self.config.quantity
        )

        if projected_exposure > max_exposure:
            self.logger.log(
                "[RISK] New opening order blocked: "
                f"live_position={live_position}, "
                f"pending_open={pending_open_quantity}, "
                f"new_order={self.config.quantity}, "
                f"max_exposure={max_exposure}",
                "WARNING",
            )
            return False

        return True

    async def _place_and_monitor_open_order(self) -> bool:
        """Place an order and monitor its execution."""
        try:
            if self.config.dry_run:
                dry_run_price = await self.exchange_client.get_order_price(
                    self.config.direction
                )

                self.logger.log(
                    f"[DRY-RUN] Would place "
                    f"{self.config.direction.upper()} "
                    f"{self.config.quantity} {self.config.ticker} "
                    f"at {dry_run_price}; no order was sent",
                    "INFO",
                )

                self.last_open_order_time = time.time()

                # Keep dry-run pacing consistent with the real strategy loop.
                # The outer loop increments last_close_orders after this method.
                self.last_close_orders = len(self.active_close_orders) - 1

                return True

            # Reset state before placing order
            self.order_filled_event.clear()
            self.current_order_status = 'OPEN'
            self.order_filled_amount = 0.0

            # Hard live exposure guard before every opening attempt.
            if not await self._has_open_capacity():
                return False

            # Place the order
            order_result = await self.exchange_client.place_open_order(
                self.config.contract_id,
                self.config.quantity,
                self.config.direction
            )

            if not order_result.success:
                return False

            if self._telegram_flag(
                "TELEGRAM_NOTIFY_ORDER_SUBMITTED",
                True,
            ):
                await self._notify_order_event(
                    "🟡 Arcus QQQ 新开仓订单",
                    order_result.order_id,
                    self.config.direction,
                    order_result.filled_size
                    or order_result.size,
                    order_result.price,
                    order_result.status,
                    False,
                )

            if order_result.status == 'FILLED':
                return await self._handle_order_result(order_result)
            elif not self.order_filled_event.is_set():
                try:
                    await asyncio.wait_for(self.order_filled_event.wait(), timeout=10)
                except asyncio.TimeoutError:
                    pass

            # Handle order result
            return await self._handle_order_result(order_result)

        except Exception as e:
            error_text = str(e)

            if "HTTP 401" in error_text or "HTTP 403" in error_text:
                self.logger.log(
                    "Authentication/permission error; "
                    "stopping bot to prevent repeated order attempts",
                    "ERROR",
                )
                self.shutdown_requested = True

            self.logger.log(
                f"Error placing order: {e}",
                "ERROR",
            )
            self.logger.log(
                f"Traceback: {traceback.format_exc()}",
                "ERROR",
            )
            return False

    async def _place_close_order_and_notify(
        self,
        contract_id,
        quantity,
        price,
        side,
    ):
        close_order_result = await (
            self.exchange_client.place_close_order(
                contract_id,
                quantity,
                price,
                side,
            )
        )

        if (
            close_order_result.success
            and self._telegram_flag(
                "TELEGRAM_NOTIFY_ORDER_SUBMITTED",
                True,
            )
        ):
            await self._notify_order_event(
                "🟣 Arcus QQQ 新止盈平仓单",
                close_order_result.order_id,
                side,
                close_order_result.size,
                close_order_result.price,
                close_order_result.status,
                True,
            )

        return close_order_result

    async def _handle_order_result(self, order_result) -> bool:
        """Handle the result of an order placement."""
        order_id = order_result.order_id
        filled_price = order_result.price

        if self.order_filled_event.is_set() or order_result.status == 'FILLED':
            if self._telegram_flag(
                "TELEGRAM_NOTIFY_ORDER_FILLED",
                True,
            ):
                await self._notify_order_event(
                    "✅ Arcus QQQ 开仓成交",
                    order_result.order_id,
                    self.config.direction,
                    self.order_filled_amount
                    or order_result.filled_size
                    or order_result.size,
                    filled_price,
                    "FILLED",
                    False,
                )

            if self.config.boost_mode:
                close_order_result = await self.exchange_client.place_market_order(
                    self.config.contract_id,
                    self.config.quantity,
                    self.config.close_order_side
                )
            else:
                self.last_open_order_time = time.time()
                # Place close order
                close_side = self.config.close_order_side
                if close_side == 'sell':
                    close_price = filled_price * (1 + self.config.take_profit/100)
                else:
                    close_price = filled_price * (1 - self.config.take_profit/100)

                close_order_result = await self._place_close_order_and_notify(
                    self.config.contract_id,
                    self.config.quantity,
                    close_price,
                    close_side
                )
                if self.config.exchange == "lighter":
                    await asyncio.sleep(1)

                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")
                    raise Exception(f"[CLOSE] Failed to place close order: {close_order_result.error_message}")

                return True

        else:
            new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            def should_wait(direction: str, new_order_price: Decimal, order_result_price: Decimal) -> bool:
                if direction == "buy":
                    return new_order_price <= order_result_price
                elif direction == "sell":
                    return new_order_price >= order_result_price
                return False

            if self.config.exchange == "lighter":
                current_order_status = self.exchange_client.current_order.status
            else:
                order_info = await self.exchange_client.get_order_info(order_id)
                current_order_status = order_info.status

            while (
                should_wait(self.config.direction, new_order_price, order_result.price)
                and current_order_status == "OPEN"
            ):
                self.logger.log(f"[OPEN] [{order_id}] Waiting for order to be filled @ {order_result.price}", "INFO")
                await asyncio.sleep(5)
                if self.config.exchange == "lighter":
                    current_order_status = self.exchange_client.current_order.status
                else:
                    order_info = await self.exchange_client.get_order_info(order_id)
                    if order_info is not None:
                        current_order_status = order_info.status
                new_order_price = await self.exchange_client.get_order_price(self.config.direction)

            self.order_canceled_event.clear()
            # Cancel the order if it's still open
            self.logger.log(f"[OPEN] [{order_id}] Cancelling order and placing a new order", "INFO")
            if self.config.exchange == "lighter":
                cancel_result = await self.exchange_client.cancel_order(order_id)
                start_time = time.time()
                while (time.time() - start_time < 10 and self.exchange_client.current_order.status != 'CANCELED' and
                        self.exchange_client.current_order.status != 'FILLED'):
                    await asyncio.sleep(0.1)

                if self.exchange_client.current_order.status not in ['CANCELED', 'FILLED']:
                    raise Exception(f"[OPEN] Error cancelling order: {self.exchange_client.current_order.status}")
                else:
                    self.order_filled_amount = self.exchange_client.current_order.filled_size
            else:
                try:
                    cancel_result = await self.exchange_client.cancel_order(order_id)
                    if not cancel_result.success:
                        self.order_canceled_event.set()
                        self.logger.log(f"[CLOSE] Failed to cancel order {order_id}: {cancel_result.error_message}", "WARNING")
                    else:
                        self.current_order_status = "CANCELED"

                except Exception as e:
                    self.order_canceled_event.set()
                    self.logger.log(f"[CLOSE] Error canceling order {order_id}: {e}", "ERROR")

                if self.config.exchange == "backpack" or self.config.exchange == "extended":
                    self.order_filled_amount = cancel_result.filled_size
                else:
                    # Wait for cancel event or timeout
                    if not self.order_canceled_event.is_set():
                        try:
                            await asyncio.wait_for(self.order_canceled_event.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            order_info = await self.exchange_client.get_order_info(order_id)
                            self.order_filled_amount = order_info.filled_size

            if self.order_filled_amount > 0:
                close_side = self.config.close_order_side
                if self.config.boost_mode:
                    close_order_result = await self._place_close_order_and_notify(
                        self.config.contract_id,
                        self.order_filled_amount,
                        filled_price,
                        close_side
                    )
                else:
                    if close_side == 'sell':
                        close_price = filled_price * (1 + self.config.take_profit/100)
                    else:
                        close_price = filled_price * (1 - self.config.take_profit/100)

                    close_order_result = await self._place_close_order_and_notify(
                        self.config.contract_id,
                        self.order_filled_amount,
                        close_price,
                        close_side
                    )
                    if self.config.exchange == "lighter":
                        await asyncio.sleep(1)

                self.last_open_order_time = time.time()
                if not close_order_result.success:
                    self.logger.log(f"[CLOSE] Failed to place close order: {close_order_result.error_message}", "ERROR")

            return True

        return False

    async def _log_status_periodically(self):
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                # Get active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Get positions
                position_amt = await self.exchange_client.get_account_positions()
                position_amt = abs(position_amt)

                # Calculate active closing amount
                active_close_amount = sum(
                    Decimal(order.get('size', 0))
                    for order in self.active_close_orders
                    if isinstance(order, dict)
                )

                self.logger.log(f"Current Position: {position_amt} | Active closing amount: {active_close_amount} | "
                                f"Order quantity: {len(self.active_close_orders)}")

                await self._notify_status_snapshot(
                    position_amt,
                    active_close_amount,
                )

                self.last_log_time = time.time()
                # Check for position mismatch
                if abs(position_amt - active_close_amount) > (2 * self.config.quantity):
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    error_message += "Please manually rebalance your position and take-profit orders\n"
                    error_message += "请手动平衡当前仓位和正在关闭的仓位\n"
                    error_message += f"current position: {position_amt} | active closing amount: {active_close_amount} | "f"Order quantity: {len(self.active_close_orders)}\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    self.logger.log(error_message, "ERROR")

                    await self.send_notification(error_message.lstrip())

                    if not self.shutdown_requested:
                        self.shutdown_requested = True

                    mismatch_detected = True
                else:
                    mismatch_detected = False

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            print("--------------------------------")

    async def _meet_grid_step_condition(self) -> bool:
        if self.active_close_orders:
            picker = min if self.config.direction == "buy" else max
            next_close_order = picker(self.active_close_orders, key=lambda o: o["price"])
            next_close_price = next_close_order["price"]

            best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
            if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
                raise ValueError("No bid/ask data available")

            if self.config.direction == "buy":
                new_order_close_price = best_ask * (1 + self.config.take_profit/100)
                if next_close_price / new_order_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            elif self.config.direction == "sell":
                new_order_close_price = best_bid * (1 - self.config.take_profit/100)
                if new_order_close_price / next_close_price > 1 + self.config.grid_step/100:
                    return True
                else:
                    return False
            else:
                raise ValueError(f"Invalid direction: {self.config.direction}")
        else:
            return True

    async def _check_price_condition(self) -> bool:
        stop_trading = False
        pause_trading = False

        if self.config.pause_price == self.config.stop_price == -1:
            return stop_trading, pause_trading

        best_bid, best_ask = await self.exchange_client.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0 or best_bid >= best_ask:
            raise ValueError("No bid/ask data available")

        if self.config.stop_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.stop_price:
                    stop_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.stop_price:
                    stop_trading = True

        if self.config.pause_price != -1:
            if self.config.direction == "buy":
                if best_ask >= self.config.pause_price:
                    pause_trading = True
            elif self.config.direction == "sell":
                if best_bid <= self.config.pause_price:
                    pause_trading = True

        return stop_trading, pause_trading

    def _telegram_flag(self, name: str, default: bool = True) -> bool:
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in ("1", "true", "yes", "on")

    async def _notify_order_event(
        self,
        title: str,
        order_id: str,
        side: str,
        quantity,
        price,
        status: str,
        reduce_only: bool,
    ):
        if not self._telegram_flag(
            "TELEGRAM_NOTIFY_ORDER_SUBMITTED",
            True,
        ):
            return

        message = (
            f"{title}\n"
            f"交易所：Arcus\n"
            f"市场：{self.config.contract_id}\n"
            f"方向：{str(side).upper()}\n"
            f"数量：{quantity}\n"
            f"价格：{price}\n"
            f"订单 ID：{order_id}\n"
            f"状态：{status}\n"
            f"ReduceOnly：{'是' if reduce_only else '否'}"
        )

        await self.send_notification(message)

    async def _notify_status_snapshot(
        self,
        position_amount,
        active_close_amount,
    ):
        if not self._telegram_flag(
            "TELEGRAM_NOTIFY_STATUS",
            True,
        ):
            return

        interval = int(
            os.getenv("TELEGRAM_STATUS_INTERVAL", "300")
        )

        now = time.time()

        if now - self.last_telegram_status_time < interval:
            return

        snapshot_method = getattr(
            self.exchange_client,
            "get_account_snapshot",
            None,
        )

        if snapshot_method is None:
            return

        try:
            account = await snapshot_method()

            equity = account.get("equity", "N/A")
            free_collateral = account.get(
                "freeCollateral",
                "N/A",
            )
            net_quote_balance = account.get(
                "netQuoteBalance",
                "N/A",
            )

            positions = account.get("positions", {})
            position_row = None

            for row in positions.values():
                if str(row.get("marketDisplayName", "")).upper() == str(
                    self.config.contract_id
                ).upper():
                    position_row = row
                    break

            if position_row is None and positions:
                position_row = next(iter(positions.values()))

            if position_row:
                side = position_row.get("side", "FLAT")
                size = position_row.get("size", "0")
                entry = position_row.get(
                    "averageEntryPrice",
                    "N/A",
                )
                mark = position_row.get("markPx", "N/A")
                notional = position_row.get(
                    "positionValueNotional",
                    "N/A",
                )
                margin = position_row.get(
                    "marginUsed",
                    "N/A",
                )
                leverage = position_row.get(
                    "leverage",
                    "N/A",
                )
                unrealized = position_row.get(
                    "unrealizedPnl",
                    "0",
                )
                funding = position_row.get(
                    "cumulativeFunding",
                    "N/A",
                )
            else:
                side = "FLAT"
                size = "0"
                entry = "N/A"
                mark = "N/A"
                notional = "0"
                margin = "0"
                leverage = "N/A"
                unrealized = "0"
                funding = "N/A"

            message = (
                "📊 Arcus QQQ 状态\n\n"
                f"方向：{side}\n"
                f"仓位：{size}\n"
                f"平均入场价：{entry}\n"
                f"标记价格：{mark}\n"
                f"名义价值：{notional}\n"
                f"保证金占用：{margin}\n"
                f"杠杆：{leverage}\n"
                f"未实现 PnL：{unrealized}\n"
                f"累计资金费：{funding}\n\n"
                f"账户权益：{equity}\n"
                f"可用保证金：{free_collateral}\n"
                f"净报价余额：{net_quote_balance}\n\n"
                f"活动平仓数量：{active_close_amount}\n"
                f"保护数量：{active_close_amount}\n"
                f"风险状态："
                f"{'正常' if abs(position_amount) <= abs(active_close_amount) else '需要检查'}"
            )

            await self.send_notification(message)
            self.last_telegram_status_time = now

        except Exception as exc:
            self.logger.log(
                f"Telegram status snapshot failed: {exc}",
                "WARNING",
            )

    async def send_notification(self, message: str):
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as lark_bot:
                await lark_bot.send_text(message)

        telegram_enabled = os.getenv(
            "TELEGRAM_NOTIFY_ENABLED",
            "true",
        ).strip().lower() in ("1", "true", "yes", "on")

        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if telegram_enabled and telegram_token and telegram_chat_id:
            def send_telegram():
                with TelegramBot(
                    telegram_token,
                    telegram_chat_id,
                ) as tg_bot:
                    return tg_bot.send_text(message)

            await asyncio.to_thread(send_telegram)

    async def run(self):
        """Main trading loop."""
        try:
            self.config.contract_id, self.config.tick_size = await self.exchange_client.get_contract_attributes()

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Contract ID: {self.config.contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"Take Profit: {self.config.take_profit}%", "INFO")
            self.logger.log(f"Direction: {self.config.direction}", "INFO")
            self.logger.log(f"Max Orders: {self.config.max_orders}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log(f"Grid Step: {self.config.grid_step}%", "INFO")
            self.logger.log(f"Stop Price: {self.config.stop_price}", "INFO")
            self.logger.log(f"Pause Price: {self.config.pause_price}", "INFO")
            self.logger.log(f"Boost Mode: {self.config.boost_mode}", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.exchange_client.connect()

            # wait for connection to establish
            await asyncio.sleep(5)

            # Main trading loop
            while not self.shutdown_requested:
                # Update active orders
                active_orders = await self.exchange_client.get_active_orders(self.config.contract_id)

                # Filter close orders
                self.active_close_orders = []
                for order in active_orders:
                    if order.side == self.config.close_order_side:
                        self.active_close_orders.append({
                            'id': order.order_id,
                            'price': order.price,
                            'size': order.size
                        })

                # Periodic logging
                mismatch_detected = await self._log_status_periodically()

                stop_trading, pause_trading = await self._check_price_condition()
                if stop_trading:
                    msg = f"\n\nWARNING: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] \n"
                    msg += "Stopped trading due to stop price triggered\n"
                    msg += "价格已经达到停止交易价格，脚本将停止交易\n"
                    await self.send_notification(msg.lstrip())
                    await self.graceful_shutdown(msg)
                    continue

                if pause_trading:
                    await asyncio.sleep(5)
                    continue

                if not mismatch_detected:
                    # This blocks only new opening orders. Existing close
                    # orders remain active and may continue to fill.
                    if not await self.exchange_client.can_open_new_orders():
                        await asyncio.sleep(5)
                        continue

                    wait_time = self._calculate_wait_time()

                    if wait_time > 0:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        meet_grid_step_condition = await self._meet_grid_step_condition()
                        if not meet_grid_step_condition:
                            await asyncio.sleep(1)
                            continue

                        await self._place_and_monitor_open_order()
                        self.last_close_orders += 1

        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")
