"""
Funding Arbitrage Bot - Long spot + Short perp strategy
"""

import os
import traceback
from statistics import quantiles
import time
import asyncio
import sys
import signal
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from typing import Literal, Optional, Tuple
import dotenv
from pathlib import Path

from exchanges import ExchangeFactory
from exchanges.backpack import PositionData
from helpers import TradingLogger
from helpers.lark_bot import LarkBot


@dataclass
class FundingArbConfig:
    """Configuration class for funding arbitrage trading parameters."""
    ticker: str                   # 交易标的，如 "ETH", "BTC"
    tick_size: Decimal            # 最小价格变动单位
    contract_id: str
    quantity: Decimal             # 单次开/关仓数量
    max_size: Decimal             # 最大持仓数量限制
    wait_time: int                # 等待 spot 和 future limit order 都成交的最大时间(秒)
    exchange: str                 # 交易所名称，目前支持 "backpack"
    leverage: int                 # 期货杠杆倍数
    open_max_diff_rate: float     # 开仓最大价差率阈值（超过则不开仓）%
    close_min_diff_rate: float    # 关仓最小价差率阈值（低于则不关仓）%
    max_liquidation_rate: float   # 最大清算风险率（超过则触发减仓）0 ~ 1
    min_order_notional: Decimal   # 最下单价值（根据交易所配置）
    close_order_side: str
    spot_maker_fee: float
    spot_taker_fee: float
    is_unified_account: bool = False  # 是否为统一账户，默认为否
    action: Literal["watching", "open", "close"] = "watching"  # 当前策略状态：观察/开仓/关仓
    
    def copy(self) -> 'FundingArbConfig':
        """Create a copy of the configuration."""
        from copy import deepcopy
        return deepcopy(self)


class FundingArbBot:
    """
    Funding Arbitrage Bot implementing long spot + short perp strategy.
    
    Strategy: Maintain equal positions in spot (long) and perp (short)
    to collect positive funding rates while staying market neutral.
    """
    
    def __init__(self, config: FundingArbConfig):
        """Initialize the funding arbitrage bot."""
        self.config = config
        self.spot_contract_id = ""
        self.perp_contract_id = ""
        self.spot_tick_size = ""
        self.perp_tick_size = ""
        self.spot_exchange_client = ExchangeFactory.create_exchange(config.exchange, config.copy())
        self.perp_exchange_client = ExchangeFactory.create_exchange(config.exchange, config.copy())
        self.logger = TradingLogger(config.exchange, config.ticker, log_to_console=True, file_name_prefix="funding_arb")
        self._running = False
        self._current_spot_position = Decimal('0')
        self._current_perp_position = Decimal('0')
        self._current_tasks = set()  # Track running tasks
        self._bbo = {
            'spot': {'best_bid': Decimal(0), 'best_ask': Decimal(0)},
            'perp': {'best_bid': Decimal(0), 'best_ask': Decimal(0)},
        }
        self._price_diff = {
            'open': 0.0,
            'close': 0.0,
        }
        self._diff_rate = {
            'open': 0.0,
            'close': 0.0,
        }
        
        # Order event handling
        self.current_order_status = {
            'spot': "",
            'perp': "",
        }
        self.order_filled_event = {
            'spot': asyncio.Event(),
            'perp': asyncio.Event()
        }
        self.order_canceled_event = {
            'spot': asyncio.Event(),
            'perp': asyncio.Event()
        }
        self.order_filled_amount = {
            'spot': 0,
            'perp': 0,
        }
        self.shutdown_requested = False
        self.last_log_time = 0
    
    async def graceful_shutdown(self, reason: str = "Unknown"):
        """Perform graceful shutdown of the trading bot."""
        self.logger.log(f"Starting graceful shutdown: {reason}", "INFO")
        self.shutdown_requested = True

        try:
            # Wait for current tasks to complete
            if self._current_tasks:
                self.logger.log(f"Waiting for {len(self._current_tasks)} tasks to complete...", "INFO")
                await asyncio.wait(self._current_tasks, timeout=10.0)
                self.logger.log("Current tasks completed", "INFO")
            
            # Disconnect from exchange
            await self.spot_exchange_client.disconnect()
            await self.perp_exchange_client.disconnect()
            self.logger.log("Graceful shutdown completed", "INFO")

        except Exception as e:
            self.logger.log(f"Error during graceful shutdown: {e}", "ERROR")
    
    def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for order updates."""
        def order_update_handler(message):
            """Handle order updates from WebSocket."""
            try:
                # Debug: Log all incoming messages
                self.logger.log(f"WebSocket message received: {message}", "INFO")
                
                # Check if this is for our contract
                market_type = ""
                if message.get('contract_id') == self.spot_contract_id:
                    market_type = "spot"
                elif message.get('contract_id') == self.perp_contract_id:
                    market_type = "perp"
                else:
                    self.logger.log(f"WebSocket message for unknown contract: {message.get('contract_id')}", "DEBUG")
                    return 

                order_id = message.get('order_id')
                status = message.get('status')
                side = message.get('side', '')
                order_type = message.get('order_type', '')
                filled_size = Decimal(message.get('filled_size'))
                self.current_order_status[market_type] = status

                if status == 'FILLED':
                    self.order_filled_amount[market_type] = filled_size
                    # Ensure thread-safe interaction with asyncio event loop
                    if self.loop is not None:
                        self.loop.call_soon_threadsafe(self.order_filled_event[market_type].set)
                    else:
                        # Fallback (should not happen after run() starts)
                        self.order_filled_event[market_type].set()

                    self.logger.log(f"[{market_type.upper()}] [{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                    self.logger.log_transaction(order_id, side, message.get('size'), message.get('price'), status)
                elif status == "CANCELED":
                    self.order_filled_amount[market_type] = filled_size
                    if self.loop is not None:
                        self.loop.call_soon_threadsafe(self.order_canceled_event[market_type].set)
                    else:
                        self.order_canceled_event[market_type].set()

                    if self.order_filled_amount[market_type] > 0:
                        self.logger.log_transaction(order_id, side, self.order_filled_amount[market_type], message.get('price'), status)

                    self.logger.log(f"[{market_type.upper()}] [{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")
                elif status == "PARTIALLY_FILLED":
                    self.logger.log(f"[{market_type.upper()}] [{order_type}] [{order_id}] {status} "
                                    f"{filled_size} @ {message.get('price')}", "INFO")
                else:
                    self.logger.log(f"[{market_type.upper()}] [{order_type}] [{order_id}] {status} "
                                    f"{message.get('size')} @ {message.get('price')}", "INFO")

            except Exception as e:
                self.logger.log(f"Error handling order update: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

        # Setup order update handler
        self.spot_exchange_client.setup_order_update_handler(order_update_handler)
        self.perp_exchange_client.setup_order_update_handler(order_update_handler)
    
    async def _place_and_monitor_order(self, is_open_position: bool) -> bool:
        """Place an order and monitor its execution."""
        try:
            # Reset state before placing order
            self.order_filled_event['spot'].clear()
            self.order_filled_event['perp'].clear()
            self.current_order_status = {
                'spot': "OPEN",
                'perp': "OPEN",
            }
            self.order_filled_amount = {
                'spot': 0,
                'perp': 0,
            }
            spot_quantity = (self.config.quantity / Decimal(str(1 - self.config.spot_maker_fee / 100))).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
            perp_quanity = self.config.quantity

            # Log detailed order information
            self.logger.log(
                f"Order calculation: base_quantity={self.config.quantity}, spot_maker_fee={self.config.spot_maker_fee}%, "
                f"calculated_spot_quantity={spot_quantity}, perp_quantity={perp_quanity}"
            )

            # Place the order
            # [OPEN POSITION] spot buy, perp sell
            # [CLOSE POSITION] spot sell, perp buy
            self.logger.log(
                f"[SPOT] place [{'BUY' if is_open_position else 'SELL'}] order, size {spot_quantity} | "
                f"[PERP] place [{'BUY' if not is_open_position else 'SELL'}] order, size {perp_quanity}"
            )
            spot_order_task = self.spot_exchange_client.place_open_order(
                self.spot_contract_id,
                spot_quantity,
                "buy" if is_open_position else "sell",
                auto_borrow_and_repay=True,
            )
            perp_order_task = self.perp_exchange_client.place_open_order(
                self.perp_contract_id,
                perp_quanity,
                "sell" if is_open_position else "buy",
                auto_borrow_and_repay=True,
            )
            # Track order placement tasks
            self._current_tasks.update([spot_order_task, perp_order_task])
            try:
                spot_order_result, perp_order_result = await asyncio.gather(spot_order_task, perp_order_task)
            finally:
                self._current_tasks.discard(spot_order_task)
                self._current_tasks.discard(perp_order_task)

            if not spot_order_result.success:
                self.logger.log(f"Failed to place SPOT order: {spot_order_result.error_message}", "ERROR")
                self.logger.log(f"SPOT order details: contract_id={self.spot_contract_id}, quantity={spot_quantity}, direction={'buy' if is_open_position else 'sell'}", "ERROR")
                return False
            if not perp_order_result.success:
                self.logger.log(f"Failed to place PERP order: {perp_order_result.error_message}", "ERROR")
                self.logger.log(f"PERP order details: contract_id={self.perp_contract_id}, quantity={perp_quanity}, direction={'sell' if is_open_position else 'buy'}", "ERROR")
                return False

            # Wait for fill or timeout
            is_order_timeout = False
            if not self.order_filled_event['spot'].is_set() or not self.order_filled_event['perp'].is_set():
                try:
                    # Create and track event waiting tasks
                    spot_wait_task = asyncio.create_task(self.order_filled_event['spot'].wait())
                    perp_wait_task = asyncio.create_task(self.order_filled_event['perp'].wait())
                    await asyncio.wait_for(
                        asyncio.gather(spot_wait_task, perp_wait_task),
                        timeout=self.config.wait_time
                    )
                        
                except asyncio.TimeoutError:
                    is_order_timeout = True

            # Handle order result
            spot_handle_task = asyncio.create_task(self._handle_orders_result('spot', spot_order_result))
            perp_handle_task = asyncio.create_task(self._handle_orders_result('perp', perp_order_result))
            self._current_tasks.update([spot_handle_task, perp_handle_task])
            try:
                spot_res, perp_res = await asyncio.gather(spot_handle_task, perp_handle_task)
            finally:
                self._current_tasks.discard(spot_handle_task)
                self._current_tasks.discard(perp_handle_task)
            
            if is_order_timeout:
                await self._handle_position_mismatch(action=self.config.action)

            return spot_res and perp_res

        except Exception as e:
            self.logger.log(f"Error placing order: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False
    
    async def _handle_orders_result(self, market_type, order_result) -> bool:
        """Handle the result of an order placement."""

        action = self.config.action

        if self.order_filled_event[market_type].is_set():
            # Both spot and perp open order success
            log_message = f"[{action.upper()} {market_type.upper()}] [{order_result.status}] [{order_result.order_id}] @{order_result.price} size {order_result.size}"
            self.logger.log(log_message, "INFO")
            await self._lark_bot_notify(log_message.lstrip())
            return True

        else:
            self.order_canceled_event[market_type].clear()
            # Cancel the order if it's still open
            self.logger.log(f"[{action.upper()}] [{market_type.upper()}] [{order_result.order_id}] Order time out, trying to cancel order. ", "INFO")
            try:
                _exchange_client = self.spot_exchange_client if market_type == "spot" else self.perp_exchange_client
                cancel_result = await _exchange_client.cancel_order(order_result.order_id)
                self.logger.log(f"cancel_result {cancel_result}")
                if not cancel_result.success:
                    self.order_canceled_event[market_type].set()
                    self.logger.log(f"[{action.upper()}] [{market_type.upper()}] Failed to cancel order {order_result.order_id}: {cancel_result.error_message}", "ERROR")
                else:
                    self.current_order_status[market_type] = "CANCELED"
                    self.logger.log(f"[{action.upper()}] [{market_type.upper()}] cancel order {order_result.order_id} success", "INFO")

            except Exception as e:
                self.order_canceled_event[market_type].set()
                self.logger.log(f"[{action.upper()}] [{market_type.upper()}] Error canceling order {order_result.order_id}: {e}", "ERROR")

            if self.config.exchange == "backpack":
                self.order_filled_amount[market_type] = cancel_result.filled_size if 'cancel_result' in locals() else 0
            else:
                # Wait for cancel event or timeout
                if not self.order_canceled_event[market_type].is_set():
                    try:
                        await asyncio.wait_for(self.order_canceled_event[market_type].wait(), timeout=5)
                    except asyncio.TimeoutError:
                        order_info = await _exchange_client.get_order_info(order_result.order_id)
                        self.order_filled_amount[market_type] = order_info.filled_size

            return True

        return False
    
    async def _handle_position_mismatch(self, action:str) -> bool:
        # handle unfilled orders
        await self.get_positions()
        mismatch_amount = self.order_filled_amount['perp'] - self.order_filled_amount['spot']
        min_amount = self._min_order_amount()
        if abs(mismatch_amount) > min_amount:
            action = self.config.action
            is_positive = mismatch_amount > Decimal(0)
            if action == "open" and is_positive:
                market_type = "PERP"
                contract_id = self.perp_contract_id
                direction = "sell"
            elif action == "open" and not is_positive:
                market_type = "SPOT"
                contract_id = self.spot_contract_id
                direction = "buy"
            elif action == "close" and is_positive:
                market_type = "SPOT"
                contract_id = self.spot_contract_id
                direction = "sell"
            elif action == "close" and not is_positive:
                market_type = "PERP"
                contract_id = self.perp_contract_id
                direction = "buy"
            else:
                return
            self.logger.log(f"place market order to match position [{action.upper()}] [{market_type.upper()}] size {abs(mismatch_amount)}")
            mismatch_amount = abs(mismatch_amount.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP))
            _exchange_client = self.spot_exchange_client if market_type == "SPOT" else self.perp_exchange_client
            market_order_result = await _exchange_client.place_market_order(
                contract_id=contract_id,
                quantity=mismatch_amount,
                direction=direction,
                auto_borrow_and_repay=True,
            )
            if not market_order_result.success:
                log_message = f"Failed to place [{direction.upper()}] [{market_type.upper()}] [{direction.upper()}] order: {market_order_result.error_message}"
                self.logger.log(log_message, "ERROR")
                self._lark_bot_notify(log_message)
                return False
            self._lark_bot_notify(f"MarketOrder [{direction.upper()}] [{market_type.upper()}] [{direction.upper()}]: {market_order_result.error_message}")
        else:
            # order amount too small
            self.logger.log(f"handle_order_unfilled: order amount too small, size {mismatch_amount}, min order notional {min_amount}")
            return False
        
        return True

    async def calculate_price_diff_rate(self, action: str) -> float:
        """
        Calculate price difference rate for opening or closing positions.
        
        Args:
            action: "open" or "close" to determine pricing method
        
        Returns:
            float: Price difference rate (diff_rate = price_diff / spot_price)
        """
        # Get spot and perp prices using Backpack exchange client
        spot_best_bid, spot_best_ask = await self.spot_exchange_client.fetch_bbo_prices(self.spot_contract_id)
        perp_best_bid, perp_best_ask = await self.perp_exchange_client.fetch_bbo_prices(self.perp_contract_id)
        
        if action == "open":
            # Open: spot best_ask - perp best_bid (期望 spot < perp)
            spot_price = spot_best_ask
            perp_price = perp_best_bid
        else:  # close
            # Close: spot best_bid - perp best_ask (期望 spot > perp)  
            spot_price = spot_best_bid
            perp_price = perp_best_ask
        
        price_diff = spot_price - perp_price
        diff_rate = float(price_diff / spot_price) if spot_price > 0 else 0.0

        # update _bbo, price_diff, diff_rate
        self._bbo = {
            'spot': {'best_bid': spot_best_bid, 'best_ask': spot_best_ask},
            'perp': {'best_bid': perp_best_bid, 'best_ask': perp_best_ask},
        }
        self._price_diff = {
            'open': 0.0,
            'close': 0.0,
        }
        self._diff_rate = {
            'open': 0.0,
            'close': 0.0,
        }

        self._price_diff[action] = price_diff
        self._diff_rate[action] = diff_rate

        return price_diff, diff_rate
    
    async def get_positions(self):
        # Create and track balance/position tasks
        balance_task = asyncio.create_task(self.spot_exchange_client.get_balance())
        position_task = asyncio.create_task(self.perp_exchange_client.get_account_positions())
        self._current_tasks.update([balance_task, position_task])
        
        try:
            balances, perp_position = await asyncio.gather(balance_task, position_task)
        finally:
            self._current_tasks.discard(balance_task)
            self._current_tasks.discard(position_task)
            
        self._current_spot_position = balances['base']['total']
        self._current_perp_position = perp_position
        return self._current_spot_position, self._current_perp_position
    
    async def open_position(self) -> bool:
        """
        Execute opening position logic: spot buy limit + perp sell limit.
        
        Returns:
            bool: True if position opened successfully
        """
        try:
            await self._place_and_monitor_order(is_open_position=True)
            return True
        except Exception as e:
            self.logger.log(f"Error handling open_position: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False
    
    async def close_position(self) -> bool:
        """
        Execute closing position logic: spot sell limit + perp buy limit.
        
        Returns:
            bool: True if position closed successfully
        """
        try:
            await self._place_and_monitor_order(is_open_position=False)
            return True
        except Exception as e:
            self.logger.log(f"Error handling close_position: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            return False
    
    async def watching_position(self) -> bool:
        liq_rate = await self.calculate_liquidation_rate()
        if liq_rate >= self.config.max_liquidation_rate:
            await self.emergency_reduce_position()
            time.sleep(3)
        return await self._log_status_periodically()

    async def calculate_liquidation_rate(self) -> float:
        """
        Calculate perp position liquidation risk rate.
        
        Returns:
            float: Liquidation rate (0-1, where 1 means liquidation imminent)
        """
        position: Optional[PositionData] = await self.perp_exchange_client.get_account_position()
        if not position:
            return 0
        entry_price = Decimal(str(position.entryPrice))
        mark_price = Decimal(str(position.markPrice))
        liquidation_price = Decimal(str(position.estLiquidationPrice))
        liquidation_rate = (mark_price - entry_price) / (liquidation_price - entry_price)
        return liquidation_rate
    
    async def emergency_reduce_position(self) -> bool:
        """
        Emergency reduction of positions when liquidation risk is high.
        Reduces both spot and perp positions by 1/4 using market orders.
        
        Returns:
            bool: True if emergency reduction executed successfully
        """
        await self.get_positions()
        order_size = max(self._current_spot_position, self._current_perp_position) / Decimal("4")
        self.logger.log(f"emergency_reduce_position: close position size {order_size}")
        # [CLOSE POSITION] spot sell, perp buy
        spot_order_task = self.spot_exchange_client.place_market_order(
            contract_id=self.spot_contract_id,
            quantity=order_size,
            direction="sell"
        )
        perp_order_task = self.perp_exchange_client.place_market_order(
            contract_id=self.perp_contract_id,
            quantity=order_size,
            direction="buy"
        )
        # Track emergency reduction tasks
        self._current_tasks.update([spot_order_task, perp_order_task])
        try:
            spot_order_result, perp_order_result = await asyncio.gather(spot_order_task, perp_order_task)
        finally:
            self._current_tasks.discard(spot_order_task)
            self._current_tasks.discard(perp_order_task)

        if not spot_order_result.success or not perp_order_result.success:
            await self._handle_position_mismatch(action="close")

    def _min_order_amount(self) -> Decimal:
        mid_price = (self._bbo['perp']['best_bid'] + self._bbo['perp']['best_ask']) / Decimal("2")
        if mid_price == Decimal("0"):
            return Decimal("0.025")
        return (self.config.min_order_notional / mid_price).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)

    async def _log_status_periodically(self) -> bool:
        """Log status information periodically, including positions."""
        if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
            print("--------------------------------")
            try:
                action = self.config.action
                # Get spot and perp position
                (spot_position, perp_position) = await self.get_positions()

                self.logger.log(f"Current Positions: [SPOT] {spot_position} | [PERP] {perp_position} | "
                                f"[{action.upper()} MODE] PriceDiff: {self._price_diff[action]:.2f} DiffRate: {self._diff_rate[action] * 100:.4f}%")
                self.last_log_time = time.time()
                # Check for position mismatch
                if abs(spot_position - perp_position) > (2 * self.config.quantity):
                    error_message = f"\n\nERROR: [{self.config.exchange.upper()}_{self.config.ticker.upper()}] "
                    error_message += "Position mismatch detected\n"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    error_message += "Please manually rebalance your [SPOT] position and [PERP] position\n"
                    error_message += "请手动平衡当前现货仓位和永续合约仓位\n"
                    error_message += f"Current Positions: [SPOT] {spot_position} | [PERP] {perp_position} | "
                    error_message += f"[{action.upper()}] Price Diff: {self._price_diff[action]:.2f} Diff Rate: {self._diff_rate[action] * 100:.4f}%"
                    error_message += "###### ERROR ###### ERROR ###### ERROR ###### ERROR #####\n"
                    self.logger.log(error_message, "ERROR")

                    await self._lark_bot_notify(error_message.lstrip())

                    if not self.shutdown_requested:
                        self.shutdown_requested = True

                    mismatch_detected = True
                else:
                    mismatch_detected = False

                return mismatch_detected

            except Exception as e:
                self.logger.log(f"Error in periodic status check: {e}", "ERROR")
                self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
                return False

        return False

    async def _lark_bot_notify(self, message: str):
        lark_token = os.getenv("LARK_TOKEN")
        if lark_token:
            async with LarkBot(lark_token) as bot:
                await bot.send_text(message)

    async def run(self):
        """
        Main execution loop for funding arbitrage strategy.
        
        Continuously monitors price differences and manages positions
        according to the funding arbitrage strategy.
        """
        try:

            self.spot_contract_id, self.spot_tick_size = await self.spot_exchange_client.get_contract_attributes(exchange_type="SPOT")
            self.perp_contract_id, self.perp_tick_size = await self.perp_exchange_client.get_contract_attributes(exchange_type="PERP")

            # Log current TradingConfig
            self.logger.log("=== Trading Configuration ===", "INFO")
            self.logger.log(f"Action: {self.config.action}", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"SPOT Contract ID: {self.spot_contract_id}", "INFO")
            self.logger.log(f"PERP Contract ID: {self.perp_contract_id}", "INFO")
            self.logger.log(f"Quantity: {self.config.quantity}", "INFO")
            self.logger.log(f"max_size: {self.config.max_size}", "INFO")
            self.logger.log(f"Leverage: {self.config.leverage}", "INFO")
            self.logger.log(f"Open max diff rate: {self.config.open_max_diff_rate}%", "INFO")
            self.logger.log(f"Close min diff rate: {self.config.close_min_diff_rate}%", "INFO")
            self.logger.log(f"Max Liquidation Rate: {self.config.max_liquidation_rate}", "INFO")
            self.logger.log(f"Wait Time: {self.config.wait_time}s", "INFO")
            self.logger.log(f"Exchange: {self.config.exchange}", "INFO")
            self.logger.log("=============================", "INFO")

            # Capture the running event loop for thread-safe callbacks
            self.loop = asyncio.get_running_loop()
            # Connect to exchange
            await self.spot_exchange_client.connect()
            await self.perp_exchange_client.connect()
            
            # Setup WebSocket handlers AFTER connecting
            self._setup_websocket_handlers()

            target_size = self.config.max_size # TODO: calc target_size by balance
            action = self.config.action
            
            # Main trading loop
            while not self.shutdown_requested:

                # Create tasks and track them
                price_task = asyncio.create_task(self.calculate_price_diff_rate(action))
                position_task = asyncio.create_task(self.get_positions())
                self._current_tasks.update([price_task, position_task])
                
                try:
                    (price_diff, diff_rate), (spot_position, perp_position) = await asyncio.gather(
                        price_task, position_task
                    )
                finally:
                    # Remove completed tasks
                    self._current_tasks.discard(price_task)
                    self._current_tasks.discard(position_task)

                self.logger.log(f"spot_position {spot_position} | perp_position {perp_position} | target_size {target_size} | price_diff {price_diff} | diff_rate {diff_rate*100:.4f}%")
                if action == "open":
                    if max(spot_position, perp_position) < target_size and diff_rate <= self.config.open_max_diff_rate / 100:
                        await self.open_position()
                elif action == "close" and diff_rate >= self.config.close_min_diff_rate / 100:
                    if min(spot_position, perp_position) >= self._min_order_amount():
                        await self.close_position()
                
                await self.watching_position()
                
                time.sleep(1)
        
        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user")
            await self.graceful_shutdown("User interruption (Ctrl+C)")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            await self.graceful_shutdown(f"Critical error: {e}")
            raise
        finally:
            # Ensure all connections are closed even if graceful shutdown fails
            try:
                await self.spot_exchange_client.disconnect()
                await self.perp_exchange_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error disconnecting from exchange: {e}", "ERROR")

    
    def stop(self):
        """Stop the funding arbitrage bot."""
        self._running = False
        self.logger.info("Funding arbitrage bot stop signal received")


async def main():
    """Main entry point."""
    env_path = Path("env/.env.funding_arb")
    if not env_path.exists():
        print(f"Env file not find: {env_path.resolve()}")
        sys.exit(1)
    dotenv.load_dotenv(env_path)

    # Create configuration
    config = FundingArbConfig(
        ticker="ETH",
        tick_size=Decimal(0),
        contract_id="",
        quantity=Decimal("0.0030"),
        max_size=Decimal("0.0330"),
        wait_time=1*60,
        exchange="backpack",
        leverage=3,
        open_max_diff_rate=0.03,
        close_min_diff_rate=0.05,
        max_liquidation_rate=0.5,
        min_order_notional=Decimal("10"),
        spot_maker_fee=0.08,
        spot_taker_fee=0.10,
        close_order_side="buy",
        is_unified_account=True,
        action="close",
    )

    # Create and run the bot
    bot = FundingArbBot(config)
    
    # Setup global signal handlers for graceful shutdown
    def signal_handler():
        print("Received shutdown signal, initiating graceful shutdown...")
        asyncio.create_task(bot.graceful_shutdown("Signal received"))
    
    # Register signal handlers
    loop = asyncio.get_running_loop()
    for sig in [signal.SIGTERM, signal.SIGINT]:
        loop.add_signal_handler(sig, signal_handler)
    
    try:
        await bot.run()
    except KeyboardInterrupt:
        print("KeyboardInterrupt caught, shutting down...")
        await bot.graceful_shutdown("KeyboardInterrupt")
    except Exception as e:
        print(f"Bot execution failed: {e}")
        await bot.graceful_shutdown(f"Exception: {e}")
        return


if __name__ == "__main__":
    asyncio.run(main())