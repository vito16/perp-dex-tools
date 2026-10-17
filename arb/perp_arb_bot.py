"""
Perpetual Arbitrage Bot - Cross-exchange arbitrage trading
"""

import os
import sys
from pathlib import Path
import argparse
import time
import asyncio
import traceback
from dataclasses import dataclass
import dotenv
from decimal import Decimal
from typing import Dict, Literal, Tuple, Optional

from exchanges import ExchangeFactory
from exchanges.base import OrderResult
from helpers import TradingLogger
from helpers.lark_bot import LarkBot
from helpers.telegram_bot import TelegramBot


@dataclass
class PerpArbConfig:
    """Configuration class for perpetual arbitrage trading."""
    ticker: str
    contract_id_leg1: str
    contract_id_leg2: str
    exchange_leg1: str
    exchange_leg2: str
    max_quantity: Decimal          # 单次套利下单的最大数量
    max_total_size: Decimal        # 持仓总数量上限
    order_interval: int            # 下单最小时间间隔 ms
    loop_interval: int             # 轮询时间间隔 ms
    order_timeout: int             # 订单超时时间 ms
    min_price_diff_open: Decimal   # 套利开仓的最小价差
    min_price_diff_close: Decimal  # 套利关仓的最大价差
    min_order_size: Decimal        # 下单的最小数量
    tick_size_leg1: Decimal = Decimal('0')
    tick_size_leg2: Decimal = Decimal('0')
    max_slippage_leg1: Decimal = Decimal('0.50')
    max_slippage_leg2: Decimal = Decimal('0.50')
    close_order_side: str = "" 


@dataclass
class OrderState:
    """Order execution state tracking."""
    leg1_success: bool = False
    leg2_success: bool = False
    leg1_filled_size: Decimal = Decimal('0')
    leg2_filled_size: Decimal = Decimal('0')
    leg1_filled_price: Decimal = Decimal('0')
    leg2_filled_price: Decimal = Decimal('0')
    leg1_order_id: Optional[str] = None
    leg2_order_id: Optional[str] = None


class PerpArbBot:
    """Perpetual Arbitrage Trading Bot."""

    def __init__(self, config: PerpArbConfig):
        self.config = config
        self.logger = TradingLogger(
            exchange=f"PERP_ARB_{config.exchange_leg1}_{config.exchange_leg2}",
            ticker=config.ticker,
            log_to_console=True
        )

        # Create exchange clients for both legs
        self.leg1_client = None
        self.leg2_client = None
        
        # Trading state
        self.shutdown_requested = False
        self.last_order_time = 0
        self.loop = None

        # Order tracking
        self.order_filled_event = {
            1: asyncio.Event(),
            2: asyncio.Event(),
        }
        self.order_canceled_event = {
            1: asyncio.Event(),
            2: asyncio.Event(),
        }

        # time-weighted averange price_diff
        # max 3600 seconds (1 hour)
        self.price_diff_twa = {
            '12': 0,
            '21': 0,
        }
        self.price_diff_start_save_time = 0
        self.last_price_diff_save_time = 0
        
        self.last_log_time = 0

    async def _initialize_clients(self):
        """Initialize both exchange clients."""
        try:
            # Create leg1 client
            leg1_config_dict = {
                'ticker': self.config.ticker,
                'contract_id': self.config.contract_id_leg1,
                'quantity': self.config.max_quantity,
                'tick_size': self.config.tick_size_leg1,
                'max_slippage': self.config.max_slippage_leg1,
                'close_order_side': 'buy',  # Will be overridden per trade
            }
            
            self.leg1_client = ExchangeFactory.create_exchange(
                self.config.exchange_leg1,
                type('Config', (), leg1_config_dict)(),
                self.logger,
            )
            
            # Create leg2 client
            leg2_config_dict = {
                'ticker': self.config.ticker,
                'contract_id': self.config.contract_id_leg2,
                'quantity': self.config.max_quantity,
                'tick_size': self.config.tick_size_leg2,
                'max_slippage': self.config.max_slippage_leg2,
                'close_order_side': 'sell',  # Will be overridden per trade
            }
            
            self.leg2_client = ExchangeFactory.create_exchange(
                self.config.exchange_leg2,
                type('Config', (), leg2_config_dict)(),
                self.logger,
            )
            
            self.logger.log("Exchange clients initialized", "INFO")
            
        except Exception as e:
            self.logger.log(f"Failed to initialize clients: {e}", "ERROR")
            raise

    async def _setup_websocket_handlers(self):
        """Setup WebSocket handlers for both legs."""
        
        def create_order_handler(leg_num: int):
            def handler(message):
                try:
                    order_id = message.get('order_id')
                    status = message.get('status')
                        
                    if status == 'FILLED':
                        filled_size = Decimal(message.get('filled_size', 0))

                        # Ensure thread-safe interaction with asyncio event loop
                        if self.loop is not None:
                            self.loop.call_soon_threadsafe(self.order_filled_event[leg_num].set)
                        else:
                            # Fallback (should not happen after run() starts)
                            self.order_filled_event[leg_num].set()
                        
                        self.logger.log(
                            f"[LEG{leg_num}] [{order_id}] FILLED "
                            f"{filled_size} @ {message.get('price')}",
                            "INFO"
                        )
                        
                    elif status in ['CANCELED', 'FAILED']:
                        self.logger.log(
                            f"[LEG{leg_num}] [{order_id}] {status}",
                            "WARNING"
                        )
                            
                except Exception as e:
                    self.logger.log(f"Error in order handler (leg{leg_num}): {e}", "ERROR")
            
            return handler
        
        # Setup handlers for both legs
        self.leg1_client.setup_order_update_handler(create_order_handler(1))
        self.leg2_client.setup_order_update_handler(create_order_handler(2))

    async def _get_positions(self) -> Tuple[Decimal, Decimal]:
        """Get positions for both legs."""
        try:
            leg1_pos = await self.leg1_client.get_account_positions_with_sign()
            leg2_pos = await self.leg2_client.get_account_positions_with_sign()
            return leg1_pos, leg2_pos
        except Exception as e:
            self.logger.log(f"Error getting positions: {e}", "ERROR")
            return Decimal('0'), Decimal('0')

    async def _fetch_depths(self) -> Dict[str, Dict[str, Tuple[Decimal, Decimal]]]:
        """Fetch orderbook depths for both legs and organize by direction."""
        try:
            # Get best bid/ask for both legs
            leg1_bid, leg1_ask, leg1_bid_qty, leg1_ask_qty  = await self.leg1_client.fetch_bbo_prices_quanities(self.config.contract_id_leg1)
            leg2_bid, leg2_ask, leg2_bid_qty, leg2_ask_qty = await self.leg2_client.fetch_bbo_prices_quanities(self.config.contract_id_leg2)
            
            # Validate prices
            if (leg1_bid <= 0 or leg1_ask <= 0 or leg2_bid <= 0 or leg2_ask <= 0 or
                leg1_bid >= leg1_ask or leg2_bid >= leg2_ask):
                raise ValueError("Invalid bid/ask prices")
            
            best_orders = {
                "12": {  # leg1 buy, leg2 sell
                    "best_ask": [leg1_ask, leg1_ask_qty],  # leg1 taker buy price
                    "best_bid": [leg2_bid, leg2_bid_qty],  # leg2 taker sell price
                },
                "21": {  # leg1 sell, leg2 buy
                    "best_bid": [leg1_bid, leg1_bid_qty],  # leg1 taker sell price
                    "best_ask": [leg2_ask, leg2_ask_qty],  # leg2 taker buy price
                }
            }
            
            return best_orders
            
        except Exception as e:
            self.logger.log(f"Error fetching depths: {e}", "ERROR")
            raise

    def _calculate_price_diff(self, best_orders: Dict) -> Dict[str, Decimal]:
        """Calculate price difference for each direction."""
        if best_orders["12"]["best_bid"][0] * best_orders["12"]["best_ask"][0] * best_orders["21"]["best_bid"][0] * best_orders["21"]["best_ask"][0] == Decimal("0"):
            self.logger.log(
                f'_calculate_price_diff Error: best price is 0.\n'
                f'12_best_bid: {best_orders["12"]["best_bid"][0]} 12_best_ask: {best_orders["12"]["best_ask"][0]}\n'
                f'21_best_bid: {best_orders["21"]["best_bid"][0]} 21_best_ask: {best_orders["21"]["best_ask"][0]}',
                "ERROR"
            )
            raise ValueError("best price is 0")
        price_diff = {}
        
        # Direction "12": leg1 buy (ask), leg2 sell (bid)
        # price_diff = sell_price - buy_price
        price_diff["12"] = best_orders["12"]["best_bid"][0] - best_orders["12"]["best_ask"][0]
        
        # Direction "21": leg1 sell (bid), leg2 buy (ask)
        # price_diff = sell_price - buy_price
        price_diff["21"] = best_orders["21"]["best_bid"][0] - best_orders["21"]["best_ask"][0]

        # save Time-weighted Average price_diff
        cur_ts = int(time.time())
        if self.last_price_diff_save_time == 0:
            self.price_diff_twa = {k: float(v) for k, v in price_diff.items()}
            self.price_diff_start_save_time = cur_ts
            self.last_price_diff_save_time = cur_ts
        elif cur_ts - self.last_price_diff_save_time >= 1:
            # max 3600 seconds (1 hour)
            delta_t = cur_ts - self.last_price_diff_save_time
            prev_t = min(self.last_price_diff_save_time - self.price_diff_start_save_time, max(3600 - delta_t, 0))
            total_t = prev_t + delta_t
            
            if total_t > 0:
                self.price_diff_twa["12"] = (self.price_diff_twa["12"] * prev_t + float(price_diff["12"]) * delta_t) / total_t
                self.price_diff_twa["21"] = (self.price_diff_twa["21"] * prev_t + float(price_diff["21"]) * delta_t) / total_t
                self.last_price_diff_save_time = cur_ts

        return price_diff

    async def _check_arb_conditions(
        self,
        price_diff: Dict[str, Decimal],
        leg1_pos: Decimal,
        leg2_pos: Decimal
    ) -> Tuple[Optional[str], Optional[Literal["OPEN", "CLOSE"]]]:
        """Check if arbitrage conditions are met."""
        
        max_pos = max(abs(leg1_pos), abs(leg2_pos))
        min_pos = min(abs(leg1_pos), abs(leg2_pos))
        exsiting_direction = "12" if leg1_pos > Decimal("0") and leg2_pos < Decimal("0") else "21"
        
        # Check close conditions
        if min_pos >= self.config.min_order_size:
            for direction in ["12", "21"]:
                if direction == exsiting_direction:
                    continue
                
                # set close_order_side
                self.leg1_client.config.close_order_side = "buy" if direction == "12" else "sell"
                self.leg2_client.config.close_order_side = "sell" if direction == "12" else "buy"
                diff = price_diff[direction]
                if diff >= max(self.price_diff_twa[direction], self.config.min_price_diff_close):
                    self.logger.log(
                        f"CLOSE opportunity found: direction={direction}, "
                        f"price_diff={diff}, positions=({leg1_pos}, {leg2_pos})",
                        "INFO"
                    )
                    return direction, "CLOSE"

        # Check each direction to open
        for direction in ["12", "21"]:
            diff = price_diff[direction]
            
            # Check open conditions
            if max_pos < self.config.max_total_size - self.config.min_order_size:
                if diff >= max(self.price_diff_twa[direction] * 1.20, self.config.min_price_diff_open):
                    self.logger.log(
                        f"OPEN opportunity found: direction={direction}, "
                        f"price_diff={diff} ({self.price_diff_twa[direction]}), positions=({leg1_pos}, {leg2_pos})",
                        "INFO"
                    )
                    return direction, "OPEN"
        
        return None, None

    async def _execute_orders(
        self,
        direction: str,
        action: Literal["OPEN", "CLOSE"],
        best_orders: Dict
    ) -> OrderState:
        """Execute orders on both legs simultaneously."""
        
        # Calculate order quantity
        if direction == "12":
            leg1_direction = "buy"   # leg1 long
            leg1_available = best_orders["12"]["best_ask"][1]
            leg2_direction = "sell"  # leg2 short
            leg2_available = best_orders["12"]["best_bid"][1]
        else:  # "21"
            leg1_direction = "sell"  # leg1 short
            leg1_available = best_orders["21"]["best_bid"][1]
            leg2_direction = "buy"   # leg2 long
            leg2_available = best_orders["21"]["best_ask"][1]
        
        order_quantity = min(
            leg1_available,
            leg2_available,
            self.config.max_quantity
        )
        
        # Skip if quantity too small
        if order_quantity < self.config.min_order_size:
            self.logger.log(
                f"Order quantity {order_quantity} < min_order_size {self.config.min_order_size}, skipping",
                "INFO"
            )
            return OrderState()
        
        self.logger.log(
            f"Executing {action} orders: direction={direction}, "
            f"quantity={order_quantity}, leg1={leg1_direction}, leg2={leg2_direction}",
            "INFO"
        )
        
        # Place orders simultaneously with retry logic
        order_state = await self._place_orders_with_retry(
            order_quantity,
            leg1_direction,
            leg2_direction,
            max_retries=3,
            reduce_only= action == "CLOSE"
        )
        
        return order_state

    async def _place_orders_with_retry(
        self,
        quantity: Decimal,
        leg1_direction: str,
        leg2_direction: str,
        max_retries: int = 3, # market order max retries
        reduce_only: bool = False,
    ) -> OrderState:
        """Place orders on both legs with retry logic."""
        self.order_filled_event[1].clear()
        self.order_filled_event[2].clear()
        
        order_state = OrderState()

        # leg1 order process
        # binance perp ETHUSDC: limit order (0 fees) -> timeout -> market order (0.0275%)
        # limit order fist
        leg1_order_result: OrderResult = await self.leg1_client.place_open_order(self.config.contract_id_leg1, quantity, leg1_direction)

        # wait limit order filled, cancel if timeout
        try:
            await asyncio.wait_for(self.order_filled_event[1].wait(), timeout=self.config.order_timeout / 1000)
        except asyncio.TimeoutError:
            order_id = leg1_order_result.order_id
            if order_id:
                self.logger.log("[LEG1] Order timeout", "WARNING")
                self.order_canceled_event[1].clear()
                # Cancel the order if it's still open
                self.logger.log(f"[LEG1] [{order_id}] Cancelling order", "INFO")
                try:
                    cancel_result = await self.leg1_client.cancel_order(order_id)
                    if not cancel_result.success:
                        # refetch order info, check if order already filled
                        order_info = await self.leg1_client.get_order_info(order_id)
                        self.current_order_status = order_info.status
                        if order_info.filled_size > Decimal("0"):
                            leg1_order_result.success = True
                            leg1_order_result.status = order_info.status
                            leg1_order_result.order_id = order_info.order_id
                            leg1_order_result.filled_size = order_info.filled_size
                            leg1_order_result.side = order_info.side
                            leg1_order_result.price = order_info.price
                        else:
                            self.order_canceled_event[1].set()
                            self.logger.log(f"[LEG1] Failed to cancel order {order_id}: {cancel_result.error_message}", "WARNING")
                    else:
                        self.current_order_status = "CANCELED"

                except Exception as e:
                    self.order_canceled_event[1].set()
                    self.logger.log(f"[LEG1] Error canceling order {order_id}: {e}", "ERROR")

        if leg1_order_result.success and self.order_filled_event[1].is_set():
            order_state.leg1_success = True
            order_state.leg1_filled_size = leg1_order_result.size
            order_state.leg1_filled_price = leg1_order_result.price
            order_state.leg1_order_id = leg1_order_result.order_id
            self.logger.log(
                f"[LEG1] Order filled: {leg1_order_result.size} @ {leg1_order_result.price}",
                "INFO"
            )

            for attempt in range(max_retries):
                try:
                    leg2_order_result = await self.leg2_client.place_market_order(
                        self.config.contract_id_leg2,
                        quantity,
                        leg2_direction,
                        reduce_only,
                    )
                    if leg2_order_result.success:
                        order_state.leg2_success = True
                        order_state.leg2_filled_size = leg2_order_result.size
                        order_state.leg2_filled_price = leg2_order_result.price
                        order_state.leg2_order_id = leg2_order_result.order_id
                        self.logger.log(
                            f"[LEG2] Order filled: {leg2_order_result.size} @ {leg2_order_result.price}",
                            "INFO"
                        )
                        break
                    else:
                        if attempt > 0 and attempt < max_retries - 1:
                            self.logger.log(
                                f"One order failed (attempt {attempt + 1}/{max_retries}), retrying...",
                                "WARNING"
                            )

                except Exception as e:
                    self.logger.log(f"Error placing orders (attempt {attempt + 1}): {e}", "ERROR")
                    self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")

            if leg2_order_result.success and self.order_filled_event[2].is_set():
                order_state.leg2_success = True
                order_state.leg2_filled_size = leg2_order_result.size
                order_state.leg2_filled_price = leg2_order_result.price
                order_state.leg2_order_id = leg2_order_result.order_id
                self.logger.log(
                    f"[LEG2] Order filled: {leg2_order_result.size} @ {leg2_order_result.price}",
                    "INFO"
                )

        return order_state
            

    async def _check_mismatch(self, leg1_pos: Decimal, leg2_pos: Decimal) -> bool:
        """Check position mismatch and handle accordingly."""
        
        mismatch_size = abs(abs(leg1_pos) - abs(leg2_pos))
        
        if mismatch_size < self.config.min_order_size:
            # Ignore small mismatch
            return False
        
        elif mismatch_size >= self.config.max_quantity:
            # Critical mismatch - exit script
            error_msg = (
                f"\n\n<b>[CRITICAL ERROR]</b> Position Mismatch Detected\n"
                f"Exchange: {self.config.exchange_leg1} / {self.config.exchange_leg2}\n"
                f"Ticker: {self.config.ticker}\n"
                f"Leg1 Position: {leg1_pos}\n"
                f"Leg2 Position: {leg2_pos}\n"
                f"Mismatch Size: {mismatch_size}\n"
                f"Max Quantity: {self.config.max_quantity}\n"
                f"⚠️ Mismatch exceeds max_quantity threshold!\n"
                f"⚠️ Script will exit. Please manually rebalance positions.\n"
            )
            
            self.logger.log(error_msg, "ERROR")
            await self.send_notification(error_msg)
            
            return True
        
        return False

    async def send_notification(self, message: str):
        """Send notification via Lark and Telegram."""
        try:
            lark_token = os.getenv("LARK_TOKEN")
            if lark_token:
                async with LarkBot(lark_token) as lark_bot:
                    await lark_bot.send_text(message)
            
            telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
            telegram_chat_id = os.getenv("TELEGRAM_CHAT_ID")
            if telegram_token and telegram_chat_id:
                with TelegramBot(telegram_token, telegram_chat_id) as tg_bot:
                    tg_bot.send_text(message)
        except Exception as e:
            self.logger.log(f"Error sending notification: {e}", "ERROR")

    async def run(self):
        """Main trading loop."""
        try:
            # Initialize clients
            await self._initialize_clients()
            
            # Get contract attributes
            _, self.config.tick_size_leg1 = \
                await self.leg1_client.get_contract_attributes()
            _, self.config.tick_size_leg2 = \
                await self.leg2_client.get_contract_attributes()
            
            # Log configuration
            self.logger.log("=== Perpetual Arbitrage Configuration ===", "INFO")
            self.logger.log(f"Ticker: {self.config.ticker}", "INFO")
            self.logger.log(f"Leg1: {self.config.exchange_leg1} - {self.config.contract_id_leg1}", "INFO")
            self.logger.log(f"Leg2: {self.config.exchange_leg2} - {self.config.contract_id_leg2}", "INFO")
            self.logger.log(f"Max Quantity: {self.config.max_quantity}", "INFO")
            self.logger.log(f"Max Total Size: {self.config.max_total_size}", "INFO")
            self.logger.log(f"Min Price Diff (Open): {self.config.min_price_diff_open}", "INFO")
            self.logger.log(f"Max Price Diff (Close): {self.config.min_price_diff_close}", "INFO")
            self.logger.log(f"Min Order Size: {self.config.min_order_size}", "INFO")
            self.logger.log(f"Slippage: {self.config.max_slippage_leg2}", "INFO")
            self.logger.log("========================================", "INFO")
            
            # Connect to exchanges
            await self.leg1_client.connect()
            await self.leg2_client.connect()
            
            # Setup WebSocket handlers
            await self._setup_websocket_handlers()
            
            # Wait for connections to establish
            await asyncio.sleep(5)
            
            # Capture event loop
            self.loop = asyncio.get_running_loop()
            
            # Initial position check
            leg1_pos, leg2_pos = await self._get_positions()
            mismatch_size = abs(abs(leg1_pos) - abs(leg2_pos))
            self.logger.log(f"mismatch_size {mismatch_size}")
            
            if mismatch_size >= self.config.min_order_size:
                error_msg = (
                    f"\n\n[WARNING] Initial Position Mismatch Detected\n"
                    f"Leg1 Position: {leg1_pos}\n"
                    f"Leg2 Position: {leg2_pos}\n"
                    f"Mismatch Size: {mismatch_size}\n"
                    f"⚠️ Please manually rebalance before starting bot.\n"
                )
                self.logger.log(error_msg, "WARNING")
                await self.send_notification(error_msg)
                
                if mismatch_size >= self.config.max_quantity:
                    raise ValueError("Initial mismatch too large, exiting")
            
            # Main trading loop
            while not self.shutdown_requested:
                try:
                    # Check order interval
                    current_time = time.time() * 1000  # Convert to ms
                    if current_time - self.last_order_time < self.config.order_interval:
                        await asyncio.sleep(self.config.loop_interval / 1000)
                        continue
                    
                    # 1. Get positions
                    leg1_pos, leg2_pos = await self._get_positions()
                    
                    # 2. Fetch depths
                    best_orders = await self._fetch_depths()
                    
                    # 3. Calculate price differences
                    price_diff = self._calculate_price_diff(best_orders)
                    
                    # 4. Check arbitrage conditions
                    direction, action = await self._check_arb_conditions(
                        price_diff,
                        leg1_pos,
                        leg2_pos
                    )
                    
                    # Log current state
                    if time.time() - self.last_log_time > 60 or self.last_log_time == 0:
                        self.logger.log(
                            f"Price Diff: 12={price_diff['12']:.2f} ({self.price_diff_twa['12']:.2f}), 21={price_diff['21']:.2f} ({self.price_diff_twa['21']:.2f})| "
                            f"Positions: L1={leg1_pos:.4f}, L2={leg2_pos:.4f} | "
                            f"mismatch: {abs(leg1_pos) - abs(leg2_pos):.4f}",
                            # f"direction: {direction if direction else '--'}, action: {action if action else '--'}",
                            "INFO"
                        )
                        self.last_log_time = time.time()

                        # Check mismatch after order execution
                        critical_mismatch = await self._check_mismatch(leg1_pos, leg2_pos)
                        
                        if critical_mismatch:
                            self.shutdown_requested = True
                            break
                    
                    # 5. Execute orders if conditions met
                    if direction and action:
                        order_state = await self._execute_orders(
                            direction,
                            action,
                            best_orders
                        )

                        if order_state.leg1_success or order_state.leg2_success:

                            self.last_order_time = time.time() * 1000
                            filled_amount = min(order_state.leg1_filled_size, order_state.leg2_filled_size)
                            _msg = (
                                "\n"
                                f"Action: <b>[{action}]</b>\n"
                                f"Direction: <b>{'leg1 long, leg2 short' if direction == '12' else 'leg1 short, leg2 long'}</b>\n"
                                f"Ideal Price diff: <b>{price_diff[direction]:.2f}</b> (TWA <b>{self.price_diff_twa[direction]:.2f}</b>)\n"
                                f"Order Result:\n"
                                f"  leg1 filled {order_state.leg1_filled_size:.6f} @ {order_state.leg1_filled_price:.2f}\n"
                                f"  leg2 filled {order_state.leg2_filled_size:.6f} @ {order_state.leg2_filled_price:.2f}\n"
                            )
                            if action == "OPEN":
                                _msg += f"Except profit: <b>{(price_diff[direction] * filled_amount):.2f}</b>\n"

                            self.logger.log(_msg)
                            await self.send_notification(_msg)
                    
                    # Wait before next loop
                    await asyncio.sleep(self.config.loop_interval / 1000)
                    
                except Exception as e:
                    self.logger.log(f"Error in trading loop: {e}", "ERROR")
                    self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
                    await asyncio.sleep(5)
            
        except KeyboardInterrupt:
            self.logger.log("Bot stopped by user", "INFO")
        except Exception as e:
            self.logger.log(f"Critical error: {e}", "ERROR")
            self.logger.log(f"Traceback: {traceback.format_exc()}", "ERROR")
            raise
        finally:
            # Cleanup
            try:
                if self.leg1_client:
                    await self.leg1_client.disconnect()
                if self.leg2_client:
                    await self.leg2_client.disconnect()
            except Exception as e:
                self.logger.log(f"Error during cleanup: {e}", "ERROR")



def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Perp Arbitrage Bot - Supports multiple exchanges')

    # Trading parameters
    parser.add_argument('--ticker', type=str, default='ETH',
                        help='Ticker (default: ETH)')
    parser.add_argument('--quantity', type=Decimal, default=Decimal("0.03"),
                        help='Order quantity (default: 0.03)')
    parser.add_argument('--total', type=Decimal, default=Decimal("0.06"),
                        help='Total Order quantity (default: 0.06)')
    parser.add_argument('--open', type=Decimal, default=Decimal("4.00"),
                        help='min_price_diff_open (default: 4.00)')
    parser.add_argument('--close', type=Decimal, default=Decimal("-1.00"),
                        help='min_price_diff_open (default: -1.00)')
    parser.add_argument('--slippage', type=Decimal, default=Decimal("4.00"),
                        help='max_slippage (default: 4.00)')
    parser.add_argument('--order-interval', type=Decimal, default=Decimal("60"),
                        help='max_slippage (default: 60)')
    parser.add_argument('--env-file', type=str, default=".env",
                        help=".env file path (default: .env)")

    return parser.parse_args()


async def main():
    """Main entry point."""
    args = parse_arguments()

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Env file not find: {env_path.resolve()}")
        sys.exit(1)
    dotenv.load_dotenv(args.env_file)

    config = PerpArbConfig(
        ticker="ETH",
        contract_id_leg1="ETHUSDC",
        contract_id_leg2="0",
        exchange_leg1="binance",
        exchange_leg2="lighter",
        max_quantity=args.quantity,
        max_total_size=args.total,
        order_interval=args.order_interval*1000,       # 1 second
        loop_interval=1*1000,        # 1 second
        order_timeout=5*1000,       # 10 seconds
        min_price_diff_open=args.open,
        min_price_diff_close=args.close,
        min_order_size=Decimal("0.025"),
        max_slippage_leg1=args.slippage,
        max_slippage_leg2=args.slippage,
    )
    
    bot = PerpArbBot(config)
    try:
        await bot.run()
    except Exception as e:
        print(f"Bot execution failed: {e}")
        # The bot's run method already handles graceful shutdown
        return


if __name__ == "__main__":
    asyncio.run(main())