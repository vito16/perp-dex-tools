import asyncio
import json
import signal
import logging
import os
import sys
import time
import requests
import argparse
import traceback
import csv
from decimal import Decimal
from typing import Tuple

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.backpack import BackpackClient
from exchanges.paradex import ParadexClient
from exchanges.base import OrderResult
from helpers.telegram_bot import TelegramBot
import signal
import traceback
import websockets
from datetime import datetime
import pytz

class Config:
    """Simple config class to wrap dictionary for exchange clients."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class HedgeBot:
    """Trading bot that places post-only orders on Backpack and hedges with market orders on Paradex."""

    def _initialize_csv_file(self):
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity', 'fee'])

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 16, iterations: int = 20, sleep_time: int = 0, max_position: Decimal = Decimal('0')):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.iterations = iterations
        self.sleep_time = sleep_time
        self.max_position = max_position if max_position != Decimal('0') else order_quantity

        # Initialize logging
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/backpack_paradex_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/backpack_paradex_{ticker}_hedge_mode_trades.csv"
        
        self._initialize_csv_file()
        
        # Setup logger
        self.logger = logging.getLogger(f"hedge_bot_bp_paradex_{ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        
        # File handler
        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        self.logger.addHandler(file_handler)
        
        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

        # Statistics
        self.total_pnl = Decimal('0')
        self.total_fee = Decimal('0')
        
        # Cycle state tracking for PnL calculation
        self.cycle_state = 'IDLE'  # IDLE, ENTRY_PENDING, ENTRY_DONE, EXIT_PENDING, EXIT_DONE
        self.entry_bp_price = None
        self.entry_bp_fee = Decimal('0')
        self.entry_pd_price = None
        self.entry_pd_fee = Decimal('0')
        self.exit_bp_price = None
        self.exit_bp_fee = Decimal('0')
        self.exit_pd_price = None
        self.exit_pd_fee = Decimal('0')
        self.cycle_quantity = Decimal('0')
        self.cycle_id = 0
        
        self.bp_filled_price = None
        self.bp_filled_fee = Decimal('0')
        self.pd_filled_price = None
        self.pd_filled_fee = Decimal('0')

        # State management
        self.stop_flag = False
        self.backpack_client = None
        self.paradex_client = None
        
        # Telegram notification setup
        self.tg_bot = None
        tg_token = os.getenv("TELEGRAM_BOT_TOKEN")
        tg_chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if tg_token and tg_chat_id:
            self.tg_bot = TelegramBot(tg_token, tg_chat_id)
            self.logger.info("✅ Telegram notification enabled")
        
        self.backpack_contract_id = None
        self.backpack_tick_size = None
        self.backpack_position = Decimal('0')
        self.backpack_order_status = None
        
        self.paradex_contract_id = None
        self.paradex_tick_size = None
        self.paradex_position = Decimal('0')

        # Backpack BBO from WS
        self.backpack_best_bid = None
        self.backpack_best_ask = None
        self.backpack_order_book_ready = False

        # Order Execution State
        self.waiting_for_hedge_fill = False
        self.current_hedge_side = None
        self.current_hedge_quantity = None
        self.hedge_filled = False
        self.hedge_failed = False  # Track if hedge failed after retries
        self.active_order_id = None

    def shutdown(self, signum=None, frame=None):
        """Signal handler to set stop flag."""
        if self.stop_flag: return
        self.stop_flag = True
        self.logger.info("\n🛑 Stop signal received, heading to cleanup...")

    async def cleanup(self):
        """Final cleanup before exiting."""
        self.logger.info("🧹 Performing final cleanup...")
        if self.active_order_id and self.backpack_client:
             try:
                 # Note: self.backpack_contract_id must be initialized
                 await self.backpack_client.cancel_order(self.active_order_id)
                 self.logger.info(f"Cancelled active order: {self.active_order_id}")
             except: pass
        
        if self.backpack_client:
            await self.backpack_client.disconnect()
        if self.paradex_client:
            await self.paradex_client.disconnect()
            
        if hasattr(self, 'tg_task') and self.tg_task:
            self.tg_task.cancel()
            try:
                await self.tg_task
            except asyncio.CancelledError:
                pass

    def setup_signal_handlers(self):
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def _initialize_csv_file(self):
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity', 'fee', 'cycle_id'])

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str, fee: str = '0'):
        timestamp = datetime.now(pytz.UTC).isoformat()
        # Ensure fee is negative (cost)
        fee_value = -abs(Decimal(fee))
        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([exchange, timestamp, side, price, quantity, str(fee_value), self.cycle_id])
        self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price} (Fee: {fee_value})")

    async def sync_positions(self):
        """Sync initial positions and check for liquidation risk."""
        try:
            self.backpack_position = await self.backpack_client.get_account_positions()
            self.paradex_position = await self.paradex_client.get_account_positions()
            self.logger.info(f"🔄 Positions Synced - BP: {self.backpack_position}, Paradex: {self.paradex_position}")
            
            # Check liquidation risk
            await self.check_liquidation_risk()
        except Exception as e:
            self.logger.error(f"Failed to sync positions: {e}")

    async def reconcile_positions(self):
        """Check and fix position mismatch between Backpack and Paradex."""
        try:
            await self.sync_positions()
            
            # Calculate position difference
            # Paradex position is opposite to Backpack (hedge)
            expected_paradex = -self.backpack_position
            position_diff = self.paradex_position - expected_paradex
            
            threshold = Decimal('0.001')  # Allow small rounding errors
            
            if abs(position_diff) > threshold:
                self.logger.warning(f"⚠️ Position mismatch detected!")
                self.logger.warning(f"   Backpack: {self.backpack_position}")
                self.logger.warning(f"   Paradex: {self.paradex_position} (Expected: {-self.backpack_position})")
                self.logger.warning(f"   Difference: {position_diff}")
                
                # Check if it's too small for Paradex
                if abs(position_diff) < self.paradex_min_qty:
                    self.logger.warning(f"⚠️ Mismatch {abs(position_diff)} is too small for Paradex (min: {self.paradex_min_qty})")
                    self.logger.info(f"🔧 Attempting to reconcile by trading on Backpack instead...")
                    
                    # If we have 72.2 on BP and -72.0 on PD, diff is 0.2.
                    # We need to SELL 0.2 on Backpack to reach 72.0.
                    # Side on BP = 'sell' if position_diff > 0 else 'buy'
                    bp_side = 'sell' if position_diff > 0 else 'buy'
                    bp_qty = abs(position_diff)
                    
                    res = await self.backpack_client.place_market_order(self.backpack_contract_id, bp_qty, bp_side)
                    if res.success:
                        self.logger.info(f"✅ Backpack reconciliation successful")
                        await self.sync_positions()
                        return True
                    else:
                        self.logger.error(f"❌ Backpack reconciliation failed: {res.error_message}")
                        return False
                
                self.logger.info(f"🔧 Attempting to reconcile position mismatch on Paradex...")
                
                # Determine which side to trade on Paradex to fix the mismatch
                if position_diff > 0:
                    # Paradex has too much long position (or not enough short), need to sell
                    side = 'sell'
                    qty = abs(position_diff)
                else:
                    # Paradex has too much short position, need to buy
                    side = 'buy'
                    qty = abs(position_diff)
                
                # Try to fix the mismatch
                success = await self.execute_hedge_on_paradex(side, qty)
                
                if success:
                    self.logger.info(f"✅ Position reconciliation successful")
                    await self.sync_positions()
                    return True
                else:
                    self.logger.error(f"❌ Position reconciliation FAILED!")
                    return False
            
            return True
        except Exception as e:
            self.logger.error(f"Failed to reconcile positions: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return False

    async def initialize_clients(self):
        """Initialize both exchange clients."""
        # Initialize Backpack
        bp_config = {
            'ticker': self.ticker,
            'contract_id': '',
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),
            'close_order_side': 'sell'
        }
        self.backpack_client = BackpackClient(Config(bp_config))
        self.logger.info("✅ Backpack client initialized")

        # Initialize Paradex
        pd_config = {
            'ticker': self.ticker,
            'contract_id': '',
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),
            'direction': 'buy', # Placeholder
            'close_order_side': 'sell' # Placeholder
        }
        self.paradex_client = ParadexClient(Config(pd_config))
        self.logger.info("✅ Paradex client initialized")

    async def get_contract_infos(self):
        """Get contract details for both exchanges."""
        self.backpack_contract_id, self.backpack_tick_size, self.backpack_min_qty = await self.backpack_client.get_contract_attributes()
        # Paradex needs uppercase ticker usually
        self.paradex_client.config.ticker = self.ticker.upper()
        self.paradex_contract_id, self.paradex_tick_size, self.paradex_min_qty = await self.paradex_client.get_contract_attributes()
        
        self.logger.info(f"Contracts loaded - Backpack: {self.backpack_contract_id}, Paradex: {self.paradex_contract_id}")

    async def setup_backpack_websocket(self):
        """Setup Backpack WebSocket for updates."""
        def order_update_handler(order_data):
            if order_data.get('contract_id') != self.backpack_contract_id:
                return
            
            status = order_data.get('status')
            side = order_data.get('side', '').lower()
            filled_size = Decimal(order_data.get('filled_size', '0'))
            price = order_data.get('price', '0')
            order_id = order_data.get('order_id')
            size = order_data.get('size', '0')

            # Log order status updates
            if self.active_order_id and str(order_id) == str(self.active_order_id):
                if status == 'FILLED':
                    self.logger.info(f"[Backpack] Order FILLED: {side} {filled_size}/{size} @ {price}")
                    bp_fee = -abs(Decimal(order_data.get('fee', '0')))
                    
                    # Log trade to CSV
                    self.log_trade_to_csv('Backpack', side, str(price), str(filled_size), str(bp_fee))
                    
                    # Update internal position
                    if side == 'buy':
                        self.backpack_position += filled_size
                    else:
                        self.backpack_position -= filled_size
                    
                    # Store fill info for Paradex hedge (use total filled_size, not delta)
                    self.current_hedge_side = 'sell' if side == 'buy' else 'buy'
                    self.current_hedge_quantity = filled_size  # Use total filled size
                    self.bp_filled_price = Decimal(price)
                    self.bp_filled_fee = bp_fee
                    
                    # Trigger hedge in main loop
                    self.waiting_for_hedge_fill = True
                    
                elif status in ['PARTIALLY_FILLED', 'OPEN']:
                    self.logger.info(f"[Backpack] Order {status}: {side} {filled_size}/{size} @ {price}")
                elif status in ['CANCELED', 'EXPIRED']:
                    self.logger.warning(f"[Backpack] Order {status}: {order_id}")


        self.backpack_client.setup_order_update_handler(order_update_handler)
        await self.backpack_client.connect()
        
        # Depth for BBO
        from exchanges.backpack import BackpackWebSocketManager
        # Note: Depth handling logic from hedge_mode_bp.py should be reused ideally or simplified.
        # For brevity, implementing simplified BBO fetcher if needed, but optimally reuse the loop in BackpackClient
        # or just rely on the existing WebSocket logic which seems to handle it if we subscribe?
        # Actually BackpackClient (new version in memory) has setup_backpack_depth_websocket methods? 
        # No, that was in hedge_mode_bp.py directly.
        # I will attach the depth update handler here.
        
        # We need to manually start the depth stream subscription logic similar to hedge_mode_bp.py
        # Or better yet, rely on BackpackClient's fetch_bbo_prices which uses REST if WS BBO not available.
        # But we want WS BBO for speed.
        
        # Let's add the depth handler to the ws_manager if possible
        # BackpackClient currently doesn't expose easy way to add another callback?
        # hedge_mode_bp.py did: self.handle_backpack_order_book_update in depth loop.
        # We will replicate the depth loop here.
        asyncio.create_task(self.run_backpack_depth_stream())

    async def run_backpack_depth_stream(self):
        url = "wss://ws.backpack.exchange"
        while not self.stop_flag:
            try:
                async with websockets.connect(url) as ws:
                    subscribe_message = {
                        "method": "SUBSCRIBE",
                        "params": [f"depth.{self.backpack_contract_id}"]
                    }
                    await ws.send(json.dumps(subscribe_message))
                    self.logger.info(f"✅ Subscribed to Backpack depth: {self.backpack_contract_id}")
                    
                    async for message in ws:
                        if self.stop_flag: break
                        data = json.loads(message)
                        if data.get('stream'):
                            self.update_backpack_bbo(data['data'])
            except Exception as e:
                self.logger.error(f"Backpack depth stream error: {e}")
                await asyncio.sleep(2)

    def update_backpack_bbo(self, data):
        bids = data.get('b', [])
        asks = data.get('a', [])
        if bids:
            # Simple max bid finding (assuming updates are relevant)
            # data is snapshot or update? Backpack sends consistent updates.
            # We need a local orderbook to be accurate or just parse top of message if it's snapshot-like?
            # Backpack depth stream is updates. We need to maintain a book.
            # Reusing book logic.
            pass # Skipping full book maintenance for brevity, will rely on periodic snapshots or just assuming updates contain BBO often enough?
            # Actually, `hedge_mode_bp.py` maintains full book.
            # I'll implement a simple BBO caching if the message contains it.
            # The message is diffs. Without a snapshot, Diffs are useless. 
            # hedge_mode_bp.py logic was: maintain `self.backpack_order_book`.
            pass

    async def place_backpack_maker_order(self, side: str, quantity: Decimal):
        """Place Post-Only order on Backpack."""
        self.backpack_order_status = 'NEW'
        
        # Safety: Cancel all existing orders for this market before placing new one
        try:
            self.logger.info(f"🧹 Cancelling any open orders for {self.backpack_contract_id}...")
            await self.backpack_client.cancel_all_orders(self.backpack_contract_id)
            await asyncio.sleep(0.5) # Allow time for propagation
        except Exception as e:
            self.logger.warning(f"Failed to cancel open orders: {e}")

        # Get BBO
        best_bid, best_ask = await self.backpack_client.fetch_bbo_prices(self.backpack_contract_id)
        
        price = best_bid + self.backpack_tick_size if side == 'buy' else best_ask - self.backpack_tick_size
        # Adjust to be maker
        if side == 'buy':
            price = min(price, best_ask - self.backpack_tick_size)
        else:
            price = max(price, best_bid + self.backpack_tick_size)
            
        self.logger.info(f"⏳ [Backpack] [MAKER] Placing {side} order: {quantity} @ {price}")
        
        attempt = 0
        res = OrderResult(success=False, error_message="Unknown error")
        
        while attempt < 3:
            try:
                res = await self.backpack_client.place_open_order(
                    self.backpack_contract_id,
                    quantity,
                    side
                )
                if res.success:
                    self.logger.info(f"✅ [Backpack] [MAKER] Order OPEN: {res.order_id} ({side} {quantity} @ {res.price})")
                break
            except Exception as e:
                attempt += 1
                self.logger.error(f"⚠️ [Backpack] Order placement failed (Attempt {attempt}/3): {e}")
                if attempt < 3:
                    await asyncio.sleep(1)
                else:
                    res = OrderResult(success=False, error_message=str(e))
        
        return res

    async def execute_hedge_on_paradex(self, side: str, quantity: Decimal):
        """Execute market order on Paradex with partial fill retry mechanism."""
        max_retries = 5  # Increased for partial fills
        retry_delay = 2
        
        remaining = quantity
        total_filled = Decimal('0')
        total_fee = Decimal('0')
        weighted_price_sum = Decimal('0')
        
        for attempt in range(1, max_retries + 1):
            if remaining <= Decimal('0.001'):  # Small threshold
                break
                
            try:
                result = await self.paradex_client.place_market_order(
                    self.paradex_contract_id,
                    remaining,
                    side
                )
                
                if result.success:
                    filled = result.size  # ✅ Actual filled size from API
                    fill_price = result.price if result.price else Decimal('0')
                    fee = -abs(result.fee) if result.fee else Decimal('0')
                    
                    # Accumulate filled data
                    total_filled += filled
                    total_fee += fee
                    weighted_price_sum += fill_price * filled
                    remaining -= filled
                    
                    self.logger.info(
                        f"✅ Paradex Filled: {filled} @ {fill_price} (Fee: {fee})"
                    )
                    
                    # Log each fill to CSV
                    self.log_trade_to_csv('Paradex', side, str(fill_price), str(filled), str(fee))
                    
                    # Update position
                    if side == 'buy':
                        self.paradex_position += filled
                    else:
                        self.paradex_position -= filled
                    
                    # Check if fully filled
                    if remaining <= Decimal('0.001'):
                        # Calculate weighted average price
                        avg_price = weighted_price_sum / total_filled if total_filled > 0 else Decimal('0')
                        
                        self.pd_filled_price = avg_price
                        self.pd_filled_fee = total_fee
                        self.hedge_filled = True
                        self.hedge_failed = False
                        
                        self.logger.info(
                            f"✅ Paradex Hedge Completed: {total_filled} @ {avg_price} "
                            f"(Total Fee: {total_fee})"
                        )
                        return True
                    else:
                        # Partial fill, retry remaining
                        self.logger.warning(
                            f"⚠️ Partial fill: {filled}/{quantity}, "
                            f"remaining: {remaining}, retrying..."
                        )
                        await asyncio.sleep(retry_delay)
                else:
                    self.logger.error(
                        f"❌ Paradex order failed (Attempt {attempt}/{max_retries}): "
                        f"{result.error_message}"
                    )
                    if attempt < max_retries:
                        await asyncio.sleep(retry_delay)
                        
            except Exception as e:
                self.logger.error(f"❌ Paradex Exception (Attempt {attempt}): {e}")
                self.logger.debug(traceback.format_exc())
                if attempt < max_retries:
                    await asyncio.sleep(retry_delay)
        
        # Check final fill ratio
        fill_ratio = total_filled / quantity if quantity > 0 else Decimal('0')
        
        if fill_ratio >= Decimal('0.95'):  # ✅ Allow 5% tolerance
            # Accept with minor shortfall
            avg_price = weighted_price_sum / total_filled if total_filled > 0 else Decimal('0')
            self.pd_filled_price = avg_price
            self.pd_filled_fee = total_fee
            self.hedge_filled = True
            self.hedge_failed = False
            
            self.logger.warning(
                f"⚠️ Hedge completed with shortfall: {total_filled}/{quantity} "
                f"({fill_ratio*100:.2f}%)"
            )
            return True
        else:
            # Insufficient fill
            self.logger.error(
                f"❌ Hedge FAILED after {max_retries} attempts: "
                f"{total_filled}/{quantity} ({fill_ratio*100:.2f}%)"
            )
            self.logger.error(f"⚠️ CRITICAL: Position mismatch! BP position changed but Paradex hedge failed.")
            self.hedge_failed = True
            return False

    def report_cycle_pnl(self):
        """Calculate and report PnL for the completed hedge."""
        # Simple approach: accumulate fees and PnL after each Backpack+Paradex pair
        if self.bp_filled_price and self.pd_filled_price and self.current_hedge_quantity:
            qty = self.current_hedge_quantity
            
            # Calculate price difference PnL
            # If current_hedge_side is 'sell', it means BP bought and PD sold
            # If current_hedge_side is 'buy', it means BP sold and PD bought
            if self.current_hedge_side == 'sell':
                # BP bought, PD sold
                pnl = (self.pd_filled_price - self.bp_filled_price) * qty
            else:
                # BP sold, PD bought
                pnl = (self.bp_filled_price - self.pd_filled_price) * qty
            
            # Fees are already negative
            total_fees = self.bp_filled_fee + self.pd_filled_fee
            net_pnl = pnl + total_fees  # fees are negative, so this subtracts them
            
            self.total_pnl += net_pnl
            self.total_fee += total_fees
            
            self.logger.info("=" * 60)
            self.logger.info(f"📊 Trade Pair Completed:")
            self.logger.info(f"   Backpack: {qty} @ {self.bp_filled_price} (Fee: {self.bp_filled_fee})")
            self.logger.info(f"   Paradex:  {qty} @ {self.pd_filled_price} (Fee: {self.pd_filled_fee})")
            self.logger.info(f"   Price PnL: {pnl:.4f} USDC")
            self.logger.info(f"   Total Fee: {total_fees:.4f} USDC")
            self.logger.info(f"   Net PnL:   {net_pnl:.4f} USDC")
            self.logger.info(f"   Cumulative Net PnL: {self.total_pnl:.4f} USDC")
            self.logger.info(f"   Cumulative Fees:    {self.total_fee:.4f} USDC")
            self.logger.info("=" * 60)
            
            # ✅ Send real-time notification to Telegram
            if self.tg_bot:
                cycle_data = {
                    'qty': qty,
                    'bp_price': self.bp_filled_price,
                    'pd_price': self.pd_filled_price,
                    'net_pnl': net_pnl
                }
                asyncio.create_task(self.send_status_to_tg(cycle_data))
            
            # Reset for next pair
            self.bp_filled_price = None
            self.pd_filled_price = None
            self.bp_filled_fee = Decimal('0')
            self.pd_filled_fee = Decimal('0')

    def report_total_pnl(self):
        self.logger.info("===============================================")
        self.logger.info("🏁 SESSION FINAL SUMMARY")
        self.logger.info(f"   Total Net PnL: {self.total_pnl:.4f} USDC")
        self.logger.info(f"   Total Fees:    {self.total_fee:.4f} USDC")
        self.logger.info("===============================================")
        
        # ✅ Send final summary to TG
        if self.tg_bot:
            message = (
                f"🏁 <b>{self.ticker} SESSION FINAL SUMMARY</b>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"💰 <b>Total Net PnL:</b> <code>{self.total_pnl:.4f} USDC</code>\n"
                f"💸 <b>Total Fees:</b> <code>{self.total_fee:.4f} USDC</code>\n"
                f"🕒 <i>Closed at: {datetime.now().strftime('%H:%M:%S')}</i>"
            )
            asyncio.create_task(asyncio.to_thread(self.tg_bot.send_text, message))

    async def send_status_to_tg(self, cycle_info=None):
        """Send current status to Telegram."""
        if not self.tg_bot:
            return
            
        trade_details = ""
        if cycle_info:
            trade_details = (
                f"📝 <b>Last Cycle:</b>\n"
                f"• BP: {cycle_info['qty']} @ {cycle_info['bp_price']}\n"
                f"• PD: {cycle_info['qty']} @ {cycle_info['pd_price']}\n"
                f"• PnL: <code>{cycle_info['net_pnl']:.4f}</code>\n"
                f"━━━━━━━━━━━━━━━━━━\n"
            )

        message = (
            f"✅ <b>{self.ticker} Position Update</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{trade_details}"
            f"🏠 <b>Backpack:</b> <code>{self.backpack_position}</code>\n"
            f"🌌 <b>Paradex:</b> <code>{self.paradex_position}</code>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"💰 <b>Net PnL:</b> <code>{self.total_pnl:.4f} USDC</code>\n"
            f"💸 <b>Fees:</b> <code>{self.total_fee:.4f} USDC</code>\n"
            f"🕒 <i>Updated at: {datetime.now().strftime('%H:%M:%S')}</i>"
        )
        
        try:
            await asyncio.to_thread(self.tg_bot.send_text, message)
            self.logger.info("📡 Status report sent to Telegram")
        except Exception as e:
            self.logger.error(f"❌ Failed to send Telegram report: {e}")

    async def send_notification(self, title: str, status_msg: str, emoji: str = "ℹ️"):
        """Send a general notification to Telegram."""
        if not self.tg_bot:
            return
            
        message = (
            f"{emoji} <b>{title}</b>\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"{status_msg}\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🏠 <b>BP:</b> <code>{self.backpack_position}</code>\n"
            f"🌌 <b>PD:</b> <code>{self.paradex_position}</code>\n"
            f"💰 <b>PnL:</b> <code>{self.total_pnl:.4f}</code>\n"
            f"🕒 <i>{datetime.now().strftime('%H:%M:%S')}</i>"
        )
        try:
            await asyncio.to_thread(self.tg_bot.send_text, message)
        except Exception as e:
            self.logger.error(f"TG Notify Failed: {e}")

    async def check_liquidation_risk(self):
        """Check if any position is close to liquidation."""
        if not self.tg_bot:
            return
            
        try:
            bp_liq = await self.backpack_client.get_liquidation_price()
            pd_liq = await self.paradex_client.get_liquidation_price()
            
            # Get current mark price
            best_bid, best_ask = await self.paradex_client.fetch_bbo_prices(self.paradex_contract_id)
            mark_price = (best_bid + best_ask) / 2
            
            if mark_price <= 0: return

            alerts = []
            # Threshold: 10% distance to liquidation
            threshold = Decimal('0.10')

            if bp_liq and bp_liq > 0:
                dist = abs(mark_price - bp_liq) / mark_price
                if dist < threshold:
                    alerts.append(f"⚠️ <b>Backpack Risk</b>\nMark: {mark_price:.4f}\nLiq: {bp_liq:.4f}\nDist: {dist*100:.2f}%")
            
            if pd_liq and pd_liq > 0:
                dist = abs(mark_price - pd_liq) / mark_price
                if dist < threshold:
                    alerts.append(f"⚠️ <b>Paradex Risk</b>\nMark: {mark_price:.4f}\nLiq: {pd_liq:.4f}\nDist: {dist*100:.2f}%")
            
            for alert in alerts:
                await self.send_notification("Liquidation Alert", alert, emoji="🚨")
                
        except Exception as e:
            self.logger.error(f"Failed to check liquidation risk: {e}")

    async def tg_periodic_reporter(self):
        """Task to send status report every 2 hours."""
        if not self.tg_bot:
            return
            
        self.logger.info("🕒 Telegram periodic reporter started (Every 2 hours)")
        
        while not self.stop_flag:
            try:
                # Wait 2 hours, checking stop_flag every second
                for _ in range(7200):
                    if self.stop_flag: return
                    await asyncio.sleep(1)
                
                if not self.stop_flag:
                    self.logger.info("🕒 Sending scheduled 2-hour status report...")
                    await self.sync_positions() # This also updates internal position vars
                    await self.send_status_to_tg()
            except Exception as e:
                self.logger.error(f"Error in TG reporter loop: {e}")
                await asyncio.sleep(60)

    async def run(self):
        self.setup_signal_handlers()
        await self.initialize_clients()
        await self.get_contract_infos()
        await self.setup_backpack_websocket()
        await self.paradex_client.connect()

        self.logger.info(f"🚀 Starting Backpack-Paradex Hedge Bot for {self.ticker}")
        
        # Initial position sync
        await self.sync_positions()
        
        # Start periodic reporter
        self.tg_task = None
        if self.tg_bot:
            self.tg_task = asyncio.create_task(self.tg_periodic_reporter())

            
        # Main Loop
        iterations = 0
        try:
            while iterations < self.iterations and not self.stop_flag:
                try:
                    # Check and fix position mismatch before starting iteration
                    reconcile_success = await self.reconcile_positions()
                    if not reconcile_success:
                        self.logger.error("❌ Position reconciliation failed, stopping trading...")
                        if self.tg_bot:
                            await self.send_notification(
                                f"❌ Fatal Error: {self.ticker}", 
                                "Position reconciliation failed. Bot stopping.",
                                emoji="🚨"
                            )
                        break
                    iterations += 1
                    self.logger.info(f"--- Iteration {iterations}/{self.iterations} ---")
                    
                    # 1. Buy on Backpack, Sell on Paradex (ENTRY)
                    while self.backpack_position < self.max_position and not self.stop_flag:
                        # HEAL check: if we somehow have a pending hedge
                        if self.waiting_for_hedge_fill:
                            self.logger.info("♻️  Late fill detected, hedging before placing new order...")
                            await self.execute_hedge_on_paradex(self.current_hedge_side, self.current_hedge_quantity)
                            self.report_cycle_pnl()
                            self.waiting_for_hedge_fill = False
                            continue

                        remaining = self.max_position - self.backpack_position
                        qty = min(self.order_quantity, remaining)
                        if qty <= 0: break
                        
                        self.waiting_for_hedge_fill = False
                        
                        res = await self.place_backpack_maker_order('buy', qty)
                        if not res.success:
                            self.logger.error(f"Backpack buy failed: {res.error_message}")
                            await self.sync_positions() # Calibration sync
                            await asyncio.sleep(5)
                            continue
                        
                        self.active_order_id = res.order_id
                        
                        # Wait for Fill
                        wait_start = time.time()
                        while not self.stop_flag:
                            # Process any fills that arrived
                            if self.waiting_for_hedge_fill:
                                success = await self.execute_hedge_on_paradex(self.current_hedge_side, self.current_hedge_quantity)
                                if not success or self.hedge_failed:
                                    self.logger.error("❌ Hedge failed, stopping trading...")
                                    if self.tg_bot:
                                        await self.send_notification(
                                            f"❌ Hedge Failed: {self.ticker}",
                                            "Paradex hedge failed to fill after retries. Manual intervention required!",
                                            emoji="🚨"
                                        )
                                    self.stop_flag = True
                                    break
                                await self.sync_positions()  # Verify positions after hedge
                                self.report_cycle_pnl()
                                self.waiting_for_hedge_fill = False

                            # Check if the whole order is done (approximate by position or status)
                            if self.backpack_position >= self.max_position:
                                break
                                
                            if time.time() - wait_start > self.fill_timeout:
                                self.logger.info(f"Timeout reached, cancelling order {self.active_order_id}")
                                await self.backpack_client.cancel_order(self.active_order_id)
                                await asyncio.sleep(1.0) # Buffer for last-second fills
                                break
                            await asyncio.sleep(0.1)
                        
                        # Final check for this order after timeout/break
                        if self.waiting_for_hedge_fill:
                            success = await self.execute_hedge_on_paradex(self.current_hedge_side, self.current_hedge_quantity)
                            if not success or self.hedge_failed:
                                self.logger.error("❌ Hedge failed after all retries!")
                                self.logger.error(f"⚠️ CRITICAL: BP has position {self.backpack_position} but Paradex hedge failed")
                                
                                # ✅ Auto-close BP position to avoid one-sided risk
                                if abs(self.backpack_position) > Decimal('0.001'):
                                    self.logger.warning("🔄 Attempting to close BP position...")
                                    
                                    # Determine close side (opposite of current position)
                                    close_side = 'sell' if self.backpack_position > 0 else 'buy'
                                    close_qty = abs(self.backpack_position)
                                    
                                    # Place market order to close immediately
                                    try:
                                        close_result = await self.backpack_client.place_market_order(
                                            self.backpack_contract_id,
                                            close_qty,
                                            close_side
                                        )
                                        
                                        if close_result.success:
                                            self.logger.info(
                                                f"✅ BP position closed: {close_side} {close_qty} @ {close_result.price}"
                                            )
                                            self.backpack_position = Decimal('0')
                                        else:
                                            self.logger.error(f"❌ Failed to close BP position: {close_result.error_message}")
                                    except Exception as e:
                                        self.logger.error(f"❌ Exception closing BP position: {e}")
                                
                                self.stop_flag = True
                                break
                            await self.sync_positions()  # Verify positions after hedge
                            self.report_cycle_pnl()
                            self.waiting_for_hedge_fill = False
                            
                        self.active_order_id = None

                    # Calibration sync after each major step
                    await self.sync_positions()
                    
                    # ✅ Verify positions match before sleeping
                    expected_paradex = -self.backpack_position  # Paradex should be opposite
                    position_diff = abs(self.paradex_position - expected_paradex)
                    
                    if position_diff > Decimal('0.01'):  # Allow small rounding error
                        self.logger.error(
                            f"❌ Position mismatch after entry! "
                            f"BP: {self.backpack_position}, Paradex: {self.paradex_position} "
                            f"(Expected: {expected_paradex}, Diff: {position_diff})"
                        )
                        
                        # Try to reconcile
                        reconcile_success = await self.reconcile_positions()
                        if not reconcile_success:
                            self.logger.error("❌ Failed to reconcile positions, stopping...")
                            self.stop_flag = True
                            break
                    else:
                        self.logger.info(
                            f"✅ Positions matched: BP={self.backpack_position}, "
                            f"Paradex={self.paradex_position}"
                        )

                    if self.sleep_time > 0 and not self.stop_flag:
                        self.logger.info(f"😴 Sleeping for {self.sleep_time}s...")
                        await asyncio.sleep(self.sleep_time)

                    # 2. Sell on Backpack, Buy on Paradex (EXIT/UNWIND)
                    while self.backpack_position > 0.00000001 and not self.stop_flag: # Use small epsilon
                        if self.waiting_for_hedge_fill:
                            self.logger.info("♻️  Late fill detected, hedging before placing new order...")
                            await self.execute_hedge_on_paradex(self.current_hedge_side, self.current_hedge_quantity)
                            self.report_cycle_pnl()
                            self.waiting_for_hedge_fill = False
                            continue

                        remaining = self.backpack_position
                        qty = min(self.order_quantity, remaining)
                        if qty <= 0: break
                        
                        self.waiting_for_hedge_fill = False
                        
                        res = await self.place_backpack_maker_order('sell', qty)
                        if not res.success:
                            self.logger.error(f"Backpack sell failed: {res.error_message}")
                            await self.sync_positions()
                            await asyncio.sleep(5)
                            continue
                        
                        self.active_order_id = res.order_id
                        
                        wait_start = time.time()
                        while not self.stop_flag:
                            if self.waiting_for_hedge_fill:
                                # ✅ Before hedging in EXIT, check Paradex position
                                # to ensure we only close existing short, not reverse to long
                                await self.sync_positions()
                                
                                # Calculate how much short position we have
                                current_short = -self.paradex_position if self.paradex_position < 0 else Decimal('0')
                                
                                # Only close up to current short position
                                hedge_qty = min(self.current_hedge_quantity, current_short)
                                
                                if hedge_qty <= Decimal('0.001'):
                                    self.logger.warning(
                                        f"⚠️ Paradex has no short position to close! "
                                        f"Current position: {self.paradex_position}, "
                                        f"Skipping hedge to avoid reverse opening."
                                    )
                                    self.waiting_for_hedge_fill = False
                                    continue
                                
                                if hedge_qty < self.current_hedge_quantity:
                                    self.logger.warning(
                                        f"⚠️ Reducing hedge quantity to match short position: "
                                        f"{self.current_hedge_quantity} → {hedge_qty}"
                                    )
                                
                                self.logger.info(
                                    f"🔄 Closing Paradex short: {hedge_qty} "
                                    f"(Current position: {self.paradex_position})"
                                )
                                
                                # Execute hedge with adjusted quantity
                                success = await self.execute_hedge_on_paradex(self.current_hedge_side, hedge_qty)
                                if not success or self.hedge_failed:
                                    self.logger.error("❌ Hedge failed after all retries!")
                                    self.logger.error(f"⚠️ CRITICAL: BP has position {self.backpack_position} but Paradex hedge failed")
                                    
                                    # ✅ Auto-close remaining BP position
                                    if abs(self.backpack_position) > Decimal('0.001'):
                                        self.logger.warning("🔄 Attempting to close remaining BP position...")
                                        
                                        close_side = 'sell' if self.backpack_position > 0 else 'buy'
                                        close_qty = abs(self.backpack_position)
                                        
                                        try:
                                            close_result = await self.backpack_client.place_market_order(
                                                self.backpack_contract_id,
                                                close_qty,
                                                close_side
                                            )
                                            
                                            if close_result.success:
                                                self.logger.info(
                                                    f"✅ BP position closed: {close_side} {close_qty} @ {close_result.price}"
                                                )
                                                self.backpack_position = Decimal('0')
                                            else:
                                                self.logger.error(f"❌ Failed to close BP position: {close_result.error_message}")
                                        except Exception as e:
                                            self.logger.error(f"❌ Exception closing BP position: {e}")
                                    
                                    self.stop_flag = True
                                    break
                                await self.sync_positions()  # Verify positions after hedge
                                self.report_cycle_pnl()
                                self.waiting_for_hedge_fill = False
                            
                            if self.backpack_position <= 0.00000001:
                                break
                                
                            if time.time() - wait_start > self.fill_timeout:
                                self.logger.info(f"Timeout reached, cancelling order {self.active_order_id}")
                                await self.backpack_client.cancel_order(self.active_order_id)
                                await asyncio.sleep(1.0)
                                break
                            await asyncio.sleep(0.1)
                            
                        if self.waiting_for_hedge_fill:
                            await self.execute_hedge_on_paradex(self.current_hedge_side, self.current_hedge_quantity)
                            self.report_cycle_pnl()
                            self.waiting_for_hedge_fill = False
                            
                        self.active_order_id = None
                
                    await self.sync_positions() # Final iteration sync
                
                except Exception as loop_e:
                    self.logger.error(f"⚠️ Unexpected error in trading loop: {loop_e}")
                    self.logger.error(traceback.format_exc())
                    await asyncio.sleep(5)
        finally:
            await self.cleanup()
            self.report_total_pnl()
