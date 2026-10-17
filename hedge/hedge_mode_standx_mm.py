import asyncio
import json
import signal
import logging
import os
import sys
import time
import argparse
import traceback
import csv
from decimal import Decimal
from typing import Tuple, Optional, Set

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.standx import StandXClient
from datetime import datetime
import pytz
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Simple config class to wrap dictionary for StandX client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class MMBot:
    """Market making bot that places dual-sided quotes on StandX."""

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 5,
                 iterations: int = 20, offset_bps: int = 9, cooldown_seconds: int = 1800):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.iterations = iterations
        self.offset_bps = Decimal(str(offset_bps)) / Decimal('10000')
        self.cooldown_seconds = cooldown_seconds
        self.last_hedge_time: float = 0

        # Initialize logging
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/standx_{ticker}_mm_log.txt"
        self.csv_filename = f"logs/standx_{ticker}_mm_trades.csv"

        self._initialize_csv_file()

        # Setup logger
        self.logger = logging.getLogger(f"mm_bot_standx_{ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()

        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        self.logger.propagate = False

        # State
        self.stop_flag = False
        self.standx_client = None
        self.standx_contract_id = None
        self.standx_tick_size = None

        # Position tracking
        self.initial_position = Decimal('0')

        # Fill time tracking: {hour_str: [fill_times_in_seconds]}
        self.fill_times_by_hour: dict = {}
        self.current_order_start_time: float = 0

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")

        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)

    def _initialize_csv_file(self):
        """Initialize CSV file with headers."""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['timestamp', 'type', 'side', 'price', 'quantity', 'pnl'])

    def log_trade_to_csv(self, trade_type: str, side: str, price: str, quantity: str, pnl: str = ''):
        """Log trade details to CSV file."""
        timestamp = datetime.now(pytz.UTC).isoformat()
        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([timestamp, trade_type, side, price, quantity, pnl])

    def record_fill_time(self, fill_time_seconds: float):
        """Record a fill time and group by hour."""
        hour_str = datetime.now(pytz.UTC).strftime('%Y-%m-%d %H:00')
        if hour_str not in self.fill_times_by_hour:
            self.fill_times_by_hour[hour_str] = []
        self.fill_times_by_hour[hour_str].append(fill_time_seconds)
        self.logger.info(f"⏱️ Fill time: {fill_time_seconds:.2f}s")

    def log_fill_time_stats(self):
        """Log fill time statistics by hour."""
        if not self.fill_times_by_hour:
            return

        self.logger.info("📊 Fill Time Statistics (by hour):")
        for hour, times in sorted(self.fill_times_by_hour.items()):
            avg_time = sum(times) / len(times)
            min_time = min(times)
            max_time = max(times)
            self.logger.info(f"  {hour}: avg={avg_time:.2f}s, min={min_time:.2f}s, max={max_time:.2f}s, count={len(times)}")

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def initialize_standx_client(self):
        """Initialize the StandX client."""
        config_dict = {
            'ticker': self.ticker,
            'contract_id': '',
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),
            'close_order_side': 'sell'
        }
        config = Config(config_dict)
        self.standx_client = StandXClient(config)
        self.logger.info("✅ StandX client initialized")
        return self.standx_client

    async def get_standx_contract_info(self) -> Tuple[str, Decimal]:
        """Get StandX contract ID and tick size."""
        contract_id, tick_size = await self.standx_client.get_contract_attributes()
        return contract_id, tick_size

    async def get_position(self) -> Decimal:
        """Get current position from StandX."""
        return await self.standx_client.get_account_positions()

    async def fetch_mark_price(self) -> Decimal:
        """Fetch mark price from StandX."""
        return await self.standx_client.fetch_mark_price(self.standx_contract_id)

    async def fetch_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        """Fetch BBO prices from StandX."""
        return await self.standx_client.fetch_bbo_prices(self.standx_contract_id)

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to tick size."""
        if self.standx_tick_size is None:
            return price
        return (price / self.standx_tick_size).quantize(Decimal('1')) * self.standx_tick_size

    async def place_dual_quotes(self) -> Tuple[Optional[str], Optional[str], Decimal, Decimal]:
        """
        Place dual-sided quotes (bid and ask).
        Returns: (bid_order_id, ask_order_id, bid_price, ask_price)
        """
        mark_price = await self.fetch_mark_price()
        best_bid, best_ask = await self.fetch_bbo_prices()

        bid_price = self.round_to_tick(mark_price * (Decimal('1') - self.offset_bps))
        ask_price = self.round_to_tick(mark_price * (Decimal('1') + self.offset_bps))

        self.logger.info(f"📊 Mark: {mark_price}, BBO: {best_bid}/{best_ask}")
        self.logger.info(f"📝 Quoting: Bid {bid_price} | Ask {ask_price}")

        # Get current open orders
        open_orders_before = await self.standx_client.get_active_orders(self.standx_contract_id)
        order_ids_before = {o.order_id for o in open_orders_before}

        # Place bid order
        bid_payload = {
            "symbol": self.standx_contract_id,
            "side": "buy",
            "order_type": "limit",
            "qty": str(self.order_quantity),
            "price": str(bid_price),
            "time_in_force": "alo",
            "reduce_only": False
        }
        bid_result = await self.standx_client._make_request("POST", "/api/new_order",
                                                             data=bid_payload, signed=True)

        # Place ask order
        ask_payload = {
            "symbol": self.standx_contract_id,
            "side": "sell",
            "order_type": "limit",
            "qty": str(self.order_quantity),
            "price": str(ask_price),
            "time_in_force": "alo",
            "reduce_only": False
        }
        ask_result = await self.standx_client._make_request("POST", "/api/new_order",
                                                             data=ask_payload, signed=True)

        if bid_result.get('code', -1) != 0:
            self.logger.warning(f"Bid order rejected: {bid_result.get('message')}")
        if ask_result.get('code', -1) != 0:
            self.logger.warning(f"Ask order rejected: {ask_result.get('message')}")

        # Wait and find order IDs
        await asyncio.sleep(1)
        open_orders_after = await self.standx_client.get_active_orders(self.standx_contract_id)

        bid_order_id = None
        ask_order_id = None

        for order in open_orders_after:
            if order.order_id not in order_ids_before:
                if order.side == 'buy':
                    bid_order_id = order.order_id
                elif order.side == 'sell':
                    ask_order_id = order.order_id

        self.logger.info(f"📋 Orders placed - Bid: {bid_order_id}, Ask: {ask_order_id}")

        # Record order start time for fill time tracking
        self.current_order_start_time = time.time()

        return bid_order_id, ask_order_id, bid_price, ask_price

    async def cancel_order_safe(self, order_id: str):
        """Cancel an order safely, ignoring errors."""
        if order_id:
            result = await self.standx_client.cancel_order(order_id)
            if not result.success:
                self.logger.warning(f"Cancel order {order_id} failed: {result.error_message}")

    async def cancel_all_orders(self, bid_order_id: Optional[str] = None, ask_order_id: Optional[str] = None):
        """Cancel ALL open orders for the contract."""
        open_orders = await self.standx_client.get_active_orders(self.standx_contract_id)
        if open_orders:
            self.logger.info(f"🧹 Cancelling {len(open_orders)} open orders...")
            for order in open_orders:
                await self.cancel_order_safe(order.order_id)
        await asyncio.sleep(0.5)

    async def close_position_market(self, position: Decimal, max_retries: int = 3) -> Tuple[bool, Decimal]:
        """
        Close position with market order.
        Returns (success, fill_price_estimate).
        Retries up to max_retries times if position is not fully closed.
        """
        if position == Decimal('0'):
            return True, Decimal('0')

        original_position = position
        fill_price = Decimal('0')

        for attempt in range(max_retries):
            # Determine close direction based on current position
            if position > 0:
                close_side = 'sell'
                quantity = abs(position)
            else:
                close_side = 'buy'
                quantity = abs(position)

            self.logger.info(f"🔄 Closing position (attempt {attempt + 1}): {close_side} {quantity}")

            result = await self.standx_client.place_market_order(
                self.standx_contract_id, quantity, close_side)

            if not result.success:
                self.logger.error(f"❌ Market close failed: {result.error_message}")
                await asyncio.sleep(0.5)
                continue

            # Estimate fill price from BBO
            best_bid, best_ask = await self.fetch_bbo_prices()
            fill_price = best_ask if close_side == 'buy' else best_bid
            self.logger.info(f"✅ Market order placed, estimated price: {fill_price}")

            # Wait for position to update
            await asyncio.sleep(1)

            # Verify position was actually closed
            current_position = await self.get_position()
            remaining = current_position - self.initial_position

            if remaining == Decimal('0'):
                self.logger.info(f"✅ Position fully closed")
                return True, fill_price

            # Position not fully closed, try again with remaining
            self.logger.warning(f"⚠️ Position not fully closed, remaining: {remaining}")
            position = remaining

        self.logger.error(f"❌ Failed to close position after {max_retries} attempts")
        return False, fill_price

    async def check_position_change(self) -> Tuple[Decimal, str]:
        """
        Check if position changed from initial.
        Returns: (position_change, filled_side)
        """
        current_position = await self.get_position()
        position_change = current_position - self.initial_position

        if position_change > Decimal('0'):
            return position_change, 'buy'  # Bid filled, now long
        elif position_change < Decimal('0'):
            return abs(position_change), 'sell'  # Ask filled, now short
        else:
            return Decimal('0'), 'none'

    async def monitor_orders_and_close(self, bid_order_id: Optional[str], ask_order_id: Optional[str],
                                       bid_price: Decimal, ask_price: Decimal) -> Tuple[str, Decimal]:
        """
        Monitor orders until one fills (detected via position change), then close.
        Returns: (result_type, pnl)
        """
        start_time = time.time()

        while not self.stop_flag:
            # Check position change to detect fills
            position_change, filled_side = await self.check_position_change()

            # Debug: log position check every 2 seconds (avoid spam)
            if int(time.time()) % 2 == 0:
                current_pos = await self.get_position()
                self.logger.debug(f"Position check: current={current_pos}, initial={self.initial_position}, change={position_change}")

            if position_change > Decimal('0'):
                # Position changed - a fill happened
                fill_time = time.time() - self.current_order_start_time
                self.record_fill_time(fill_time)

                self.logger.info(f"📈 Position changed: {filled_side} {position_change}")

                # Cancel remaining orders first
                await self.cancel_all_orders(bid_order_id, ask_order_id)

                # Log the maker fill
                if filled_side == 'buy':
                    fill_price = bid_price
                    self.log_trade_to_csv('MAKER', 'buy', str(fill_price), str(position_change))
                else:
                    fill_price = ask_price
                    self.log_trade_to_csv('MAKER', 'sell', str(fill_price), str(position_change))

                self.logger.info(f"✅ Maker fill @ {fill_price}")

                # Get current position (may have changed if both sides filled)
                current_position = await self.get_position()
                net_position = current_position - self.initial_position

                if net_position != Decimal('0'):
                    # Close the position
                    success, close_price = await self.close_position_market(net_position)
                    if success:
                        # Calculate PnL
                        if net_position > 0:
                            # Was long, sold to close
                            pnl = (close_price - bid_price) * abs(net_position)
                        else:
                            # Was short, bought to close
                            pnl = (ask_price - close_price) * abs(net_position)

                        self.logger.info(f"✅ Closed @ {close_price}, PnL: {pnl}")
                        close_side = 'sell' if net_position > 0 else 'buy'
                        self.log_trade_to_csv('TAKER', close_side, str(close_price), str(abs(net_position)), str(pnl))

                        # Wait for position to settle
                        await asyncio.sleep(1)
                        final_position = await self.get_position()
                        self.logger.info(f"📊 Final position: {final_position}")

                        return filled_side, pnl

                    self.logger.error("❌ Failed to close position")
                    return filled_side, Decimal('0')
                else:
                    # Position is back to initial (both sides filled equally)
                    self.logger.info("⚡ Position back to initial (both sides filled)")
                    return 'both', Decimal('0')

            # Check if need to requote due to price movement
            if time.time() - start_time > self.fill_timeout:
                new_mark = await self.fetch_mark_price()
                new_bid = self.round_to_tick(new_mark * (Decimal('1') - self.offset_bps))
                new_ask = self.round_to_tick(new_mark * (Decimal('1') + self.offset_bps))

                if new_bid != bid_price or new_ask != ask_price:
                    self.logger.info(f"📉 Price moved, requoting...")

                    # Cancel orders first
                    await self.cancel_all_orders(bid_order_id, ask_order_id)

                    # Check if any fill happened during cancellation
                    position_change, filled_side = await self.check_position_change()
                    if position_change > Decimal('0'):
                        self.logger.info(f"⚠️ Fill detected during requote: {filled_side} {position_change}")
                        # Handle the fill
                        current_position = await self.get_position()
                        net_position = current_position - self.initial_position
                        if net_position != Decimal('0'):
                            success, close_price = await self.close_position_market(net_position)
                            if success:
                                if net_position > 0:
                                    pnl = (close_price - bid_price) * abs(net_position)
                                else:
                                    pnl = (ask_price - close_price) * abs(net_position)
                                return filled_side, pnl
                        return filled_side, Decimal('0')

                    return 'requote', Decimal('0')

                start_time = time.time()

            await asyncio.sleep(0.5)

        return 'stopped', Decimal('0')

    async def trading_loop(self):
        """Main trading loop."""
        self.logger.info(f"🚀 Starting StandX MM bot for {self.ticker}")
        self.logger.info(f"📊 Quantity: {self.order_quantity}, Offset: {self.offset_bps * 10000} bps")

        # Initialize
        self.initialize_standx_client()
        self.standx_contract_id, self.standx_tick_size = await self.get_standx_contract_info()
        self.logger.info(f"📋 Contract: {self.standx_contract_id}, Tick: {self.standx_tick_size}")

        # Connect
        await self.standx_client.connect()
        self.logger.info("✅ StandX connected")

        await asyncio.sleep(3)

        # Get initial position
        self.initial_position = await self.get_position()
        self.logger.info(f"📊 Initial position: {self.initial_position}")

        total_pnl = Decimal('0')
        iteration = 0

        while iteration < self.iterations and not self.stop_flag:
            iteration += 1
            self.logger.info("=" * 50)
            self.logger.info(f"🔄 Iteration {iteration}/{self.iterations}")
            self.logger.info("=" * 50)

            # Check cooldown period
            if self.last_hedge_time > 0:
                elapsed = time.time() - self.last_hedge_time
                remaining = self.cooldown_seconds - elapsed
                if remaining > 0:
                    self.logger.info(f"⏳ Cooldown active, waiting {remaining:.0f}s...")
                    await asyncio.sleep(min(remaining, 30))  # Sleep in 30s chunks to allow interruption
                    iteration -= 1  # Don't consume iteration during cooldown
                    continue

            # Update initial position for this iteration
            self.initial_position = await self.get_position()
            self.logger.info(f"📊 Current position: {self.initial_position}")

            # Place dual quotes
            bid_id, ask_id, bid_price, ask_price = await self.place_dual_quotes()

            if not bid_id and not ask_id:
                self.logger.warning("⚠️ No orders placed, retrying...")
                await asyncio.sleep(1)
                iteration -= 1
                continue

            # Monitor and close
            result, pnl = await self.monitor_orders_and_close(bid_id, ask_id, bid_price, ask_price)

            if result == 'requote':
                iteration -= 1  # Don't count requotes
                continue

            if result == 'stopped':
                break

            total_pnl += pnl

            # Cooldown period after hedge
            if result in ('buy', 'sell', 'both'):
                self.last_hedge_time = time.time()
                self.logger.info(f"⏳ Hedge triggered, entering {self.cooldown_seconds}s cooldown period...")
            self.logger.info(f"💰 Total PnL: {total_pnl}")

        # Final cleanup - ensure no open orders
        self.logger.info("🧹 Final cleanup...")
        open_orders = await self.standx_client.get_active_orders(self.standx_contract_id)
        for order in open_orders:
            await self.cancel_order_safe(order.order_id)

        # Check final position and close if needed
        final_position = await self.get_position()
        if final_position != Decimal('0'):
            self.logger.info(f"⚠️ Remaining position: {final_position}, closing...")
            await self.close_position_market(final_position)
            await asyncio.sleep(1)
            final_position = await self.get_position()
        self.logger.info(f"📊 Final position: {final_position}")

        # Log fill time statistics
        self.log_fill_time_stats()

        self.logger.info("=" * 50)
        self.logger.info(f"🏁 Trading complete. Final PnL: {total_pnl}")
        self.logger.info("=" * 50)

    async def run(self):
        """Run the MM bot."""
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Interrupted...")
        except Exception as e:
            self.logger.error(f"❌ Error: {e}")
        finally:
            self.logger.info("🔄 Cleaning up...")
            # Cancel all open orders before disconnecting
            if self.standx_client and self.standx_contract_id:
                try:
                    open_orders = await self.standx_client.get_active_orders(self.standx_contract_id)
                    if open_orders:
                        self.logger.info(f"🧹 Cancelling {len(open_orders)} open orders...")
                        for order in open_orders:
                            await self.cancel_order_safe(order.order_id)
                except Exception as e:
                    self.logger.warning(f"Error cancelling orders: {e}")
            if self.standx_client:
                await self.standx_client.disconnect()
            self.shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='StandX Market Making Bot')
    parser.add_argument('--ticker', type=str, required=True, help='Trading pair (e.g. BTC)')
    parser.add_argument('--quantity', type=str, required=True, help='Order quantity')
    parser.add_argument('--fill-timeout', type=int, default=5, help='Fill timeout in seconds')
    parser.add_argument('--iterations', type=int, default=20, help='Number of iterations')
    parser.add_argument('--offset-bps', type=int, default=9, help='Offset in basis points')
    parser.add_argument('--cooldown', type=int, default=1800, help='Cooldown period in seconds after hedge (default: 1800 = 30 minutes)')

    args = parser.parse_args()

    bot = MMBot(
        ticker=args.ticker,
        order_quantity=Decimal(args.quantity),
        fill_timeout=args.fill_timeout,
        iterations=args.iterations,
        offset_bps=args.offset_bps,
        cooldown_seconds=args.cooldown
    )

    asyncio.run(bot.run())
