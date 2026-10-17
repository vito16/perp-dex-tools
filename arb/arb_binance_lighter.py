"""
Arbitrage strategy scaffold for Binance (maker) x Lighter (taker/hedge).

This module only defines the interface and high-level control flow notes so
that the detailed implementation can be added and reviewed incrementally.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
import os
from dataclasses import dataclass, field
from decimal import Decimal
import logging
from typing import Optional, Dict, Tuple, Any, Literal, Callable

import websockets
from lighter import SignerClient, ApiClient, Configuration, OrderApi, AccountApi

# Suppress Lighter SDK debug logs
logging.getLogger('lighter').setLevel(logging.WARNING)
# Also suppress root logger DEBUG messages that might be coming from Lighter SDK
root_logger = logging.getLogger()
if root_logger.level == logging.DEBUG:
    root_logger.setLevel(logging.WARNING)


@dataclass
class BinanceLighterConfig:
    """Configuration bundle for the cross-exchange arbitrage workflow."""

    ticker: str
    max_order_size: Decimal
    min_order_size: Decimal
    max_position_size: Decimal
    order_timeout: float  # seconds
    loop_interval: float  # seconds
    binance_contract_id: str = ""
    lighter_contract_id: str = ""
    lighter_slippage_pct: Decimal = Decimal("0.002")
    binance_tick_size: Decimal = Decimal("0.1")
    lighter_price_multiplier: int = 0
    lighter_size_multiplier: int = 0
    open_spread_threshold: Decimal = Decimal("0")
    close_spread_threshold: Decimal = Decimal("0")


class BinanceLighterArb:
    """
    Core orchestrator.

    Responsibilities (implementation pending):
      * Maintain Binance maker leg and Lighter taker hedge.
      * Subscribe to Binance depth or trades (reuse existing client).
      * Run Lighter websocket loop with offset/integrity handling similar to HedgeBot.
      * Emit structured events so the caller can hook logging/alert systems.
    """

    def __init__(
        self,
        config: BinanceLighterConfig,
        *,
        binance_client_factory: Callable[..., Any],
        logger: Optional[Any] = None,
    ) -> None:
        self.config = config
        self.logger = logger

        # Exchange clients are constructed lazily to keep the init side-effect free.
        if self.config.binance_contract_id == "":
            self.config.binance_contract_id = f"{self.config.ticker}USDC"
        self._binance_client = None
        self._lighter_client = None
        self._lighter_base_url = os.getenv("LIGHTER_BASE_URL", "https://mainnet.zklighter.elliot.ai")
        self._lighter_ws_url = os.getenv(
            "LIGHTER_WS_URL", "wss://mainnet.zklighter.elliot.ai/stream"
        )
        self._lighter_account_index = int(os.getenv("LIGHTER_ACCOUNT_INDEX", "0"))
        self._lighter_api_key_index = int(os.getenv("LIGHTER_API_KEY_INDEX", "0"))
        self._lighter_api_client: Optional[ApiClient] = None
        self._lighter_account_api: Optional[AccountApi] = None

        # Factory is stored so tests can inject doubles/mocks for Binance leg.
        self._binance_client_factory = binance_client_factory

        # Lighter order book mirror (populated by websocket callbacks).
        self._lighter_order_book: Dict[str, Dict[Decimal, Decimal]] = {
            "bids": {},
            "asks": {},
        }
        self._lighter_order_book_ready = asyncio.Event()
        self._lighter_order_book_lock = asyncio.Lock()
        self._lighter_ws_task: Optional[asyncio.Task] = None
        self._lighter_best_bid: Optional[Decimal] = None
        self._lighter_best_ask: Optional[Decimal] = None
        self._lighter_offset: int = 0

        # Order lifecycle flags.
        self._binance_order_filled = asyncio.Event()
        self._binance_order_canceled = asyncio.Event()
        self._last_binance_order: Optional[Dict[str, Any]] = None
        self._waiting_for_lighter_fill = asyncio.Event()
        self._lighter_fill_event = asyncio.Event()
        self._last_lighter_fill: Optional[Dict[str, Any]] = None

        self.last_log_time = 0

        # External cancellation hook.
        self._shutdown = asyncio.Event()

    # ------------------------------------------------------------------
    # Client bootstrap
    # ------------------------------------------------------------------

    async def _reset_lighter_state(self) -> None:
        """Clear local Lighter order book markers before reconnecting."""
        async with self._lighter_order_book_lock:
            self._lighter_order_book["bids"].clear()
            self._lighter_order_book["asks"].clear()
            self._lighter_best_bid = None
            self._lighter_best_ask = None
            self._lighter_offset = 0
        self._lighter_order_book_ready.clear()
        self._lighter_fill_event.clear()

    async def _load_lighter_market_metadata(self) -> None:
        """Populate Lighter market identifiers and multipliers."""
        if self.config.lighter_contract_id:
            return

        if self._lighter_api_client is None:
            raise RuntimeError("Lighter API client not initialized")

        order_api = OrderApi(self._lighter_api_client)
        order_books = await order_api.order_books()

        market_info = None
        for market in getattr(order_books, "order_books", []):
            if getattr(market, "symbol", None) == self.config.ticker:
                market_info = market
                break

        if market_info is None:
            raise ValueError(f"Lighter market not found for ticker {self.config.ticker}")

        self.config.lighter_contract_id = str(market_info.market_id)
        self.config.lighter_size_multiplier = pow(10, market_info.supported_size_decimals)
        self.config.lighter_price_multiplier = pow(10, market_info.supported_price_decimals)

    async def initialize_clients(self) -> None:
        """
        Lazily instantiate both exchange clients and ensure credentials are valid.
        """
        if self._binance_client is None:
            candidate = self._binance_client_factory(self.config, leg="binance", logger=self.logger)
            if asyncio.iscoroutine(candidate):
                candidate = await candidate
            self._binance_client = candidate
            init_hook = getattr(candidate, "initialize", None)
            if callable(init_hook):
                maybe = init_hook()
                if asyncio.iscoroutine(maybe):
                    await maybe

        if self._lighter_client is None:
            private_key = os.getenv("API_KEY_PRIVATE_KEY")
            if not private_key:
                raise ValueError("API_KEY_PRIVATE_KEY environment variable not set")

            client = SignerClient(
                url=self._lighter_base_url,
                private_key=private_key,
                account_index=self._lighter_account_index,
                api_key_index=self._lighter_api_key_index,
            )
            err = client.check_client()
            if err is not None:
                raise RuntimeError(f"Lighter SignerClient check failed: {err}")

            self._lighter_client = client

        if self._lighter_api_client is None:
            self._lighter_api_client = ApiClient(configuration=Configuration(host=self._lighter_base_url))
            self._lighter_account_api = AccountApi(self._lighter_api_client)

        await self._load_lighter_market_metadata()

    async def connect(self) -> None:
        """
        Prepare websocket subscriptions and start background tasks.

        This should:
          - Call `initialize_clients()` if not already done.
          - Register Binance order update callbacks if they are not created elsewhere.
          - Start the Lighter websocket consumer task (see `_run_lighter_ws()`).
        """
        await self.initialize_clients()

        if hasattr(self._binance_client, "connect"):
            maybe = self._binance_client.connect()
            if asyncio.iscoroutine(maybe):
                await maybe
        if hasattr(self._binance_client, "setup_order_update_handler"):
            self._binance_client.setup_order_update_handler(self._handle_binance_order_update)

        if self._lighter_ws_task is None or self._lighter_ws_task.done():
            self._lighter_ws_task = asyncio.create_task(self._run_lighter_ws())

    # ------------------------------------------------------------------
    # Lighter websocket handling
    # ------------------------------------------------------------------

    async def _run_lighter_ws(self) -> None:
        """
        Mirror of HedgeBot.handle_lighter_ws with logging hooks.

        Key behaviours to port:
          - Reset local book state on reconnect.
          - Subscribe to `order_book/{market}` and `account_orders/...` with expiring token.
          - Validate offset monotonicity; request snapshot when a gap is detected.
          - Populate `_lighter_best_bid/_lighter_best_ask` for pricing decisions.
          - Dispatch fills to `_handle_lighter_fill`.
        """
        url = self._lighter_ws_url
        while not self._shutdown.is_set():
            try:
                await self._reset_lighter_state()
                async with websockets.connect(url) as ws:
                    await self._subscribe_lighter_channels(ws)
                    await self._consume_lighter_stream(ws)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                if self.logger:
                    self.logger.log(f"Lighter WS error: {exc}", "WARNING")
                await asyncio.sleep(2)

    async def _subscribe_lighter_channels(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Subscribe to order book and account channels."""
        market = self.config.lighter_contract_id
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{market}"}))

        try:
            deadline = int(time.time() + 10 * 60)
            auth_token, err = self._lighter_client.create_auth_token_with_expiry(deadline)
            if err is None:
                account_ch = f"account_orders/{market}/{self._lighter_account_index}"
                await ws.send(json.dumps({"type": "subscribe", "channel": account_ch, "auth": auth_token}))
            elif self.logger:
                self.logger.log(f"Lighter auth token error: {err}", "WARNING")
        except Exception as exc:
            if self.logger:
                self.logger.log(f"Lighter auth subscribe failed: {exc}", "WARNING")

    async def _consume_lighter_stream(self, ws: websockets.WebSocketClientProtocol) -> None:
        """Process messages until shutdown or reconnect is required."""
        timeout_count = 0
        while not self._shutdown.is_set():
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=1)
                data = json.loads(raw)
                timeout_count = 0
                await self._process_lighter_message(ws, data)
            except asyncio.TimeoutError:
                timeout_count += 1
                if timeout_count % 3 == 0 and self.logger:
                    self.logger.log("Lighter WS idle >3s", "DEBUG")
            except websockets.WebSocketException:
                break
            except json.JSONDecodeError as exc:
                if self.logger:
                    self.logger.log(f"Lighter JSON decode error: {exc}", "WARNING")

    async def _process_lighter_message(
        self,
        ws: websockets.WebSocketClientProtocol,
        data: Dict[str, Any],
    ) -> None:
        """Route Lighter websocket messages."""
        msg_type = data.get("type")
        if msg_type == "subscribed/order_book":
            await self._handle_lighter_snapshot(data.get("order_book", {}))
        elif msg_type == "update/order_book":
            await self._handle_lighter_delta(data.get("order_book", {}))
        elif msg_type == "update/account_orders":
            orders = data.get("orders", {}).get(str(self.config.lighter_contract_id), [])
            for order in orders:
                await self._handle_lighter_fill(order)
        elif msg_type == "ping":
            await ws.send(json.dumps({"type": "pong"}))

    async def _handle_lighter_snapshot(self, payload: Dict[str, Any]) -> None:
        """Replace local order book state using the initial snapshot payload."""
        offset = payload.get("offset", 0)
        bids = payload.get("bids", [])
        asks = payload.get("asks", [])

        async with self._lighter_order_book_lock:
            self._lighter_order_book["bids"].clear()
            self._lighter_order_book["asks"].clear()

            self._apply_lighter_levels("bids", bids)
            self._apply_lighter_levels("asks", asks)

            self._lighter_offset = offset
            self._lighter_best_bid = max(self._lighter_order_book["bids"], default=None)
            self._lighter_best_ask = min(self._lighter_order_book["asks"], default=None)

        self._lighter_order_book_ready.set()

    async def _handle_lighter_delta(self, payload: Dict[str, Any]) -> None:
        """Apply incremental updates, ensuring offsets increase strictly."""
        new_offset = payload.get("offset")
        if not new_offset or new_offset <= self._lighter_offset:
            return

        bids = payload.get("bids", [])
        asks = payload.get("asks", [])

        async with self._lighter_order_book_lock:
            self._apply_lighter_levels("bids", bids)
            self._apply_lighter_levels("asks", asks)

            self._lighter_offset = new_offset
            self._lighter_best_bid = max(self._lighter_order_book["bids"], default=None)
            self._lighter_best_ask = min(self._lighter_order_book["asks"], default=None)

    def _handle_binance_order_update(self, order: Dict[str, Any]) -> None:
        """Callback from Binance client websocket thread."""
        status = order.get("status")
        self._last_binance_order = order
        if status == "FILLED":
            if not self._binance_order_filled.is_set():
                self._binance_order_filled.set()
        elif status in {"CANCELED", "EXPIRED"}:
            if not self._binance_order_canceled.is_set():
                self._binance_order_canceled.set()

    async def _await_binance_fill(self, timeout: float) -> bool:
        """Wait until Binance order is filled or canceled."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if self._binance_order_filled.is_set():
                return True
            if self._binance_order_canceled.is_set():
                return False
            await asyncio.sleep(0.05)
        return False

    async def _handle_lighter_fill(self, order: Dict[str, Any]) -> None:
        """
        React to filled Lighter orders.

        Expected actions:
          - Update net position tracking.
          - Record fill price/size into a queue consumed by the trading loop.
          - Set `_lighter_fill_event` to unblock awaiting coroutines.
        """
        try:
            base_amount = Decimal(order.get("filled_base_amount", "0"))
            quote_amount = Decimal(order.get("filled_quote_amount", "0"))
            if base_amount <= 0:
                return
            avg_price = quote_amount / base_amount if quote_amount > 0 else None
            side = "sell" if order.get("is_ask") else "buy"
            self._last_lighter_fill = {
                "side": side,
                "size": base_amount,
                "price": avg_price,
                "raw": order,
            }
            self._lighter_fill_event.set()
            self._waiting_for_lighter_fill.clear()
        except Exception as exc:
            if self.logger:
                self.logger.log(f"Lighter fill handling failed: {exc}", "ERROR")

    async def _get_lighter_best_levels(self) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        """Return best bid/ask using the local mirror."""
        async with self._lighter_order_book_lock:
            bid = self._lighter_best_bid
            ask = self._lighter_best_ask
        return bid, ask

    @staticmethod
    def _to_decimal(value: Any) -> Optional[Decimal]:
        """Convert API numeric value to Decimal safely."""
        if value is None:
            return None
        if isinstance(value, Decimal):
            return value
        try:
            return Decimal(str(value))
        except (ArithmeticError, ValueError, TypeError):
            return None

    def _apply_lighter_levels(self, side: str, levels: Any) -> None:
        """Apply order book levels to local cache."""
        book = self._lighter_order_book[side]
        for level in levels:
            if isinstance(level, (list, tuple)) and len(level) >= 2:
                price_raw, size_raw = level[0], level[1]
            elif isinstance(level, dict):
                price_raw = level.get("price") or level.get("p")
                size_raw = level.get("size") or level.get("s")
            else:
                continue

            price = self._to_decimal(price_raw)
            size = self._to_decimal(size_raw)
            if price is None or size is None:
                continue

            if size > 0:
                book[price] = size
            else:
                book.pop(price, None)

    async def _fetch_lighter_position(self) -> Decimal:
        """Fetch signed position from Lighter account API."""
        if self._lighter_api_client is None:
            return Decimal("0")
        if self._lighter_account_api is None:
            self._lighter_account_api = AccountApi(self._lighter_api_client)

        account_data = await self._lighter_account_api.account(
            by="index", value=str(self._lighter_account_index)
        )
        accounts = getattr(account_data, "accounts", [])
        if not accounts:
            return Decimal("0")

        for position in getattr(accounts[0], "positions", []):
            if str(getattr(position, "market_id", "")) == self.config.lighter_contract_id:
                amount = Decimal(str(getattr(position, "position", "0")))
                sign = getattr(position, "sign", 1)
                return -amount if sign == -1 else amount
        return Decimal("0")

    async def _get_current_positions(self) -> Tuple[Decimal, Decimal]:
        """Return current signed positions on Binance and Lighter."""
        binance_pos = Decimal("0")
        lighter_pos = Decimal("0")

        get_pos = getattr(self._binance_client, "get_account_positions_with_sign", None)
        if callable(get_pos):
            try:
                binance_pos = Decimal(str(await get_pos()))
            except Exception as exc:
                if self.logger:
                    self.logger.log(f"Failed to fetch Binance position: {exc}", "WARNING")

        try:
            lighter_pos = await self._fetch_lighter_position()
        except Exception as exc:
            if self.logger:
                self.logger.log(f"Failed to fetch Lighter position: {exc}", "WARNING")

        return binance_pos, lighter_pos

    # ------------------------------------------------------------------
    # Trading loop scaffolding
    # ------------------------------------------------------------------

    async def _compute_opportunity(self) -> Optional[Tuple[str, Decimal, Decimal]]:
        """
        Placeholder for opportunity detection.

        Should return (direction, size) when conditions are met, otherwise None.
        Direction can align with HedgeBot semantics: "open_long", "close_long", etc.
        """
        if not self._lighter_order_book_ready.is_set():
            return None

        binance_pos, lighter_pos = await self._get_current_positions()
        abs_leg1 = abs(binance_pos)
        abs_leg2 = abs(lighter_pos)
        mismatch = abs(abs_leg1 - abs_leg2)

        mismatch_limit = self.config.max_order_size
        if mismatch_limit > 0 and mismatch >= mismatch_limit:
            if self.logger:
                self.logger.log(
                    f"Position mismatch {mismatch} exceeds max_order_size {mismatch_limit}",
                    "ERROR",
                )
            self._shutdown.set()
            return None

        fetch_depth = getattr(self._binance_client, "fetch_bbo_prices_quanities", None)
        if not callable(fetch_depth):
            return None

        bid, ask, bid_qty, ask_qty = await fetch_depth(self.config.binance_contract_id)
        lighter_bid, lighter_ask = await self._get_lighter_best_levels()
        if lighter_bid is None or lighter_ask is None:
            return None

        max_size = min(self.config.max_order_size, bid_qty, ask_qty)
        if max_size < self.config.min_order_size:
            return None

        lighter_bn_spread = bid - lighter_ask
        bn_lighter_spread = lighter_bid - ask
        
        open_spread = bn_lighter_spread
        close_spread = lighter_bn_spread
        if binance_pos < Decimal("0") and lighter_pos > Decimal("0"):
            open_spread = lighter_bn_spread
            close_spread = bn_lighter_spread

        if self.logger:
            now = time.time()
            if self.last_log_time == 0 or now - self.last_log_time >= 60:
                self.logger.log(
                    f"Spreads bn→lighter={bn_lighter_spread:.4f}, lighter→bn={lighter_bn_spread:.4f} | "
                    f"Positions binance={binance_pos:.4f}, lighter={lighter_pos:.4f} | "
                    f"Mismatch={mismatch:.4f}",
                    "INFO",
                )
                self.last_log_time = now

        closing_position = min(abs_leg1, abs_leg2)
        min_close_threshold = self.config.close_spread_threshold
        if closing_position >= self.config.min_order_size and close_spread >= min_close_threshold:
            close_size = min(max_size, closing_position)
            return "close_long", close_size, close_spread

        # If position exceeds cap, only consider closing opportunities
        if self.config.max_position_size > 0 and max(abs_leg1, abs_leg2) >= self.config.max_position_size:
            return None

        if open_spread >= self.config.open_spread_threshold:
            return "open_long", max_size, open_spread

        return None

    async def _execute_cycle(self, direction: str, size: Decimal, spread: Decimal) -> None:
        """
        Execute one arbitrage cycle.

        Outline:
          1. Send Binance maker order and await confirmation/cancellation.
          2. When Binance fill arrives, derive hedge instructions and call `_place_lighter_order`.
          3. Await `_lighter_fill_event` with timeout and handle fallback.
        """
        binance_side = "buy" if direction == "open_long" else "sell"
        reduce_only = direction.startswith("close")

        place_fn_name = "place_close_order" if reduce_only else "place_open_order"
        place_fn = getattr(self._binance_client, place_fn_name, None)
        if not callable(place_fn):
            raise AttributeError(f"Binance client missing {place_fn_name}")

        self._binance_order_filled.clear()
        self._binance_order_canceled.clear()

        order_result = await place_fn(self.config.binance_contract_id, size, binance_side)
        if not getattr(order_result, "success", False):
            if self.logger:
                self.logger.log("Binance order failed, aborting cycle", "WARNING")
            return

        if not await self._await_binance_fill(self.config.order_timeout):
            if self.logger:
                self.logger.log("Binance order not filled within timeout", "ERROR")
            return

        order_info = self._last_binance_order or {}
        filled_size = getattr(order_result, "size", size)
        raw_filled = order_info.get("filled_size")
        if raw_filled:
            try:
                filled_size = Decimal(str(raw_filled))
            except Exception:
                pass

        filled_price = getattr(order_result, "price", Decimal("0"))
        raw_price = order_info.get("price")
        if raw_price:
            try:
                filled_price = Decimal(str(raw_price))
            except Exception:
                pass

        hedge_side = "sell" if binance_side == "buy" else "buy"
        await self._place_lighter_order(hedge_side, filled_size, filled_price)

        fill = await self._await_lighter_fill()
        if fill is None and self.logger:
            self.logger.log("Lighter hedge timeout", "ERROR")

        # Post-trade logging and mismatch check
        if self.logger:
            self.logger.log(
                f"Executed {direction} size={filled_size} (spread={spread:.4f})", "INFO"
            )

        binance_pos, lighter_pos = await self._get_current_positions()
        mismatch = abs(abs(binance_pos) - abs(lighter_pos))
        mismatch_limit = self.config.max_order_size
        if mismatch_limit > 0 and mismatch >= mismatch_limit:
            if self.logger:
                self.logger.log(
                    f"Post-trade mismatch {mismatch} exceeds max_order_size {mismatch_limit}",
                    "ERROR",
                )
            self._shutdown.set()

    async def _place_lighter_order(
        self,
        side: Literal["buy", "sell"],
        size: Decimal,
        reference_price: Decimal,
        *,
        reduce_only: bool = False,
    ) -> None:
        """
        Submit a Lighter order at a slippage-adjusted price.

        Final implementation should:
          - Read `_lighter_best_bid/_lighter_best_ask`.
          - Apply `lighter_slippage_pct`.
          - Sign and send the transaction via the client factory output.
          - Start a monitor coroutine similar to `monitor_lighter_order()` in HedgeBot.
        """
        bid, ask = await self._get_lighter_best_levels()
        if side == "buy" and ask is None:
            raise RuntimeError("Missing Lighter ask for buy hedge")
        if side == "sell" and bid is None:
            raise RuntimeError("Missing Lighter bid for sell hedge")

        base_price = ask if side == "buy" else bid
        slip_adjust = base_price * self.config.lighter_slippage_pct
        if side == "buy":
            price = base_price + slip_adjust
        else:
            price = base_price - slip_adjust
        price = price.quantize(Decimal("0.0001"))

        size_multiplier = self.config.lighter_size_multiplier or 1
        price_multiplier = self.config.lighter_price_multiplier or 1
        client_order_index = int(time.time() * 1000)

        tx_info, error = self._lighter_client.sign_create_order(
            market_index=self.config.lighter_contract_id,
            client_order_index=client_order_index,
            base_amount=int(size * size_multiplier),
            price=int(price * price_multiplier),
            is_ask=(side == "sell"),
            order_type=self._lighter_client.ORDER_TYPE_LIMIT,
            time_in_force=self._lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
            reduce_only=reduce_only,
            trigger_price=0,
        )
        if error is not None:
            raise RuntimeError(f"Lighter sign_create_order failed: {error}")

        await self._lighter_client.send_tx(
            tx_type=self._lighter_client.TX_TYPE_CREATE_ORDER,
            tx_info=tx_info,
        )
        self._waiting_for_lighter_fill.set()

    async def _await_lighter_fill(self) -> Optional[Dict[str, Any]]:
        """Wait for Lighter fill event with timeout."""
        self._lighter_fill_event.clear()
        try:
            await asyncio.wait_for(
                self._lighter_fill_event.wait(),
                timeout=self.config.order_timeout,
            )
            fill = self._last_lighter_fill
            self._lighter_fill_event.clear()
            return fill
        except asyncio.TimeoutError:
            return None

    async def run(self) -> None:
        """
        Public entry point for the arbitrage loop.

        Implementation sketch:
          - Ensure connections are ready.
          - Loop until `_shutdown` is set:
              * Pull opportunity via `_compute_opportunity`.
              * Call `_execute_cycle` when actionable.
              * Sleep for `config.loop_interval` between iterations.
          - Await graceful shutdown of background tasks in `finally`.
        """
        await self.connect()
        try:
            while not self._shutdown.is_set():
                opportunity = await self._compute_opportunity()
                if opportunity:
                    direction, size, spread = opportunity
                    await self._execute_cycle(direction, size, spread)
                    if self._shutdown.is_set():
                        break
                await asyncio.sleep(self.config.loop_interval)
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """
        Trigger graceful termination.

        Must cancel outstanding tasks, close websocket connections, and leave
        clients in a clean state so the process can exit without resource leaks.
        """
        if self._shutdown.is_set():
            return
        self._shutdown.set()

        if self._lighter_ws_task:
            self._lighter_ws_task.cancel()
            with contextlib.suppress(Exception):
                await self._lighter_ws_task

        if hasattr(self._binance_client, "disconnect"):
            with contextlib.suppress(Exception):
                maybe = self._binance_client.disconnect()
                if asyncio.iscoroutine(maybe):
                    await maybe

        if hasattr(self._lighter_client, "close"):
            with contextlib.suppress(Exception):
                maybe = self._lighter_client.close()
                if asyncio.iscoroutine(maybe):
                    await maybe
        if self._lighter_api_client:
            with contextlib.suppress(Exception):
                await self._lighter_api_client.close()
