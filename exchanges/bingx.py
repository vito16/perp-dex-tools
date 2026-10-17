"""
BingX exchange client implementation.
"""

import asyncio
import os
from decimal import Decimal, ROUND_HALF_UP, InvalidOperation
from typing import Any, Dict, List, Optional, Tuple

import ccxt.async_support as ccxt  # type: ignore
from ccxt.base.errors import ExchangeError, NetworkError  # type: ignore

from .base import BaseExchangeClient, OrderInfo, OrderResult, query_retry
from helpers.logger import TradingLogger


def _to_decimal(value: Any) -> Decimal:
    if value is None:
        return Decimal('0')
    return Decimal(str(value))


class BingxClient(BaseExchangeClient):
    """BingX exchange client implementation based on ccxt async support."""

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)

        self.api_key = os.getenv('BINGX_API_KEY')
        self.api_secret = os.getenv('BINGX_API_SECRET')
        self.environment = os.getenv('BINGX_ENVIRONMENT', 'prod').lower()

        # Initialize logger
        self.logger = TradingLogger(exchange="bingx", ticker=self.config.ticker, log_to_console=False)

        # Initialize ccxt client
        self.exchange = ccxt.bingx({
            'apiKey': self.api_key,
            'secret': self.api_secret,
            'options': {
                'defaultType': 'swap',
            },
        })

        if self.environment in ('test', 'testnet', 'sandbox'):
            try:
                self.exchange.set_sandbox_mode(True)
            except Exception as exc:
                self.logger.log(f"Failed to enable sandbox mode: {exc}", "WARNING")

        self._order_update_handler = None
        self._polling_task: Optional[asyncio.Task] = None
        self._stop_polling = asyncio.Event()
        self._tracked_orders: Dict[str, Dict[str, Any]] = {}
        self._poll_interval = float(os.getenv('BINGX_ORDER_POLL_INTERVAL', '1.0'))
        self._market: Optional[Dict[str, Any]] = None

    # --------------------------------------------------------------------- #
    # Base method implementations
    # --------------------------------------------------------------------- #

    def _validate_config(self) -> None:
        missing = [env for env in ('BINGX_API_KEY', 'BINGX_API_SECRET') if not os.getenv(env)]
        if missing:
            raise ValueError(f"Missing required environment variables: {missing}")

    async def connect(self) -> None:
        """Initialize market data and start order polling task."""
        try:
            await self.exchange.load_markets()
            if not getattr(self.config, 'contract_id', None):
                await self.get_contract_attributes()

            if getattr(self.config, 'contract_id', None):
                self._market = self.exchange.market(self.config.contract_id)

            self._stop_polling.clear()
            if self._polling_task is None or self._polling_task.done():
                self._polling_task = asyncio.create_task(self._poll_order_updates())
        except Exception as exc:
            self.logger.log(f"Error connecting to BingX: {exc}", "ERROR")
            raise

    async def disconnect(self) -> None:
        """Stop polling task and close ccxt client."""
        try:
            self._stop_polling.set()
            if self._polling_task:
                await self._polling_task
        finally:
            try:
                await self.exchange.close()
            except Exception as exc:
                self.logger.log(f"Error closing BingX client: {exc}", "WARNING")

    def get_exchange_name(self) -> str:
        return "bingx"

    def setup_order_update_handler(self, handler) -> None:
        self._order_update_handler = handler

    # --------------------------------------------------------------------- #
    # Order placement helpers
    # --------------------------------------------------------------------- #

    def _get_tick_size(self) -> Decimal:
        if self._market:
            tick = self._market.get('limits', {}).get('price', {}).get('min')
            if tick:
                return _to_decimal(tick)
            tick = self._market.get('info', {}).get('tickSize')
            if tick:
                return _to_decimal(tick)
            precision = self._market.get('precision', {}).get('price')
            if precision is not None:
                return Decimal('1') / (Decimal(10) ** Decimal(str(precision)))
        return getattr(self.config, 'tick_size', Decimal('0.01'))

    def _quantize_amount(self, amount: Decimal) -> str:
        if not self._market:
            return str(amount)
        precision = self._market.get('precision', {}).get('amount')
        if precision is None:
            return str(amount)
        step = Decimal('1') / (Decimal(10) ** Decimal(str(precision)))
        return str(amount.quantize(step, rounding=ROUND_HALF_UP))

    def _build_tp_sl_payload(
        self,
        order_kind: str,
        quantity: Decimal,
        price: Decimal,
        order_type: str
    ) -> Dict[str, Any]:
        """
        Build BingX-attached TP/SL payloads for ccxt create_order params.
        """
        normalized_kind = order_kind.strip().lower()
        normalized_type = (order_type or 'market').strip().lower()

        if normalized_kind not in {'take_profit', 'stop_loss'}:
            raise ValueError(f"Unsupported order_kind '{order_kind}' for TP/SL payload.")
        if normalized_type not in {'limit', 'market'}:
            raise ValueError(f"Unsupported TP/SL order_type '{order_type}'.")

        rounded_price = self.round_to_tick(price)
        price_str = str(rounded_price)
        quantity_str = self._quantize_amount(quantity)

        type_mapping = {
            ('take_profit', 'limit'): 'TAKE_PROFIT',
            ('take_profit', 'market'): 'TAKE_PROFIT_MARKET',
            ('stop_loss', 'limit'): 'STOP',
            ('stop_loss', 'market'): 'STOP_MARKET',
        }
        payload_type = type_mapping[(normalized_kind, normalized_type)]

        payload: Dict[str, Any] = {
            'stopPrice': price_str,
            'type': payload_type,
            'quantity': quantity_str,
            'workingType': 'MARK_PRICE'
        }

        if normalized_type == 'limit':
            payload['price'] = price_str

        return payload

    def _normalize_status(self, status: Optional[str]) -> str:
        if not status:
            return "UNKNOWN"
        status = status.upper()
        mapping = {
            'OPEN': 'OPEN',
            'NEW': 'OPEN',
            'PARTIALLY_FILLED': 'PARTIALLY_FILLED',
            'CANCELED': 'CANCELED',
            'CANCELLED': 'CANCELED',
            'FILLED': 'FILLED',
            'CLOSED': 'FILLED',
            'REJECTED': 'REJECTED'
        }
        return mapping.get(status, status)

    async def _create_limit_order(
        self,
        contract_id: str,
        quantity: Decimal,
        side: str,
        price: Decimal,
        *,
        reduce_only: bool = False,
        post_only: Optional[bool] = True,
        time_in_force: Optional[str] = None,
        extra_params: Optional[Dict[str, Any]] = None
    ) -> OrderResult:
        params: Dict[str, Any] = {'reduceOnly': reduce_only}

        if post_only is not None:
            params['postOnly'] = post_only
            if post_only and not time_in_force:
                time_in_force = 'PO'
        if time_in_force:
            params['timeInForce'] = time_in_force

        if extra_params:
            params.update({k: v for k, v in extra_params.items() if v is not None})

        order_price = self.round_to_tick(price)
        amount_str = self._quantize_amount(quantity)
        price_str = str(order_price)

        try:
            order = await self.exchange.create_order(
                contract_id,
                'limit',
                side,
                amount_str,
                price_str,
                params
            )
        except (ExchangeError, NetworkError) as exc:
            self.logger.log(f"[{side.upper()}] Error creating limit order: {exc}", "ERROR")
            return OrderResult(success=False, error_message=str(exc))

        order_id = order.get('id')
        status = self._normalize_status(order.get('status', 'OPEN'))
        filled_amount = _to_decimal(order.get('filled', 0))

        if not order_id:
            return OrderResult(success=False, error_message="No order id returned from BingX")

        self._tracked_orders[order_id] = {
            'status': status,
            'symbol': contract_id,
            'side': side,
            'last_filled': filled_amount
        }

        return OrderResult(
            success=status not in {'REJECTED', 'CANCELED'},
            order_id=order_id,
            side=side,
            size=quantity,
            price=order_price,
            status=status,
            filled_size=filled_amount
        )

    @query_retry(default_return=(Decimal('0'), Decimal('0')))
    async def fetch_bbo_prices(self, contract_id: str) -> Tuple[Decimal, Decimal]:
        order_book = await self.exchange.fetch_order_book(contract_id, limit=5)
        bids = order_book.get('bids', [])
        asks = order_book.get('asks', [])

        best_bid = Decimal(str(bids[0][0])) if bids else Decimal('0')
        best_ask = Decimal(str(asks[0][0])) if asks else Decimal('0')
        return best_bid, best_ask

    async def get_order_price(self, direction: str) -> Decimal:
        best_bid, best_ask = await self.fetch_bbo_prices(self.config.contract_id)
        if best_bid <= 0 or best_ask <= 0:
            raise ValueError("Invalid bid/ask prices")

        tick = self._get_tick_size()
        if direction == 'buy':
            return self.round_to_tick(best_ask - tick)
        elif direction == 'sell':
            return self.round_to_tick(best_bid + tick)
        else:
            raise ValueError(f"Unsupported direction: {direction}")

    async def place_open_order(self, contract_id: str, quantity: Decimal, direction: str) -> OrderResult:
        best_bid, best_ask = await self.fetch_bbo_prices(contract_id)

        if best_bid <= 0 or best_ask <= 0:
            return OrderResult(success=False, error_message='Invalid bid/ask prices')

        tick = self._get_tick_size()

        if direction == 'buy':
            order_price = best_ask - tick
        elif direction == 'sell':
            order_price = best_bid + tick
        else:
            raise ValueError(f"Invalid direction: {direction}")

        return await self._create_limit_order(
            contract_id=contract_id,
            quantity=quantity,
            side=direction,
            price=order_price,
            reduce_only=False,
            post_only=True,
            time_in_force='PO'
        )

    async def place_close_order(self, contract_id: str, quantity: Decimal, price: Decimal, side: str) -> OrderResult:
        best_bid, best_ask = await self.fetch_bbo_prices(contract_id)
        if best_bid <= 0 or best_ask <= 0:
            return OrderResult(success=False, error_message='Invalid bid/ask prices')

        tick = self._get_tick_size()
        adjusted_price = price

        if side.lower() == 'sell' and price <= best_bid:
            adjusted_price = best_bid + tick
        elif side.lower() == 'buy' and price >= best_ask:
            adjusted_price = best_ask - tick

        return await self._create_limit_order(
            contract_id=contract_id,
            quantity=quantity,
            side=side.lower(),
            price=adjusted_price,
            reduce_only=True,
            post_only=True,
            time_in_force='PO'
        )

    async def place_limit_order(
        self,
        contract_id: str,
        quantity: Decimal,
        side: str,
        price: Decimal,
        *,
        reduce_only: bool = False,
        post_only: bool = False,
        time_in_force: Optional[str] = None,
        take_profit_price: Optional[Decimal] = None,
        stop_loss_price: Optional[Decimal] = None,
        tp_sl_order_type: str = 'market'
    ) -> OrderResult:
        extra_params: Dict[str, Any] = {}
        normalized_tp_sl = (tp_sl_order_type or '').strip().lower()
        tp_sl_mode = 'limit' if normalized_tp_sl == 'limit' else 'market'

        if take_profit_price is not None and tp_sl_mode == 'limit':
            extra_params['takeProfit'] = self._build_tp_sl_payload(
                'take_profit',
                quantity,
                take_profit_price,
                'limit'
            )
        elif take_profit_price is not None:
            extra_params['takeProfit'] = self._build_tp_sl_payload(
                'take_profit',
                quantity,
                take_profit_price,
                'market'
            )
        if stop_loss_price is not None and tp_sl_mode == 'limit':
            extra_params['stopLoss'] = self._build_tp_sl_payload(
                'stop_loss',
                quantity,
                stop_loss_price,
                'limit'
            )
        elif stop_loss_price is not None:
            extra_params['stopLoss'] = self._build_tp_sl_payload(
                'stop_loss',
                quantity,
                stop_loss_price,
                'market'
            )

        return await self._create_limit_order(
            contract_id=contract_id,
            quantity=quantity,
            side=side,
            price=price,
            reduce_only=reduce_only,
            post_only=post_only,
            time_in_force=time_in_force,
            extra_params=extra_params
        )

    async def place_market_order(
        self,
        contract_id: str,
        quantity: Decimal,
        side: str,
        *,
        take_profit_price: Optional[Decimal] = None,
        stop_loss_price: Optional[Decimal] = None,
        tp_sl_order_type: str = 'market'
    ) -> OrderResult:
        amount_str = self._quantize_amount(quantity)
        params: Dict[str, Any] = {'reduceOnly': side.lower() == self.config.close_order_side}
        tp_sl_mode = 'limit' if (tp_sl_order_type or '').strip().lower() == 'limit' else 'market'

        if take_profit_price is not None and tp_sl_mode == 'limit':
            params['takeProfit'] = self._build_tp_sl_payload(
                'take_profit',
                quantity,
                take_profit_price,
                'limit'
            )
        elif take_profit_price is not None:
            params['takeProfit'] = self._build_tp_sl_payload(
                'take_profit',
                quantity,
                take_profit_price,
                'market'
            )
        if stop_loss_price is not None and tp_sl_mode == 'limit':
            params['stopLoss'] = self._build_tp_sl_payload(
                'stop_loss',
                quantity,
                stop_loss_price,
                'limit'
            )
        elif stop_loss_price is not None:
            params['stopLoss'] = self._build_tp_sl_payload(
                'stop_loss',
                quantity,
                stop_loss_price,
                'market'
            )

        try:
            order = await self.exchange.create_order(
                contract_id,
                'market',
                side,
                amount_str,
                None,
                params
            )
        except (ExchangeError, NetworkError) as exc:
            self.logger.log(f"[{side.upper()}] Error creating market order: {exc}", "ERROR")
            return OrderResult(success=False, error_message=str(exc))

        order_id = order.get('id')
        average_price = _to_decimal(order.get('average', order.get('price', 0)))
        status = self._normalize_status(order.get('status', 'FILLED'))
        filled = _to_decimal(order.get('filled', quantity))

        return OrderResult(
            success=status == 'FILLED',
            order_id=order_id,
            side=side,
            size=filled,
            price=average_price,
            status=status,
            filled_size=filled
        )

    async def cancel_order(self, order_id: str) -> OrderResult:
        try:
            result = await self.exchange.cancel_order(order_id, self.config.contract_id)
            status = self._normalize_status(result.get('status', 'CANCELED'))
            self._tracked_orders.pop(order_id, None)
            return OrderResult(success=True, status=status)
        except (ExchangeError, NetworkError) as exc:
            return OrderResult(success=False, error_message=str(exc))

    @query_retry(default_return=None)
    async def get_order_info(self, order_id: str) -> Optional[OrderInfo]:
        try:
            order = await self.exchange.fetch_order(order_id, self.config.contract_id)
        except (ExchangeError, NetworkError):
            return None

        status = self._normalize_status(order.get('status'))
        side = order.get('side', '').lower()
        amount = _to_decimal(order.get('amount', 0))
        price = _to_decimal(order.get('price', 0))
        filled = _to_decimal(order.get('filled', 0))
        remaining = amount - filled

        return OrderInfo(
            order_id=order.get('id', ''),
            side=side,
            size=amount,
            price=price,
            status=status,
            filled_size=filled,
            remaining_size=max(remaining, Decimal('0'))
        )

    @query_retry(default_return=[])
    async def get_active_orders(self, contract_id: str) -> List[OrderInfo]:
        try:
            orders = await self.exchange.fetch_open_orders(contract_id)
        except (ExchangeError, NetworkError):
            return []

        order_infos: List[OrderInfo] = []
        for order in orders:
            status = self._normalize_status(order.get('status'))
            side = order.get('side', '').lower()
            amount = _to_decimal(order.get('amount', 0))
            price = _to_decimal(order.get('price', 0))
            filled = _to_decimal(order.get('filled', 0))
            remaining = amount - filled

            order_infos.append(OrderInfo(
                order_id=order.get('id', ''),
                side=side,
                size=amount,
                price=price,
                status=status,
                filled_size=filled,
                remaining_size=max(remaining, Decimal('0'))
            ))

        return order_infos

    @query_retry(default_return=Decimal('0'))
    async def get_account_positions(self) -> Decimal:
        try:
            positions = await self.exchange.fetch_positions([self.config.contract_id])
        except (ExchangeError, NetworkError):
            return Decimal('0')

        for position in positions:
            if position.get('symbol') != self.config.contract_id:
                continue

            for key in ('contracts', 'size', 'positionAmt', 'contractSize'):
                value = position.get(key)
                if value not in (None, ''):
                    return abs(_to_decimal(value))
        return Decimal('0')

    @query_retry(default_return=Decimal('0'))
    async def get_signed_position(self) -> Decimal:
        """Return the signed position amount for the configured contract."""
        try:
            positions = await self.exchange.fetch_positions([self.config.contract_id])
        except (ExchangeError, NetworkError):
            return Decimal('0')

        for position in positions:
            if position.get('symbol') != self.config.contract_id:
                continue

            quantity = Decimal('0')
            for key in ('positionAmt', 'contracts', 'size', 'contractSize'):
                value = position.get(key)
                if value in (None, ''):
                    continue
                try:
                    quantity = _to_decimal(value)
                except (InvalidOperation, ValueError, TypeError):
                    quantity = Decimal('0')
                break

            if quantity == 0:
                info = position.get('info') or {}
                raw_amount = info.get('positionAmt')
                if raw_amount not in (None, ''):
                    try:
                        quantity = _to_decimal(raw_amount)
                    except (InvalidOperation, ValueError, TypeError):
                        quantity = Decimal('0')

            side = (position.get('side') or position.get('direction') or '').strip().lower()
            if side in {'short', 'sell'}:
                quantity = -abs(quantity)
            elif side in {'long', 'buy'}:
                quantity = abs(quantity)

            return quantity

        return Decimal('0')

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        await self.exchange.load_markets()
        ticker = self.config.ticker.upper()

        for symbol, market in self.exchange.markets.items():
            if not market.get('swap'):
                continue
            if market.get('base') != ticker or market.get('quote') != 'USDT':
                continue

            self._market = market
            self.config.contract_id = symbol

            tick_size = self._get_tick_size()
            if tick_size <= 0:
                tick_size = Decimal('0.01')

            self.config.tick_size = tick_size

            min_quantity = market.get('limits', {}).get('amount', {}).get('min')
            if min_quantity:
                min_quantity_dec = _to_decimal(min_quantity)
                if self.config.quantity < min_quantity_dec:
                    raise ValueError(
                        f"Order quantity is less than min quantity: {self.config.quantity} < {min_quantity_dec}"
                    )

            return self.config.contract_id, self.config.tick_size

        raise ValueError(f"Contract not found on BingX for ticker: {ticker}")

    # --------------------------------------------------------------------- #
    # Background polling
    # --------------------------------------------------------------------- #

    async def _poll_order_updates(self) -> None:
        """Poll tracked orders and emit updates to the handler."""
        while not self._stop_polling.is_set():
            if not self._tracked_orders or self._order_update_handler is None:
                await asyncio.sleep(self._poll_interval)
                continue

            removable: List[str] = []
            for order_id, meta in list(self._tracked_orders.items()):
                symbol = meta.get('symbol', self.config.contract_id)
                try:
                    order = await self.exchange.fetch_order(order_id, symbol)
                except (ExchangeError, NetworkError) as exc:
                    self.logger.log(f"Error fetching order {order_id}: {exc}", "WARNING")
                    continue

                status = self._normalize_status(order.get('status'))
                side = order.get('side', '').lower()
                amount = _to_decimal(order.get('amount', 0))
                filled = _to_decimal(order.get('filled', 0))
                price = _to_decimal(order.get('average', order.get('price', 0)))

                if status != meta.get('status') or filled != meta.get('last_filled'):
                    order_type = "CLOSE" if side == getattr(self.config, 'close_order_side', '') else "OPEN"

                    try:
                        self._order_update_handler({
                            'order_id': order_id,
                            'side': side,
                            'order_type': order_type,
                            'status': status,
                            'size': str(amount),
                            'price': str(price),
                            'contract_id': symbol,
                            'filled_size': str(filled)
                        })
                    except Exception as exc:
                        self.logger.log(f"Error in order update handler: {exc}", "ERROR")

                    meta['status'] = status
                    meta['last_filled'] = filled

                if status in {'FILLED', 'CANCELED', 'REJECTED'}:
                    removable.append(order_id)

            for order_id in removable:
                self._tracked_orders.pop(order_id, None)

            await asyncio.sleep(self._poll_interval)

