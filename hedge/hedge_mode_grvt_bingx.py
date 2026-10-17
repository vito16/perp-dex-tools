import asyncio
import os
import signal
import sys
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Dict, List, Optional, Tuple
import logging

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.grvt import GrvtClient
from exchanges.bingx import BingxClient


class Config:
    """Simple attribute-style config wrapper."""

    def __init__(self, config_dict: Dict[str, Any]):
        for key, value in config_dict.items():
            setattr(self, key, value)


class HedgeBot:
    """
    Hedge bot that runs maker orders on GRVT and hedges fills on BingX.
    """

    def __init__(
        self,
        ticker: str,
        order_quantity: Decimal,
        fill_timeout: int = 10,
        iterations: int = 10,
        sleep_time: int = 0,
        tp_roi: Optional[Decimal] = None,
        sl_roi: Optional[Decimal] = None,
        entry_tick_price: Optional[Decimal] = None,
        gap_threshold: Optional[Decimal] = None,
        leverage: Optional[Decimal] = None,
        bingx_order_type: Optional[str] = None,
        bingx_limit_offset_ticks: Optional[Decimal] = None,
        bingx_attach_tp_sl: Optional[bool] = None,
        bingx_time_in_force: Optional[str] = None,
        bingx_simultaneous_limit: Optional[bool] = None,
        strict_mode: Optional[bool] = None,
        grvt_attach_tp_sl: Optional[bool] = None,
        grvt_tpsl_trigger_by: Optional[str] = None,
    ):
        self.ticker = ticker.upper()
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.iterations = iterations
        self.sleep_time = sleep_time
        self.tp_roi: Optional[Decimal] = None
        self.sl_roi: Optional[Decimal] = None
        self.leverage: Decimal = Decimal('1')

        self.stop_flag = False
        self.loop: Optional[asyncio.AbstractEventLoop] = None

        self.grvt_client: Optional[GrvtClient] = None
        self.bingx_client: Optional[BingxClient] = None

        self.grvt_contract_id: Optional[str] = None
        self.grvt_tick_size: Optional[Decimal] = None
        self.bingx_contract_id: Optional[str] = None
        self.bingx_tick_size: Optional[Decimal] = None

        self.grvt_position = Decimal('0')
        self.bingx_position = Decimal('0')
        self.current_entry_price: Optional[Decimal] = None
        self.current_entry_side: Optional[str] = None
        self.current_entry_size: Optional[Decimal] = None
        self.current_entry_timestamp: Optional[float] = None
        self.current_take_profit_price: Optional[Decimal] = None
        self.current_stop_loss_price: Optional[Decimal] = None
        self.roi_poll_interval: float = 1.0
        self.max_roi_wait: float = max(self.fill_timeout * 60, 120)
        self.last_roi_reason: Optional[str] = None
        self.pending_grvt_price: Optional[Tuple[str, Decimal]] = None
        self.parallel_bingx_order: Optional[Dict[str, Any]] = None
        self.grvt_exit_order_ids: Dict[str, Optional[str]] = {'tp': None, 'sl': None}

        config_warnings: List[str] = []

        def _parse_leverage(value: Any) -> Decimal:
            if value is None:
                return Decimal('1')
            try:
                lev = Decimal(str(value))
            except (InvalidOperation, ValueError):
                config_warnings.append(f"leverage='{value}' is invalid; defaulting to 1.")
                return Decimal('1')
            if lev <= 0:
                config_warnings.append(f"leverage='{lev}' is invalid; defaulting to 1.")
                return Decimal('1')
            return lev

        leverage_source = leverage if leverage is not None else os.getenv('GRVT_BINGX_LEVERAGE')
        self.leverage = _parse_leverage(leverage_source)

        def _parse_roi(value: Any, label: str, allow_negative: bool = False) -> Optional[Decimal]:
            if value is None:
                return None
            try:
                roi_value = Decimal(str(value))
            except (InvalidOperation, ValueError):
                config_warnings.append(f"{label}='{value}' is invalid; ignoring.")
                return None
            if roi_value == 0:
                config_warnings.append(f"{label} is zero; ignoring.")
                return None
            if roi_value < 0:
                if allow_negative:
                    roi_value = abs(roi_value)
                else:
                    config_warnings.append(f"{label} must be positive; ignoring.")
                    return None
            return roi_value

        def _normalize_roi(raw_roi: Optional[Decimal]) -> Optional[Decimal]:
            if raw_roi is None:
                return None
            leverage_value = self.leverage if self.leverage and self.leverage > 0 else Decimal('1')
            adjusted = raw_roi / leverage_value
            if adjusted <= 0:
                return None
            return adjusted

        raw_tp_roi = _parse_roi(tp_roi, 'tp_roi', allow_negative=False)
        raw_sl_roi = _parse_roi(sl_roi, 'sl_roi', allow_negative=True)

        self.tp_roi = _normalize_roi(raw_tp_roi)
        self.sl_roi = _normalize_roi(raw_sl_roi)

        def _coerce_decimal(value: Any, default: Decimal, label: str) -> Decimal:
            if value is None:
                return default
            try:
                return Decimal(str(value))
            except (InvalidOperation, ValueError):
                config_warnings.append(f"{label}='{value}' is invalid; falling back to {default}.")
                return default

        def _parse_bool(value: Optional[str], label: str) -> Optional[bool]:
            if value is None:
                return None
            normalized = value.strip().lower()
            if normalized in {'1', 'true', 't', 'yes', 'y', 'on'}:
                return True
            if normalized in {'0', 'false', 'f', 'no', 'n', 'off'}:
                return False
            config_warnings.append(f"{label}='{value}' is invalid; ignoring.")
            return None

        def _coerce_int(value: Any, default: int, label: str) -> int:
            if value is None:
                return default
            try:
                return int(str(value))
            except (ValueError, TypeError):
                config_warnings.append(f"{label}='{value}' is invalid; falling back to {default}.")
                return default

        def _coerce_float(value: Any, default: float, label: str) -> float:
            if value is None:
                return default
            try:
                return float(str(value))
            except (ValueError, TypeError):
                config_warnings.append(f"{label}='{value}' is invalid; falling back to {default}.")
                return default

        order_type_source = bingx_order_type or os.getenv('BINGX_HEDGE_ORDER_TYPE') or 'market'
        order_type_value = order_type_source.strip().lower()
        if order_type_value not in {'market', 'limit'}:
            config_warnings.append(
                f"BINGX hedge order type '{order_type_source}' is not supported; using 'market'."
            )
            order_type_value = 'market'
        self.bingx_hedge_order_type = order_type_value

        if bingx_limit_offset_ticks is not None:
            self.bingx_hedge_limit_offset_ticks = _coerce_decimal(
                bingx_limit_offset_ticks, Decimal('0'), 'bingx_limit_offset_ticks'
            )
        else:
            env_offset = os.getenv('BINGX_HEDGE_LIMIT_OFFSET_TICKS')
            self.bingx_hedge_limit_offset_ticks = _coerce_decimal(
                env_offset, Decimal('0'), 'BINGX_HEDGE_LIMIT_OFFSET_TICKS'
            )

        tif_source = bingx_time_in_force or os.getenv('BINGX_HEDGE_TIME_IN_FORCE')
        if tif_source:
            tif_value = tif_source.strip().upper()
            if tif_value in {'', 'NONE'}:
                tif_value = None
        else:
            tif_value = 'IOC' if self.bingx_hedge_order_type == 'limit' else None
        self.bingx_hedge_time_in_force = tif_value

        if entry_tick_price is not None:
            coerced_tick = _coerce_decimal(entry_tick_price, Decimal('0'), 'entry_tick_price')
            self.entry_tick_price = coerced_tick if coerced_tick > 0 else None
        else:
            env_tick = os.getenv('GRVT_BINGX_ENTRY_TICK_PRICE')
            tick_value = _coerce_decimal(env_tick, Decimal('0'), 'GRVT_BINGX_ENTRY_TICK_PRICE')
            self.entry_tick_price = tick_value if tick_value > 0 else None

        if gap_threshold is not None:
            self.gap_threshold = _coerce_decimal(gap_threshold, Decimal('0'), 'gap_threshold')
        else:
            env_gap = os.getenv('GRVT_BINGX_GAP_THRESHOLD')
            self.gap_threshold = _coerce_decimal(env_gap, Decimal('0'), 'GRVT_BINGX_GAP_THRESHOLD')
        if self.gap_threshold <= 0:
            fallback_tick = self.bingx_tick_size or Decimal('0.01')
            if self.gap_threshold < 0:
                config_warnings.append(
                    f"gap_threshold='{self.gap_threshold}' is invalid; defaulting to {fallback_tick}."
                )
            self.gap_threshold = fallback_tick

        env_sim_limit = _parse_bool(os.getenv('BINGX_SIMULTANEOUS_LIMIT'), 'BINGX_SIMULTANEOUS_LIMIT')
        if bingx_simultaneous_limit is not None:
            self.bingx_simultaneous_limit = bool(bingx_simultaneous_limit)
        elif env_sim_limit is not None:
            self.bingx_simultaneous_limit = env_sim_limit
        else:
            self.bingx_simultaneous_limit = False

        self._auto_tp_sl_enabled = False
        if bingx_attach_tp_sl is not None:
            attach_value = bool(bingx_attach_tp_sl)
        else:
            env_attach = _parse_bool(os.getenv('BINGX_HEDGE_ATTACH_TPSL'), 'BINGX_HEDGE_ATTACH_TPSL')
            if env_attach is None:
                attach_value = (self.tp_roi is not None) or (self.sl_roi is not None)
                self._auto_tp_sl_enabled = attach_value
            else:
                attach_value = env_attach
        self.bingx_attach_tp_sl = attach_value

        env_grvt_attach = _parse_bool(os.getenv('GRVT_ATTACH_TPSL'), 'GRVT_ATTACH_TPSL')
        if grvt_attach_tp_sl is not None:
            self.grvt_attach_tp_sl = bool(grvt_attach_tp_sl)
        elif env_grvt_attach is not None:
            self.grvt_attach_tp_sl = env_grvt_attach
        else:
            self.grvt_attach_tp_sl = (self.tp_roi is not None) or (self.sl_roi is not None)

        trigger_source = grvt_tpsl_trigger_by or os.getenv('GRVT_TPSL_TRIGGER_BY') or 'LAST'
        trigger_value = (trigger_source or 'LAST').strip().upper()
        allowed_triggers = {'UNSPECIFIED', 'INDEX', 'LAST', 'MID', 'MARK'}
        if trigger_value not in allowed_triggers:
            config_warnings.append(
                f"GRVT TPSL trigger '{trigger_source}' is invalid; defaulting to 'LAST'."
            )
            trigger_value = 'LAST'
        self.grvt_tpsl_trigger_by = trigger_value

        default_cycle_retry_delay = float(self.sleep_time) if self.sleep_time > 0 else 3.0
        self.cycle_retry_delay = max(
            0.5,
            _coerce_float(
                os.getenv('GRVT_BINGX_CYCLE_RETRY_DELAY'),
                default_cycle_retry_delay,
                'GRVT_BINGX_CYCLE_RETRY_DELAY'
            )
        )
        self.max_cycle_retries = _coerce_int(
            os.getenv('GRVT_BINGX_MAX_CYCLE_RETRIES'),
            3,
            'GRVT_BINGX_MAX_CYCLE_RETRIES'
        )
        if self.max_cycle_retries < 0:
            config_warnings.append(
                f"GRVT_BINGX_MAX_CYCLE_RETRIES='{self.max_cycle_retries}' is invalid; using 0 (no limit)."
            )
            self.max_cycle_retries = 0

        self.hedge_retry_delay = max(
            0.5,
            _coerce_float(
                os.getenv('GRVT_BINGX_HEDGE_RETRY_DELAY'),
                1.0,
                'GRVT_BINGX_HEDGE_RETRY_DELAY'
            )
        )
        self.max_hedge_retries = _coerce_int(
            os.getenv('GRVT_BINGX_MAX_HEDGE_RETRIES'),
            3,
            'GRVT_BINGX_MAX_HEDGE_RETRIES'
        )
        if self.max_hedge_retries < 0:
            config_warnings.append(
                f"GRVT_BINGX_MAX_HEDGE_RETRIES='{self.max_hedge_retries}' is invalid; using 0 (no limit)."
            )
            self.max_hedge_retries = 0

        self.position_tolerance = _coerce_decimal(
            os.getenv('GRVT_BINGX_POSITION_TOLERANCE'),
            Decimal('0'),
            'GRVT_BINGX_POSITION_TOLERANCE'
        )
        if self.position_tolerance < 0:
            self.logger.warning(
                "GRVT_BINGX_POSITION_TOLERANCE='%s' is negative; using 0.",
                self.position_tolerance
            )
            self.position_tolerance = Decimal('0')

        env_strict_mode = _parse_bool(os.getenv('GRVT_BINGX_STRICT_MODE'), 'GRVT_BINGX_STRICT_MODE')
        if strict_mode is not None:
            self.strict_mode = bool(strict_mode)
        elif env_strict_mode is not None:
            self.strict_mode = env_strict_mode
        else:
            self.strict_mode = True

        default_close_poll = max(0.5, float(self.fill_timeout) if self.fill_timeout > 0 else 2.0)
        self.position_close_poll_interval = max(
            0.5,
            _coerce_float(
                os.getenv('GRVT_BINGX_POSITION_CLOSE_POLL_INTERVAL'),
                default_close_poll,
                'GRVT_BINGX_POSITION_CLOSE_POLL_INTERVAL'
            )
        )
        self.position_close_retry_delay = max(
            self.position_close_poll_interval,
            _coerce_float(
                os.getenv('GRVT_BINGX_POSITION_CLOSE_RETRY_DELAY'),
                max(30.0, default_close_poll * 5),
                'GRVT_BINGX_POSITION_CLOSE_RETRY_DELAY'
            )
        )
        self.position_close_timeout = max(
            0.0,
            _coerce_float(
                os.getenv('GRVT_BINGX_POSITION_CLOSE_TIMEOUT'),
                300.0,
                'GRVT_BINGX_POSITION_CLOSE_TIMEOUT'
            )
        )

        self.grvt_fill_event = asyncio.Event()
        self.last_grvt_fill: Optional[Dict[str, Any]] = None

        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/grvt_bingx_{self.ticker.lower()}_hedge_log.txt"

        self.logger = logging.getLogger(f"hedge_grvt_bingx_{self.ticker}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.logger.handlers.clear()

        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(file_formatter)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter('%(levelname)s: %(message)s')
        console_handler.setFormatter(console_formatter)

        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        for message in config_warnings:
            self.logger.warning(message)

        self.logger.info(
            "BingX hedge config | type=%s | limit_offset_ticks=%s | time_in_force=%s | attach_tp_sl=%s",
            self.bingx_hedge_order_type,
            self.bingx_hedge_limit_offset_ticks,
            self.bingx_hedge_time_in_force or 'DEFAULT',
            self.bingx_attach_tp_sl,
        )
        if self.entry_tick_price is not None:
            self.logger.info("Entry tick-price override enabled: %s", self.entry_tick_price)
        else:
            self.logger.info("Entry tick-price override disabled.")
        self.logger.info(
            "Gap threshold: %s (BingX tick=%s)",
            self.gap_threshold,
            self.bingx_tick_size or 'UNKNOWN'
        )
        self.logger.info(
            "BingX parallel limit entries: %s",
            "ENABLED" if self.bingx_simultaneous_limit else "DISABLED"
        )
        if self.leverage and self.leverage > 1:
            self.logger.info("Effective leverage: %sx (ROI scaled accordingly)", self.leverage)
        if self.bingx_attach_tp_sl:
            attachment_reason = "auto (ROI targets configured)" if self._auto_tp_sl_enabled else "explicit"
            self.logger.info("BingX TP/SL attachments ENABLED (%s).", attachment_reason)
        elif self.tp_roi is not None or self.sl_roi is not None:
            self.logger.info("ROI targets configured but BingX TP/SL attachments are disabled.")
        self.logger.info(
            "GRVT TP/SL attachments: %s (trigger_by=%s)",
            "ENABLED" if self.grvt_attach_tp_sl else "DISABLED",
            self.grvt_tpsl_trigger_by
        )
        self.logger.info("Strict cycle mode: %s", "ENABLED" if self.strict_mode else "DISABLED")
        self.logger.info(
            "Position close guard | poll=%.1fs | retry=%.1fs | timeout=%s",
            self.position_close_poll_interval,
            self.position_close_retry_delay,
            f"{self.position_close_timeout:.1f}s" if self.position_close_timeout > 0 else "DISABLED"
        )

    # ------------------------------------------------------------------ #
    # Initialization helpers
    # ------------------------------------------------------------------ #

    def setup_signal_handlers(self) -> None:
        def handler(signum, frame):
            self.logger.info("Received shutdown signal, stopping hedge bot...")
            self.stop_flag = True

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def _build_grvt_config(self) -> Config:
        return Config({
            'ticker': self.ticker,
            'contract_id': '',
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),
            'direction': 'buy',
            'close_order_side': 'sell'
        })

    def _build_bingx_config(self) -> Config:
        return Config({
            'ticker': self.ticker,
            'contract_id': '',
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),
            'direction': 'buy',
            'close_order_side': 'sell'
        })

    def initialize_clients(self) -> None:
        if self.grvt_client is None:
            self.grvt_client = GrvtClient(self._build_grvt_config())
        if self.bingx_client is None:
            self.bingx_client = BingxClient(self._build_bingx_config())

    async def load_contract_metadata(self) -> None:
        assert self.grvt_client is not None
        assert self.bingx_client is not None

        self.grvt_contract_id, self.grvt_tick_size = await self.grvt_client.get_contract_attributes()
        self.bingx_contract_id, self.bingx_tick_size = await self.bingx_client.get_contract_attributes()

        self.logger.info(f"GRVT contract: {self.grvt_contract_id} (tick {self.grvt_tick_size})")
        self.logger.info(f"BingX contract: {self.bingx_contract_id} (tick {self.bingx_tick_size})")

    # ------------------------------------------------------------------ #
    # Order handling
    # ------------------------------------------------------------------ #

    def _handle_grvt_order_update(self, message: Dict[str, Any]) -> None:
        if self.grvt_contract_id is None:
            return
        if message.get('contract_id') != self.grvt_contract_id:
            return
        if message.get('order_type') != 'OPEN':
            return

        status = message.get('status')
        side = message.get('side', '').lower()
        filled_size = Decimal(str(message.get('filled_size', '0')))
        price = Decimal(str(message.get('price', '0')))
        order_id = message.get('order_id')

        if status == 'FILLED':
            if side == 'buy':
                self.grvt_position += filled_size
            else:
                self.grvt_position -= filled_size

            self.last_grvt_fill = {
                'order_id': order_id,
                'side': side,
                'size': filled_size,
                'price': price
            }

            self.logger.info(
                f"[GRVT] FILLED {side.upper()} {filled_size} @ {price} | Position={self.grvt_position}"
            )
            self.grvt_fill_event.set()

    async def setup_grvt_websocket(self) -> None:
        assert self.grvt_client is not None
        self.grvt_client.setup_order_update_handler(self._handle_grvt_order_update)
        await self.grvt_client.connect()

    async def setup_bingx(self) -> None:
        assert self.bingx_client is not None
        await self.bingx_client.connect()

    async def place_grvt_order(
        self,
        side: str,
        quantity: Optional[Decimal] = None,
        price_override: Optional[Decimal] = None
    ) -> Optional[Dict[str, Any]]:
        assert self.grvt_client is not None
        assert self.grvt_contract_id is not None

        self.grvt_client.config.direction = side
        self.grvt_client.config.close_order_side = 'sell' if side == 'buy' else 'buy'

        self.grvt_fill_event.clear()
        self.last_grvt_fill = None

        if price_override is not None:
            self.logger.info(f"[GRVT] Using override price {price_override} for {side} order")
        else:
            try:
                price_override = await self.grvt_client.get_order_price(side)
                self.logger.info(f"[GRVT] Computed maker price {price_override} for {side} order")
            except Exception as exc:
                self.logger.warning(f"[GRVT] Failed to compute maker price for {side} order: {exc}")
                price_override = None

        order_quantity = quantity if quantity is not None else self.order_quantity
        if order_quantity is None or order_quantity <= 0:
            self.logger.warning(
                "[GRVT] Invalid %s order quantity=%s; skipping order placement.",
                side,
                order_quantity
            )
            return None

        self.grvt_client.config.quantity = order_quantity

        tp_metadata = sl_metadata = None
        if self.grvt_attach_tp_sl and price_override is not None:
            tp_metadata, sl_metadata = self._compute_grvt_tpsl_metadata(side, price_override)
            if tp_metadata:
                self.logger.info(
                    "[GRVT] Attaching TP metadata (trigger=%s price=%s)",
                    tp_metadata['trigger_by'],
                    tp_metadata['trigger_price']
                )
            if sl_metadata:
                self.logger.info(
                    "[GRVT] Attaching SL metadata (trigger=%s price=%s)",
                    sl_metadata['trigger_by'],
                    sl_metadata['trigger_price']
                )

        order_result = await self.grvt_client.place_open_order(
            contract_id=self.grvt_contract_id,
            quantity=order_quantity,
            direction=side,
            price=price_override,
            tp_metadata=tp_metadata,
            sl_metadata=sl_metadata,
            respect_price=price_override is not None
        )

        if not order_result.success or not order_result.order_id:
            self.logger.error(f"[GRVT] Failed to place {side} order: {order_result.error_message}")
            return None

        self.logger.info(
            "[GRVT] Order placed %s (%s) qty=%s @ %s",
            order_result.order_id,
            side,
            order_quantity,
            order_result.price
        )

        if order_result.status == 'FILLED':
            if order_result.size is not None:
                if side == 'buy':
                    self.grvt_position += order_result.size
                else:
                    self.grvt_position -= order_result.size
            self.last_grvt_fill = {
                'order_id': order_result.order_id,
                'side': side,
                'size': order_result.size,
                'price': order_result.price
            }
            self.grvt_fill_event.set()
            return self.last_grvt_fill

        try:
            await asyncio.wait_for(self.grvt_fill_event.wait(), timeout=self.fill_timeout)
        except asyncio.TimeoutError:
            self.logger.warning(f"[GRVT] {side} order {order_result.order_id} timed out, cancelling")
            cancel_result = await self.grvt_client.cancel_order(order_result.order_id)
            if not cancel_result.success:
                self.logger.error(f"[GRVT] Failed to cancel order {order_result.order_id}: {cancel_result.error_message}")
            return None

        return self.last_grvt_fill

    async def place_bingx_hedge(self, fill: Dict[str, Any]) -> bool:
        assert self.bingx_client is not None
        assert self.bingx_contract_id is not None

        side = str(fill['side']).lower()
        try:
            size = Decimal(str(fill['size']))
        except (InvalidOperation, ValueError, TypeError):
            self.logger.error(f"[BINGX] Invalid hedge size from fill: {fill.get('size')}")
            return False
        if size <= 0:
            self.logger.warning("[BINGX] Hedge size is non-positive; skipping hedge.")
            return False

        hedge_side = 'sell' if side == 'buy' else 'buy'
        self.bingx_client.config.direction = hedge_side
        self.bingx_client.config.close_order_side = 'buy' if hedge_side == 'sell' else 'sell'

        if self.bingx_simultaneous_limit:
            size, used_parallel = await self._consume_parallel_bingx_order(hedge_side, size)
            if size <= 0:
                if used_parallel:
                    self.logger.info("[BINGX] Parallel limit order fully hedged GRVT %s fill.", side.upper())
                return True

        entry_price: Optional[Decimal]
        try:
            entry_price = Decimal(str(fill.get('price')))
        except (InvalidOperation, ValueError, TypeError):
            entry_price = None

        total_executed = Decimal('0')
        executed_orders: List[Tuple[str, Any]] = []

        if self.bingx_hedge_order_type == 'limit':
            limit_result = await self._place_bingx_limit_hedge(size, hedge_side, entry_price)
            if limit_result is not None:
                executed_orders.append(('limit', limit_result))
                filled_limit = self._extract_filled_size(limit_result)
                if filled_limit is None:
                    filled_limit = Decimal('0')
                if filled_limit > 0:
                    total_executed += filled_limit
                if filled_limit < size:
                    remaining = size - filled_limit
                    if remaining > 0:
                        self.logger.warning(
                            "[BINGX] Limit hedge filled %s of %s; executing market hedge for remaining %s.",
                            filled_limit,
                            size,
                            remaining
                        )
                        market_remaining = await self._place_bingx_market_hedge(
                            remaining,
                            hedge_side,
                            entry_price
                        )
                        if market_remaining is not None:
                            executed_orders.append(('market', market_remaining))
                            filled_market_remaining = self._extract_filled_size(market_remaining)
                            if filled_market_remaining is None or filled_market_remaining <= 0:
                                filled_market_remaining = remaining
                            total_executed += filled_market_remaining
            else:
                self.logger.info("[BINGX] Limit hedge unavailable; falling back to market order.")

        if self.bingx_hedge_order_type != 'limit' and total_executed == 0:
            market_result = await self._place_bingx_market_hedge(size, hedge_side, entry_price)
            if market_result is not None:
                executed_orders.append(('market', market_result))
                filled_market = self._extract_filled_size(market_result)
                if filled_market is None or filled_market <= 0:
                    filled_market = size
                total_executed += filled_market
        elif total_executed == 0:
            # Limit mode but nothing executed yet (e.g., limit failed completely)
            market_result = await self._place_bingx_market_hedge(size, hedge_side, entry_price)
            if market_result is not None:
                executed_orders.append(('market', market_result))
                filled_market = self._extract_filled_size(market_result)
                if filled_market is None or filled_market <= 0:
                    filled_market = size
                total_executed += filled_market

        if total_executed <= 0:
            self.logger.error("[BINGX] Failed to execute hedge order for %s %s.", hedge_side, size)
            return False

        for order_type, order_result in executed_orders:
            filled_size = self._extract_filled_size(order_result)
            if filled_size is None or filled_size <= 0:
                filled_size = order_result.size or Decimal('0')
            self.logger.info(
                "[BINGX] %s %s %s @ %s | status=%s",
                order_type.upper(),
                hedge_side.upper(),
                filled_size,
                order_result.price,
                order_result.status,
            )

        if hedge_side == 'buy':
            self.bingx_position += total_executed
        else:
            self.bingx_position -= total_executed

        self.logger.info(
            "[BINGX] Hedge complete | side=%s | executed=%s | position=%s",
            hedge_side.upper(),
            total_executed,
            self.bingx_position
        )
        return True

    async def _ensure_bingx_hedge(self, fill: Dict[str, Any]) -> bool:
        attempts = 0
        while not self.stop_flag:
            attempts += 1

            hedge_success = await self.place_bingx_hedge(fill)
            if hedge_success:
                return True

            self.logger.warning(
                "[BINGX] Hedge attempt %s failed for %s %s. GRVT position=%s | BingX position=%s",
                attempts,
                fill.get('side'),
                fill.get('size'),
                self.grvt_position,
                self.bingx_position
            )

            if self.max_hedge_retries > 0 and attempts >= self.max_hedge_retries:
                self.logger.error(
                    "Exceeded max BingX hedge retries (%s); stopping to avoid unhedged exposure.",
                    self.max_hedge_retries
                )
                self.stop_flag = True
                return False

            await asyncio.sleep(self.hedge_retry_delay)

        return False

    async def _place_bingx_market_hedge(
        self,
        quantity: Decimal,
        hedge_side: str,
        entry_price: Optional[Decimal]
    ):
        assert self.bingx_client is not None
        assert self.bingx_contract_id is not None

        if quantity <= 0:
            return None

        best_bid = best_ask = None
        try:
            best_bid, best_ask = await self.bingx_client.fetch_bbo_prices(self.bingx_contract_id)
        except Exception as exc:
            self.logger.warning(f"[BINGX] Failed to fetch BBO before market hedge: {exc}")

        take_profit_price, stop_loss_price = self._compute_tp_sl_targets(
            hedge_side,
            entry_price,
            best_bid=best_bid,
            best_ask=best_ask
        )
        self.logger.info(
            "[BINGX] Hedging %s %s via market order (tp=%s, sl=%s)",
            hedge_side,
            quantity,
            take_profit_price,
            stop_loss_price
        )

        try:
            result = await self.bingx_client.place_market_order(
                contract_id=self.bingx_contract_id,
                quantity=quantity,
                side=hedge_side,
                take_profit_price=take_profit_price,
                stop_loss_price=stop_loss_price,
                tp_sl_order_type='limit'
            )
        except Exception as exc:
            self.logger.error(f"[BINGX] Market hedge exception: {exc}")
            return None

        if not result.success:
            self.logger.error(f"[BINGX] Market hedge failed: {result.error_message}")
            return None

        return result

    def _compute_tp_sl_targets(
        self,
        hedge_side: str,
        entry_price: Optional[Decimal],
        *,
        best_bid: Optional[Decimal] = None,
        best_ask: Optional[Decimal] = None
    ) -> Tuple[Optional[Decimal], Optional[Decimal]]:
        if not self.bingx_attach_tp_sl:
            return None, None

        if entry_price is None or entry_price <= 0:
            self.logger.warning("⚠️ Cannot compute BingX TP/SL due to invalid entry price.")
            return None, None

        hundred = Decimal('100')
        take_profit_price: Optional[Decimal] = None
        stop_loss_price: Optional[Decimal] = None

        if self.tp_roi is not None:
            tp_factor = self.tp_roi / hundred
            if hedge_side == 'sell':
                take_profit_price = entry_price * (Decimal('1') - tp_factor)
            else:
                take_profit_price = entry_price * (Decimal('1') + tp_factor)

        if self.sl_roi is not None:
            sl_factor = self.sl_roi / hundred
            if hedge_side == 'sell':
                stop_loss_price = entry_price * (Decimal('1') + sl_factor)
            else:
                stop_loss_price = entry_price * (Decimal('1') - sl_factor)

        if take_profit_price is not None and take_profit_price <= 0:
            self.logger.warning("⚠️ Computed BingX take-profit price is non-positive; ignoring TP.")
            take_profit_price = None
        if stop_loss_price is not None and stop_loss_price <= 0:
            self.logger.warning("⚠️ Computed BingX stop-loss price is non-positive; ignoring SL.")
            stop_loss_price = None

        reference_price = None
        if hedge_side == 'sell':
            reference_price = best_bid
        elif hedge_side == 'buy':
            reference_price = best_ask

        tick_size = self.bingx_tick_size or getattr(self.bingx_client.config, 'tick_size', None) if self.bingx_client else None
        tick = tick_size if isinstance(tick_size, Decimal) and tick_size > 0 else Decimal('0.01')

        if reference_price is not None and reference_price > 0:
            if hedge_side == 'sell' and take_profit_price is not None and take_profit_price >= reference_price:
                adjusted_tp = reference_price - tick
                if adjusted_tp > 0:
                    self.logger.warning(
                        "⚠️ BingX requires SHORT TP < last price (%.8f). Adjusting TP from %s to %s.",
                        reference_price,
                        take_profit_price,
                        adjusted_tp
                    )
                    take_profit_price = adjusted_tp
                else:
                    self.logger.warning("⚠️ SHORT TP constraint adjustment failed; removing TP.")
                    take_profit_price = None
            if hedge_side == 'buy' and take_profit_price is not None and take_profit_price <= reference_price:
                adjusted_tp = reference_price + tick
                self.logger.warning(
                    "⚠️ BingX requires LONG TP > last price (%.8f). Adjusting TP from %s to %s.",
                    reference_price,
                    take_profit_price,
                    adjusted_tp
                )
                take_profit_price = adjusted_tp

            if hedge_side == 'sell' and stop_loss_price is not None and stop_loss_price <= reference_price:
                adjusted_sl = reference_price + tick
                self.logger.warning(
                    "⚠️ BingX requires SHORT SL > last price (%.8f). Adjusting SL from %s to %s.",
                    reference_price,
                    stop_loss_price,
                    adjusted_sl
                )
                stop_loss_price = adjusted_sl
            if hedge_side == 'buy' and stop_loss_price is not None and stop_loss_price >= reference_price:
                adjusted_sl = reference_price - tick
                if adjusted_sl > 0:
                    self.logger.warning(
                        "⚠️ BingX requires LONG SL < last price (%.8f). Adjusting SL from %s to %s.",
                        reference_price,
                        stop_loss_price,
                        adjusted_sl
                    )
                    stop_loss_price = adjusted_sl
                else:
                    self.logger.warning("⚠️ LONG SL constraint adjustment failed; removing SL.")
                    stop_loss_price = None

        return take_profit_price, stop_loss_price

    def _format_grvt_trigger_price(self, price: Decimal) -> str:
        quant = Decimal('0.000000001')
        return str(price.quantize(quant, rounding=ROUND_HALF_UP))

    def _compute_grvt_tpsl_metadata(
        self,
        side: str,
        entry_price: Optional[Decimal]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        if not self.grvt_attach_tp_sl:
            return None, None
        if entry_price is None or entry_price <= 0:
            return None, None

        hundred = Decimal('100')
        one = Decimal('1')
        tp_metadata: Optional[Dict[str, Any]] = None
        sl_metadata: Optional[Dict[str, Any]] = None

        def build_metadata(price: Decimal) -> Optional[Dict[str, Any]]:
            if price <= 0:
                return None
            return {
                'trigger_by': self.grvt_tpsl_trigger_by,
                'trigger_price': self._format_grvt_trigger_price(price),
                'close_position': True
            }

        if self.tp_roi is not None:
            tp_factor = self.tp_roi / hundred
            if side == 'buy':
                target_price = entry_price * (one + tp_factor)
            else:
                target_price = entry_price * (one - tp_factor)
            tp_metadata = build_metadata(target_price)

        if self.sl_roi is not None:
            sl_factor = self.sl_roi / hundred
            if side == 'buy':
                target_price = entry_price * (one - sl_factor)
            else:
                target_price = entry_price * (one + sl_factor)
            sl_metadata = build_metadata(target_price)

        return tp_metadata, sl_metadata

    async def _place_bingx_limit_hedge(
        self,
        quantity: Decimal,
        hedge_side: str,
        entry_price: Optional[Decimal]
    ):
        assert self.bingx_client is not None
        assert self.bingx_contract_id is not None

        try:
            best_bid, best_ask = await self.bingx_client.fetch_bbo_prices(self.bingx_contract_id)
        except Exception as exc:
            self.logger.warning(f"[BINGX] Failed to fetch BingX BBO for limit hedge: {exc}")
            return None

        tick_size = self.bingx_tick_size or getattr(self.bingx_client.config, 'tick_size', None) or Decimal('0.01')
        offset_ticks = self.bingx_hedge_limit_offset_ticks
        if offset_ticks < 0:
            offset_ticks = Decimal('0')
        price_offset = tick_size * offset_ticks

        if hedge_side == 'sell':
            if best_bid <= 0:
                self.logger.warning("[BINGX] Best bid is unavailable; cannot place limit hedge sell.")
                return None
            limit_price = best_bid - price_offset
        else:
            if best_ask <= 0:
                self.logger.warning("[BINGX] Best ask is unavailable; cannot place limit hedge buy.")
                return None
            limit_price = best_ask + price_offset

        if limit_price <= 0:
            self.logger.warning("[BINGX] Computed limit hedge price is non-positive; skipping limit hedge.")
            return None

        take_profit_price, stop_loss_price = self._compute_tp_sl_targets(
            hedge_side,
            entry_price,
            best_bid=best_bid,
            best_ask=best_ask
        )

        time_in_force = self.bingx_hedge_time_in_force
        if time_in_force and time_in_force.upper() == 'PO':
            self.logger.warning("BingX hedge time_in_force 'PO' conflicts with non-post-only limit; forcing 'IOC'.")
            time_in_force = 'IOC'

        self.logger.info(
            "[BINGX] Hedging %s %s via limit order @ %s (offset_ticks=%s, tif=%s, tp=%s, sl=%s)",
            hedge_side,
            quantity,
            limit_price,
            self.bingx_hedge_limit_offset_ticks,
            time_in_force or 'DEFAULT',
            take_profit_price,
            stop_loss_price,
        )

        try:
            return await self.bingx_client.place_limit_order(
                contract_id=self.bingx_contract_id,
                quantity=quantity,
                side=hedge_side,
                price=limit_price,
                reduce_only=False,
                post_only=False,
                time_in_force=time_in_force,
                take_profit_price=take_profit_price,
                stop_loss_price=stop_loss_price,
                tp_sl_order_type='limit'
            )
        except Exception as exc:
            self.logger.error(f"[BINGX] Limit hedge exception: {exc}")
            return None

    async def _submit_parallel_bingx_limit(
        self,
        hedge_side: str,
        quantity: Decimal,
        entry_price_hint: Optional[Decimal]
    ) -> None:
        if not self.bingx_simultaneous_limit:
            return
        if self.bingx_hedge_order_type != 'limit':
            self.logger.warning("Parallel BingX limit requested but hedge order type is not 'limit'; skipping.")
            return
        if self.bingx_client is None or self.bingx_contract_id is None:
            return
        if quantity <= 0:
            return

        if self.parallel_bingx_order is not None:
            await self._cancel_parallel_bingx_order("Replacing existing parallel order")

        result = await self._place_bingx_limit_hedge(quantity, hedge_side, entry_price_hint)
        if result is None or not result.success or not result.order_id:
            self.logger.warning("[BINGX] Failed to submit parallel limit order for hedge preparation.")
            return

        self.parallel_bingx_order = {
            'order_id': result.order_id,
            'side': hedge_side,
            'quantity': quantity,
            'timestamp': time.time(),
        }
        self.logger.info(
            "[BINGX] Parallel %s LIMIT order submitted (%s) qty=%s @ %s",
            hedge_side.upper(),
            result.order_id,
            quantity,
            result.price
        )

    async def _cancel_parallel_bingx_order(self, reason: str) -> None:
        if self.parallel_bingx_order is None:
            return
        order_id = self.parallel_bingx_order.get('order_id')
        self.parallel_bingx_order = None
        if not order_id:
            return
        if self.bingx_client is None:
            return
        try:
            await self.bingx_client.cancel_order(order_id)
            self.logger.info("[BINGX] Cancelled parallel limit order %s (%s).", order_id, reason)
        except Exception as exc:
            self.logger.warning("[BINGX] Failed to cancel parallel order %s (%s): %s", order_id, reason, exc)

    async def _consume_parallel_bingx_order(
        self,
        hedge_side: str,
        required_size: Decimal
    ) -> Tuple[Decimal, bool]:
        """
        Inspect any pre-submitted BingX limit order and determine how much hedge size remains.
        Returns (remaining_size, used_parallel_order).
        """
        if self.parallel_bingx_order is None or required_size <= 0:
            return required_size, False

        if self.parallel_bingx_order.get('side') != hedge_side:
            await self._cancel_parallel_bingx_order(
                f"Parallel order side mismatch (expected {hedge_side})."
            )
            return required_size, False

        order_id = self.parallel_bingx_order.get('order_id')
        if not order_id or self.bingx_client is None:
            await self._cancel_parallel_bingx_order("Missing order id or client for parallel order.")
            return required_size, False

        used = False
        info = await self.bingx_client.get_order_info(order_id)
        if info is None:
            await self._cancel_parallel_bingx_order("Unable to fetch parallel order info.")
            return required_size, False

        filled = info.filled_size or Decimal('0')
        credited = min(required_size, filled)
        if credited > 0:
            used = True
            if hedge_side == 'buy':
                self.bingx_position += credited
            else:
                self.bingx_position -= credited
            required_size -= credited
            self.logger.info(
                "[BINGX] Parallel order filled %s (remaining hedge %s).",
                credited,
                required_size
            )

        status = (info.status or '').upper()
        if status in {'FILLED', 'CANCELED', 'REJECTED'} or required_size <= 0:
            # Cancel any remainder if hedge satisfied
            await self._cancel_parallel_bingx_order("Parallel order settled.")
        else:
            await self._cancel_parallel_bingx_order("Parallel order insufficient; cancelling to retry.")

        return required_size, used

    @staticmethod
    def _extract_filled_size(result) -> Optional[Decimal]:
        if result is None:
            return None
        filled = result.filled_size
        if filled is not None and isinstance(filled, Decimal):
            return filled
        if filled is not None:
            try:
                return Decimal(str(filled))
            except (InvalidOperation, ValueError, TypeError):
                return None
        if getattr(result, 'status', None) == 'FILLED' and getattr(result, 'size', None) is not None:
            try:
                return Decimal(str(result.size))
            except (InvalidOperation, ValueError, TypeError):
                return None
        return None

    def _reset_entry_state(self) -> None:
        self.current_entry_price = None
        self.current_entry_side = None
        self.current_entry_size = None
        self.current_entry_timestamp = None
        self.current_take_profit_price = None
        self.current_stop_loss_price = None
        self.last_roi_reason = None
        self.pending_grvt_price = None

    async def _register_entry(self, fill: Dict[str, Any], previous_position: Decimal) -> None:
        tolerance = self.position_tolerance
        net_position = self.grvt_position

        if abs(net_position) <= tolerance:
            self.logger.info(
                "GRVT position flattened after %s fill; clearing ROI tracking.",
                str(fill.get('side', '')).upper()
            )
            await self._clear_entry_state("grvt flat")
            return

        try:
            price = Decimal(str(fill.get('price')))
        except Exception:
            price = None

        side = 'buy' if net_position > 0 else 'sell'
        size = abs(net_position)

        flip_detected = (
            abs(previous_position) <= tolerance or
            (previous_position > 0 and net_position < 0) or
            (previous_position < 0 and net_position > 0)
        )

        if flip_detected:
            if price is None or price <= 0:
                self.logger.warning("⚠️ Unable to register new %s entry due to invalid fill price.", side.upper())
                await self._clear_entry_state("invalid entry price")
                return
            self.current_entry_price = price
        elif self.current_entry_price is None and price is not None and price > 0:
            self.current_entry_price = price

        self.current_entry_side = side
        self.current_entry_size = size
        self.current_entry_timestamp = time.time()
        self.last_roi_reason = None

        if self.current_entry_price is not None:
            self._update_roi_targets(side, self.current_entry_price)
            await self._refresh_grvt_exit_orders()
        else:
            self.logger.warning("⚠️ ROI targets disabled due to missing entry price for %s position.", side.upper())

    def _update_roi_targets(self, side: str, entry_price: Decimal) -> None:
        self.current_take_profit_price = None
        self.current_stop_loss_price = None

        if entry_price <= 0:
            return

        hundred = Decimal('100')
        one = Decimal('1')
        messages = []

        if self.tp_roi is not None:
            tp_factor = self.tp_roi / hundred
            if side == 'buy':
                self.current_take_profit_price = entry_price * (one + tp_factor)
            else:
                self.current_take_profit_price = entry_price * (one - tp_factor)
            messages.append(f"TP @ {self.current_take_profit_price} ({self.tp_roi}% ROI)")

        if self.sl_roi is not None:
            sl_factor = self.sl_roi / hundred
            if side == 'buy':
                self.current_stop_loss_price = entry_price * (one - sl_factor)
            else:
                self.current_stop_loss_price = entry_price * (one + sl_factor)
            messages.append(f"SL @ {self.current_stop_loss_price} (-{self.sl_roi}% ROI)")

        if messages:
            self.logger.info(f"🎯 ROI targets set ({side.upper()}): {', '.join(messages)}")

    async def _compute_entry_price_override(self, trade_side: str) -> Optional[Decimal]:
        if self.entry_tick_price is None or self.entry_tick_price <= 0:
            return None
        if (
            self.grvt_client is None
            or self.bingx_client is None
            or self.grvt_contract_id is None
            or self.bingx_contract_id is None
        ):
            return None

        try:
            grvt_bid, grvt_ask = await self.grvt_client.fetch_bbo_prices(self.grvt_contract_id)
            bingx_bid, bingx_ask = await self.bingx_client.fetch_bbo_prices(self.bingx_contract_id)
        except Exception as exc:
            self.logger.warning("⚠️ Unable to fetch BBO data for tick-price override: %s", exc)
            return None

        mids: List[Decimal] = []
        for bid, ask in ((grvt_bid, grvt_ask), (bingx_bid, bingx_ask)):
            if bid is None or ask is None:
                continue
            if bid > 0 and ask > 0:
                mids.append((bid + ask) / Decimal('2'))

        if not mids:
            self.logger.warning("⚠️ Unable to derive combined mid price for tick-price override; skipping.")
            return None

        combined_mid = sum(mids) / Decimal(len(mids))
        offset = self.entry_tick_price

        normalized_side = trade_side.strip().lower()
        if normalized_side == 'buy':
            target_price = combined_mid - offset
        else:
            target_price = combined_mid + offset

        if target_price <= 0:
            self.logger.warning("⚠️ Tick-price override produced non-positive price; skipping.")
            return None

        try:
            rounded_price = self.grvt_client.round_to_tick(target_price)
        except Exception:
            rounded_price = target_price

        self.logger.info(
            "🎯 Tick-price override | side=%s | mid=%s | offset=%s | price=%s",
            trade_side.upper(),
            combined_mid,
            offset,
            rounded_price
        )
        return rounded_price

    def _target_position_for_side(self, side: str) -> Decimal:
        normalized = side.strip().lower()
        if normalized == 'buy':
            return self.order_quantity
        return -self.order_quantity

    def _compute_trade_delta(self, target_side: str) -> Decimal:
        target = self._target_position_for_side(target_side)
        return target - self.grvt_position

    async def _fetch_exchange_mids(self) -> Optional[Dict[str, Decimal]]:
        if (
            self.grvt_client is None
            or self.bingx_client is None
            or self.grvt_contract_id is None
            or self.bingx_contract_id is None
        ):
            return None

        try:
            grvt_task = asyncio.create_task(self.grvt_client.fetch_bbo_prices(self.grvt_contract_id))
            bingx_task = asyncio.create_task(self.bingx_client.fetch_bbo_prices(self.bingx_contract_id))
            grvt_bid, grvt_ask = await grvt_task
            bingx_bid, bingx_ask = await bingx_task
        except Exception as exc:
            self.logger.warning(f"⚠️ Failed to fetch exchange mids: {exc}")
            return None

        if min(grvt_bid, grvt_ask, bingx_bid, bingx_ask) <= 0:
            return None

        return {
            'grvt_bid': grvt_bid,
            'grvt_ask': grvt_ask,
            'grvt_mid': (grvt_bid + grvt_ask) / Decimal('2'),
            'bingx_bid': bingx_bid,
            'bingx_ask': bingx_ask,
            'bingx_mid': (bingx_bid + bingx_ask) / Decimal('2')
        }

    def _render_gap_meter(
        self,
        grvt_mid: Decimal,
        bingx_mid: Decimal,
        gap: Decimal,
        threshold: Decimal
    ) -> None:
        if threshold <= 0:
            threshold = Decimal('0.01')
        ratio = float(gap / threshold)
        ratio = max(0.0, ratio)
        bar_width = 30
        filled = min(bar_width, int(ratio * (bar_width / 2)))
        bar = '#'
        bar = '#' * filled + '-' * (bar_width - filled)
        self.logger.info(
            "📊 GAP | GRVT=%.3f | BingX=%.3f | Δ=%.4f (%.2fx tick) | [%s]",
            float(grvt_mid),
            float(bingx_mid),
            float(gap),
            ratio,
            bar
        )

    async def _plan_gap_trade(self) -> Optional[Tuple[str, Optional[Decimal]]]:
        mids = await self._fetch_exchange_mids()
        if mids is None:
            return None

        grvt_mid = mids['grvt_mid']
        bingx_mid = mids['bingx_mid']
        gap = abs(grvt_mid - bingx_mid)
        threshold = self.gap_threshold
        self._render_gap_meter(grvt_mid, bingx_mid, gap, threshold)

        if gap <= threshold:
            self.logger.info(
                "Gap %.6f is within threshold %.6f; waiting for better opportunity.",
                gap,
                threshold
            )
            return None

        combined_mid = (grvt_mid + bingx_mid) / Decimal('2')
        grvt_tick = self.grvt_tick_size or Decimal('0.01')

        if grvt_mid <= bingx_mid:
            side = 'buy'
            desired_price = combined_mid
            cap_price = mids['grvt_ask'] - grvt_tick
            price_override = min(desired_price, cap_price)
        else:
            side = 'sell'
            desired_price = combined_mid
            cap_price = mids['grvt_bid'] + grvt_tick
            price_override = max(desired_price, cap_price)

        if price_override <= 0:
            price_override = None
        else:
            try:
                price_override = self.grvt_client.round_to_tick(price_override)
            except Exception:
                pass

        self.logger.info(
            "Gap trade | grvt_mid=%s | bingx_mid=%s | gap=%s | side=%s | target_price=%s",
            grvt_mid,
            bingx_mid,
            gap,
            side.upper(),
            price_override
        )
        return side, price_override

    async def _close_grvt_position_with_roi(self, target_price: Optional[Decimal], trigger: str) -> bool:
        quantity = abs(self.grvt_position)
        if quantity <= self.position_tolerance:
            self.logger.info("No GRVT position to close for ROI trigger %s.", trigger)
            return True

        side = 'sell' if self.grvt_position > 0 else 'buy'
        if target_price is not None and target_price <= 0:
            target_price = None

        self.logger.info(
            "[GRVT] Executing ROI %s via %s %s @ %s",
            trigger,
            side.upper(),
            quantity,
            target_price or 'AUTO'
        )

        fill = await self.place_grvt_order(
            side,
            quantity=quantity,
            price_override=target_price
        )
        if not fill:
            self.logger.error("[GRVT] ROI %s order failed; retaining position.", trigger)
            return False

        hedge_success = await self._ensure_bingx_hedge(fill)
        if not hedge_success:
            self.logger.error("[BINGX] ROI %s hedge failed; manual intervention required.", trigger)
            return False

        await self._enforce_balanced_positions(f"ROI {trigger}")
        await self._refresh_grvt_exit_orders()
        await self._cancel_grvt_exit_orders(f"ROI {trigger}")
        self._reset_entry_state()
        self.logger.info("✅ ROI %s execution complete; positions hedged.", trigger)
        return True

    async def _cancel_grvt_exit_orders(self, reason: str = "") -> None:
        if not any(self.grvt_exit_order_ids.values()):
            return
        if self.grvt_client is None:
            self.grvt_exit_order_ids = {'tp': None, 'sl': None}
            return
        for label, order_id in list(self.grvt_exit_order_ids.items()):
            if not order_id:
                continue
            try:
                await self.grvt_client.cancel_order(order_id)
                self.logger.info(
                    "[GRVT] Cancelled %s exit order %s (%s).",
                    label.upper(),
                    order_id,
                    reason or 'cleanup'
                )
            except Exception as exc:
                self.logger.warning(
                    "[GRVT] Failed to cancel %s exit order %s: %s",
                    label.upper(),
                    order_id,
                    exc
                )
        self.grvt_exit_order_ids = {'tp': None, 'sl': None}

    async def _submit_grvt_exit_order(
        self,
        label: str,
        side: str,
        quantity: Decimal,
        price: Decimal
    ) -> None:
        if price <= 0 or quantity <= 0:
            return
        if self.grvt_client is None or self.grvt_contract_id is None:
            return
        try:
            result = await self.grvt_client.place_open_order(
                contract_id=self.grvt_contract_id,
                quantity=quantity,
                direction=side,
                price=price,
                respect_price=True
            )
        except Exception as exc:
            self.logger.warning("[GRVT] Failed to submit %s exit order: %s", label.upper(), exc)
            return

        if not result.success or not result.order_id:
            self.logger.warning(
                "[GRVT] %s exit order rejected: %s",
                label.upper(),
                getattr(result, 'error_message', 'unknown error')
            )
            return

        self.grvt_exit_order_ids[label] = result.order_id
        self.logger.info(
            "[GRVT] %s exit order submitted | side=%s | qty=%s | price=%s | id=%s",
            label.upper(),
            side.upper(),
            quantity,
            price,
            result.order_id
        )

    async def _refresh_grvt_exit_orders(self) -> None:
        await self._cancel_grvt_exit_orders("refresh")

        if (
            self.current_entry_side is None
            or self.current_entry_size is None
            or self.current_entry_size <= 0
            or self.grvt_client is None
        ):
            return

        exit_side = 'sell' if self.current_entry_side == 'buy' else 'buy'
        quantity = self.current_entry_size

        if self.current_take_profit_price and self.current_take_profit_price > 0:
            await self._submit_grvt_exit_order('tp', exit_side, quantity, self.current_take_profit_price)
        if self.current_stop_loss_price and self.current_stop_loss_price > 0:
            await self._submit_grvt_exit_order('sl', exit_side, quantity, self.current_stop_loss_price)

    async def _clear_entry_state(self, reason: str = "") -> None:
        await self._cancel_grvt_exit_orders(reason or "reset")
        self._reset_entry_state()

    def _per_exchange_positions_flat(
        self,
        grvt_position: Decimal,
        bingx_position: Decimal
    ) -> bool:
        tolerance = self.position_tolerance
        return abs(grvt_position) <= tolerance and abs(bingx_position) <= tolerance

    def _positions_are_flat(self) -> bool:
        tolerance = self.position_tolerance
        if self.strict_mode:
            return self._per_exchange_positions_flat(self.grvt_position, self.bingx_position)
        net_exposure = self.grvt_position + self.bingx_position
        return abs(net_exposure) <= tolerance

    async def _enforce_balanced_positions(self, context: str) -> bool:
        """Ensure BingX position mirrors GRVT within tolerance; rebalance if necessary."""
        grvt_position, bingx_position = await self._sync_positions_from_exchanges()
        net_exposure = grvt_position + bingx_position
        if abs(net_exposure) <= self.position_tolerance:
            await self._cancel_grvt_exit_orders("positions balanced")
            return True

        self.logger.warning(
            "⚠️ Position mismatch detected (%s) | GRVT=%s | BingX=%s | net=%s. Attempting BingX rebalance.",
            context,
            grvt_position,
            bingx_position,
            net_exposure
        )

        pseudo_side = 'sell' if net_exposure < 0 else 'buy'
        pseudo_fill = {
            'side': pseudo_side,
            'size': abs(net_exposure),
            'price': self.current_entry_price or None
        }
        success = await self._ensure_bingx_hedge(pseudo_fill)
        if success:
            self.logger.info(
                "[BINGX] Rebalance executed (%s %s) during %s.",
                ('BUY' if pseudo_side == 'sell' else 'SELL'),
                abs(net_exposure),
                context.upper()
            )
            await self._sync_positions_from_exchanges()
            return True

        self.logger.error("❌ Unable to rebalance BingX position during %s.", context)
        return False

    async def _fetch_signed_positions(self) -> Tuple[Decimal, Decimal]:
        grvt_position = self.grvt_position
        bingx_position = self.bingx_position

        if self.grvt_client is not None and self.grvt_contract_id is not None:
            try:
                grvt_position = await self.grvt_client.get_signed_position()
            except Exception as exc:
                self.logger.warning(
                    "⚠️ Failed to fetch GRVT position from exchange API (%s); using local state %s",
                    exc,
                    grvt_position
                )

        if self.bingx_client is not None and self.bingx_contract_id is not None:
            try:
                bingx_position = await self.bingx_client.get_signed_position()
            except Exception as exc:
                self.logger.warning(
                    "⚠️ Failed to fetch BingX position from exchange API (%s); using local state %s",
                    exc,
                    bingx_position
                )

        return grvt_position, bingx_position

    async def _sync_positions_from_exchanges(self) -> Tuple[Decimal, Decimal]:
        grvt_position, bingx_position = await self._fetch_signed_positions()
        self.grvt_position = grvt_position
        self.bingx_position = bingx_position
        return grvt_position, bingx_position

    async def _place_grvt_limit_close(self, position: Decimal) -> bool:
        assert self.grvt_client is not None
        assert self.grvt_contract_id is not None

        quantity = abs(position)
        if quantity <= 0:
            return True

        side = 'sell' if position > 0 else 'buy'
        self.grvt_client.config.direction = side
        self.grvt_client.config.close_order_side = 'sell' if side == 'buy' else 'buy'

        self.logger.info("[GRVT] Placing limit %s order to close %s", side.upper(), quantity)
        try:
            result = await self.grvt_client.place_open_order(
                contract_id=self.grvt_contract_id,
                quantity=quantity,
                direction=side
            )
        except Exception as exc:
            self.logger.error("[GRVT] Failed to submit limit close order: %s", exc)
            return False

        if not result or not result.success:
            error = getattr(result, 'error_message', 'Unknown error') if result else 'No response'
            self.logger.error("[GRVT] Limit close order rejected: %s", error)
            return False

        self.logger.info(
            "[GRVT] Close order accepted | id=%s | side=%s | qty=%s | price=%s | status=%s",
            result.order_id,
            side.upper(),
            quantity,
            result.price,
            result.status
        )
        return True

    async def _place_bingx_limit_close(self, position: Decimal) -> bool:
        assert self.bingx_client is not None
        assert self.bingx_contract_id is not None

        quantity = abs(position)
        if quantity <= 0:
            return True

        side = 'sell' if position > 0 else 'buy'
        self.bingx_client.config.direction = side
        self.bingx_client.config.close_order_side = 'sell' if side == 'buy' else 'buy'

        self.logger.info("[BINGX] Placing limit %s order to close %s", side.upper(), quantity)
        try:
            result = await self.bingx_client.place_open_order(
                contract_id=self.bingx_contract_id,
                quantity=quantity,
                direction=side
            )
        except Exception as exc:
            self.logger.error("[BINGX] Failed to submit limit close order: %s", exc)
            return False

        if not result or not result.success:
            error = getattr(result, 'error_message', 'Unknown error') if result else 'No response'
            self.logger.error("[BINGX] Limit close order rejected: %s", error)
            return False

        self.logger.info(
            "[BINGX] Close order accepted | id=%s | side=%s | qty=%s | price=%s | status=%s",
            result.order_id,
            side.upper(),
            quantity,
            result.price,
            result.status
        )
        return True

    async def close_positions_with_limit_orders(self) -> None:
        """
        Place limit OPEN orders on both GRVT and BingX to flatten existing positions.
        """
        self.logger.info(
            "🔚 Initiating GRVT+BingX limit-open position close routine (strict=%s).",
            "ON" if self.strict_mode else "OFF"
        )

        if self.grvt_client is None or self.bingx_client is None:
            self.initialize_clients()

        if self.grvt_contract_id is None or self.bingx_contract_id is None:
            try:
                await self.load_contract_metadata()
            except Exception as exc:
                self.logger.error(f"Unable to load contract metadata for position close: {exc}")
                return

        tolerance = self.position_tolerance
        pending_close = False
        previous_positions: Optional[Tuple[Decimal, Decimal]] = None
        last_submission_time: Optional[float] = None
        start_time = time.time()

        while not self.stop_flag:
            grvt_position, bingx_position = await self._fetch_signed_positions()
            self.grvt_position = grvt_position
            self.bingx_position = bingx_position

            if self._per_exchange_positions_flat(grvt_position, bingx_position):
                self.logger.info("✅ Both GRVT and BingX positions are flat. Close routine finished.")
                return

            if self.position_close_timeout > 0 and time.time() - start_time >= self.position_close_timeout:
                self.logger.error(
                    "❌ Position close timeout (%.1fs). Residual positions | GRVT=%s | BingX=%s",
                    time.time() - start_time,
                    grvt_position,
                    bingx_position
                )
                return

            if pending_close:
                positions_changed = False
                if previous_positions is None:
                    positions_changed = True
                else:
                    if abs(grvt_position - previous_positions[0]) > tolerance:
                        positions_changed = True
                    if abs(bingx_position - previous_positions[1]) > tolerance:
                        positions_changed = True

                if positions_changed:
                    pending_close = False
                    previous_positions = (grvt_position, bingx_position)
                else:
                    if (
                        self.position_close_retry_delay > 0
                        and last_submission_time is not None
                        and time.time() - last_submission_time < self.position_close_retry_delay
                    ):
                        await asyncio.sleep(self.position_close_poll_interval)
                        continue
                    self.logger.warning("⚠️ Close orders show no fills; resubmitting.")
                    pending_close = False

            tasks = []
            if abs(grvt_position) > tolerance:
                tasks.append(self._place_grvt_limit_close(grvt_position))
            else:
                self.logger.info("GRVT position already flat (size=%s); skipping GRVT close order.", grvt_position)

            if abs(bingx_position) > tolerance:
                tasks.append(self._place_bingx_limit_close(bingx_position))
            else:
                self.logger.info("BingX position already flat (size=%s); skipping BingX close order.", bingx_position)

            if not tasks:
                # Within tolerance but not strictly flat; continue polling.
                await asyncio.sleep(self.position_close_poll_interval)
                continue

            results = await asyncio.gather(*tasks, return_exceptions=True)
            failures = [
                result for result in results
                if isinstance(result, Exception) or result is False
            ]

            if failures:
                self.logger.warning("⚠️ Some limit close orders failed. Review logs for details.")
                pending_close = False
            else:
                self.logger.info("✅ Limit close orders submitted. Awaiting fills...")
                pending_close = True
                previous_positions = (grvt_position, bingx_position)
                last_submission_time = time.time()

            await asyncio.sleep(self.position_close_poll_interval)

    async def wait_for_roi(self) -> None:
        if self.stop_flag:
            return
        if self.tp_roi is None and self.sl_roi is None:
            return
        if self.current_entry_price is None or self.current_entry_side is None:
            return
        if abs(self.grvt_position) <= self.position_tolerance:
            self.logger.info("No active GRVT position to monitor for ROI; skipping.")
            return
        if self.grvt_client is None or self.grvt_contract_id is None:
            self.logger.warning("⚠️ Cannot wait for ROI without GRVT client or contract id.")
            return

        entry_price = self.current_entry_price
        if entry_price <= 0:
            self.logger.warning("⚠️ Cannot evaluate ROI targets because entry price is non-positive.")
            return

        if not await self._enforce_balanced_positions("ROI wait start"):
            self.logger.warning("Skipping ROI wait due to unresolved hedge imbalance.")
            return

        hundred = Decimal('100')
        start_time = time.time()
        self.logger.info("⏳ Waiting for ROI targets before executing opposite GRVT cycle...")

        while not self.stop_flag:
            if abs(self.grvt_position + self.bingx_position) > self.position_tolerance:
                balanced = await self._enforce_balanced_positions("ROI wait loop")
                if not balanced:
                    self.logger.error("Stopping ROI wait due to persistent hedge imbalance.")
                    return

            try:
                best_bid, best_ask = await self.grvt_client.fetch_bbo_prices(self.grvt_contract_id)
            except Exception as exc:
                self.logger.warning(f"⚠️ Failed to fetch GRVT prices while waiting for ROI: {exc}")
                await asyncio.sleep(self.roi_poll_interval)
                continue

            reference_price = None
            roi = None
            if self.current_entry_side == 'buy' and best_bid and best_bid > 0:
                reference_price = best_bid
                roi = (reference_price - entry_price) / entry_price * hundred
            elif self.current_entry_side == 'sell' and best_ask and best_ask > 0:
                reference_price = best_ask
                roi = (entry_price - reference_price) / entry_price * hundred

            if roi is not None:
                roi_float = float(roi)
                take_profit_hit = self.tp_roi is not None and roi >= self.tp_roi
                stop_loss_hit = self.sl_roi is not None and roi <= -self.sl_roi

                if take_profit_hit:
                    self.last_roi_reason = f"take_profit ({roi_float:.4f}%)"
                    self.logger.info(f"🎯 ROI take profit reached: {roi_float:.4f}% (target {self.tp_roi}%)")
                    if self.grvt_attach_tp_sl:
                        self.logger.info("Waiting for GRVT TP trigger to execute automatically.")
                        return
                    success = await self._close_grvt_position_with_roi(
                        self.current_take_profit_price,
                        'take_profit'
                    )
                    if success:
                        return
                    self.logger.warning("ROI take profit execution failed; continuing to monitor.")

                if stop_loss_hit:
                    self.last_roi_reason = f"stop_loss ({roi_float:.4f}%)"
                    self.logger.info(f"🛑 ROI stop loss reached: {roi_float:.4f}% (threshold -{self.sl_roi}%)")
                    if self.grvt_attach_tp_sl:
                        self.logger.info("Waiting for GRVT SL trigger to execute automatically.")
                        return
                    success = await self._close_grvt_position_with_roi(
                        self.current_stop_loss_price,
                        'stop_loss'
                    )
                    if success:
                        return
                    self.logger.warning("ROI stop loss execution failed; continuing to monitor.")

            elapsed = time.time() - start_time
            if elapsed >= self.max_roi_wait:
                self.last_roi_reason = f"timeout ({elapsed:.1f}s)"
                self.logger.info(f"⏱️ ROI wait timed out after {elapsed:.1f}s; proceeding to next cycle.")
                self.pending_grvt_price = None
                return

            await asyncio.sleep(self.roi_poll_interval)

    async def execute_cycle(self, side: str, price_override: Optional[Decimal] = None) -> bool:
        effective_price_override = price_override
        if self.pending_grvt_price and self.pending_grvt_price[0] == side:
            effective_price_override = self.pending_grvt_price[1]
        elif effective_price_override is None and self.entry_tick_price is not None:
            tick_override = await self._compute_entry_price_override(side)
            if tick_override is not None:
                effective_price_override = tick_override

        trade_delta = self._compute_trade_delta(side)
        if trade_delta == 0:
            self.logger.info(
                "Skipping %s cycle; GRVT position already at target (position=%s).",
                side.upper(),
                self.grvt_position
            )
            if self.pending_grvt_price and self.pending_grvt_price[0] == side:
                self.pending_grvt_price = None
            return True

        trade_side = 'buy' if trade_delta > 0 else 'sell'
        trade_quantity = abs(trade_delta)

        if trade_quantity <= 0:
            self.logger.warning("Computed non-positive trade quantity for %s cycle; aborting.", side.upper())
            return False

        if trade_side != side:
            self.logger.warning(
                "Target side %s requires executing %s to rebalance positions (delta=%s).",
                side.upper(),
                trade_side.upper(),
                trade_delta
            )

        if (
            effective_price_override is not None
            and self.pending_grvt_price
            and self.pending_grvt_price[0] != trade_side
        ):
            self.logger.warning(
                "Pending GRVT price scheduled for %s but actual trade side is %s; ignoring override.",
                self.pending_grvt_price[0].upper(),
                trade_side.upper()
            )
            effective_price_override = None

        parallel_bingx_task: Optional[asyncio.Task] = None
        if self.bingx_simultaneous_limit and trade_quantity > 0:
            hedge_side = 'sell' if trade_side == 'buy' else 'buy'
            entry_hint = effective_price_override
            parallel_bingx_task = asyncio.create_task(
                self._submit_parallel_bingx_limit(hedge_side, trade_quantity, entry_hint)
            )

        previous_position = self.grvt_position

        fill = await self.place_grvt_order(
            trade_side,
            quantity=trade_quantity,
            price_override=effective_price_override
        )

        if parallel_bingx_task is not None:
            try:
                await parallel_bingx_task
            except Exception as exc:
                self.logger.warning("Parallel BingX limit submission raised an error: %s", exc)

        if not fill or self.stop_flag:
            await self._cancel_parallel_bingx_order("GRVT order failed or bot stopping.")
            await self._clear_entry_state("GRVT order failure")
            return False

        if (
            effective_price_override is not None
            and self.pending_grvt_price
            and self.pending_grvt_price[0] == trade_side
        ):
            self.pending_grvt_price = None

        await self._register_entry(fill, previous_position)
        hedge_success = await self._ensure_bingx_hedge(fill)
        if not hedge_success:
            self.logger.error(
                "Unable to complete BingX hedge for GRVT %s fill; halting cycle to avoid exposure.",
                trade_side.upper()
            )
            await self._clear_entry_state("hedge failure")
            return False

        balanced = await self._enforce_balanced_positions("post-cycle hedging")
        if not balanced:
            await self._clear_entry_state("hedge imbalance")
            return False

        if self.sleep_time > 0 and not self.stop_flag:
            await asyncio.sleep(self.sleep_time)

        return True

    async def _run_cycle_phase(self, side: str, price_override: Optional[Decimal] = None) -> bool:
        attempt = 0
        while not self.stop_flag:
            attempt += 1
            success = await self.execute_cycle(side, price_override=price_override)
            if success:
                return True

            self.logger.warning(
                "Cycle %s attempt %s failed; GRVT position=%s | BingX position=%s",
                side.upper(),
                attempt,
                self.grvt_position,
                self.bingx_position
            )

            if self.max_cycle_retries > 0 and attempt >= self.max_cycle_retries:
                self.logger.error(
                    "Exceeded max retries (%s) for %s cycle; stopping to prevent compounding positions.",
                    self.max_cycle_retries,
                    side.upper()
                )
                self.stop_flag = True
                return False

            await asyncio.sleep(self.cycle_retry_delay)

        return False

    # ------------------------------------------------------------------ #
    # Main run loop
    # ------------------------------------------------------------------ #

    async def trading_loop(self) -> None:
        self.logger.info(f"Starting GRVT+BingX hedge bot | ticker={self.ticker} | size={self.order_quantity}")

        self.initialize_clients()
        try:
            await self.load_contract_metadata()
            await self.setup_grvt_websocket()
            await self.setup_bingx()
        except Exception as exc:
            self.logger.error(f"Initialization failed: {exc}")
            self.stop_flag = True
            return

        await asyncio.sleep(2)

        iteration = 0
        while not self.stop_flag and iteration < self.iterations:
            iteration += 1
            self.logger.info(f"----- Gap Iteration {iteration}/{self.iterations} -----")

            if abs(self.grvt_position) > self.position_tolerance or abs(self.bingx_position) > self.position_tolerance:
                self.logger.info(
                    "Active positions detected (GRVT=%s | BingX=%s); waiting for ROI or manual close.",
                    self.grvt_position,
                    self.bingx_position
                )
                await asyncio.sleep(self.cycle_retry_delay)
                continue

            opportunity = await self._plan_gap_trade()
            if opportunity is None:
                await asyncio.sleep(self.cycle_retry_delay)
                continue

            target_side, price_override = opportunity
            cycle_completed = await self._run_cycle_phase(target_side, price_override=price_override)
            if self.stop_flag or not cycle_completed:
                break

            await self.wait_for_roi()
            if self.stop_flag:
                break

            if not self._positions_are_flat():
                balanced = await self._enforce_balanced_positions("post-iteration")
                if not balanced:
                    self.logger.error(
                        "Residual positions detected after iteration %s | GRVT=%s | BingX=%s. Halting to prevent compounding.",
                        iteration,
                        self.grvt_position,
                        self.bingx_position
                    )
                    self.stop_flag = True
                    break

        self.logger.info("Trading loop finished")

    async def cleanup(self) -> None:
        if self.grvt_client:
            try:
                await self.grvt_client.disconnect()
            except Exception as exc:
                self.logger.warning(f"Error disconnecting GRVT client: {exc}")
        if self.bingx_client:
            try:
                await self.bingx_client.disconnect()
            except Exception as exc:
                self.logger.warning(f"Error disconnecting BingX client: {exc}")

    async def run(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.setup_signal_handlers()

        start_time = time.time()
        try:
            await self.trading_loop()
        except Exception as exc:
            self.logger.error(f"Unexpected error: {exc}")
        finally:
            await self.cleanup()
            elapsed = time.time() - start_time
            self.logger.info(f"Hedge bot stopped after {elapsed:.1f}s")
