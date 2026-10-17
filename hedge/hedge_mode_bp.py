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
from typing import Tuple, Optional, Dict, Any, List
from enum import Enum
from dataclasses import dataclass, field

from lighter.signer_client import SignerClient
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from exchanges.backpack import BackpackClient
import websockets
from datetime import datetime
import pytz

class Config:
    """Simple config class to wrap dictionary for Backpack client."""
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)


class RiskState(Enum):
    NORMAL = "normal"
    WIDE = "wide"
    STRESSED = "stressed"


class RiskEvent(Enum):
    MARKET_DATA_UNHEALTHY = "market_data_unhealthy"
    MARKET_DATA_RECOVERED = "market_data_recovered"
    BASIS_WIDE = "basis_wide"
    BASIS_NORMAL = "basis_normal"
    SOFT_EXPOSURE_BREACH = "soft_exposure_breach"
    HARD_EXPOSURE_BREACH = "hard_exposure_breach"
    LIGHTER_HEDGE_TIMEOUT = "lighter_hedge_timeout"
    LIGHTER_HEDGE_ERROR = "lighter_hedge_error"
    TRADE_COMPLETION_TIMEOUT = "trade_completion_timeout"

@dataclass
class _NetAccumulator:
    quantity: Decimal = Decimal("0")
    notional: Decimal = Decimal("0")

    def vwap(self) -> Decimal:
        if self.quantity <= 0:
            return Decimal("0")
        return self.notional / self.quantity

    def remove_proportional(self, remove_qty: Decimal):
        if remove_qty <= 0 or self.quantity <= 0:
            return
        remove_qty = min(remove_qty, self.quantity)
        avg = self.vwap()
        self.quantity -= remove_qty
        self.notional -= avg * remove_qty
        if self.quantity <= 0:
            self.quantity = Decimal("0")
            self.notional = Decimal("0")


@dataclass
class HedgeNettingBuffer:
    """Accumulates filled maker legs and produces net hedge orders (VWAP reference).

    buy_bucket represents hedges where Lighter side should be 'buy' (e.g., BP sell fills).
    sell_bucket represents hedges where Lighter side should be 'sell' (e.g., BP buy fills).
    """

    buy_bucket: _NetAccumulator = field(default_factory=_NetAccumulator)
    sell_bucket: _NetAccumulator = field(default_factory=_NetAccumulator)

    def add_fill(self, lighter_side: str, quantity: Decimal, reference_price: Decimal):
        if quantity <= 0 or reference_price <= 0:
            return
        bucket = self.buy_bucket if lighter_side.lower() == "buy" else self.sell_bucket
        bucket.quantity += quantity
        bucket.notional += quantity * reference_price
        self._net_internal()

    def _net_internal(self):
        net_qty = min(self.buy_bucket.quantity, self.sell_bucket.quantity)
        if net_qty <= 0:
            return
        self.buy_bucket.remove_proportional(net_qty)
        self.sell_bucket.remove_proportional(net_qty)

    def has_pending(self) -> bool:
        return self.buy_bucket.quantity > 0 or self.sell_bucket.quantity > 0

    def pop_next_order(self) -> Optional[Tuple[str, Decimal, Decimal]]:
        """Return (lighter_side, qty, vwap_reference_price) and reduce buffer."""
        if not self.has_pending():
            return None
        if self.buy_bucket.quantity >= self.sell_bucket.quantity:
            side = "buy"
            bucket = self.buy_bucket
        else:
            side = "sell"
            bucket = self.sell_bucket
        qty = bucket.quantity
        px = bucket.vwap()
        bucket.quantity = Decimal("0")
        bucket.notional = Decimal("0")
        return side, qty, px


class HedgeBot:
    """Trading bot that places post-only orders on Backpack and hedges with market orders on Lighter."""

    def __init__(self, ticker: str, order_quantity: Decimal, fill_timeout: int = 5, iterations: int = 20, sleep_time: int = 0, max_position: Decimal = Decimal('0')):
        self.ticker = ticker
        self.order_quantity = order_quantity
        self.fill_timeout = fill_timeout
        self.lighter_order_filled = False
        self.iterations = iterations
        self.sleep_time = sleep_time
        self.current_order = {}
        if max_position == Decimal('0'):
            self.max_position = order_quantity
        else:
            self.max_position = max_position        
        self.max_lighter_slippage = min(Decimal('0.002'), max(Decimal('0.0001'), self._safe_decimal_env('HEDGE_MAX_LIGHTER_SLIPPAGE', Decimal('0.0012'))))
        self.base_lighter_slippage = min(self.max_lighter_slippage, self._safe_decimal_env('HEDGE_BASE_LIGHTER_SLIPPAGE', Decimal('0.0004')))
        self.max_basis_bps = max(Decimal('1'), self._safe_decimal_env('HEDGE_MAX_BASIS_BPS', Decimal('15')))
        self.basis_wait_timeout = float(self._safe_decimal_env('HEDGE_BASIS_WAIT_TIMEOUT_SEC', Decimal('3')))
        self.strategy = os.getenv("HEDGE_STRATEGY", "legacy").strip().lower() if os.getenv("HEDGE_STRATEGY") else "legacy"
        self.funding_enabled = self._safe_bool_env("HEDGE_FUNDING_ENABLED", self.strategy == "core_satellite")
        self.funding_refresh_seconds = float(self._safe_decimal_env("HEDGE_FUNDING_REFRESH_SEC", Decimal("3600")))
        self.funding_switch_threshold_seconds = float(self._safe_decimal_env("HEDGE_FUNDING_SWITCH_THRESHOLD_SEC", Decimal("3600")))
        self.funding_ramp_interval_seconds = float(self._safe_decimal_env("HEDGE_FUNDING_RAMP_INTERVAL_SEC", Decimal("60")))
        self.funding_ramp_step_quantity = self._safe_decimal_env("HEDGE_FUNDING_RAMP_STEP_QTY", self.order_quantity)
        self.funding_startup_retries = max(1, self._safe_int_env("HEDGE_FUNDING_STARTUP_RETRIES", 3))
        self.funding_startup_retry_sleep_seconds = float(
            self._safe_decimal_env("HEDGE_FUNDING_STARTUP_RETRY_SLEEP_SEC", Decimal("2"))
        )
        self.funding_exit_unwind_max_rounds = max(1, self._safe_int_env("HEDGE_FUNDING_EXIT_UNWIND_MAX_ROUNDS", 100))
        self.netting_window_seconds = float(self._safe_decimal_env("HEDGE_NETTING_WINDOW_SEC", Decimal("1")))
        self.satellite_enabled = self._safe_bool_env("HEDGE_SAT_ENABLED", True)
        self.satellite_order_quantity = self._safe_decimal_env("HEDGE_SAT_ORDER_QTY", self.order_quantity)
        self.satellite_band = max(Decimal("0"), self._safe_decimal_env("HEDGE_SAT_BAND", self.max_position))
        self.satellite_trades_per_iter = max(1, self._safe_int_env("HEDGE_SAT_TRADES_PER_ITER", 10))
        self.satellite_core_tolerance = max(
            Decimal("0.000001"),
            self._safe_decimal_env("HEDGE_SAT_CORE_TOLERANCE", self.order_quantity),
        )
        self.exit_flatten_tolerance = max(
            Decimal("0.000001"),
            self._safe_decimal_env("HEDGE_EXIT_FLATTEN_TOLERANCE", Decimal("0.001")),
        )
        self.satellite_max_net_exposure = self.order_quantity * max(Decimal('2'), self._safe_decimal_env("HEDGE_SAT_MAX_NET_EXPOSURE_MULT", Decimal("4")))
        self.core_target_abs_position = max(Decimal("0"), self._safe_decimal_env("HEDGE_CORE_TARGET_ABS_POSITION", Decimal("0")))
        self.core_long_exchange = (os.getenv("HEDGE_CORE_LONG_EXCHANGE", "backpack").strip().lower()
                                   if os.getenv("HEDGE_CORE_LONG_EXCHANGE") else "backpack")
        self.core_rebalance_interval_seconds = float(self._safe_decimal_env("HEDGE_CORE_REBALANCE_INTERVAL_SEC", Decimal("30")))
        self.pre_trade_alignment_enabled = self._safe_bool_env('HEDGE_PRE_TRADE_ALIGNMENT_ENABLED', False)
        self.pre_trade_max_basis_bps = max(Decimal('0.5'), self._safe_decimal_env('HEDGE_PRE_TRADE_MAX_BASIS_BPS', Decimal('3')))
        self.pre_trade_wait_timeout_seconds = float(self._safe_decimal_env('HEDGE_PRE_TRADE_WAIT_TIMEOUT_SEC', Decimal('1.5')))
        self.pre_trade_retry_sleep_seconds = float(self._safe_decimal_env('HEDGE_PRE_TRADE_RETRY_SLEEP_SEC', Decimal('0.2')))
        self.hedge_alignment_enabled = self._safe_bool_env('HEDGE_ALIGNMENT_ENABLED', True)
        self.hedge_alignment_initial_bps = max(Decimal('0.5'), self._safe_decimal_env('HEDGE_ALIGNMENT_INITIAL_BPS', Decimal('3')))
        self.hedge_alignment_max_bps = max(
            self.hedge_alignment_initial_bps,
            self._safe_decimal_env('HEDGE_ALIGNMENT_MAX_BPS', self.max_basis_bps)
        )
        self.hedge_alignment_widen_step_bps = max(Decimal('0'), self._safe_decimal_env('HEDGE_ALIGNMENT_WIDEN_STEP_BPS', Decimal('1')))
        self.hedge_alignment_widen_interval_seconds = float(
            self._safe_decimal_env('HEDGE_ALIGNMENT_WIDEN_INTERVAL_SEC', Decimal('0.5'))
        )
        self.hedge_alignment_wait_timeout_seconds = float(
            self._safe_decimal_env('HEDGE_ALIGNMENT_WAIT_TIMEOUT_SEC', Decimal(str(self.basis_wait_timeout)))
        )
        self.hedge_alignment_retry_sleep_seconds = float(
            self._safe_decimal_env('HEDGE_ALIGNMENT_RETRY_SLEEP_SEC', Decimal('0.1'))
        )
        self.hedge_alignment_force_on_timeout = self._safe_bool_env('HEDGE_ALIGNMENT_FORCE_ON_TIMEOUT', True)
        self.requote_price_drift_ticks = max(Decimal('1'), self._safe_decimal_env('HEDGE_REQUOTE_DRIFT_TICKS', Decimal('2')))
        self.requote_min_age_seconds = float(self._safe_decimal_env('HEDGE_REQUOTE_MIN_AGE_SEC', Decimal('0.8')))
        self.requote_secondary_timeout_seconds = float(self._safe_decimal_env('HEDGE_REQUOTE_SECONDARY_TIMEOUT_SEC', Decimal('0')))
        self.hedge_stats_log_interval_seconds = float(self._safe_decimal_env('HEDGE_STATS_LOG_INTERVAL_SEC', Decimal('30')))
        self.hedge_stats_log_every_n_fills = max(1, self._safe_int_env('HEDGE_STATS_LOG_EVERY_N_FILLS', 50))
        self.soft_exposure_limit = self.order_quantity * max(Decimal('1'), self._safe_decimal_env('HEDGE_SOFT_EXPOSURE_MULTIPLIER', Decimal('1.5')))
        self.hard_exposure_limit = self.order_quantity * max(Decimal('1'), self._safe_decimal_env('HEDGE_HARD_EXPOSURE_MULTIPLIER', Decimal('2')))
        if self.soft_exposure_limit > self.hard_exposure_limit:
            self.soft_exposure_limit = self.hard_exposure_limit
        self.event_cooldown_seconds = float(self._safe_decimal_env('HEDGE_EVENT_COOLDOWN_SEC', Decimal('1')))
        self.wide_basis_bps = max(self.max_basis_bps, self._safe_decimal_env('HEDGE_WIDE_BASIS_BPS', Decimal('25')))
        self.lighter_fill_timeout_seconds = float(self._safe_decimal_env('HEDGE_LIGHTER_FILL_TIMEOUT_SEC', Decimal('30')))
        self.lighter_timeout_retry_sleep_seconds = float(self._safe_decimal_env('HEDGE_LIGHTER_TIMEOUT_RETRY_SLEEP_SEC', Decimal('2')))
        self.lighter_cancel_after_seconds = float(self._safe_decimal_env('HEDGE_LIGHTER_CANCEL_AFTER_SEC', Decimal('300')))
        self.absolute_exposure_limit = self.order_quantity * max(Decimal('100'), self._safe_decimal_env('HEDGE_ABSOLUTE_EXPOSURE_MULTIPLIER', Decimal('100')))
        self.state_changed_at = time.time()
        self.risk_state = RiskState.NORMAL
        self._last_event_time: Dict[str, float] = {}

        # Initialize logging to file
        os.makedirs("logs", exist_ok=True)
        self.log_filename = f"logs/backpack_{ticker}_hedge_mode_log.txt"
        self.csv_filename = f"logs/backpack_{ticker}_hedge_mode_trades.csv"
        self.original_stdout = sys.stdout

        # Initialize CSV file with headers if it doesn't exist
        self._initialize_csv_file()

        # Setup logger
        self.logger = logging.getLogger(f"hedge_bot_{ticker}")
        self.logger.setLevel(logging.INFO)

        # Clear any existing handlers to avoid duplicates
        self.logger.handlers.clear()

        # Disable verbose logging from external libraries
        logging.getLogger('urllib3').setLevel(logging.WARNING)
        logging.getLogger('requests').setLevel(logging.WARNING)
        logging.getLogger('websockets').setLevel(logging.WARNING)

        # Create file handler
        file_handler = logging.FileHandler(self.log_filename)
        file_handler.setLevel(logging.INFO)

        # Create console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)

        # Create different formatters for file and console
        file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        console_formatter = logging.Formatter('%(levelname)s:%(name)s:%(message)s')

        file_handler.setFormatter(file_formatter)
        console_handler.setFormatter(console_formatter)

        # Add handlers to logger
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)

        # Prevent propagation to root logger to avoid duplicate messages
        self.logger.propagate = False

        # State management
        self.stop_flag = False
        self.order_counter = 0

        # Backpack state
        self.backpack_client = None
        self.backpack_contract_id = None
        self.backpack_tick_size = None
        self.backpack_order_status = None

        # Backpack order book state for websocket-based BBO
        self.backpack_order_book = {'bids': {}, 'asks': {}}
        self.backpack_best_bid = None
        self.backpack_best_ask = None
        self.backpack_order_book_ready = False

        # Lighter order book state
        self.lighter_client = None
        self.lighter_order_book = {"bids": {}, "asks": {}}
        self.lighter_best_bid = None
        self.lighter_best_ask = None
        self.lighter_order_book_ready = False
        self.lighter_order_book_offset = 0
        self.lighter_order_book_sequence_gap = False
        self.lighter_snapshot_loaded = False
        self.lighter_order_book_lock = asyncio.Lock()

        # Lighter WebSocket state
        self.lighter_ws_task = None
        self.lighter_order_result = None

        # Lighter order management
        self.lighter_order_status = None
        self.lighter_order_price = None
        self.lighter_order_side = None
        self.lighter_order_size = None
        self.lighter_order_start_time = None
        self.active_lighter_client_order_id = None
        self.lighter_order_failed = False
        self.lighter_order_failure_reason = None

        # Strategy state
        self.waiting_for_lighter_fill = False
        self.wait_start_time = None
        self.circuit_breaker_triggered = False
        self.last_requote_reason = ""

        # Order execution tracking
        self.order_execution_complete = False

        # Current order details for immediate execution
        self.current_lighter_side = None
        self.current_lighter_quantity = None
        self.current_lighter_price = None
        self.lighter_order_info = None
        self.pending_lighter_hedges: List[Dict[str, Any]] = []
        self.backpack_order_hedged_size: Dict[str, Decimal] = {}
        self.lighter_order_contexts: Dict[str, Dict[str, Any]] = {}
        self.netting_buffer = HedgeNettingBuffer()
        self.netting_task = None
        self.current_hedge_component = "legacy"
        self.hedge_stage_stats: Dict[str, Dict[str, Decimal]] = {}
        self.total_hedge_fills = 0
        self._last_hedge_stats_log_time = time.time()
        self.funding_task = None
        self.funding_ramp_task = None
        self.last_backpack_funding_rate = None
        self.last_lighter_funding_rate = None
        self.last_backpack_next_funding_ts = None
        self.last_lighter_next_funding_ts = None
        self.core_bp_target_live: Optional[Decimal] = None
        self.sat_touched_upper = False
        self.sat_touched_lower = False

        # Lighter API configuration
        self.lighter_base_url = "https://mainnet.zklighter.elliot.ai"
        self.account_index = int(os.getenv('LIGHTER_ACCOUNT_INDEX'))
        self.api_key_index = int(os.getenv('LIGHTER_API_KEY_INDEX'))

        # Backpack configuration
        self.backpack_public_key = os.getenv('BACKPACK_PUBLIC_KEY')
        self.backpack_secret_key = os.getenv('BACKPACK_SECRET_KEY')
        self.backpack_min_quantity = Decimal('0')

    def _safe_decimal_env(self, key: str, default: Decimal) -> Decimal:
        raw = os.getenv(key)
        if raw is None or raw == "":
            return default
        try:
            return Decimal(raw)
        except Exception:
            if hasattr(self, "logger") and self.logger:
                self.logger.warning(f"Invalid decimal env {key}={raw}, fallback to {default}")
            else:
                print(f"Warning: invalid decimal env {key}={raw}, fallback to {default}")
            return default

    def _safe_bool_env(self, key: str, default: bool) -> bool:
        raw = os.getenv(key)
        if raw is None or raw == "":
            return default
        value = str(raw).strip().lower()
        if value in {"1", "true", "yes", "y", "on"}:
            return True
        if value in {"0", "false", "no", "n", "off"}:
            return False
        if hasattr(self, "logger") and self.logger:
            self.logger.warning(f"Invalid bool env {key}={raw}, fallback to {default}")
        else:
            print(f"Warning: invalid bool env {key}={raw}, fallback to {default}")
        return default

    def _safe_int_env(self, key: str, default: int) -> int:
        raw = os.getenv(key)
        if raw is None or raw == "":
            return default
        try:
            return int(raw)
        except Exception:
            if hasattr(self, "logger") and self.logger:
                self.logger.warning(f"Invalid int env {key}={raw}, fallback to {default}")
            else:
                print(f"Warning: invalid int env {key}={raw}, fallback to {default}")
            return default

    def log_execution_control_config(self):
        self.logger.info(
            "Execution controls: "
            f"strategy={self.strategy}, "
            f"funding_enabled={self.funding_enabled}, "
            f"funding_refresh={self.funding_refresh_seconds}s, "
            f"funding_switch_threshold={self.funding_switch_threshold_seconds}s, "
            f"funding_startup_retries={self.funding_startup_retries}, "
            f"funding_startup_retry_sleep={self.funding_startup_retry_sleep_seconds}s, "
            f"funding_exit_unwind_max_rounds={self.funding_exit_unwind_max_rounds}, "
            f"pre_trade_alignment={self.pre_trade_alignment_enabled}, "
            f"pre_trade_max_basis_bps={self.pre_trade_max_basis_bps}, "
            f"pre_trade_wait={self.pre_trade_wait_timeout_seconds}s, "
            f"hedge_alignment={self.hedge_alignment_enabled}, "
            f"hedge_align_init={self.hedge_alignment_initial_bps}bps, "
            f"hedge_align_max={self.hedge_alignment_max_bps}bps, "
            f"hedge_align_wait={self.hedge_alignment_wait_timeout_seconds}s, "
            f"hedge_force_on_timeout={self.hedge_alignment_force_on_timeout}, "
            f"lighter_cancel_after={self.lighter_cancel_after_seconds}s, "
            f"netting_window={self.netting_window_seconds}s, "
            f"sat_enabled={self.satellite_enabled}, "
            f"sat_order_qty={self.satellite_order_quantity}, "
            f"sat_band={self.satellite_band}, "
            f"sat_trades_per_iter={self.satellite_trades_per_iter}, "
            f"sat_core_tolerance={self.satellite_core_tolerance}, "
            f"sat_max_net_exposure={self.satellite_max_net_exposure}, "
            f"core_abs_target={self.core_target_abs_position}, "
            f"core_long_exchange={self.core_long_exchange}, "
            f"exit_flatten_tolerance={self.exit_flatten_tolerance}, "
            f"requote_drift_ticks={self.requote_price_drift_ticks}, "
            f"requote_min_age={self.requote_min_age_seconds}s, "
            f"requote_secondary_timeout={self.get_secondary_requote_timeout()}s, "
            f"stats_interval={self.hedge_stats_log_interval_seconds}s, "
            f"stats_every_fills={self.hedge_stats_log_every_n_fills}"
        )

    @staticmethod
    def funding_receive_side(funding_rate: Decimal) -> str:
        """Return which side receives funding on an exchange for a given rate.

        Assumption (standard perp convention): positive rate => longs pay, shorts receive.
        """
        if funding_rate > 0:
            return "short"
        if funding_rate < 0:
            return "long"
        return "neutral"

    @staticmethod
    def funding_pnl_per_notional(funding_rate: Decimal, side: str) -> Decimal:
        side = (side or "").lower()
        if funding_rate == 0:
            return Decimal("0")
        if funding_rate > 0:
            return funding_rate if side == "short" else -funding_rate
        # funding_rate < 0: longs receive abs(rate)
        return (-funding_rate) if side == "long" else funding_rate

    @staticmethod
    def position_side(position: Decimal) -> str:
        if position > 0:
            return "long"
        if position < 0:
            return "short"
        return "flat"

    def seconds_to_next_funding(self) -> Optional[float]:
        now = time.time()
        candidates = []
        if self.last_backpack_next_funding_ts:
            candidates.append(float(self.last_backpack_next_funding_ts) - now)
        if self.last_lighter_next_funding_ts:
            candidates.append(float(self.last_lighter_next_funding_ts) - now)
        if not candidates:
            return None
        return max(0.0, min(candidates))

    def compute_desired_core_bp_target(self, backpack_rate: Decimal, lighter_rate: Decimal) -> Decimal:
        """Choose which exchange to be long on to maximize funding PnL (per notional) while remaining hedged."""
        if self.core_target_abs_position <= 0:
            return Decimal("0")
        abs_pos = self.core_target_abs_position

        pnl_if_bp_long = self.funding_pnl_per_notional(backpack_rate, "long") + self.funding_pnl_per_notional(lighter_rate, "short")
        pnl_if_bp_short = self.funding_pnl_per_notional(backpack_rate, "short") + self.funding_pnl_per_notional(lighter_rate, "long")
        return abs_pos if pnl_if_bp_long >= pnl_if_bp_short else -abs_pos

    def _parse_funding_payload(self, payload: Any, *, symbol: str, market_id: Optional[int] = None) -> Tuple[Decimal, Optional[float]]:
        """Best-effort parse for funding rate + next timestamp."""
        def _extract(obj: Dict[str, Any]) -> Optional[Tuple[Decimal, Optional[float]]]:
            raw_rate = (obj.get("fundingRate") or obj.get("funding_rate") or obj.get("rate") or obj.get("funding"))
            if raw_rate is None:
                return None
            try:
                rate = Decimal(str(raw_rate))
            except Exception:
                return None
            ts = (
                obj.get("intervalEndTimestamp")
                or obj.get("nextFundingTimestamp")
                or obj.get("next_funding_timestamp")
                or obj.get("next_funding_time")
                or obj.get("fundingTime")
            )
            next_ts = None
            if ts is not None:
                try:
                    next_ts = float(ts) / 1000.0 if float(ts) > 10_000_000_000 else float(ts)
                except Exception:
                    next_ts = None
            return rate, next_ts

        if isinstance(payload, dict):
            # common wrappers
            for key in ("fundingRates", "funding_rates", "data", "result"):
                if key in payload:
                    inner = payload.get(key)
                    parsed = self._parse_funding_payload(inner, symbol=symbol, market_id=market_id)
                    if parsed[0] is not None:
                        return parsed
            # direct object
            parsed = _extract(payload)
            if parsed is not None:
                return parsed
        if isinstance(payload, list):
            for obj in payload:
                if not isinstance(obj, dict):
                    continue
                sym = str(obj.get("symbol", obj.get("market", ""))).upper()
                mid = obj.get("marketId") or obj.get("market_id") or obj.get("market_index") or obj.get("marketIndex")
                if (sym and sym == symbol.upper()) or (market_id is not None and mid is not None and int(mid) == int(market_id)):
                    parsed = _extract(obj)
                    if parsed is not None:
                        return parsed
            # fallback: first parseable
            for obj in payload:
                if isinstance(obj, dict):
                    parsed = _extract(obj)
                    if parsed is not None:
                        return parsed
        raise ValueError("Unrecognized funding payload shape")

    def get_backpack_funding(self) -> Tuple[Decimal, Optional[float]]:
        url = "https://api.backpack.exchange/api/v1/fundingRates"
        params = {"symbol": self.backpack_contract_id} if self.backpack_contract_id else None
        headers = {"accept": "application/json", "user-agent": "perp-dex-tools/hedge_mode_bp"}
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        return self._parse_funding_payload(payload, symbol=self.backpack_contract_id)

    def get_lighter_funding(self) -> Tuple[Decimal, Optional[float]]:
        url = f"{self.lighter_base_url}/api/v1/funding-rates"
        headers = {"accept": "application/json", "user-agent": "perp-dex-tools/hedge_mode_bp"}
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        payload = resp.json()
        return self._parse_funding_payload(payload, symbol=self.ticker, market_id=self.lighter_market_index)

    def refresh_funding_or_raise(self):
        bp_rate, bp_next = self.get_backpack_funding()
        lt_rate, lt_next = self.get_lighter_funding()
        self.last_backpack_funding_rate = bp_rate
        self.last_lighter_funding_rate = lt_rate
        self.last_backpack_next_funding_ts = bp_next
        self.last_lighter_next_funding_ts = lt_next
        self.logger.info(
            f"[FUNDING] backpack rate={bp_rate} recv={self.funding_receive_side(bp_rate)} next={bp_next} "
            f"| lighter rate={lt_rate} recv={self.funding_receive_side(lt_rate)} next={lt_next}"
        )

    async def funding_refresh_loop(self):
        while not self.stop_flag:
            try:
                self.refresh_funding_or_raise()
            except Exception as e:
                self.logger.error(f"❌ Funding refresh failed: {e}")
            await asyncio.sleep(max(60.0, self.funding_refresh_seconds))

    async def funding_startup_check_with_retries(self) -> bool:
        for attempt in range(1, self.funding_startup_retries + 1):
            try:
                self.logger.info(f"[FUNDING] Startup check attempt {attempt}/{self.funding_startup_retries}")
                self.refresh_funding_or_raise()
                self.logger.info("[FUNDING] Startup check succeeded")
                return True
            except Exception as e:
                self.logger.error(f"❌ Funding startup check failed on attempt {attempt}: {e}")
                if attempt < self.funding_startup_retries:
                    await asyncio.sleep(max(0.2, self.funding_startup_retry_sleep_seconds))
        return False

    async def _flatten_residual_lighter_only(self, max_rounds: int) -> bool:
        rounds = 0
        while rounds < max_rounds and not self.stop_flag:
            await self.refresh_positions()
            if abs(self.lighter_position) <= self.order_quantity:
                return True
            rounds += 1
            qty = min(abs(self.lighter_position), self.order_quantity)
            side = "sell" if self.lighter_position > 0 else "buy"
            reference = self.backpack_best_bid if side == "sell" else self.backpack_best_ask
            if reference is None:
                best_bid, best_ask = self.get_lighter_best_levels()
                if best_bid and best_ask:
                    reference = best_bid[0] if side == "sell" else best_ask[0]
            if reference is None:
                self.logger.error("❌ Unable to get reference price for residual lighter flatten")
                return False
            self.logger.warning(f"[EXIT-UNWIND] Flatten residual lighter via {side} {qty}, round={rounds}")
            prev_component = self.current_hedge_component
            self.current_hedge_component = "exit"
            try:
                result = await self.place_lighter_market_order(side, qty, reference)
                if result is None:
                    await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)
                    return False
            finally:
                self.current_hedge_component = prev_component
        await self.refresh_positions()
        return abs(self.lighter_position) <= self.order_quantity

    async def graceful_flatten_positions_before_exit(self) -> bool:
        self.logger.warning("[EXIT-UNWIND] Funding unavailable, attempting graceful flatten before exit")
        max_rounds = self.funding_exit_unwind_max_rounds
        rounds = 0
        while rounds < max_rounds and not self.stop_flag:
            await self.refresh_positions()
            bp_abs = abs(self.backpack_position)
            lt_abs = abs(self.lighter_position)
            if bp_abs <= self.order_quantity and lt_abs <= self.order_quantity:
                self.logger.info(
                    f"[EXIT-UNWIND] Flatten completed: backpack={self.backpack_position}, lighter={self.lighter_position}"
                )
                return True

            rounds += 1
            if bp_abs > self.order_quantity:
                side = "sell" if self.backpack_position > 0 else "buy"
                qty = self.normalize_backpack_order_quantity(min(bp_abs, self.order_quantity), allow_skip=True)
                if qty <= 0:
                    self.logger.warning("[EXIT-UNWIND] Backpack qty below min, switch to lighter residual flatten")
                    return await self._flatten_residual_lighter_only(max_rounds - rounds)
                self.logger.warning(
                    f"[EXIT-UNWIND] Close backpack via {side} {qty}, backpack={self.backpack_position}, lighter={self.lighter_position}, round={rounds}"
                )
                prev_component = self.current_hedge_component
                self.current_hedge_component = "exit"
                try:
                    await self.place_backpack_post_only_order(side, qty)
                    if self.strategy == "core_satellite":
                        await self.flush_netting_buffer(force=True, component="exit")
                    elif self.waiting_for_lighter_fill:
                        await self.process_pending_lighter_hedges()
                finally:
                    self.current_hedge_component = prev_component
            else:
                ok = await self._flatten_residual_lighter_only(max_rounds - rounds)
                if not ok:
                    break
            await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)

        await self.refresh_positions()
        self.logger.error(
            f"❌ [EXIT-UNWIND] Unable to fully flatten within limit: backpack={self.backpack_position}, lighter={self.lighter_position}"
        )
        return False

    def update_satellite_trade_progress(
        self,
        backpack_position: Decimal,
        core_bp_target: Decimal,
        lower_bound: Decimal,
        upper_bound: Decimal,
    ) -> bool:
        """Count one satellite trade when both bounds were visited and position returns near core target."""
        if backpack_position >= upper_bound - self.satellite_core_tolerance:
            self.sat_touched_upper = True
        if backpack_position <= lower_bound + self.satellite_core_tolerance:
            self.sat_touched_lower = True
        if self.sat_touched_upper and self.sat_touched_lower:
            if abs(backpack_position - core_bp_target) <= self.satellite_core_tolerance:
                self.sat_touched_upper = False
                self.sat_touched_lower = False
                return True
        return False

    async def flatten_all_positions_on_completion(self) -> bool:
        """Best-effort full flatten on normal completion: both exchanges to near-zero."""
        self.logger.info("[EXIT-FLAT] Iterations completed, flattening all positions to zero")
        max_rounds = self.funding_exit_unwind_max_rounds
        rounds = 0
        while rounds < max_rounds and not self.stop_flag:
            await self.refresh_positions()
            bp_abs = abs(self.backpack_position)
            lt_abs = abs(self.lighter_position)
            if bp_abs <= self.exit_flatten_tolerance and lt_abs <= self.exit_flatten_tolerance:
                self.logger.info(
                    f"[EXIT-FLAT] Completed: backpack={self.backpack_position}, lighter={self.lighter_position}"
                )
                return True

            rounds += 1
            if bp_abs > self.exit_flatten_tolerance:
                side = "sell" if self.backpack_position > 0 else "buy"
                qty = self.normalize_backpack_order_quantity(min(bp_abs, self.order_quantity), allow_skip=True)
                if qty > 0:
                    self.logger.info(
                        f"[EXIT-FLAT] Backpack close round={rounds} side={side} qty={qty} "
                        f"bp={self.backpack_position} lt={self.lighter_position}"
                    )
                    prev_component = self.current_hedge_component
                    self.current_hedge_component = "exit"
                    try:
                        await self.place_backpack_post_only_order(side, qty)
                        if self.strategy == "core_satellite":
                            await self.flush_netting_buffer(force=True, component="exit")
                        elif self.waiting_for_lighter_fill:
                            await self.process_pending_lighter_hedges()
                    finally:
                        self.current_hedge_component = prev_component
                    await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)
                    continue

            if lt_abs > self.exit_flatten_tolerance:
                side = "sell" if self.lighter_position > 0 else "buy"
                qty = min(lt_abs, self.order_quantity)
                reference_price = self.backpack_best_bid if side == "sell" else self.backpack_best_ask
                if reference_price is None:
                    best_bid, best_ask = self.get_lighter_best_levels()
                    if best_bid and best_ask:
                        reference_price = best_bid[0] if side == "sell" else best_ask[0]
                if reference_price is None:
                    self.logger.error("[EXIT-FLAT] Missing reference price, abort flatten")
                    return False
                self.logger.info(
                    f"[EXIT-FLAT] Lighter close round={rounds} side={side} qty={qty} "
                    f"bp={self.backpack_position} lt={self.lighter_position}"
                )
                prev_component = self.current_hedge_component
                self.current_hedge_component = "exit"
                try:
                    result = await self.place_lighter_market_order(side, qty, reference_price)
                    if result is None:
                        await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)
                        continue
                finally:
                    self.current_hedge_component = prev_component
            await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)

        await self.refresh_positions()
        self.logger.error(
            f"[EXIT-FLAT] Failed within limit: backpack={self.backpack_position}, lighter={self.lighter_position}"
        )
        return False

    async def funding_ramp_loop(self):
        while not self.stop_flag:
            try:
                if not self.funding_enabled or self.strategy != "core_satellite" or self.core_target_abs_position <= 0:
                    await asyncio.sleep(10.0)
                    continue
                if self.last_backpack_funding_rate is None or self.last_lighter_funding_rate is None:
                    await asyncio.sleep(10.0)
                    continue

                seconds_to = self.seconds_to_next_funding()
                if seconds_to is None or seconds_to > self.funding_switch_threshold_seconds:
                    await asyncio.sleep(self.funding_ramp_interval_seconds)
                    continue

                desired = self.compute_desired_core_bp_target(self.last_backpack_funding_rate, self.last_lighter_funding_rate)
                if self.core_bp_target_live is None:
                    self.core_bp_target_live = desired

                # Determine whether current BP side is paying funding
                bp_side = self.position_side(self.backpack_position)
                bp_recv = self.funding_receive_side(self.last_backpack_funding_rate)
                paying_bp = (bp_side != "flat" and bp_recv != "neutral" and bp_side != bp_recv)

                lt_side = self.position_side(self.lighter_position)
                lt_recv = self.funding_receive_side(self.last_lighter_funding_rate)
                paying_lt = (lt_side != "flat" and lt_recv != "neutral" and lt_side != lt_recv)

                if not (paying_bp or paying_lt):
                    await asyncio.sleep(self.funding_ramp_interval_seconds)
                    continue

                step = max(Decimal("0"), self.funding_ramp_step_quantity)
                if step <= 0:
                    await asyncio.sleep(self.funding_ramp_interval_seconds)
                    continue

                delta = desired - self.core_bp_target_live
                if abs(delta) <= step:
                    self.core_bp_target_live = desired
                else:
                    self.core_bp_target_live += step if delta > 0 else -step

                # Clamp to abs target
                if self.core_bp_target_live > self.core_target_abs_position:
                    self.core_bp_target_live = self.core_target_abs_position
                if self.core_bp_target_live < -self.core_target_abs_position:
                    self.core_bp_target_live = -self.core_target_abs_position

                self.logger.warning(
                    f"⚠️ Funding ramp active (t_to_funding={seconds_to:.0f}s): desired_bp_target={desired}, live_bp_target={self.core_bp_target_live}, "
                    f"paying_bp={paying_bp}, paying_lt={paying_lt}"
                )
            except Exception as e:
                self.logger.error(f"❌ Funding ramp error: {e}")
            await asyncio.sleep(max(10.0, self.funding_ramp_interval_seconds))

    def _ensure_stage_stats(self, stage: str) -> Dict[str, Decimal]:
        if stage not in self.hedge_stage_stats:
            self.hedge_stage_stats[stage] = {
                "fills": Decimal("0"),
                "volume_usd": Decimal("0"),
                "wear_usd": Decimal("0"),
            }
        return self.hedge_stage_stats[stage]

    def record_hedge_fill_stats(self, stage: str, reference_price: Decimal, filled_price: Decimal, filled_quantity: Decimal):
        if reference_price <= 0 or filled_quantity <= 0:
            return
        stats = self._ensure_stage_stats(stage or "unknown")
        volume_usd = abs(reference_price * filled_quantity)
        wear_usd = abs(filled_price - reference_price) * filled_quantity
        stats["fills"] += Decimal("1")
        stats["volume_usd"] += volume_usd
        stats["wear_usd"] += wear_usd
        self.total_hedge_fills += 1
        self.maybe_log_hedge_stage_stats()

    def maybe_log_hedge_stage_stats(self, force: bool = False):
        now = time.time()
        stale_order_ids = [
            order_id for order_id, context in self.lighter_order_contexts.items()
            if now - float(context.get("created_at", now)) > 3600
        ]
        for order_id in stale_order_ids:
            self.lighter_order_contexts.pop(order_id, None)
        if not force:
            interval_ready = (now - self._last_hedge_stats_log_time) >= self.hedge_stats_log_interval_seconds
            count_ready = (self.total_hedge_fills % self.hedge_stats_log_every_n_fills) == 0
            if not interval_ready and not count_ready:
                return
        self._last_hedge_stats_log_time = now
        if not self.hedge_stage_stats:
            return

        total_volume = sum(v["volume_usd"] for v in self.hedge_stage_stats.values())
        total_wear = sum(v["wear_usd"] for v in self.hedge_stage_stats.values())
        total_bps = (total_wear / total_volume * Decimal("10000")) if total_volume > 0 else Decimal("0")
        self.logger.info(
            f"[HEDGE-STATS][TOTAL] fills={self.total_hedge_fills} volume={total_volume:.2f}USD "
            f"wear={total_wear:.4f}USD wear_bps={total_bps:.3f}"
        )
        for stage in sorted(self.hedge_stage_stats.keys()):
            item = self.hedge_stage_stats[stage]
            stage_bps = (item["wear_usd"] / item["volume_usd"] * Decimal("10000")) if item["volume_usd"] > 0 else Decimal("0")
            self.logger.info(
                f"[HEDGE-STATS][{stage}] fills={int(item['fills'])} volume={item['volume_usd']:.2f}USD "
                f"wear={item['wear_usd']:.4f}USD wear_bps={stage_bps:.3f}"
            )

    def trigger_circuit_breaker(self, reason: str):
        """Trigger a controlled stop when critical hedging risk is detected."""
        if self.circuit_breaker_triggered:
            return
        self.circuit_breaker_triggered = True
        self.stop_flag = True
        self.transition_risk_state(RiskState.STRESSED, reason)
        self.logger.error(f"🚨 Circuit breaker triggered: {reason}")

    def transition_risk_state(self, next_state: RiskState, reason: str):
        """State machine transition helper."""
        if self.risk_state == next_state:
            return
        prev_state = self.risk_state
        self.risk_state = next_state
        self.state_changed_at = time.time()
        self.logger.warning(f"Risk state transition: {prev_state.value} -> {next_state.value}, reason={reason}")

    def _event_on_cooldown(self, event: RiskEvent) -> bool:
        now = time.time()
        last = self._last_event_time.get(event.value)
        if last is None:
            self._last_event_time[event.value] = now
            return False
        if now - last < self.event_cooldown_seconds:
            return True
        self._last_event_time[event.value] = now
        return False

    def publish_risk_event(self, event: RiskEvent, detail: str = "", force: bool = False):
        """Event-driven risk entrypoint."""
        if not force and self._event_on_cooldown(event):
            return

        if event == RiskEvent.MARKET_DATA_UNHEALTHY:
            self.transition_risk_state(RiskState.STRESSED, detail or "market_data_unhealthy")
        elif event == RiskEvent.MARKET_DATA_RECOVERED:
            if self.risk_state == RiskState.STRESSED and not self.circuit_breaker_triggered:
                self.transition_risk_state(RiskState.NORMAL, detail or "market_data_recovered")
        elif event == RiskEvent.BASIS_WIDE:
            if self.risk_state == RiskState.NORMAL:
                self.transition_risk_state(RiskState.WIDE, detail or "basis_wide")
        elif event == RiskEvent.BASIS_NORMAL:
            if self.risk_state == RiskState.WIDE:
                self.transition_risk_state(RiskState.NORMAL, detail or "basis_normal")
        elif event == RiskEvent.SOFT_EXPOSURE_BREACH:
            self.transition_risk_state(RiskState.WIDE, detail or "soft_exposure")
        elif event == RiskEvent.HARD_EXPOSURE_BREACH:
            self.trigger_circuit_breaker(detail or event.value)
        elif event in [RiskEvent.LIGHTER_HEDGE_TIMEOUT, RiskEvent.TRADE_COMPLETION_TIMEOUT]:
            self.transition_risk_state(RiskState.WIDE, detail or event.value)
        elif event == RiskEvent.LIGHTER_HEDGE_ERROR:
            self.transition_risk_state(RiskState.STRESSED, detail or event.value)

    def get_state_adjusted_sleep_seconds(self) -> float:
        if self.risk_state == RiskState.STRESSED:
            return 2.0
        if self.risk_state == RiskState.WIDE:
            return 0.5
        return 0.0

    def get_state_adjusted_order_quantity(self, requested_qty: Decimal) -> Decimal:
        if requested_qty <= 0:
            return Decimal('0')
        if self.risk_state == RiskState.WIDE:
            reduced = requested_qty / Decimal("2")
            if reduced >= self.order_quantity / Decimal("10"):
                requested_qty = reduced
        return self.normalize_backpack_order_quantity(requested_qty)

    def normalize_backpack_order_quantity(self, requested_qty: Decimal, allow_skip: bool = False) -> Decimal:
        if requested_qty <= 0:
            return Decimal('0')
        min_qty = self.backpack_min_quantity if self.backpack_min_quantity else Decimal('0')
        if min_qty > 0 and requested_qty < min_qty:
            if allow_skip:
                return Decimal('0')
            return min_qty
        return requested_qty

    def shutdown(self, signum=None, frame=None):
        """Graceful shutdown handler."""
        self.stop_flag = True
        self.logger.info("\n🛑 Stopping...")
        self.maybe_log_hedge_stage_stats(force=True)

        # Close WebSocket connections
        if self.backpack_client:
            try:
                # Note: disconnect() is async, but shutdown() is sync
                # We'll let the cleanup happen naturally
                self.logger.info("🔌 Backpack WebSocket will be disconnected")
            except Exception as e:
                self.logger.error(f"Error disconnecting Backpack WebSocket: {e}")

        # Cancel Lighter WebSocket task
        if self.lighter_ws_task and not self.lighter_ws_task.done():
            try:
                self.lighter_ws_task.cancel()
                self.logger.info("🔌 Lighter WebSocket task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling Lighter WebSocket task: {e}")

        if self.netting_task and not self.netting_task.done():
            try:
                self.netting_task.cancel()
                self.logger.info("🔌 Netting task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling netting task: {e}")

        if self.funding_task and not self.funding_task.done():
            try:
                self.funding_task.cancel()
                self.logger.info("🔌 Funding task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling funding task: {e}")

        if self.funding_ramp_task and not self.funding_ramp_task.done():
            try:
                self.funding_ramp_task.cancel()
                self.logger.info("🔌 Funding ramp task cancelled")
            except Exception as e:
                self.logger.error(f"Error cancelling funding ramp task: {e}")

        # Close logging handlers properly
        for handler in self.logger.handlers[:]:
            try:
                handler.close()
                self.logger.removeHandler(handler)
            except Exception:
                pass

    def _initialize_csv_file(self):
        """Initialize CSV file with headers if it doesn't exist."""
        if not os.path.exists(self.csv_filename):
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['exchange', 'timestamp', 'side', 'price', 'quantity'])

    def log_trade_to_csv(self, exchange: str, side: str, price: str, quantity: str):
        """Log trade details to CSV file."""
        timestamp = datetime.now(pytz.UTC).isoformat()

        with open(self.csv_filename, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                exchange,
                timestamp,
                side,
                price,
                quantity
            ])

        self.logger.info(f"📊 Trade logged to CSV: {exchange} {side} {quantity} @ {price}")

    def handle_lighter_order_result(self, order_data):
        """Handle Lighter order result from WebSocket."""
        try:
            order_data["avg_filled_price"] = (Decimal(order_data["filled_quote_amount"]) /
                                              Decimal(order_data["filled_base_amount"]))
            if order_data["is_ask"]:
                order_data["side"] = "SHORT"
                order_type = "OPEN"
                self.lighter_position -= Decimal(order_data["filled_base_amount"])
            else:
                order_data["side"] = "LONG"
                order_type = "CLOSE"
                self.lighter_position += Decimal(order_data["filled_base_amount"])
            
            client_order_index = order_data["client_order_id"]
            context = self.lighter_order_contexts.pop(str(client_order_index), None)

            self.logger.info(f"[{client_order_index}] [{order_type}] [Lighter] [FILLED]: "
                             f"{order_data['filled_base_amount']} @ {order_data['avg_filled_price']}")
            if context:
                filled_qty = Decimal(order_data["filled_base_amount"])
                filled_price = Decimal(order_data["avg_filled_price"])
                reference_price = Decimal(context.get("reference_price", "0"))
                stage = str(context.get("stage", "unknown"))
                self.record_hedge_fill_stats(stage, reference_price, filled_price, filled_qty)

            # Log Lighter trade to CSV
            self.log_trade_to_csv(
                exchange='Lighter',
                side=order_data['side'],
                price=str(order_data['avg_filled_price']),
                quantity=str(order_data['filled_base_amount'])
            )

            # Mark execution as complete
            self.lighter_order_filled = True  # Mark order as filled
            self.lighter_order_failed = False
            self.lighter_order_failure_reason = None
            self.active_lighter_client_order_id = None
            self.order_execution_complete = True

        except Exception as e:
            self.logger.error(f"Error handling Lighter order result: {e}")

    def handle_lighter_order_terminal_update(self, order_data):
        """Handle non-filled terminal update (cancel/reject/expire) for active Lighter order."""
        try:
            status = str(order_data.get("status", "")).lower()
            if status not in {"canceled", "cancelled", "rejected", "expired"}:
                return
            client_order_id = str(order_data.get("client_order_id", ""))
            if not client_order_id:
                return
            if self.active_lighter_client_order_id and client_order_id != str(self.active_lighter_client_order_id):
                return

            self.lighter_order_status = status.upper()
            self.lighter_order_filled = False
            self.lighter_order_failed = True
            self.lighter_order_failure_reason = f"order_{status}"
            self.active_lighter_client_order_id = None
            self.lighter_order_contexts.pop(client_order_id, None)
            self.logger.warning(
                f"[{client_order_id}] [Lighter] terminal status received: {status}, will retry hedge"
            )
        except Exception as e:
            self.logger.error(f"Error handling Lighter terminal update: {e}")

    async def reset_lighter_order_book(self):
        """Reset Lighter order book state."""
        async with self.lighter_order_book_lock:
            self.lighter_order_book["bids"].clear()
            self.lighter_order_book["asks"].clear()
            self.lighter_order_book_offset = 0
            self.lighter_order_book_sequence_gap = False
            self.lighter_snapshot_loaded = False
            self.lighter_best_bid = None
            self.lighter_best_ask = None

    def update_lighter_order_book(self, side: str, levels: list):
        """Update Lighter order book with new levels."""
        for level in levels:
            # Handle different data structures - could be list [price, size] or dict {"price": ..., "size": ...}
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(level[0])
                size = Decimal(level[1])
            elif isinstance(level, dict):
                price = Decimal(level.get("price", 0))
                size = Decimal(level.get("size", 0))
            else:
                self.logger.warning(f"⚠️ Unexpected level format: {level}")
                continue

            if size > 0:
                self.lighter_order_book[side][price] = size
            else:
                # Remove zero size orders
                self.lighter_order_book[side].pop(price, None)

    def validate_order_book_offset(self, new_offset: int) -> bool:
        """Validate order book offset sequence."""
        if new_offset <= self.lighter_order_book_offset:
            self.logger.warning(
                f"⚠️ Out-of-order update: new_offset={new_offset}, current_offset={self.lighter_order_book_offset}")
            return False
        return True

    def validate_order_book_integrity(self) -> bool:
        """Validate order book integrity."""
        # Check for negative prices or sizes
        for side in ["bids", "asks"]:
            for price, size in self.lighter_order_book[side].items():
                if price <= 0 or size <= 0:
                    self.logger.error(f"❌ Invalid order book data: {side} price={price}, size={size}")
                    return False
        return True

    def get_lighter_best_levels(self) -> Tuple[Tuple[Decimal, Decimal], Tuple[Decimal, Decimal]]:
        """Get best bid and ask levels from Lighter order book."""
        best_bid = None
        best_ask = None

        if self.lighter_order_book["bids"]:
            best_bid_price = max(self.lighter_order_book["bids"].keys())
            best_bid_size = self.lighter_order_book["bids"][best_bid_price]
            best_bid = (best_bid_price, best_bid_size)

        if self.lighter_order_book["asks"]:
            best_ask_price = min(self.lighter_order_book["asks"].keys())
            best_ask_size = self.lighter_order_book["asks"][best_ask_price]
            best_ask = (best_ask_price, best_ask_size)

        return best_bid, best_ask

    def get_lighter_mid_price(self) -> Decimal:
        """Get mid price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate mid price - missing order book data")

        mid_price = (best_bid[0] + best_ask[0]) / Decimal('2')
        return mid_price

    def get_lighter_order_price(self, is_ask: bool) -> Decimal:
        """Get order price from Lighter order book."""
        best_bid, best_ask = self.get_lighter_best_levels()

        if best_bid is None or best_ask is None:
            raise Exception("Cannot calculate order price - missing order book data")

        if is_ask:
            order_price = best_bid[0] + self.tick_size
        else:
            order_price = best_ask[0] - self.tick_size

        return order_price

    def calculate_adjusted_price(self, original_price: Decimal, side: str, adjustment_percent: Decimal) -> Decimal:
        """Calculate adjusted price for order modification."""
        adjustment = original_price * adjustment_percent

        if side.lower() == 'buy':
            # For buy orders, increase price to improve fill probability
            return original_price + adjustment
        else:
            # For sell orders, decrease price to improve fill probability
            return original_price - adjustment

    async def request_fresh_snapshot(self, ws):
        """Request fresh order book snapshot."""
        await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

    async def handle_lighter_ws(self):
        """Handle Lighter WebSocket connection and messages."""
        url = "wss://mainnet.zklighter.elliot.ai/stream"
        cleanup_counter = 0

        while not self.stop_flag:
            timeout_count = 0
            try:
                # Reset order book state before connecting
                await self.reset_lighter_order_book()

                async with websockets.connect(url) as ws:
                    # Subscribe to order book updates
                    await ws.send(json.dumps({"type": "subscribe", "channel": f"order_book/{self.lighter_market_index}"}))

                    # Subscribe to account orders updates
                    account_orders_channel = f"account_orders/{self.lighter_market_index}/{self.account_index}"

                    # Get auth token for the subscription
                    try:
                        auth_token, err = self.lighter_client.create_auth_token_with_expiry(api_key_index=self.api_key_index)
                        if err is not None:
                            self.logger.warning(f"⚠️ Failed to create auth token for account orders subscription: {err}")
                        else:
                            auth_message = {
                                "type": "subscribe",
                                "channel": account_orders_channel,
                                "auth": auth_token
                            }
                            await ws.send(json.dumps(auth_message))
                            self.logger.info("✅ Subscribed to account orders with auth token (expires in 10 minutes)")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Error creating auth token for account orders subscription: {e}")

                    while not self.stop_flag:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=1)

                            try:
                                data = json.loads(msg)
                            except json.JSONDecodeError as e:
                                self.logger.warning(f"⚠️ JSON parsing error in Lighter websocket: {e}")
                                continue

                            # Reset timeout counter on successful message
                            timeout_count = 0

                            async with self.lighter_order_book_lock:
                                if data.get("type") == "subscribed/order_book":
                                    # Initial snapshot - clear and populate the order book
                                    self.lighter_order_book["bids"].clear()
                                    self.lighter_order_book["asks"].clear()

                                    # Handle the initial snapshot
                                    order_book = data.get("order_book", {})
                                    if order_book and "offset" in order_book:
                                        self.lighter_order_book_offset = order_book["offset"]
                                        self.logger.info(f"✅ Initial order book offset set to: {self.lighter_order_book_offset}")

                                    # Debug: Log the structure of bids and asks
                                    bids = order_book.get("bids", [])
                                    asks = order_book.get("asks", [])
                                    if bids:
                                        self.logger.debug(f"📊 Sample bid structure: {bids[0] if bids else 'None'}")
                                    if asks:
                                        self.logger.debug(f"📊 Sample ask structure: {asks[0] if asks else 'None'}")

                                    self.update_lighter_order_book("bids", bids)
                                    self.update_lighter_order_book("asks", asks)
                                    self.lighter_snapshot_loaded = True
                                    self.lighter_order_book_ready = True
                                    self.publish_risk_event(RiskEvent.MARKET_DATA_RECOVERED, "lighter_snapshot_loaded")

                                    self.logger.info(f"✅ Lighter order book snapshot loaded with "
                                                     f"{len(self.lighter_order_book['bids'])} bids and "
                                                     f"{len(self.lighter_order_book['asks'])} asks")

                                elif data.get("type") == "update/order_book" and self.lighter_snapshot_loaded:
                                    # Extract offset from the message
                                    order_book = data.get("order_book", {})
                                    if not order_book or "offset" not in order_book:
                                        self.logger.warning("⚠️ Order book update missing offset, skipping")
                                        continue

                                    new_offset = order_book["offset"]

                                    # Validate offset sequence
                                    if not self.validate_order_book_offset(new_offset):
                                        self.lighter_order_book_sequence_gap = True
                                        self.lighter_order_book_ready = False
                                        self.publish_risk_event(RiskEvent.MARKET_DATA_UNHEALTHY, "lighter_sequence_gap")
                                        break

                                    # Update the order book with new data
                                    self.update_lighter_order_book("bids", order_book.get("bids", []))
                                    self.update_lighter_order_book("asks", order_book.get("asks", []))

                                    # Validate order book integrity after update
                                    if not self.validate_order_book_integrity():
                                        self.lighter_order_book_ready = False
                                        self.publish_risk_event(RiskEvent.MARKET_DATA_UNHEALTHY, "lighter_book_integrity_failed")
                                        self.logger.warning("🔄 Order book integrity check failed, requesting fresh snapshot...")
                                        break

                                    # Get the best bid and ask levels
                                    best_bid, best_ask = self.get_lighter_best_levels()

                                    # Update global variables
                                    if best_bid is not None:
                                        self.lighter_best_bid = best_bid[0]
                                    if best_ask is not None:
                                        self.lighter_best_ask = best_ask[0]

                                elif data.get("type") == "ping":
                                    # Respond to ping with pong
                                    await ws.send(json.dumps({"type": "pong"}))
                                elif data.get("type") == "update/account_orders":
                                    # Handle account orders updates
                                    orders = data.get("orders", {}).get(str(self.lighter_market_index), [])
                                    for order in orders:
                                        if str(order.get("status", "")).lower() == "filled":
                                            self.handle_lighter_order_result(order)
                                        else:
                                            self.handle_lighter_order_terminal_update(order)
                                elif data.get("type") == "update/order_book" and not self.lighter_snapshot_loaded:
                                    # Ignore updates until we have the initial snapshot
                                    continue

                            # Periodic cleanup outside the lock
                            cleanup_counter += 1
                            if cleanup_counter >= 1000:
                                cleanup_counter = 0

                            # Handle sequence gap and integrity issues outside the lock
                            if self.lighter_order_book_sequence_gap:
                                try:
                                    await self.request_fresh_snapshot(ws)
                                    self.lighter_order_book_sequence_gap = False
                                except Exception as e:
                                    self.logger.error(f"⚠️ Failed to request fresh snapshot: {e}")
                                    break

                        except asyncio.TimeoutError:
                            timeout_count += 1
                            if timeout_count % 3 == 0:
                                self.logger.warning(f"⏰ No message from Lighter websocket for {timeout_count} seconds")
                            continue
                        except websockets.exceptions.ConnectionClosed as e:
                            self.logger.warning(f"⚠️ Lighter websocket connection closed: {e}")
                            break
                        except websockets.exceptions.WebSocketException as e:
                            self.logger.warning(f"⚠️ Lighter websocket error: {e}")
                            break
                        except Exception as e:
                            self.logger.error(f"⚠️ Error in Lighter websocket: {e}")
                            self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                            break
            except Exception as e:
                self.logger.error(f"⚠️ Failed to connect to Lighter websocket: {e}")

            # Wait a bit before reconnecting
            await asyncio.sleep(2)

    def setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def initialize_lighter_client(self):
        """Initialize the Lighter client."""
        if self.lighter_client is None:
            api_key_private_key = os.getenv('API_KEY_PRIVATE_KEY')
            if not api_key_private_key:
                raise Exception("API_KEY_PRIVATE_KEY environment variable not set")

            self.lighter_client = SignerClient(
                url=self.lighter_base_url,
                account_index=self.account_index,
                api_private_keys={self.api_key_index: api_key_private_key}
            )

            # Check client
            err = self.lighter_client.check_client()
            if err is not None:
                raise Exception(f"CheckClient error: {err}")

            self.logger.info("✅ Lighter client initialized successfully")
        return self.lighter_client

    def initialize_backpack_client(self):
        """Initialize the Backpack client."""
        if not self.backpack_public_key or not self.backpack_secret_key:
            raise ValueError("BACKPACK_PUBLIC_KEY and BACKPACK_SECRET_KEY must be set in environment variables")

        # Create config for Backpack client
        config_dict = {
            'ticker': self.ticker,
            'contract_id': '',  # Will be set when we get contract info
            'quantity': self.order_quantity,
            'tick_size': Decimal('0.01'),  # Will be updated when we get contract info
            'close_order_side': 'sell'  # Default, will be updated based on strategy
        }

        # Wrap in Config class for Backpack client
        config = Config(config_dict)

        # Initialize Backpack client
        self.backpack_client = BackpackClient(config)

        self.logger.info("✅ Backpack client initialized successfully")
        return self.backpack_client

    def get_lighter_market_config(self) -> Tuple[int, int, int, Decimal]:
        """Get Lighter market configuration."""
        url = f"{self.lighter_base_url}/api/v1/orderBooks"
        headers = {"accept": "application/json"}

        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()

            if not response.text.strip():
                raise Exception("Empty response from Lighter API")

            data = response.json()

            if "order_books" not in data:
                raise Exception("Unexpected response format")

            for market in data["order_books"]:
                if market["symbol"] == self.ticker:
                    price_multiplier = pow(10, market["supported_price_decimals"])
                    return (market["market_id"], 
                           pow(10, market["supported_size_decimals"]), 
                           price_multiplier,
                           Decimal("1") / (Decimal("10") ** market["supported_price_decimals"])
                           )
            raise Exception(f"Ticker {self.ticker} not found")

        except Exception as e:
            self.logger.error(f"⚠️ Error getting market config: {e}")
            raise

    async def get_backpack_contract_info(self) -> Tuple[str, Decimal]:
        """Get Backpack contract ID and tick size."""
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        contract_id, tick_size = await self.backpack_client.get_contract_attributes()
        self.backpack_min_quantity = Decimal('0')
        try:
            markets = self.backpack_client.public_client.get_markets()
            for market in markets:
                if market.get('symbol', '') == contract_id:
                    self.backpack_min_quantity = Decimal(
                        market.get('filters', {}).get('quantity', {}).get('minQuantity', '0')
                    )
                    break
            if self.backpack_min_quantity > 0:
                self.logger.info(f"Backpack min quantity loaded: {self.backpack_min_quantity}")
        except Exception as e:
            self.logger.warning(f"⚠️ Failed to fetch Backpack min quantity: {e}")

        if self.order_quantity < self.backpack_client.config.quantity:
            raise ValueError(
                f"Order quantity is less than min quantity: {self.order_quantity} < {self.backpack_client.config.quantity}")

        return contract_id, tick_size

    async def get_backpack_position(self) -> Decimal:
        """Get Backpack position."""
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        return await self.backpack_client.get_account_positions()

    async def fetch_backpack_bbo_prices(self) -> Tuple[Decimal, Decimal]:
        """Fetch best bid/ask prices from Backpack using websocket data."""
        # Use WebSocket data if available
        if self.backpack_order_book_ready and self.backpack_best_bid and self.backpack_best_ask:
            if self.backpack_best_bid > 0 and self.backpack_best_ask > 0 and self.backpack_best_bid < self.backpack_best_ask:
                return self.backpack_best_bid, self.backpack_best_ask

        # Fallback to REST API if websocket data is not available
        self.logger.warning("WebSocket BBO data not available, falling back to REST API")
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        best_bid, best_ask = await self.backpack_client.fetch_bbo_prices(self.backpack_contract_id)

        return best_bid, best_ask

    def round_to_tick(self, price: Decimal) -> Decimal:
        """Round price to tick size."""
        if self.backpack_tick_size is None:
            return price
        return (price / self.backpack_tick_size).quantize(Decimal('1')) * self.backpack_tick_size

    def compute_lighter_slippage(self, best_bid: Decimal, best_ask: Decimal) -> Decimal:
        """Compute dynamic slippage ratio capped by configured maximum."""
        if best_bid <= 0 or best_ask <= 0:
            return self.base_lighter_slippage
        mid = (best_bid + best_ask) / Decimal('2')
        spread_ratio = (best_ask - best_bid) / mid if mid > 0 else Decimal('0')
        dynamic_slippage = self.base_lighter_slippage + (spread_ratio * Decimal('2'))
        bounded_slippage = min(self.max_lighter_slippage, max(self.base_lighter_slippage, dynamic_slippage))
        if self.risk_state == RiskState.WIDE:
            return min(bounded_slippage, self.max_lighter_slippage * Decimal('0.7'))
        if self.risk_state == RiskState.STRESSED:
            return self.max_lighter_slippage
        return bounded_slippage

    def compute_lighter_aggressive_price(self, lighter_side: str, best_bid: Decimal, best_ask: Decimal) -> Decimal:
        """Get aggressive but capped price for Lighter hedge leg."""
        slippage_ratio = self.compute_lighter_slippage(best_bid, best_ask)
        if lighter_side.lower() == 'buy':
            raw_price = best_ask * (Decimal('1') + slippage_ratio)
        else:
            raw_price = best_bid * (Decimal('1') - slippage_ratio)
        return (raw_price / self.tick_size).quantize(Decimal('1')) * self.tick_size

    def normalize_lighter_order_params(self, quantity: Decimal, price: Decimal) -> Tuple[Decimal, Decimal, int, int]:
        """Normalize quantity/price to Lighter precision and return integer payload values."""
        base_amount = int((quantity * self.base_amount_multiplier).to_integral_value())
        if base_amount <= 0:
            raise ValueError(f"Lighter normalized base_amount is zero: quantity={quantity}")
        normalized_quantity = Decimal(base_amount) / Decimal(self.base_amount_multiplier)
        normalized_price = (price / self.tick_size).quantize(Decimal('1')) * self.tick_size
        price_int = int((normalized_price * self.price_multiplier).to_integral_value())
        if price_int <= 0:
            raise ValueError(f"Lighter normalized price is zero: price={price}")
        return normalized_quantity, normalized_price, base_amount, price_int

    def calculate_basis_bps(self, reference_price: Decimal, hedge_price: Decimal) -> Decimal:
        if reference_price <= 0:
            return Decimal('0')
        return abs(hedge_price - reference_price) / reference_price * Decimal('10000')

    def get_hedge_alignment_threshold_bps(self, elapsed_seconds: float, net_exposure: Optional[Decimal] = None) -> Decimal:
        if net_exposure is not None and net_exposure >= self.hard_exposure_limit:
            return self.hedge_alignment_max_bps
        if self.hedge_alignment_widen_step_bps <= 0 or self.hedge_alignment_widen_interval_seconds <= 0:
            return self.hedge_alignment_initial_bps
        widen_rounds = int(elapsed_seconds / self.hedge_alignment_widen_interval_seconds)
        widened = self.hedge_alignment_initial_bps + self.hedge_alignment_widen_step_bps * Decimal(str(widen_rounds))
        return min(self.hedge_alignment_max_bps, widened)

    async def wait_for_pre_trade_alignment(self, side: str) -> bool:
        if not self.pre_trade_alignment_enabled:
            return True
        if self.backpack_best_bid is None or self.backpack_best_ask is None:
            return False

        reference_price = self.backpack_best_ask if side.lower() == "buy" else self.backpack_best_bid
        lighter_side = "sell" if side.lower() == "buy" else "buy"
        start_time = time.time()
        last_log_time = 0.0
        basis_bps = Decimal("0")
        while not self.stop_flag:
            best_bid, best_ask = self.get_lighter_best_levels()
            if best_bid and best_ask:
                hedge_reference = best_ask[0] if lighter_side.lower() == 'buy' else best_bid[0]
                basis_bps = self.calculate_basis_bps(reference_price, hedge_reference)
                if basis_bps <= self.pre_trade_max_basis_bps:
                    return True
                if basis_bps >= self.wide_basis_bps:
                    self.publish_risk_event(RiskEvent.BASIS_WIDE, f"basis_bps={basis_bps:.2f}")
                now = time.time()
                if now - last_log_time >= 1.0:
                    self.logger.info(
                        f"[PRE-ALIGN] side={side} basis={basis_bps:.2f}bps "
                        f"threshold={self.pre_trade_max_basis_bps}bps"
                    )
                    last_log_time = now
            if time.time() - start_time >= self.pre_trade_wait_timeout_seconds:
                self.logger.info(
                    f"Skip maker leg due to pre-trade basis: side={side}, "
                    f"basis={basis_bps:.2f}bps > {self.pre_trade_max_basis_bps}bps"
                )
                return False
            await asyncio.sleep(self.pre_trade_retry_sleep_seconds)
        return False

    async def wait_for_acceptable_basis(self, reference_price: Decimal, lighter_side: str) -> Tuple[bool, Dict[str, Decimal]]:
        """Wait for hedge price alignment window before sending Lighter hedge order."""
        if not self.hedge_alignment_enabled:
            return True, {
                "basis_bps": Decimal("0"),
                "threshold_bps": Decimal("0"),
                "stage": "alignment_disabled",
            }
        start_time = time.time()
        last_log_time = 0.0
        latest_basis_bps = Decimal("0")
        latest_threshold_bps = self.hedge_alignment_initial_bps
        while not self.stop_flag:
            best_bid, best_ask = self.get_lighter_best_levels()
            if best_bid and best_ask:
                hedge_reference = best_ask[0] if lighter_side.lower() == 'buy' else best_bid[0]
                latest_basis_bps = self.calculate_basis_bps(reference_price, hedge_reference)
                elapsed = time.time() - start_time
                net_exposure = abs(self.backpack_position + self.lighter_position)
                latest_threshold_bps = self.get_hedge_alignment_threshold_bps(elapsed, net_exposure)
                if latest_basis_bps <= latest_threshold_bps:
                    self.publish_risk_event(RiskEvent.BASIS_NORMAL, f"basis_bps={latest_basis_bps:.2f}")
                    stage = "within_initial_band" if latest_threshold_bps <= self.hedge_alignment_initial_bps else "within_widened_band"
                    return True, {
                        "basis_bps": latest_basis_bps,
                        "threshold_bps": latest_threshold_bps,
                        "stage": stage,
                    }
                if latest_basis_bps >= self.wide_basis_bps:
                    self.publish_risk_event(RiskEvent.BASIS_WIDE, f"basis_bps={latest_basis_bps:.2f}")
                now = time.time()
                if now - last_log_time >= 1.0:
                    self.logger.info(
                        f"[HEDGE-ALIGN] side={lighter_side} basis={latest_basis_bps:.2f}bps "
                        f"threshold={latest_threshold_bps:.2f}bps"
                    )
                    last_log_time = now
            if time.time() - start_time >= self.hedge_alignment_wait_timeout_seconds:
                self.logger.warning(
                    f"Basis alignment timeout: basis={latest_basis_bps:.2f}bps threshold={latest_threshold_bps:.2f}bps "
                    f"side={lighter_side} timeout={self.hedge_alignment_wait_timeout_seconds}s"
                )
                return False, {
                    "basis_bps": latest_basis_bps,
                    "threshold_bps": latest_threshold_bps,
                    "stage": "alignment_timeout",
                }
            await asyncio.sleep(self.hedge_alignment_retry_sleep_seconds)
        return False, {
            "basis_bps": latest_basis_bps,
            "threshold_bps": latest_threshold_bps,
            "stage": "alignment_interrupted",
        }

    def is_market_data_healthy(self) -> bool:
        if not self.backpack_order_book_ready or not self.lighter_order_book_ready:
            return False
        if self.lighter_order_book_sequence_gap:
            return False
        if self.backpack_best_bid is None or self.backpack_best_ask is None:
            return False
        if self.lighter_best_bid is None or self.lighter_best_ask is None:
            return False
        return True

    async def wait_for_market_data_health(self, timeout: int = 10) -> bool:
        start = time.time()
        while not self.stop_flag:
            if self.is_market_data_healthy():
                self.publish_risk_event(RiskEvent.MARKET_DATA_RECOVERED, "market_data_healthy")
                return True
            if time.time() - start > timeout:
                self.logger.warning("⚠️ Market data not healthy within timeout, skip this cycle")
                self.publish_risk_event(RiskEvent.MARKET_DATA_UNHEALTHY, "health_timeout")
                return False
            await asyncio.sleep(0.5)
        return False

    def get_dynamic_requote_timeout(self) -> float:
        """Adaptive timeout for maker order cancel/requote."""
        if self.risk_state == RiskState.STRESSED:
            return 4.0
        if self.risk_state == RiskState.WIDE:
            return 12.0
        if self.backpack_best_bid and self.backpack_best_ask and self.backpack_best_ask > self.backpack_best_bid:
            mid = (self.backpack_best_bid + self.backpack_best_ask) / Decimal('2')
            spread_ratio = (self.backpack_best_ask - self.backpack_best_bid) / mid if mid > 0 else Decimal('0')
            if spread_ratio <= Decimal('0.0002'):
                return 6.0
            if spread_ratio >= Decimal('0.0010'):
                return 12.0
        return 9.0

    def get_secondary_requote_timeout(self) -> float:
        if self.requote_secondary_timeout_seconds > 0:
            return self.requote_secondary_timeout_seconds
        return self.get_dynamic_requote_timeout()

    async def should_requote_backpack_order(self, side: str, order_price: Optional[Decimal], start_time: float) -> bool:
        elapsed = time.time() - start_time
        secondary_timeout = self.get_secondary_requote_timeout()
        if order_price is None:
            if elapsed > secondary_timeout:
                self.last_requote_reason = f"time_only elapsed={elapsed:.2f}s>{secondary_timeout:.2f}s"
                return True
            return False
        if elapsed > secondary_timeout:
            self.last_requote_reason = f"time_fallback elapsed={elapsed:.2f}s>{secondary_timeout:.2f}s"
            return True
        if self.backpack_best_bid is None or self.backpack_best_ask is None or self.backpack_tick_size is None:
            self.last_requote_reason = "price_check_unavailable"
            return False

        price_drift_ticks = self.requote_price_drift_ticks * self.backpack_tick_size
        drift_triggered = False
        drift_size = Decimal('0')
        if side.lower() == 'buy':
            drift_size = self.backpack_best_ask - order_price
            drift_triggered = drift_size > price_drift_ticks
        else:
            drift_size = order_price - self.backpack_best_bid
            drift_triggered = drift_size > price_drift_ticks

        if drift_triggered and elapsed >= self.requote_min_age_seconds:
            self.last_requote_reason = (
                f"price_drift drift={drift_size} threshold={price_drift_ticks} elapsed={elapsed:.2f}s"
            )
            return True

        if drift_triggered:
            self.last_requote_reason = (
                f"price_drift_wait drift={drift_size} threshold={price_drift_ticks} elapsed={elapsed:.2f}s"
            )
        else:
            self.last_requote_reason = (
                f"hold drift={drift_size} threshold={price_drift_ticks} elapsed={elapsed:.2f}s"
            )
        return False

    async def process_pending_lighter_hedges(self):
        """Execute queued hedge legs in FIFO order."""
        while self.pending_lighter_hedges and not self.stop_flag:
            hedge_task = self.pending_lighter_hedges[0]
            result = await self.place_lighter_market_order(
                hedge_task['lighter_side'],
                hedge_task['quantity'],
                hedge_task['price']
            )
            if result is not None:
                self.pending_lighter_hedges.pop(0)
            else:
                await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)
                break
        self.waiting_for_lighter_fill = len(self.pending_lighter_hedges) > 0

    async def check_exposure_limits(self) -> bool:
        """Apply soft/hard exposure controls."""
        net_exposure = abs(self.backpack_position + self.lighter_position)
        if net_exposure > self.absolute_exposure_limit:
            self.publish_risk_event(
                RiskEvent.HARD_EXPOSURE_BREACH,
                f"net_exposure={net_exposure} absolute_limit={self.absolute_exposure_limit}",
                force=True
            )
            raise RuntimeError("Absolute exposure limit breached")
        if net_exposure > self.hard_exposure_limit:
            self.publish_risk_event(
                RiskEvent.SOFT_EXPOSURE_BREACH,
                f"net_exposure={net_exposure} hard_limit={self.hard_exposure_limit}"
            )
            return False
        if net_exposure > self.soft_exposure_limit:
            self.publish_risk_event(
                RiskEvent.SOFT_EXPOSURE_BREACH,
                f"net_exposure={net_exposure} soft_limit={self.soft_exposure_limit}"
            )
            self.logger.warning(
                f"⚠️ Soft exposure warning: {net_exposure} > {self.soft_exposure_limit}, throttling..."
            )
            await asyncio.sleep(1 + self.get_state_adjusted_sleep_seconds())
            return False
        return True

    async def refresh_positions(self):
        self.lighter_position = self.get_lighter_position()
        self.backpack_position = await self.get_backpack_position()

    def get_core_targets(self) -> Tuple[Decimal, Decimal]:
        if self.core_target_abs_position <= 0:
            return Decimal("0"), Decimal("0")
        if self.core_bp_target_live is not None:
            return self.core_bp_target_live, -self.core_bp_target_live
        if self.core_long_exchange == "lighter":
            return -self.core_target_abs_position, self.core_target_abs_position
        return self.core_target_abs_position, -self.core_target_abs_position

    async def smooth_position_gap(self) -> bool:
        """Reduce cross-exchange position gap in chunks of order_quantity."""
        max_smooth_rounds = 50
        smooth_round = 0
        while not self.stop_flag:
            await self.refresh_positions()
            position_gap = self.backpack_position + self.lighter_position
            if abs(position_gap) <= self.order_quantity:
                return True
            if smooth_round >= max_smooth_rounds:
                self.logger.warning("⚠️ Position smoothing reached max rounds, will retry in next cycle")
                return False

            smooth_round += 1
            smooth_qty = min(abs(position_gap), self.order_quantity)
            smooth_qty = self.normalize_backpack_order_quantity(smooth_qty, allow_skip=True)
            if smooth_qty <= 0:
                self.logger.warning(
                    f"⚠️ Position gap {position_gap} below min tradable quantity {self.backpack_min_quantity}"
                )
                return True

            lighter_side = 'sell' if position_gap > 0 else 'buy'
            self.logger.warning(
                f"⚠️ Smoothing position gap={position_gap} via Lighter {lighter_side} {smooth_qty} "
                f"(round={smooth_round})"
            )
            hedge_result = await self.place_lighter_market_order(
                lighter_side,
                smooth_qty,
                self.backpack_best_bid if lighter_side == 'sell' else self.backpack_best_ask
            )
            if hedge_result is None:
                await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)
                return False

    async def place_bbo_order(self, side: str, quantity: Decimal):
        # Get best bid/ask prices
        best_bid, best_ask = await self.fetch_backpack_bbo_prices()
        if side.lower() == 'buy':
            order_price = self.round_to_tick(best_ask - self.backpack_tick_size)
        else:
            order_price = self.round_to_tick(best_bid + self.backpack_tick_size)

        # Place the order using Backpack client
        order_result = await self.backpack_client.place_open_order(
            contract_id=self.backpack_contract_id,
            quantity=quantity,
            direction=side.lower()
        )

        if order_result.success:
            return order_result.order_id, order_price
        else:
            raise Exception(f"Failed to place order: {order_result.error_message}")

    async def place_backpack_post_only_order(self, side: str, quantity: Decimal):
        """Place a post-only order on Backpack."""
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        self.backpack_order_status = None
        self.logger.info(f"[OPEN] [Backpack] [{side}] Placing Backpack POST-ONLY order")
        order_id, order_price = await self.place_bbo_order(side, quantity)

        start_time = time.time()
        while not self.stop_flag:
            if self.pending_lighter_hedges:
                await self.process_pending_lighter_hedges()
            if self.backpack_order_status == 'CANCELED':
                self.backpack_order_status = 'NEW'
                order_id, order_price = await self.place_bbo_order(side, quantity)
                start_time = time.time()
                await asyncio.sleep(0.5)
            elif self.backpack_order_status in ['NEW', 'OPEN', 'PENDING', 'CANCELING', 'PARTIALLY_FILLED']:
                await asyncio.sleep(0.5)
                if await self.should_requote_backpack_order(side, order_price, start_time):
                    self.logger.info(f"🔁 Requote Backpack order: side={side}, reason={self.last_requote_reason}")
                    try:
                        # Cancel the order using Backpack client
                        cancel_result = await self.backpack_client.cancel_order(order_id)
                        if not cancel_result.success:
                            self.logger.error(f"❌ Error canceling Backpack order: {cancel_result.error_message}")
                    except Exception as e:
                        self.logger.error(f"❌ Error canceling Backpack order: {e}")
            elif self.backpack_order_status == 'FILLED':
                break
            else:
                if self.backpack_order_status is not None:
                    self.logger.error(f"❌ Unknown Backpack order status: {self.backpack_order_status}")
                    break
                else:
                    await asyncio.sleep(0.5)

    def handle_backpack_order_book_update(self, message):
        """Handle Backpack order book updates from WebSocket."""
        try:
            if isinstance(message, str):
                message = json.loads(message)

            self.logger.debug(f"Received Backpack depth message: {message}")

            # Check if this is a depth update message
            if message.get("stream") and "depth" in message.get("stream", ""):
                data = message.get("data", {})

                if data:
                    # Update bids - format is [["price", "size"], ...]
                    # Backpack API uses 'b' for bids
                    bids = data.get('b', [])
                    for bid in bids:
                        price = Decimal(bid[0])
                        size = Decimal(bid[1])
                        if size > 0:
                            self.backpack_order_book['bids'][price] = size
                        else:
                            # Remove zero size orders
                            self.backpack_order_book['bids'].pop(price, None)

                    # Update asks - format is [["price", "size"], ...]
                    # Backpack API uses 'a' for asks
                    asks = data.get('a', [])
                    for ask in asks:
                        price = Decimal(ask[0])
                        size = Decimal(ask[1])
                        if size > 0:
                            self.backpack_order_book['asks'][price] = size
                        else:
                            # Remove zero size orders
                            self.backpack_order_book['asks'].pop(price, None)

                    # Update best bid and ask
                    if self.backpack_order_book['bids']:
                        self.backpack_best_bid = max(self.backpack_order_book['bids'].keys())
                    if self.backpack_order_book['asks']:
                        self.backpack_best_ask = min(self.backpack_order_book['asks'].keys())

                    if not self.backpack_order_book_ready:
                        self.backpack_order_book_ready = True
                        self.logger.info(f"📊 Backpack order book ready - Best bid: {self.backpack_best_bid}, "
                                         f"Best ask: {self.backpack_best_ask}")
                    else:
                        self.logger.debug(f"📊 Order book updated - Best bid: {self.backpack_best_bid}, "
                                          f"Best ask: {self.backpack_best_ask}")

        except Exception as e:
            self.logger.error(f"Error handling Backpack order book update: {e}")
            self.logger.error(f"Message content: {message}")

    def handle_backpack_order_update(self, order_data):
        """Handle Backpack order updates from WebSocket."""
        side = order_data.get('side', '').lower()
        filled_size = Decimal(order_data.get('filled_size', '0'))
        price = Decimal(order_data.get('price', '0'))
        if filled_size <= 0:
            return

        if side == 'buy':
            lighter_side = 'sell'
        else:
            lighter_side = 'buy'

        if self.strategy == "core_satellite":
            self.netting_buffer.add_fill(lighter_side, filled_size, price)
            self.waiting_for_lighter_fill = self.netting_buffer.has_pending()
            return

        # Store order details for immediate execution
        self.current_lighter_side = lighter_side
        self.current_lighter_quantity = filled_size
        self.current_lighter_price = price

        self.lighter_order_info = {
            'lighter_side': lighter_side,
            'quantity': filled_size,
            'price': price
        }

        self.pending_lighter_hedges.append(self.lighter_order_info.copy())
        self.waiting_for_lighter_fill = True

    async def flush_netting_buffer(self, force: bool = False, component: str = "sat"):
        if self.strategy != "core_satellite":
            return
        if not self.netting_buffer.has_pending():
            self.waiting_for_lighter_fill = False
            return
        if self.active_lighter_client_order_id:
            return
        order = self.netting_buffer.pop_next_order()
        if order is None:
            self.waiting_for_lighter_fill = False
            return
        lighter_side, quantity, reference_price = order
        if quantity <= 0 or reference_price <= 0:
            return

        prev_component = self.current_hedge_component
        self.current_hedge_component = component or "sat"
        try:
            result = await self.place_lighter_market_order(lighter_side, quantity, reference_price)
            if result is None:
                # put back into buffer (best-effort)
                self.netting_buffer.add_fill(lighter_side, quantity, reference_price)
        finally:
            self.current_hedge_component = prev_component
            self.waiting_for_lighter_fill = self.netting_buffer.has_pending()

    async def run_netting_loop(self):
        if self.strategy != "core_satellite":
            return
        while not self.stop_flag:
            try:
                await self.flush_netting_buffer(force=False, component="sat")
            except Exception as e:
                self.logger.error(f"❌ Netting loop error: {e}")
            await asyncio.sleep(max(0.05, self.netting_window_seconds))

    async def cancel_lighter_order(self, client_order_index: int) -> bool:
        """Cancel active Lighter order by client order index."""
        try:
            if not self.lighter_client:
                return False
            _cancel_tx, tx_hash, error = await self.lighter_client.cancel_order(
                market_index=self.lighter_market_index,
                order_index=int(client_order_index),
            )
            if error is not None:
                self.logger.error(f"❌ Failed to cancel Lighter order {client_order_index}: {error}")
                return False
            if not tx_hash:
                self.logger.error(f"❌ Failed to cancel Lighter order {client_order_index}: empty tx hash")
                return False
            self.logger.warning(f"⚠️ Lighter order cancel requested after long wait: order_id={client_order_index}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Exception canceling Lighter order {client_order_index}: {e}")
            return False


    async def place_lighter_market_order(self, lighter_side: str, quantity: Decimal, price: Decimal):
        if not self.lighter_client:
            await self.initialize_lighter_client()
        if self.active_lighter_client_order_id:
            self.logger.warning(
                f"⚠️ Skip placing new Lighter hedge, active order still tracked: {self.active_lighter_client_order_id}"
            )
            return None
        reference_price = price

        best_bid, best_ask = self.get_lighter_best_levels()
        if best_bid is None or best_ask is None:
            self.publish_risk_event(RiskEvent.LIGHTER_HEDGE_ERROR, "missing_lighter_bbo")
            self.trigger_circuit_breaker("Lighter best bid/ask unavailable during hedge")
            raise RuntimeError("Missing Lighter order book for hedge")

        # Determine order parameters
        if lighter_side.lower() == 'buy':
            order_type = "CLOSE"
            is_ask = False
        else:
            order_type = "OPEN"
            is_ask = True
        basis_ok, alignment_info = await self.wait_for_acceptable_basis(reference_price, lighter_side)
        hedge_stage = alignment_info.get("stage", "alignment_unknown")
        if not basis_ok:
            if not self.hedge_alignment_force_on_timeout:
                self.publish_risk_event(
                    RiskEvent.LIGHTER_HEDGE_ERROR,
                    f"basis_alignment_timeout side={lighter_side}"
                )
                self.logger.warning(
                    f"Skip hedge due to alignment timeout: side={lighter_side}, "
                    f"force_on_timeout={self.hedge_alignment_force_on_timeout}"
                )
                return None
            hedge_stage = "alignment_timeout_forced"
            self.logger.warning(
                f"⚠️ Basis still above threshold after {self.hedge_alignment_wait_timeout_seconds}s, proceed to avoid naked exposure"
            )
            best_bid, best_ask = self.get_lighter_best_levels()
            if best_bid is None or best_ask is None:
                self.publish_risk_event(RiskEvent.LIGHTER_HEDGE_ERROR, "missing_lighter_bbo_after_basis_wait")
                self.trigger_circuit_breaker("Lighter best bid/ask missing after basis wait")
                raise RuntimeError("Missing Lighter order book after basis wait")
        price = self.compute_lighter_aggressive_price(lighter_side, best_bid[0], best_ask[0])
        normalized_quantity, normalized_price, base_amount, price_int = self.normalize_lighter_order_params(quantity, price)


        # Reset order state
        self.lighter_order_filled = False
        self.lighter_order_failed = False
        self.lighter_order_failure_reason = None
        self.lighter_order_price = normalized_price
        self.lighter_order_side = lighter_side
        self.lighter_order_size = normalized_quantity

        try:
            client_order_index = int(time.time() * 1000)
            self.active_lighter_client_order_id = str(client_order_index)
            # Sign the order transaction
            tx, tx_hash, error = await self.lighter_client.create_order(
                market_index=self.lighter_market_index,
                client_order_index=client_order_index,
                base_amount=base_amount,
                price=price_int,
                is_ask=is_ask,
                order_type=self.lighter_client.ORDER_TYPE_LIMIT,
                time_in_force=self.lighter_client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only=False,
                trigger_price=0,
            )
            if error is not None:
                raise Exception(f"Error placing Lighter order: {error}")

            slippage_bps = self.compute_lighter_slippage(best_bid[0], best_ask[0]) * Decimal('10000')
            stage_tag = f"{self.current_hedge_component}.{hedge_stage}" if self.current_hedge_component else hedge_stage
            self.lighter_order_contexts[str(client_order_index)] = {
                "stage": stage_tag,
                "reference_price": reference_price,
                "reference_quantity": quantity,
                "basis_bps": alignment_info.get("basis_bps", Decimal("0")),
                "threshold_bps": alignment_info.get("threshold_bps", Decimal("0")),
                "created_at": time.time(),
            }
            self.logger.info(
                f"[{client_order_index}] [{order_type}] [Lighter] [OPEN]: {normalized_quantity} @ {normalized_price} "
                f"(slippage={slippage_bps:.2f}bps, stage={stage_tag}, "
                f"basis={alignment_info.get('basis_bps', Decimal('0')):.2f}bps/"
                f"{alignment_info.get('threshold_bps', Decimal('0')):.2f}bps)"
            )

            order_filled = await self.monitor_lighter_order(client_order_index)
            if not order_filled:
                return None

            return tx_hash
        except Exception as e:
            self.active_lighter_client_order_id = None
            self.logger.error(f"❌ Error placing Lighter order: {e}")
            self.publish_risk_event(RiskEvent.LIGHTER_HEDGE_ERROR, f"lighter_place_error={e}")
            return None

    async def monitor_lighter_order(self, client_order_index: int) -> bool:
        """Monitor Lighter order and adjust price if needed."""

        start_time = time.time()
        overall_start_time = start_time
        timeout_round = 0
        cancel_requested = False
        while not self.lighter_order_filled and not self.stop_flag:
            if self.lighter_order_failed:
                reason = self.lighter_order_failure_reason or "order_terminal"
                self.logger.warning(f"⚠️ Lighter order monitor ended: order_id={client_order_index}, reason={reason}")
                return False
            if (not cancel_requested) and self.lighter_cancel_after_seconds > 0:
                total_wait = time.time() - overall_start_time
                if total_wait >= self.lighter_cancel_after_seconds:
                    cancel_requested = True
                    cancel_ok = await self.cancel_lighter_order(client_order_index)
                    if cancel_ok:
                        self.lighter_order_filled = False
                        self.lighter_order_failed = True
                        self.lighter_order_failure_reason = "cancel_after_timeout"
                        self.active_lighter_client_order_id = None
                        self.lighter_order_contexts.pop(str(client_order_index), None)
                        self.logger.warning(
                            f"⚠️ Lighter order canceled after timeout window: order_id={client_order_index}, wait={total_wait:.1f}s"
                        )
                        return False
                    self.logger.warning(
                        f"⚠️ Cancel request failed for stuck Lighter order: order_id={client_order_index}, continue waiting..."
                    )
            # Check timeout window and continue waiting with retry logging
            if time.time() - start_time > self.lighter_fill_timeout_seconds:
                timeout_round += 1
                self.logger.error(f"❌ Timeout waiting for Lighter order fill after {time.time() - start_time:.1f}s")
                self.logger.error(f"❌ Order state - Filled: {self.lighter_order_filled}")

                # Hard stop on timeout to avoid fake hedging
                self.publish_risk_event(
                    RiskEvent.LIGHTER_HEDGE_TIMEOUT,
                    f"order_id={client_order_index}, rounds={timeout_round}",
                )
                start_time = time.time()
                await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)

            await asyncio.sleep(0.1)  # Check every 100ms
        return self.lighter_order_filled

    async def modify_lighter_order(self, client_order_index: int, new_price: Decimal):
        """Modify current Lighter order with new price using client_order_index."""
        try:
            if client_order_index is None:
                self.logger.error("❌ Cannot modify order - no order ID available")
                return

            # Calculate new Lighter price
            lighter_price = int(new_price * self.price_multiplier)

            self.logger.info(f"🔧 Attempting to modify order - Market: {self.lighter_market_index}, "
                             f"Client Order Index: {client_order_index}, New Price: {lighter_price}")

            # Use the native SignerClient's modify_order method
            tx_info, tx_hash, error = await self.lighter_client.modify_order(
                market_index=self.lighter_market_index,
                order_index=client_order_index,  # Use client_order_index directly
                base_amount=int(self.lighter_order_size * self.base_amount_multiplier),
                price=lighter_price,
                trigger_price=0
            )

            if error is not None:
                self.logger.error(f"❌ Lighter order modification error: {error}")
                return

            self.lighter_order_price = new_price
            self.logger.info(f"🔄 Lighter order modified successfully: {self.lighter_order_side} "
                             f"{self.lighter_order_size} @ {new_price}")

        except Exception as e:
            self.logger.error(f"❌ Error modifying Lighter order: {e}")
            import traceback
            self.logger.error(f"❌ Full traceback: {traceback.format_exc()}")

    async def setup_backpack_websocket(self):
        """Setup Backpack websocket for order updates and order book data."""
        if not self.backpack_client:
            raise Exception("Backpack client not initialized")

        def order_update_handler(order_data):
            """Handle order updates from Backpack WebSocket."""
            if order_data.get('contract_id') != self.backpack_contract_id:
                return
            try:
                order_id = order_data.get('order_id')
                status = order_data.get('status')
                side = order_data.get('side', '').lower()
                filled_size = Decimal(order_data.get('filled_size', '0'))
                size = Decimal(order_data.get('size', '0'))
                price = order_data.get('price', '0')

                if side == 'buy':
                    order_type = "OPEN"
                else:
                    order_type = "CLOSE"
                
                if status == 'CANCELED' and filled_size > 0:
                    status = 'FILLED'

                previous_hedged = self.backpack_order_hedged_size.get(order_id, Decimal('0'))
                delta_filled = filled_size - previous_hedged
                if delta_filled > 0:
                    self.backpack_order_hedged_size[order_id] = filled_size
                    if side == 'buy':
                        self.backpack_position += delta_filled
                    else:
                        self.backpack_position -= delta_filled
                    self.log_trade_to_csv(
                        exchange='Backpack',
                        side=side,
                        price=str(price),
                        quantity=str(delta_filled)
                    )
                    self.handle_backpack_order_update({
                        'order_id': order_id,
                        'side': side,
                        'status': status,
                        'size': size,
                        'price': price,
                        'contract_id': self.backpack_contract_id,
                        'filled_size': delta_filled
                    })

                if status in ['FILLED', 'CANCELED', 'EXPIRED']:
                    self.backpack_order_hedged_size.pop(order_id, None)

                if status == 'FILLED':
                    self.logger.info(f"[{order_id}] [{order_type}] [Backpack] [{status}]: {filled_size} @ {price}")
                    self.backpack_order_status = status
                elif self.backpack_order_status != 'FILLED':
                    if status == 'OPEN':
                        self.logger.info(f"[{order_id}] [{order_type}] [Backpack] [{status}]: {size} @ {price}")
                    else:
                        self.logger.info(f"[{order_id}] [{order_type}] [Backpack] [{status}]: {filled_size} @ {price}")
                    self.backpack_order_status = status

            except Exception as e:
                self.logger.error(f"Error handling Backpack order update: {e}")

        try:
            # Setup order update handler
            self.backpack_client.setup_order_update_handler(order_update_handler)
            self.logger.info("✅ Backpack WebSocket order update handler set up")

            # Connect to Backpack WebSocket
            await self.backpack_client.connect()
            self.logger.info("✅ Backpack WebSocket connection established")

            # Setup separate WebSocket connection for depth updates
            await self.setup_backpack_depth_websocket()

        except Exception as e:
            self.logger.error(f"Could not setup Backpack WebSocket handlers: {e}")

    async def setup_backpack_depth_websocket(self):
        """Setup separate WebSocket connection for Backpack depth updates."""
        try:
            import websockets

            async def handle_depth_websocket():
                """Handle depth WebSocket connection."""
                url = "wss://ws.backpack.exchange"

                while not self.stop_flag:
                    try:
                        async with websockets.connect(url) as ws:
                            # Subscribe to depth updates
                            subscribe_message = {
                                "method": "SUBSCRIBE",
                                "params": [f"depth.{self.backpack_contract_id}"]
                            }
                            await ws.send(json.dumps(subscribe_message))
                            self.logger.info(f"✅ Subscribed to depth updates for {self.backpack_contract_id}")

                            # Listen for messages
                            async for message in ws:
                                if self.stop_flag:
                                    break

                                try:
                                    # Handle ping frames
                                    if isinstance(message, bytes) and message == b'\x09':
                                        await ws.pong()
                                        continue

                                    data = json.loads(message)

                                    # Handle depth updates
                                    if data.get('stream') and 'depth' in data.get('stream', ''):
                                        self.handle_backpack_order_book_update(data)

                                except json.JSONDecodeError as e:
                                    self.logger.warning(f"Failed to parse depth WebSocket message: {e}")
                                except Exception as e:
                                    self.logger.error(f"Error handling depth WebSocket message: {e}")

                    except websockets.exceptions.ConnectionClosed:
                        self.logger.warning("Depth WebSocket connection closed, reconnecting...")
                    except Exception as e:
                        self.logger.error(f"Depth WebSocket error: {e}")

                    # Wait before reconnecting
                    if not self.stop_flag:
                        await asyncio.sleep(2)

            # Start depth WebSocket in background
            asyncio.create_task(handle_depth_websocket())
            self.logger.info("✅ Backpack depth WebSocket task started")

        except Exception as e:
            self.logger.error(f"Could not setup Backpack depth WebSocket: {e}")

    def get_lighter_position(self):
        url = "https://mainnet.zklighter.elliot.ai/api/v1/account"
        headers = {"accept": "application/json"}

        current_position = None
        parameters = {"by": "index", "value": self.account_index}
        try:
            response = requests.get(url, headers=headers, params=parameters, timeout=10)
            response.raise_for_status()  # Raise an exception for bad status codes

            # Check if response has content
            if not response.text.strip():
                print("⚠️ Empty response from Lighter API for position check")
                return self.lighter_position

            data = response.json()

            if 'accounts' not in data or not data['accounts']:
                print(f"⚠️ Unexpected response format from Lighter API: {data}")
                return self.lighter_position

            positions = data['accounts'][0].get('positions', [])
            for position in positions:
                if position.get('symbol') == self.ticker:
                    current_position = Decimal(position['position']) * position['sign']
                    break
            if current_position is None:
                current_position = 0

        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error getting position: {e}")
        except json.JSONDecodeError as e:
            print(f"⚠️ JSON parsing error in position response: {e}")
            print(f"Response text: {response.text[:200]}...")  # Show first 200 chars
        except Exception as e:
            print(f"⚠️ Unexpected error getting position: {e}")

        return current_position

    async def trading_loop(self):
        """Main trading loop implementing the new strategy."""
        self.logger.info(f"🚀 Starting hedge bot for {self.ticker}")

        # Initialize clients
        try:
            self.initialize_lighter_client()
            self.initialize_backpack_client()

            # Get contract info
            self.backpack_contract_id, self.backpack_tick_size = await self.get_backpack_contract_info()
            self.lighter_market_index, self.base_amount_multiplier, self.price_multiplier, self.tick_size = self.get_lighter_market_config()

            self.logger.info(f"Contract info loaded - Backpack: {self.backpack_contract_id}, "
                             f"Lighter: {self.lighter_market_index}")

        except Exception as e:
            self.logger.error(f"❌ Failed to initialize: {e}")
            return

        # Setup Backpack websocket
        try:
            await self.setup_backpack_websocket()
            self.logger.info("✅ Backpack WebSocket connection established")

            # Wait for initial order book data with timeout
            self.logger.info("⏳ Waiting for initial order book data...")
            timeout = 10  # seconds
            start_time = time.time()
            while not self.backpack_order_book_ready and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for WebSocket order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.backpack_order_book_ready:
                self.logger.info("✅ WebSocket order book data received")
            else:
                self.logger.warning("⚠️ WebSocket order book not ready, will use REST API fallback")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Backpack websocket: {e}")
            return

        # Setup Lighter websocket
        try:
            self.lighter_ws_task = asyncio.create_task(self.handle_lighter_ws())
            self.logger.info("✅ Lighter WebSocket task started")

            # Wait for initial Lighter order book data with timeout
            self.logger.info("⏳ Waiting for initial Lighter order book data...")
            timeout = 10  # seconds
            start_time = time.time()
            while not self.lighter_order_book_ready and not self.stop_flag:
                if time.time() - start_time > timeout:
                    self.logger.warning(f"⚠️ Timeout waiting for Lighter WebSocket order book data after {timeout}s")
                    break
                await asyncio.sleep(0.5)

            if self.lighter_order_book_ready:
                self.logger.info("✅ Lighter WebSocket order book data received")
            else:
                self.logger.warning("⚠️ Lighter WebSocket order book not ready")

        except Exception as e:
            self.logger.error(f"❌ Failed to setup Lighter websocket: {e}")
            return

        # Funding must be available before trading (fail fast)
        if self.funding_enabled:
            funding_ok = await self.funding_startup_check_with_retries()
            if not funding_ok:
                self.logger.error("❌ Funding startup check exhausted retries")
                await self.graceful_flatten_positions_before_exit()
                raise RuntimeError("Funding unavailable after retries, exited with unwind attempt")
            self.funding_task = asyncio.create_task(self.funding_refresh_loop())
            self.funding_ramp_task = asyncio.create_task(self.funding_ramp_loop())

        await asyncio.sleep(5)
        self.log_execution_control_config()

        if self.strategy == "core_satellite":
            self.netting_task = asyncio.create_task(self.run_netting_loop())
            try:
                await self.trading_loop_core_satellite()
            finally:
                if self.netting_task and not self.netting_task.done():
                    self.netting_task.cancel()
            return

        iterations = 0
        self.lighter_position = self.get_lighter_position()
        self.backpack_position = await self.get_backpack_position()
        if abs(self.backpack_position + self.lighter_position) > self.order_quantity:
            self.logger.warning(
                f"⚠️ Initial position gap detected: {self.backpack_position + self.lighter_position}, start smoothing..."
            )
            await self.smooth_position_gap()
        while iterations < self.iterations and not self.stop_flag:
            iterations += 1
            self.logger.info("-----------------------------------------------")
            self.logger.info(f"🔄 Trading loop iteration {iterations}")
            self.logger.info("-----------------------------------------------")

            while self.backpack_position < self.max_position and not self.stop_flag:
                await self.refresh_positions()
                self.logger.info(f"Buying up to {self.max_position} | Backpack position: {self.backpack_position} | Lighter position: {self.lighter_position}")
                if not await self.wait_for_market_data_health():
                    continue
                if not await self.check_exposure_limits():
                    await self.smooth_position_gap()
                    continue
                if self.risk_state == RiskState.STRESSED:
                    await asyncio.sleep(self.get_state_adjusted_sleep_seconds())
                    continue
                if abs(self.backpack_position + self.lighter_position) > self.absolute_exposure_limit:
                    self.logger.error(f"❌ Position diff is too large: {self.backpack_position + self.lighter_position}")
                    self.trigger_circuit_breaker(f"Hard exposure breached in buy loop: {self.backpack_position + self.lighter_position}")
                    raise RuntimeError("Hard exposure breach")

                self.order_execution_complete = False
                self.waiting_for_lighter_fill = False
                self.pending_lighter_hedges.clear()
                start_time = time.time()
                try:
                    # Determine side based on some logic (for now, alternate)
                    side = 'buy'
                    if not await self.wait_for_pre_trade_alignment(side):
                        await asyncio.sleep(self.pre_trade_retry_sleep_seconds)
                        continue
                    maker_quantity = self.get_state_adjusted_order_quantity(self.order_quantity)
                    if maker_quantity <= 0:
                        self.logger.warning("⚠️ Buy maker quantity is zero after normalization, skip this cycle")
                        continue
                    await self.place_backpack_post_only_order(side, maker_quantity)
                except Exception as e:
                    self.logger.error(f"⚠️ Error in trading loop: {e}")
                    self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                    self.publish_risk_event(RiskEvent.LIGHTER_HEDGE_ERROR, f"buy_loop_error={e}", force=True)
                    self.trigger_circuit_breaker(f"Buy loop failed: {e}")
                    break

                start_time = time.time()
                while not self.order_execution_complete and not self.stop_flag:
                    # Check if Backpack order filled and we need to place Lighter order
                    if self.waiting_for_lighter_fill:
                        await self.process_pending_lighter_hedges()
                        break

                    await asyncio.sleep(0.01)
                    if time.time() - start_time > 180:
                        self.publish_risk_event(
                            RiskEvent.TRADE_COMPLETION_TIMEOUT,
                            "buy_trade_completion_timeout",
                            force=True
                        )
                        start_time = time.time()
                        await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)

                if self.stop_flag:
                    break

            if self.sleep_time > 0:
                self.logger.info(f"💤 Sleeping {self.sleep_time} seconds ...")
                await asyncio.sleep(self.sleep_time + self.get_state_adjusted_sleep_seconds())

            exit_after_next_trade = False
            while self.backpack_position > -1*self.max_position and not self.stop_flag:
                await self.refresh_positions()
                self.logger.info(f"Selling up to -{self.max_position} | Backpack position: {self.backpack_position} | Lighter position: {self.lighter_position}")
                if not await self.wait_for_market_data_health():
                    continue
                if not await self.check_exposure_limits():
                    await self.smooth_position_gap()
                    continue
                if self.risk_state == RiskState.STRESSED:
                    await asyncio.sleep(self.get_state_adjusted_sleep_seconds())
                    continue
                if abs(self.backpack_position + self.lighter_position) > self.absolute_exposure_limit:
                    self.logger.error(f"❌ Position diff is too large: {self.backpack_position + self.lighter_position}")
                    self.trigger_circuit_breaker(f"Hard exposure breached in sell loop: {self.backpack_position + self.lighter_position}")
                    raise RuntimeError("Hard exposure breach")

                if iterations == self.iterations:
                    if self.backpack_position>0 and self.backpack_position <= self.order_quantity:
                        exit_after_next_trade = True

                self.order_execution_complete = False
                self.waiting_for_lighter_fill = False
                self.pending_lighter_hedges.clear()
                start_time = time.time()
                try:
                    # Determine side based on some logic (for now, alternate)
                    side = 'sell'
                    if not await self.wait_for_pre_trade_alignment(side):
                        await asyncio.sleep(self.pre_trade_retry_sleep_seconds)
                        continue
                    if exit_after_next_trade:
                        exit_quantity = self.normalize_backpack_order_quantity(abs(self.backpack_position), allow_skip=True)
                        if exit_quantity <= 0:
                            self.logger.warning(
                                f"⚠️ Residual position {self.backpack_position} below min quantity {self.backpack_min_quantity}, stop graceful exit"
                            )
                            break
                        await self.place_backpack_post_only_order(side, exit_quantity)
                    else:
                        maker_quantity = self.get_state_adjusted_order_quantity(self.order_quantity)
                        if maker_quantity <= 0:
                            self.logger.warning("⚠️ Sell maker quantity is zero after normalization, skip this cycle")
                            continue
                        await self.place_backpack_post_only_order(side, maker_quantity)
                except Exception as e:
                    self.logger.error(f"⚠️ Error in trading loop: {e}")
                    self.logger.error(f"⚠️ Full traceback: {traceback.format_exc()}")
                    self.publish_risk_event(RiskEvent.LIGHTER_HEDGE_ERROR, f"sell_loop_error={e}", force=True)
                    break

                while not self.order_execution_complete and not self.stop_flag:
                    # Check if Backpack order filled and we need to place Lighter order
                    if self.waiting_for_lighter_fill:
                        await self.process_pending_lighter_hedges()
                        break

                    await asyncio.sleep(0.01)
                    if time.time() - start_time > 180:
                        self.publish_risk_event(
                            RiskEvent.TRADE_COMPLETION_TIMEOUT,
                            "sell_trade_completion_timeout",
                            force=True
                        )
                        start_time = time.time()
                        await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)
                
                if exit_after_next_trade:
                    self.logger.info("Position back to zero. Done! Exiting...")
                    break

    async def trading_loop_core_satellite(self):
        """Core+Satellite: hold core hedge, generate volume with satellite maker fills, net hedge on Lighter."""
        await self.refresh_positions()
        core_bp_target, _core_lighter_target = self.get_core_targets()
        self.logger.info(
            f"[CORE] target_abs={self.core_target_abs_position} long_exchange={self.core_long_exchange} "
            f"bp_target={core_bp_target}"
        )

        sat_direction = "buy"
        last_core_rebalance = 0.0
        completed_iters = 0
        trades_in_current_iter = 0
        self.sat_touched_upper = False
        self.sat_touched_lower = False
        while completed_iters < self.iterations and not self.stop_flag:
            self.logger.info("-----------------------------------------------")
            self.logger.info(
                f"🔄 Core+Satellite iteration {completed_iters + 1}/{self.iterations} "
                f"(trade progress {trades_in_current_iter}/{self.satellite_trades_per_iter})"
            )
            self.logger.info("-----------------------------------------------")

            await self.refresh_positions()
            if not await self.wait_for_market_data_health():
                continue

            # Flush if net exposure too large (netting window delay)
            net_exposure = abs(self.backpack_position + self.lighter_position)
            if net_exposure > self.satellite_max_net_exposure:
                self.logger.warning(f"⚠️ Net exposure {net_exposure} exceeds satellite max {self.satellite_max_net_exposure}, forcing hedge flush")
                await self.flush_netting_buffer(force=True, component="sat")
                await asyncio.sleep(self.lighter_timeout_retry_sleep_seconds)
                continue

            # Optional low-frequency core rebalance towards target (best-effort)
            if self.core_target_abs_position > 0 and (time.time() - last_core_rebalance) >= self.core_rebalance_interval_seconds:
                last_core_rebalance = time.time()
                await self.flush_netting_buffer(force=True, component="core")
                await self.refresh_positions()
                core_bp_target, _ = self.get_core_targets()
                delta = core_bp_target - self.backpack_position
                if abs(delta) >= self.order_quantity:
                    side = "buy" if delta > 0 else "sell"
                    qty = self.normalize_backpack_order_quantity(min(abs(delta), self.order_quantity), allow_skip=True)
                    if qty > 0:
                        self.logger.info(f"[CORE] rebalance via BP {side} {qty}, delta={delta}")
                        prev_component = self.current_hedge_component
                        self.current_hedge_component = "core"
                        try:
                            if not await self.wait_for_pre_trade_alignment(side):
                                continue
                            await self.place_backpack_post_only_order(side, qty)
                            await self.flush_netting_buffer(force=True, component="core")
                        finally:
                            self.current_hedge_component = prev_component

            if not self.satellite_enabled:
                await asyncio.sleep(0.5)
                continue

            core_bp_target, _ = self.get_core_targets()
            upper = core_bp_target + self.satellite_band
            lower = core_bp_target - self.satellite_band

            if self.update_satellite_trade_progress(self.backpack_position, core_bp_target, lower, upper):
                trades_in_current_iter += 1
                self.logger.info(
                    f"[SAT] completed trade {trades_in_current_iter}/{self.satellite_trades_per_iter} "
                    f"for iteration {completed_iters + 1}/{self.iterations}"
                )
                if trades_in_current_iter >= self.satellite_trades_per_iter:
                    completed_iters += 1
                    trades_in_current_iter = 0
                    self.logger.info(f"[SAT] iteration completed: {completed_iters}/{self.iterations}")
                    if completed_iters >= self.iterations:
                        break

            if self.backpack_position >= upper:
                sat_direction = "sell"
            elif self.backpack_position <= lower:
                sat_direction = "buy"

            side = sat_direction
            if not await self.wait_for_pre_trade_alignment(side):
                await asyncio.sleep(self.pre_trade_retry_sleep_seconds)
                continue

            if side == "buy":
                remaining = max(Decimal("0"), upper - self.backpack_position)
            else:
                remaining = max(Decimal("0"), self.backpack_position - lower)

            maker_qty = min(self.satellite_order_quantity, remaining)
            maker_qty = self.get_state_adjusted_order_quantity(maker_qty)
            if maker_qty <= 0:
                await asyncio.sleep(0.2)
                continue

            prev_component = self.current_hedge_component
            self.current_hedge_component = "sat"
            try:
                await self.place_backpack_post_only_order(side, maker_qty)
            finally:
                self.current_hedge_component = prev_component

            await self.flush_netting_buffer(force=False, component="sat")
            await asyncio.sleep(self.sleep_time + self.get_state_adjusted_sleep_seconds())

        if not self.stop_flag:
            await self.flush_netting_buffer(force=True, component="exit")
            await self.flatten_all_positions_on_completion()

    async def run(self):
        """Run the hedge bot."""
        self.setup_signal_handlers()

        try:
            await self.trading_loop()
        except KeyboardInterrupt:
            self.logger.info("\n🛑 Received interrupt signal...")
        finally:
            self.logger.info("🔄 Cleaning up...")
            self.shutdown()
