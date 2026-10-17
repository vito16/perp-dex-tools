import asyncio
import importlib
import sys
import types
import time
from decimal import Decimal


def _load_hedgebot_class():
    lighter_pkg = types.ModuleType("lighter")
    signer_module = types.ModuleType("lighter.signer_client")

    class DummySignerClient:
        pass

    signer_module.SignerClient = DummySignerClient
    lighter_pkg.signer_client = signer_module
    sys.modules["lighter"] = lighter_pkg
    sys.modules["lighter.signer_client"] = signer_module

    backpack_module = types.ModuleType("exchanges.backpack")

    class DummyBackpackClient:
        pass

    backpack_module.BackpackClient = DummyBackpackClient
    sys.modules["exchanges.backpack"] = backpack_module

    module = importlib.import_module("hedge.hedge_mode_bp")
    return module.HedgeBot


class _DummyLogger:
    def __init__(self):
        self.messages = []

    def info(self, msg):
        self.messages.append(("info", str(msg)))

    def warning(self, msg):
        self.messages.append(("warning", str(msg)))

    def error(self, msg):
        self.messages.append(("error", str(msg)))


def _make_bot():
    hedge_bot_cls = _load_hedgebot_class()
    bot = hedge_bot_cls.__new__(hedge_bot_cls)
    bot.logger = _DummyLogger()
    bot.stop_flag = False
    bot.circuit_breaker_triggered = False
    bot.base_lighter_slippage = Decimal("0.0004")
    bot.max_lighter_slippage = Decimal("0.002")
    bot.tick_size = Decimal("0.01")
    bot.base_amount_multiplier = 1000
    bot.price_multiplier = 100
    bot.lighter_timeout_retry_sleep_seconds = 0.01
    bot.soft_exposure_limit = Decimal("1.5")
    bot.hard_exposure_limit = Decimal("2.0")
    bot.absolute_exposure_limit = Decimal("10.0")
    bot.backpack_position = Decimal("0")
    bot.lighter_position = Decimal("0")
    bot.pending_lighter_hedges = []
    bot.waiting_for_lighter_fill = False
    bot.order_quantity = Decimal("0.02")
    bot.backpack_min_quantity = Decimal("0.01")
    bot.event_cooldown_seconds = 0.0
    bot._last_event_time = {}
    bot.state_changed_at = 0.0
    risk_state = importlib.import_module("hedge.hedge_mode_bp").RiskState
    bot.risk_state = risk_state.NORMAL
    bot.wide_basis_bps = Decimal("25")
    bot.hedge_alignment_enabled = True
    bot.hedge_alignment_initial_bps = Decimal("3")
    bot.hedge_alignment_max_bps = Decimal("8")
    bot.hedge_alignment_widen_step_bps = Decimal("1")
    bot.hedge_alignment_widen_interval_seconds = 0.5
    bot.hedge_alignment_wait_timeout_seconds = 1.5
    bot.hedge_alignment_retry_sleep_seconds = 0.01
    bot.hedge_alignment_force_on_timeout = True
    bot.pre_trade_alignment_enabled = False
    bot.pre_trade_max_basis_bps = Decimal("3")
    bot.pre_trade_wait_timeout_seconds = 0.2
    bot.pre_trade_retry_sleep_seconds = 0.01
    bot.backpack_best_bid = Decimal("99.9")
    bot.backpack_best_ask = Decimal("100.1")
    bot.backpack_tick_size = Decimal("0.1")
    bot.requote_price_drift_ticks = Decimal("2")
    bot.requote_min_age_seconds = 0.8
    bot.requote_secondary_timeout_seconds = 0
    bot.last_requote_reason = ""
    bot.lighter_order_failed = False
    bot.lighter_order_failure_reason = None
    bot.active_lighter_client_order_id = None
    return bot


def test_dynamic_slippage_is_capped_at_point_two_percent():
    bot = _make_bot()
    slippage = bot.compute_lighter_slippage(Decimal("100"), Decimal("101"))
    assert slippage == Decimal("0.002")


def test_normalize_lighter_order_params_applies_precision():
    bot = _make_bot()
    normalized_qty, normalized_price, base_amount, price_int = bot.normalize_lighter_order_params(
        Decimal("0.0014"), Decimal("101.237")
    )
    assert normalized_qty == Decimal("0.001")
    assert normalized_price == Decimal("101.24")
    assert base_amount == 1
    assert price_int == 10124


def test_check_exposure_limits_soft_limit_throttles(monkeypatch):
    bot = _make_bot()
    bot.backpack_position = Decimal("1.2")
    bot.lighter_position = Decimal("0.4")
    sleep_calls = []

    async def fake_sleep(seconds):
        sleep_calls.append(seconds)

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    result = asyncio.run(bot.check_exposure_limits())
    assert result is False
    assert len(sleep_calls) == 1
    assert sleep_calls[0] >= 1
    assert bot.stop_flag is False


def test_check_exposure_limits_hard_limit_requests_smoothing():
    bot = _make_bot()
    bot.backpack_position = Decimal("2.2")
    bot.lighter_position = Decimal("0.2")
    result = asyncio.run(bot.check_exposure_limits())
    assert result is False
    assert bot.circuit_breaker_triggered is False


def test_check_exposure_limits_absolute_limit_triggers_circuit_breaker():
    bot = _make_bot()
    bot.backpack_position = Decimal("20")
    bot.lighter_position = Decimal("0")
    try:
        asyncio.run(bot.check_exposure_limits())
        assert False, "Expected RuntimeError"
    except RuntimeError:
        pass
    assert bot.circuit_breaker_triggered is True
    assert bot.stop_flag is True


def test_basis_events_switch_risk_state():
    module = importlib.import_module("hedge.hedge_mode_bp")
    bot = _make_bot()
    bot.publish_risk_event(module.RiskEvent.BASIS_WIDE, "wide")
    assert bot.risk_state == module.RiskState.WIDE
    bot.publish_risk_event(module.RiskEvent.BASIS_NORMAL, "normal", force=True)
    assert bot.risk_state == module.RiskState.NORMAL


def test_trade_timeout_event_triggers_circuit_breaker():
    module = importlib.import_module("hedge.hedge_mode_bp")
    bot = _make_bot()
    bot.publish_risk_event(module.RiskEvent.TRADE_COMPLETION_TIMEOUT, "timeout", force=True)
    assert bot.circuit_breaker_triggered is False
    assert bot.risk_state == module.RiskState.WIDE


def test_wide_state_quantity_respects_backpack_minimum():
    module = importlib.import_module("hedge.hedge_mode_bp")
    bot = _make_bot()
    bot.risk_state = module.RiskState.WIDE
    # requested 0.015 -> wide half => 0.0075, should clamp to 0.01 min quantity
    adjusted = bot.get_state_adjusted_order_quantity(Decimal("0.015"))
    assert adjusted == Decimal("0.01")


def test_process_pending_lighter_hedges_drains_queue():
    bot = _make_bot()
    called = []

    async def _fake_place(side, quantity, price):
        called.append((side, quantity, price))
        return "ok"

    bot.place_lighter_market_order = _fake_place
    bot.pending_lighter_hedges = [
        {"lighter_side": "buy", "quantity": Decimal("0.1"), "price": Decimal("100")},
        {"lighter_side": "sell", "quantity": Decimal("0.2"), "price": Decimal("101")},
    ]

    asyncio.run(bot.process_pending_lighter_hedges())
    assert called == [
        ("buy", Decimal("0.1"), Decimal("100")),
        ("sell", Decimal("0.2"), Decimal("101")),
    ]
    assert bot.pending_lighter_hedges == []
    assert bot.waiting_for_lighter_fill is False


def test_hedge_alignment_threshold_widens_over_time():
    bot = _make_bot()
    first = bot.get_hedge_alignment_threshold_bps(0.0, Decimal("0.1"))
    later = bot.get_hedge_alignment_threshold_bps(1.1, Decimal("0.1"))
    hard_exposure = bot.get_hedge_alignment_threshold_bps(0.1, Decimal("2.1"))
    assert first == Decimal("3")
    assert later == Decimal("5")
    assert hard_exposure == Decimal("8")


def test_wait_for_pre_trade_alignment_returns_true_when_basis_in_threshold():
    bot = _make_bot()
    bot.pre_trade_alignment_enabled = True
    bot.pre_trade_max_basis_bps = Decimal("20")
    bot.get_lighter_best_levels = lambda: ((Decimal("100.0"), Decimal("1")), (Decimal("100.2"), Decimal("1")))
    result = asyncio.run(bot.wait_for_pre_trade_alignment("buy"))
    assert result is True


def test_should_requote_prefers_price_drift_over_time():
    bot = _make_bot()
    # buy order price stale by 0.5, threshold=0.2
    start = time.time() - 1.0
    should = asyncio.run(bot.should_requote_backpack_order("buy", Decimal("99.6"), start))
    assert should is True
    assert "price_drift" in bot.last_requote_reason


def test_should_requote_uses_time_as_secondary_fallback():
    bot = _make_bot()
    bot.requote_secondary_timeout_seconds = 0.5
    # no drift: order close to ask, but too old
    start = time.time() - 1.0
    should = asyncio.run(bot.should_requote_backpack_order("buy", Decimal("100.0"), start))
    assert should is True
    assert "time_fallback" in bot.last_requote_reason


def test_handle_lighter_order_terminal_update_marks_failed():
    bot = _make_bot()
    bot.active_lighter_client_order_id = "123"
    bot.lighter_order_contexts = {"123": {"stage": "within_initial_band"}}
    bot.handle_lighter_order_terminal_update({"client_order_id": "123", "status": "canceled"})
    assert bot.lighter_order_failed is True
    assert bot.lighter_order_failure_reason == "order_canceled"
    assert bot.active_lighter_client_order_id is None
    assert "123" not in bot.lighter_order_contexts


def test_monitor_lighter_order_exits_on_terminal_failure():
    bot = _make_bot()
    bot.lighter_fill_timeout_seconds = 30
    bot.lighter_order_filled = False
    bot.lighter_order_failed = True
    bot.lighter_order_failure_reason = "order_canceled"
    result = asyncio.run(bot.monitor_lighter_order(123))
    assert result is False


def test_netting_buffer_nets_opposite_fills_and_uses_vwap():
    module = importlib.import_module("hedge.hedge_mode_bp")
    buf = module.HedgeNettingBuffer()

    # BP buy fill => lighter_side 'sell'
    buf.add_fill("sell", Decimal("0.02"), Decimal("100"))
    # BP sell fill => lighter_side 'buy' (nets out 0.01)
    buf.add_fill("buy", Decimal("0.01"), Decimal("102"))

    order = buf.pop_next_order()
    assert order is not None
    side, qty, vwap = order
    assert side == "sell"
    assert qty == Decimal("0.01")
    assert vwap == Decimal("100")


def test_netting_buffer_instances_do_not_share_state():
    module = importlib.import_module("hedge.hedge_mode_bp")
    buf1 = module.HedgeNettingBuffer()
    buf2 = module.HedgeNettingBuffer()
    buf1.add_fill("sell", Decimal("0.01"), Decimal("100"))
    assert buf1.has_pending() is True
    assert buf2.has_pending() is False


def test_funding_direction_choice_prefers_higher_net_receive():
    module = importlib.import_module("hedge.hedge_mode_bp")
    bot = _make_bot()
    bot.core_target_abs_position = Decimal("0.5")
    # BP positive => shorts receive; Lighter negative => longs receive => choose BP short
    desired = bot.compute_desired_core_bp_target(Decimal("0.0002"), Decimal("-0.0001"))
    assert desired == Decimal("-0.5")

    # BP negative => longs receive; Lighter positive => shorts receive => choose BP long
    desired2 = bot.compute_desired_core_bp_target(Decimal("-0.0002"), Decimal("0.0001"))
    assert desired2 == Decimal("0.5")


def test_funding_startup_check_retries_then_succeeds():
    bot = _make_bot()
    bot.funding_startup_retries = 3
    bot.funding_startup_retry_sleep_seconds = 0.001
    attempts = {"count": 0}

    def _mock_refresh():
        attempts["count"] += 1
        if attempts["count"] < 3:
            raise RuntimeError("funding temporary failure")

    bot.refresh_funding_or_raise = _mock_refresh
    result = asyncio.run(bot.funding_startup_check_with_retries())
    assert result is True
    assert attempts["count"] == 3


def test_funding_startup_check_retries_then_fails():
    bot = _make_bot()
    bot.funding_startup_retries = 3
    bot.funding_startup_retry_sleep_seconds = 0.001
    attempts = {"count": 0}

    def _mock_refresh():
        attempts["count"] += 1
        raise RuntimeError("funding failure")

    bot.refresh_funding_or_raise = _mock_refresh
    result = asyncio.run(bot.funding_startup_check_with_retries())
    assert result is False
    assert attempts["count"] == 3


def test_satellite_trade_progress_requires_bounds_and_return_to_core():
    bot = _make_bot()
    bot.satellite_core_tolerance = Decimal("0.01")
    bot.sat_touched_upper = False
    bot.sat_touched_lower = False
    core = Decimal("0.5")
    lower = Decimal("0.2")
    upper = Decimal("0.8")

    assert bot.update_satellite_trade_progress(Decimal("0.8"), core, lower, upper) is False
    assert bot.update_satellite_trade_progress(Decimal("0.2"), core, lower, upper) is False
    assert bot.update_satellite_trade_progress(Decimal("0.5"), core, lower, upper) is True
    assert bot.sat_touched_upper is False
    assert bot.sat_touched_lower is False


def test_satellite_trade_progress_not_counted_without_return_to_core():
    bot = _make_bot()
    bot.satellite_core_tolerance = Decimal("0.01")
    bot.sat_touched_upper = False
    bot.sat_touched_lower = False
    core = Decimal("0.5")
    lower = Decimal("0.2")
    upper = Decimal("0.8")

    assert bot.update_satellite_trade_progress(Decimal("0.8"), core, lower, upper) is False
    assert bot.update_satellite_trade_progress(Decimal("0.2"), core, lower, upper) is False
    assert bot.update_satellite_trade_progress(Decimal("0.45"), core, lower, upper) is False
