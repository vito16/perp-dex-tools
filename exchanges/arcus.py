"""Arcus adapter for perp-dex-tools.

This adapter only implements the exchange interface. Trading strategy remains
in trading_bot.py.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import time
from datetime import datetime
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat

from .base import BaseExchangeClient, OrderInfo, OrderResult
from helpers.logger import TradingLogger

def dec(value: Any) -> Decimal:
    return Decimal(str(value))

def canonical(value: dict) -> bytes:
    return json.dumps(
        value,
        separators=(",", ":"),
        sort_keys=True,
        ensure_ascii=True,
    ).encode()

class ArcusClient(BaseExchangeClient):
    """Arcus REST adapter used by the existing TradingBot."""

    def __init__(self, config):
        self.market_data: Optional[dict] = None
        self.market_loaded_at = 0.0
        self.order_update_handler = None

        super().__init__(config)

        self.logger = TradingLogger(
            exchange="arcus",
            ticker=self.config.ticker,
            log_to_console=True,
        )

        self.network = os.getenv("ARCUS_NETWORK", "mainnet").lower()
        if self.network not in ("mainnet", "testnet"):
            raise ValueError("ARCUS_NETWORK must be mainnet or testnet")

        self.base_url = (
            "https://api.arcus.xyz"
            if self.network == "mainnet"
            else "https://api.testnet.arcus.xyz"
        )

        self.address = os.getenv("ARCUS_WALLET_ADDRESS", "").lower()
        self.account_index = int(os.getenv("ARCUS_ACCOUNT_INDEX", "0"))
        self.signing_key_text = os.getenv("ARCUS_API_SIGNING_KEY", "")

        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "perp-dex-tools-arcus",
        })

        self.private_key: Optional[Ed25519PrivateKey] = None
        self.api_key: Optional[str] = None
        self.last_nonce = 0

    def _validate_config(self) -> None:
        ticker = str(getattr(self.config, "ticker", "")).upper()
        if not ticker:
            raise ValueError("Arcus ticker is required")

        # CLI uses QQQ; Arcus market uses QQQ-USD.
        ticker = ticker.removesuffix("-USD")
        self.config.ticker = ticker
        self.config.contract_id = f"{ticker}-USD"

        address = os.getenv("ARCUS_WALLET_ADDRESS", "")
        key = os.getenv("ARCUS_API_SIGNING_KEY", "")

        if not re.fullmatch(r"0x[0-9a-fA-F]{40}", address):
            raise ValueError("ARCUS_WALLET_ADDRESS is missing or invalid")

        if not key:
            raise ValueError("ARCUS_API_SIGNING_KEY is missing")

        account_index = int(os.getenv("ARCUS_ACCOUNT_INDEX", "0"))
        if not 0 <= account_index <= 9:
            raise ValueError("ARCUS_ACCOUNT_INDEX must be between 0 and 9")

    def _account_params(self) -> dict:
        return {
            "address": self.address,
            "accountIndex": self.account_index,
        }

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[dict] = None,
        body: Optional[dict] = None,
        signed_payload: Optional[dict] = None,
    ) -> dict:
        headers = {}

        if signed_payload is not None:
            if self.private_key is None or self.api_key is None:
                raise RuntimeError("Arcus signing key is not initialized")

            timestamp = int(body["timestamp"])
            headers = {
                "X-API-Key": self.api_key,
                "X-Timestamp": str(timestamp),
                "X-Signature": self.private_key.sign(
                    canonical(signed_payload)
                ).hex(),
            }

        response = self.session.request(
            method=method,
            url=self.base_url + path,
            params=params,
            json=body,
            headers=headers,
            timeout=20,
            allow_redirects=False,
        )

        if response.status_code not in (200, 202):
            raise RuntimeError(
                f"Arcus {method} {path} HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )

        try:
            data = response.json()
        except ValueError as exc:
            raise RuntimeError("Arcus returned non-JSON response") from exc

        if not isinstance(data, dict) or data.get("error"):
            raise RuntimeError(f"Invalid Arcus response from {path}")

        return data

    async def _call(self, *args, **kwargs):
        return await asyncio.to_thread(self._request, *args, **kwargs)

    def _load_signer(self) -> None:
        raw = self.signing_key_text.removeprefix("0x")

        if not re.fullmatch(r"[0-9a-fA-F]{64}", raw):
            raise ValueError(
                "ARCUS_API_SIGNING_KEY must be a 32-byte Ed25519 key"
            )

        self.private_key = Ed25519PrivateKey.from_private_bytes(
            bytes.fromhex(raw)
        )

        self.api_key = self.private_key.public_key().public_bytes(
            Encoding.Raw,
            PublicFormat.Raw,
        ).hex()

        # Arcus requires the public API key header for authenticated reads.
        self.session.headers["X-API-Key"] = self.api_key

    def _nonce(self) -> int:
        value = time.time_ns()
        self.last_nonce = max(self.last_nonce + 1, value)
        return self.last_nonce

    def _load_market_sync(self, force: bool = False) -> dict:
        if (
            self.market_data is not None
            and not force
            and time.monotonic() - self.market_loaded_at < 30
        ):
            return self.market_data

        data = self._request("GET", "/v1/markets")
        markets = data.get("markets", [])

        matches = [
            market
            for market in markets
            if market.get("marketDisplayName") == self.config.contract_id
        ]

        if len(matches) != 1:
            raise RuntimeError(
                f"Arcus market not found: {self.config.contract_id}"
            )

        market = matches[0]

        required_fields = (
            "marketId",
            "tickSize",
            "stepSize",
            "minOrderSize",
            "maxOrderSize",
            "minOrderNotional",
        )

        for field in required_fields:
            if field not in market:
                raise RuntimeError(
                    f"Arcus market field missing: {field}"
                )

        self.market_data = market
        self.market_loaded_at = time.monotonic()
        self.config.tick_size = dec(market["tickSize"])

        return market

    def _tick_size(self, price: Decimal) -> Decimal:
        market = self._load_market_sync()
        tiers = market.get("tickTiers", [])

        for tier in tiers:
            if (
                "upToPrice" not in tier
                or price < dec(tier["upToPrice"])
            ):
                return dec(tier["tick"])

        return dec(market["tickSize"])

    def _round_price(self, price: Decimal, side: str) -> Decimal:
        tick = self._tick_size(price)
        rounding = ROUND_DOWN if side == "BUY" else ROUND_UP
        return (
            price / tick
        ).to_integral_value(rounding=rounding) * tick

    def _round_quantity(self, quantity: Decimal) -> Decimal:
        market = self._load_market_sync()
        step = dec(market["stepSize"])
        return (
            quantity / step
        ).to_integral_value(rounding=ROUND_DOWN) * step

    def _order_payload(self, body: dict, market: dict) -> dict:
        def integer(value, unit):
            number = dec(value) / dec(unit)
            if number != number.to_integral_value():
                raise ValueError(
                    "Arcus order price or quantity precision mismatch"
                )
            return int(number)

        payload = {
            "ad": body["address"].lower(),
            "ai": body["accountIndex"],
            "ct": int(body["timestamp"]),
            "g": int(body["goodTilTime"]) * 1000,
            "m": body["marketId"],
            "op": 1,
            "p": integer(body["price"], market["tickSize"]),
            "q": integer(body["quantity"], market["stepSize"]),
            "r": int(bool(body["reduceOnly"])),
            "s": {"BUY": 0, "SELL": 1}[body["orderSide"]],
            "t": {"GTT": 0, "FOK": 1, "IOC": 2, "ALO": 3}[
                body["timeInForce"]
            ],
            "v": 1,
        }

        if body.get("clientId"):
            payload["c"] = body["clientId"]

        return payload

    def _make_order(
        self,
        quantity: Decimal,
        price: Decimal,
        side: str,
        reduce_only: bool,
    ) -> Tuple[dict, dict]:
        market = self._load_market_sync()

        quantity = self._round_quantity(dec(quantity))
        price = self._round_price(dec(price), side)

        if quantity <= 0:
            raise ValueError("Arcus quantity must be positive")

        if quantity > dec(market["maxOrderSize"]):
            raise ValueError("Arcus quantity exceeds maxOrderSize")

        if not reduce_only:
            if quantity < dec(market["minOrderSize"]):
                raise ValueError("Arcus quantity is below minOrderSize")

            if quantity * price < dec(market["minOrderNotional"]):
                raise ValueError(
                    "Arcus order notional is below minOrderNotional"
                )

        timestamp = self._nonce()
        client_id = (
            f"pdt_{int(time.time())}_"
            f"{hashlib.sha1(os.urandom(8)).hexdigest()[:8]}"
        )

        body = {
            **self._account_params(),
            "marketId": market["marketId"],
            "orderSide": side,
            "orderType": "LIMIT",
            "timeInForce": "ALO",
            "quantity": format(quantity, "f"),
            "price": format(price, "f"),
            "goodTilTime": str(
                timestamp // 1000 + 40 * 86400 * 1_000_000
            ),
            "timestamp": timestamp,
            "clientId": client_id,
            "reduceOnly": bool(reduce_only),
        }

        return body, self._order_payload(body, market)

    async def connect(self) -> None:
        self._load_signer()

        await self._call("GET", "/v1/time")
        await self._call(
            "GET",
            "/v1/apiKeys",
            {"address": self.address},
        )

        await asyncio.to_thread(self._load_market_sync, True)

        self.logger.log("Arcus REST connection ready", "INFO")

    async def disconnect(self) -> None:
        self.logger.log("Arcus disconnected", "INFO")

    async def get_account_snapshot(self) -> dict:
        """Return the raw Arcus account snapshot for monitoring."""
        return await self._call(
            "GET",
            "/v1/account",
            self._account_params(),
        )

    async def get_contract_attributes(self) -> Tuple[str, Decimal]:
        market = await asyncio.to_thread(self._load_market_sync)
        return self.config.contract_id, dec(market["tickSize"])

    async def fetch_bbo_prices(
        self,
        contract_id: str,
    ) -> Tuple[Decimal, Decimal]:
        data = await self._call(
            "GET",
            "/v1/bbo/" + quote(contract_id, safe=""),
        )

        bid = dec(data["bestBid"]["price"])
        ask = dec(data["bestAsk"]["price"])

        if not (0 < bid <= ask):
            raise RuntimeError("Arcus returned invalid BBO")

        return bid, ask

    async def get_order_price(self, direction: str) -> Decimal:
        bid, ask = await self.fetch_bbo_prices(self.config.contract_id)

        if direction.lower() == "buy":
            return self._round_price(bid, "BUY")

        return self._round_price(ask, "SELL")

    async def place_open_order(
        self,
        contract_id: str,
        quantity: Decimal,
        direction: str,
    ) -> OrderResult:
        side = "BUY" if direction.lower() == "buy" else "SELL"

        bid, ask = await self.fetch_bbo_prices(contract_id)
        price = bid if side == "BUY" else ask

        body, payload = self._make_order(
            quantity=quantity,
            price=price,
            side=side,
            reduce_only=False,
        )

        data = await self._call(
            "POST",
            "/v1/placeOrder",
            self._account_params(),
            body,
            payload,
        )

        order_id = str(
            data.get("orderId")
            or data.get("order", {}).get("orderId", "")
        )

        if not order_id:
            raise RuntimeError("Arcus placeOrder returned no orderId")

        return OrderResult(
            success=True,
            order_id=order_id,
            side=direction.lower(),
            size=dec(body["quantity"]),
            price=dec(body["price"]),
            status="OPEN",
        )

    async def place_close_order(
        self,
        contract_id: str,
        quantity: Decimal,
        price: Decimal,
        side: str,
    ) -> OrderResult:
        order_side = "BUY" if side.lower() == "buy" else "SELL"

        bid, ask = await self.fetch_bbo_prices(contract_id)
        tick = self._tick_size(dec(price))

        if order_side == "SELL" and dec(price) <= bid:
            price = bid + tick

        if order_side == "BUY" and dec(price) >= ask:
            price = ask - tick

        body, payload = self._make_order(
            quantity=quantity,
            price=price,
            side=order_side,
            reduce_only=True,
        )

        data = await self._call(
            "POST",
            "/v1/placeOrder",
            self._account_params(),
            body,
            payload,
        )

        order_id = str(
            data.get("orderId")
            or data.get("order", {}).get("orderId", "")
        )

        if not order_id:
            raise RuntimeError("Arcus close order returned no orderId")

        return OrderResult(
            success=True,
            order_id=order_id,
            side=side.lower(),
            size=dec(body["quantity"]),
            price=dec(body["price"]),
            status="OPEN",
        )

    def _normalize_status(self, row: dict) -> str:
        status = str(
            row.get("status")
            or row.get("state")
            or "OPEN"
        ).upper()

        return {
            "CLOSED": "FILLED",
            "CANCELLED": "CANCELED",
        }.get(status, status)

    def _to_order_info(self, row: dict) -> OrderInfo:
        size = dec(
            row.get("originalSize", row.get("quantity", 0))
        )
        filled = dec(row.get("filledSize", 0))
        remaining = dec(
            row.get(
                "remainingSize",
                max(size - filled, Decimal("0")),
            )
        )

        side = str(
            row.get("orderSide", row.get("side", ""))
        ).lower()

        return OrderInfo(
            order_id=str(
                row.get("orderId", row.get("id", ""))
            ),
            side=side,
            size=size,
            price=dec(row.get("price", 0)),
            status=self._normalize_status(row),
            filled_size=filled,
            remaining_size=remaining,
            cancel_reason=str(row.get("cancelReason", "")),
        )

    async def get_order_info(
        self,
        order_id: str,
    ) -> Optional[OrderInfo]:
        try:
            row = await self._call(
                "GET",
                "/v1/order/" + quote(str(order_id), safe=""),
                self._account_params(),
            )
        except RuntimeError as exc:
            if "HTTP 404" in str(exc):
                return None
            raise

        if "order" in row and isinstance(row["order"], dict):
            row = row["order"]

        return self._to_order_info(row)

    async def get_active_orders(
        self,
        contract_id: str,
    ) -> List[OrderInfo]:
        try:
            data = await self._call(
                "GET",
                "/v1/openOrders",
                {
                    **self._account_params(),
                    "market": contract_id,
                },
            )
        except RuntimeError as exc:
            # Arcus returns 404 for a new account with no activity.
            if (
                "HTTP 404" in str(exc)
                and "no activity" in str(exc).lower()
            ):
                return []
            raise

        return [
            self._to_order_info(row)
            for row in data.get("orders", [])
        ]

    async def cancel_order(self, order_id: str) -> OrderResult:
        market = self._load_market_sync()
        timestamp = self._nonce()

        body = {
            **self._account_params(),
            "marketId": market["marketId"],
            "timestamp": timestamp,
            "kind": "orderId",
            "orderId": str(order_id),
        }

        payload = {
            "ad": self.address,
            "ai": self.account_index,
            "ct": timestamp,
            "m": market["marketId"],
            "op": 2,
            "v": 1,
            "id": str(order_id),
        }

        await self._call(
            "POST",
            "/v1/cancelOrder",
            self._account_params(),
            body,
            payload,
        )

        return OrderResult(
            success=True,
            order_id=str(order_id),
            status="CANCELED",
        )

    async def get_account_positions(self) -> Decimal:
        try:
            data = await self._call(
                "GET",
                "/v1/account",
                self._account_params(),
            )
        except RuntimeError as exc:
            # Arcus returns 404 for a new account with no activity.
            if (
                "HTTP 404" in str(exc)
                and "no activity" in str(exc).lower()
            ):
                return Decimal("0")
            raise

        market = self._load_market_sync()
        market_id = str(market["marketId"])
        row = data.get("positions", {}).get(market_id)

        if not row:
            return Decimal("0")

        return dec(row.get("size", 0))

    async def can_open_new_orders(self) -> bool:
        """Return whether new entry orders are currently allowed.

        ARCUS_ENTRY_SESSION_ENABLED=false allows new entries all day.
        When enabled, entries are restricted to the configured New York
        session window. Existing reduce-only close orders are never blocked.
        """
        enabled = os.getenv(
            "ARCUS_ENTRY_SESSION_ENABLED",
            "false",
        ).strip().lower() in ("1", "true", "yes", "on")

        # QQQ-USD is a perpetual market. When the session restriction is
        # disabled, allow new entries around the clock.
        if not enabled:
            return True

        market = await asyncio.to_thread(self._market_sync)
        rth = market.get("regularTradingHours")

        if not rth:
            return True

        if market.get("isOutsideRth") is True:
            return False

        tz = ZoneInfo(
            rth.get("timezone", "America/New_York")
        )

        now = datetime.now(tz)

        if now.weekday() >= 5:
            return False

        start_text = os.getenv(
            "ARCUS_ENTRY_SESSION_START",
            "09:30",
        )
        end_text = os.getenv(
            "ARCUS_ENTRY_SESSION_END",
            "16:00",
        )

        start_h, start_m = (
            int(value)
            for value in start_text.split(":", 1)
        )
        end_h, end_m = (
            int(value)
            for value in end_text.split(":", 1)
        )

        current_seconds = (
            now.hour * 3600
            + now.minute * 60
            + now.second
        )
        start_seconds = start_h * 3600 + start_m * 60
        end_seconds = end_h * 3600 + end_m * 60

        return start_seconds <= current_seconds < end_seconds

    def setup_order_update_handler(self, handler) -> None:
        # Existing TradingBot polls order status through REST.
        self.order_update_handler = handler

    def get_exchange_name(self) -> str:
        return "arcus"
