#!/usr/bin/env python3

import asyncio
import sys
import contextlib
import signal
from decimal import Decimal
import argparse
from pathlib import Path
import dotenv

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from arb.arb_binance_lighter import BinanceLighterConfig, BinanceLighterArb
from exchanges.binance import BinanceClient
from helpers.logger import TradingLogger


def create_binance_client(config: BinanceLighterConfig, leg: str, logger=None):
    cfg = type(
        "Cfg",
        (),
        {
            "ticker": config.ticker,
            "contract_id": config.binance_contract_id,
            "quantity": config.max_order_size,
            "tick_size": config.binance_tick_size,
        },
    )
    return BinanceClient(cfg(), logger=logger)


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
    parser.add_argument('--slippage', type=Decimal, default=Decimal("0.20"),
                        help='max_slippage percent (default: 0.20) (%)')
    parser.add_argument('--order-interval', type=Decimal, default=Decimal("60"),
                        help='max_slippage (default: 60)')
    parser.add_argument('--env-file', type=str, default=".env",
                        help=".env file path (default: .env)")

    return parser.parse_args()

async def main():
    args = parse_arguments()
    logger = TradingLogger(exchange="BINANCE_LIGHTER_ARB", ticker=f"{args.ticker.upper()}", log_to_console=True)

    env_path = Path(args.env_file)
    if not env_path.exists():
        print(f"Env file not find: {env_path.resolve()}")
        sys.exit(1)
    dotenv.load_dotenv(args.env_file)

    symbol = f"{args.ticker.upper()}USDC"

    config = BinanceLighterConfig(
        ticker=args.ticker.upper(),
        binance_contract_id=symbol,
        max_order_size=args.quantity,
        min_order_size=Decimal("0.025"),
        max_position_size=args.total,
        order_timeout=5.0,
        loop_interval=1,
        lighter_slippage_pct=Decimal(args.slippage) / Decimal("100"),
        open_spread_threshold=args.open,
        close_spread_threshold=args.close,
    )

    bot = BinanceLighterArb(
        config=config,
        binance_client_factory=create_binance_client,
        logger=logger,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_stop(*_):
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: stop_event.set())

    run_task = asyncio.create_task(bot.run())
    stop_task = asyncio.create_task(stop_event.wait())
    try:
        await asyncio.wait(
            [run_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        stop_task.cancel()
        with contextlib.suppress(Exception):
            await stop_task
        await bot.shutdown()
        if not run_task.done():
            run_task.cancel()
            with contextlib.suppress(Exception):
                await run_task


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
