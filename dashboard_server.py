import os
import asyncio
import json
import csv
import logging
import glob
from decimal import Decimal
from datetime import datetime
from aiohttp import web

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Import exchange clients
from exchanges.backpack import BackpackClient
from exchanges.lighter import LighterClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dashboard_server")

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return super(DecimalEncoder, self).default(obj)

# Global clients cache to avoid session leaks and repeated initialization
clients_cache = {}

MONITORED_TICKERS_FILE = "monitored_tickers.json"

def load_monitored_tickers():
    if not os.path.exists(MONITORED_TICKERS_FILE):
        return []
    try:
        with open(MONITORED_TICKERS_FILE, 'r') as f:
            return json.load(f)
    except:
        return []

def save_monitored_tickers(tickers):
    with open(MONITORED_TICKERS_FILE, 'w') as f:
        json.dump(tickers, f)

class Config:
    def __init__(self, config_dict):
        for key, value in config_dict.items():
            setattr(self, key, value)

def get_tickers_from_logs():
    """Discover tickers from trade log filenames."""
    tickers = set()
    log_files = glob.glob("logs/*_hedge_mode_trades.csv")
    for f in log_files:
        basename = os.path.basename(f)
        parts = basename.split("_")
        if len(parts) >= 2:
            ticker = parts[1]
            if ticker and ticker.upper() != 'TICKER':
                tickers.add(ticker.upper())
    return sorted(list(tickers))

async def get_or_create_clients(ticker):
    """Get or create cached clients for a ticker."""
    if ticker not in clients_cache:
        bp_config = Config({
            'ticker': ticker, 'contract_id': '', 'quantity': Decimal('1000000'),
            'tick_size': Decimal('0'), 'close_order_side': 'sell'
        })
        lt_config = Config({
            'ticker': ticker, 'contract_id': '', 'quantity': Decimal('0'),
            'tick_size': Decimal('1'), 'close_order_side': 'sell'
        })
        
        bp_client = BackpackClient(bp_config)
        lt_client = LighterClient(lt_config)
        
        # Initialize loggers
        from helpers.logger import TradingLogger
        bp_client.logger = TradingLogger(exchange="backpack", ticker=ticker, log_to_console=False)
        lt_client.logger = TradingLogger(exchange="lighter", ticker=ticker, log_to_console=False)
        
        # Initialize Lighter ApiClient
        from lighter import ApiClient, Configuration
        lt_client.api_client = ApiClient(configuration=Configuration(host=lt_client.base_url))
        
        clients_cache[ticker] = {'bp': bp_client, 'lt': lt_client}
    
    return clients_cache[ticker]

async def get_exchange_data(ticker):
    """Fetch real-time data using cached clients."""
    logger.info(f"[{ticker}] Starting data fetch...")
    try:
        clients = await get_or_create_clients(ticker)
        bp_client = clients['bp']
        lt_client = clients['lt']
    except Exception as e:
        logger.error(f"[{ticker}] Error initializing clients: {e}")
        return {'ticker': ticker, 'error': str(e)}

    data = {
        'ticker': ticker,
        'timestamp': datetime.now().isoformat(),
        'backpack': {'balance': 0, 'positions': 0, 'orders': [], 'history': [], 'error': None},
        'lighter': {'balance': 0, 'positions': 0, 'orders': [], 'history': [], 'error': None},
        'status': {'imbalance': 0, 'is_balanced': True}
    }

    # Backpack Fetch
    try:
        logger.info(f"[{ticker}] Backpack: Fetching contract attributes...")
        if not bp_client.config.contract_id:
            # get_contract_attributes has sync calls inside, wrap it
            symbol, tick_size = await asyncio.wait_for(asyncio.to_thread(bp_client.get_contract_attributes_sync), timeout=10)
            bp_client.config.contract_id = symbol
            bp_client.config.tick_size = tick_size
        
        # Balance (SYNC CALL)
        logger.info(f"[{ticker}] Backpack: Fetching balances...")
        try:
            b_resp = await asyncio.wait_for(asyncio.to_thread(bp_client.account_client.get_balances), timeout=10)
            if isinstance(b_resp, list):
                for b in b_resp:
                    if b.get('asset') == 'USDC':
                        data['backpack']['balance'] = float(b.get('available', 0)) + float(b.get('locked', 0))
            elif isinstance(b_resp, dict) and 'USDC' in b_resp:
                u = b_resp['USDC']
                data['backpack']['balance'] = float(u.get('available', 0)) + float(u.get('locked', 0)) if isinstance(u, dict) else float(u)
        except Exception as e:
            logger.warning(f"[{ticker}] Backpack: Balance fetch failed: {e}")
        
        logger.info(f"[{ticker}] Backpack: Fetching positions...")
        data['backpack']['positions'] = float(await asyncio.wait_for(asyncio.to_thread(bp_client.get_account_positions_sync), timeout=10))
        
        # Active Orders
        logger.info(f"[{ticker}] Backpack: Fetching active orders...")
        try:
            orders = await asyncio.wait_for(asyncio.to_thread(bp_client.get_active_orders_sync, bp_client.config.contract_id), timeout=10)
            data['backpack']['orders'] = [{
                'id': o.order_id, 'side': o.side, 'price': float(o.price),
                'size': float(o.size), 'status': o.status, 'filled': float(o.filled_size)
            } for o in orders]
        except Exception as e:
            logger.warning(f"[{ticker}] Backpack: Orders fetch failed: {e}")
        
        # Fill History (for P&L) - SYNC CALL
        logger.info(f"[{ticker}] Backpack: Fetching fill history...")
        try:
            fills = await asyncio.to_thread(bp_client.account_client.get_fill_history, symbol=bp_client.config.contract_id, limit=20)
            if isinstance(fills, list):
                data['backpack']['history'] = [{
                    'id': f.get('id'), 'side': f.get('side'), 'price': float(f.get('price')),
                    'quantity': float(f.get('quantity')), 'p_l': f.get('realizedPnl', '--'),
                    'timestamp': f.get('timestamp')
                } for f in fills]
        except Exception as e:
            logger.warning(f"[{ticker}] Backpack: History fetch failed: {e}")

    except Exception as e:
        logger.error(f"[{ticker}] Backpack overall error: {e}")
        data['backpack']['error'] = str(e)

    # Lighter Fetch
    try:
        logger.info(f"[{ticker}] Lighter: Initializing...")
        await asyncio.wait_for(lt_client._initialize_lighter_client(), timeout=15)
        if not lt_client.config.contract_id:
            m_id, _ = await asyncio.wait_for(lt_client.get_contract_attributes(), timeout=10)
            lt_client.config.contract_id = m_id
        
        logger.info(f"[{ticker}] Lighter: Fetching positions...")
        data['lighter']['positions'] = float(await asyncio.wait_for(lt_client.get_account_positions(), timeout=10))
        
        # Active Orders
        logger.info(f"[{ticker}] Lighter: Fetching orders...")
        try:
            orders = await asyncio.wait_for(lt_client.get_active_orders(lt_client.config.contract_id), timeout=10)
            data['lighter']['orders'] = [{
                'id': o.order_id, 'side': o.side, 'price': float(o.price),
                'size': float(o.size), 'status': o.status, 'filled': float(o.filled_size)
            } for o in orders]
        except Exception as e:
            logger.warning(f"[{ticker}] Lighter: Orders fetch failed: {e}")
        
        # Lighter Balance
        logger.info(f"[{ticker}] Lighter: Fetching balance...")
        try:
            import lighter as lighter_sdk
            # Generate auth token if needed (though account() might not need it, trades() definitely does)
            auth_token, err = lt_client.lighter_client.create_auth_token_with_expiry()
            
            account_api = lighter_sdk.AccountApi(lt_client.api_client)
            a_resp = await asyncio.wait_for(account_api.account(by="index", value=str(lt_client.account_index)), timeout=10)
            if a_resp and a_resp.accounts:
                 # Try collateral first if available_balance is 0 for some reason
                 balance = float(a_resp.accounts[0].available_balance or 0)
                 if balance == 0 and hasattr(a_resp.accounts[0], 'collateral'):
                      balance = float(a_resp.accounts[0].collateral or 0)
                 data['lighter']['balance'] = balance
        except Exception as e:
            logger.warning(f"[{ticker}] Lighter: Balance fetch failed: {e}")

        # Lighter History (Trades)
        logger.info(f"[{ticker}] Lighter: Fetching history...")
        try:
            order_api = lighter_sdk.OrderApi(lt_client.api_client)
            # Use OrderApi.trades with valid sort_by enum and auth token
            trades_resp = await asyncio.wait_for(order_api.trades(
                limit=10, 
                account_index=lt_client.account_index, 
                market_id=lt_client.config.contract_id,
                sort_by="timestamp",
                sort_dir="desc",
                auth=auth_token
            ), timeout=10)
            if trades_resp and trades_resp.trades:
                data['lighter']['history'] = [{
                    'id': t.trade_id, 'side': 'sell' if t.is_ask else 'buy', 
                    'price': float(t.price) / float(lt_client.price_multiplier),
                    'quantity': float(t.base_amount) / float(lt_client.base_amount_multiplier),
                    'p_l': '--', 
                    'timestamp': float(t.timestamp) if hasattr(t, 'timestamp') else 0
                } for t in trades_resp.trades]
        except Exception as e:
            logger.warning(f"[{ticker}] Lighter: History fetch failed: {e}")

    except Exception as e:
        logger.error(f"[{ticker}] Lighter overall error: {e}")
        data['lighter']['error'] = str(e)

    # Delta logic
    bp_p = data['backpack']['positions']
    lt_p = data['lighter']['positions']
    delta = bp_p + lt_p
    data['status']['imbalance'] = delta
    data['status']['is_balanced'] = abs(delta) < 0.0001
    
    logger.info(f"[{ticker}] Positions: BP={bp_p}, LT={lt_p}, Delta={delta}")
    logger.info(f"[{ticker}] Data fetch complete.")
    return data

async def handle_get_status_all(request):
    """Handle request for all tickers with a global timeout."""
    logger.info("Handling /api/status/all request...")
    try:
        # Use manually monitored tickers instead of automatic discovery
        tickers = await asyncio.to_thread(load_monitored_tickers)
        if not tickers:
            return web.json_response({'results': []})
            
        tasks = [get_exchange_data(t) for t in tickers]
        # Use a generous but firm 45s timeout for the entire gather
        results = await asyncio.wait_for(asyncio.gather(*tasks), timeout=45)
        return web.json_response({'results': results}, dumps=lambda x: json.dumps(x, cls=DecimalEncoder))
    except asyncio.TimeoutError:
        logger.error("Timeout handling /api/status/all")
        return web.json_response({'error': 'Request timed out waiting for exchange data'}, status=504)
    except Exception as e:
        logger.error(f"Error in handle_get_status_all: {e}")
        return web.json_response({'error': str(e)}, status=500)

async def handle_get_available_tickers(request):
    """Scan logs for available tickers not yet monitored."""
    try:
        all_tickers = await asyncio.to_thread(get_tickers_from_logs)
        monitored = await asyncio.to_thread(load_monitored_tickers)
        available = [t for t in all_tickers if t not in monitored]
        return web.json_response({'available': available})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

async def handle_monitored_tickers(request):
    """GET current list or POST to update it."""
    if request.method == 'GET':
        tickers = await asyncio.to_thread(load_monitored_tickers)
        return web.json_response({'tickers': tickers})
    elif request.method == 'POST':
        try:
            data = await request.json()
            tickers = data.get('tickers', [])
            if not isinstance(tickers, list):
                 return web.json_response({'error': 'Invalid format'}, status=400)
            
            # Sanitize: upper case and unique
            tickers = sorted(list(set([str(t).upper().strip() for t in tickers if t])))
            await asyncio.to_thread(save_monitored_tickers, tickers)
            return web.json_response({'status': 'success', 'tickers': tickers})
        except Exception as e:
            return web.json_response({'error': str(e)}, status=500)

async def handle_health(request):
    return web.json_response({'status': 'ok', 'timestamp': datetime.now().isoformat()})

async def handle_index(request):
    return web.FileResponse('./static/dashboard.html')

async def on_shutdown(app):
    logger.info("Server shutting down, closing clients...")
    for ticker in clients_cache:
        try: await clients_cache[ticker]['lt'].api_client.close()
        except: pass

async def make_app():
    app = web.Application()
    app.on_shutdown.append(on_shutdown)
    app.router.add_get('/', handle_index)
    app.router.add_get('/health', handle_health)
    app.router.add_get('/api/status/all', handle_get_status_all)
    app.router.add_get('/api/available_tickers', handle_get_available_tickers)
    app.router.add_get('/api/monitored_tickers', handle_monitored_tickers)
    app.router.add_post('/api/monitored_tickers', handle_monitored_tickers)
    app.router.add_static('/static/', path='./static', name='static')
    return app

if __name__ == '__main__':
    web.run_app(make_app(), port=8080)
