import os
import asyncio
from decimal import Decimal
from exchanges.paradex import ParadexClient

class Config:
    def __init__(self, d):
        for k, v in d.items(): setattr(self, k, v)

async def main():
    pd_config = {'ticker': 'SOL', 'contract_id': 'SOL-USD-PERP', 'quantity': Decimal('0.3'), 'tick_size': Decimal('0.01')}
    client = ParadexClient(Config(pd_config))
    order_id = "PARADEX_ORDER_ID_HERE" # Need a real ID or just mock response for analysis
    # I will try to fetch the most recent order to see the structure
    try:
        res = client.paradex.api_client.fetch_orders({"market": "SOL-USD-PERP", "status": "CLOSED"})
        if res.get('results'):
            order = res['results'][0]
            print("RAW ORDER DATA:")
            import json
            print(json.dumps(order, indent=2))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
