import asyncio
import ccxt.pro as ccxt
import pykx as kx
import datetime
import json
import nats

async def watch_books_background():
    exchange = ccxt.okx()
    symbol = 'BTC/USDT'
    
    # 1. Setup NATS connection
    nc = await nats.connect("nats://localhost:4222") #
    
    try:
        # 2. Setup kdb+ connection
        with kx.SyncQConnection('localhost', 5000) as tp:
            print("Connected to TP and NATS. Streaming...")

            while True:
                orderbook = await exchange.watch_order_book(symbol, 5)
                now = datetime.datetime.now()
                
                # --- Prepare kdb+ Data (Flat List) ---
                data_kx = [now, symbol]
                bids, asks = orderbook['bids'][:5], orderbook['asks'][:5]
                data_kx.extend([float(b[0]) for b in bids]) # Prices
                data_kx.extend([float(b[1]) for b in bids]) # Sizes
                data_kx.extend([float(a[0]) for a in asks])
                data_kx.extend([float(a[1]) for a in asks])

                # --- Prepare NATS Data (JSON) ---
                # NATS often prefers structured JSON for web/microservice clients
                data_nats = {
                    "time": now.isoformat(),
                    "sym": symbol,
                    "bids": bids,
                    "asks": asks
                }
                payload = json.dumps(data_nats).encode() #

                # 3. Publish to BOTH
                tp('.u.upd', 'quotes', data_kx, wait=False)   # kdb+ push
                await nc.publish(f"quotes.{symbol}", payload) # NATS push
                
                await asyncio.sleep(1)
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        await nc.close() #
        await exchange.close()

if __name__ == '__main__':
    asyncio.run(watch_books_background())

