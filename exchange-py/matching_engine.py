import asyncio
import json
import nats
import pykx as kx
from datetime import datetime

# Importing native order matching models from your workspace
from order_matching.matching_engine import MatchingEngine
from order_matching.order import LimitOrder, MarketOrder
from order_matching.side import Side

# Initialize Engine
engine = MatchingEngine()

# Fast index map tracking which order belongs to which price/side level for quick eviction
# Format: { order_id: (side_str, price) }
order_routing_registry = {}

def print_engine_book(symbol):
    book = engine.order_book
    print("\n" + "="*45)
    print(f" LOB: {symbol} | {datetime.now().time()}")
    print("-" * 45)
    for price in sorted(book.asks.keys(), reverse=True):
        print(f"\033[91mASK\033[0m | {price:<12.2f} | {book.asks[price].total_volume:<10.4f}")
    print("-" * 15 + " SPREAD " + "-" * 15)
    for price in sorted(book.bids.keys(), reverse=True):
        print(f"\033[92mBID\033[0m | {price:<12.2f} | {book.bids[price].total_volume:<10.4f}")
    print("="*45)

async def order_processor(msg, tp):
    """
    Processes incoming market, limit, and cancellation requests.
    Differentiates logic based on NATS subject routing tokens.
    """
    global order_routing_registry
    try:
        data = json.loads(msg.data.decode())
        subject = msg.subject  # 'orders.market', 'orders.limit', or 'orders.cancel'
        
        now = datetime.now()
        symbol = data.get('pair')

        # ---------------------------------------------------------
        # Case A: Handle Cancellation Requests
        # ---------------------------------------------------------
        if "cancel" in subject:
            target_id = data.get('order_id')
            client_id = data.get('client_id', 'unknown_bot')
            
            print(f"Processing CANCEL request for Order ID: {target_id}")
            
            # Use your native engine method to drop the tracking structure from the queue
            # Wrap in try/except block to handle cases where the order already filled completely
            success = False
            try:
                engine.cancel_order(target_id)
                success = True
            except Exception as engine_err:
                print(f"Order book clean up warning: {engine_err}")

            if success:
                # Log the cancellation modification to kdb+ orders table with zeroed out size
                # Indicating a system cancellation terminal state update
                cancel_payload = [
                    kx.TimestampAtom(now),
                    kx.SymbolAtom(symbol),
                    kx.SymbolAtom(target_id),
                    kx.SymbolAtom("cancel"),
                    kx.FloatAtom(0.0),
                    kx.FloatAtom(0.0),
                    kx.SymbolAtom(client_id)
                ]
                tp('.u.upd', 'orders', cancel_payload, wait=False)
                
                response_payload = {"status": "cancelled", "order_id": target_id}
            else:
                response_payload = {"status": "rejected", "reason": "Order ID not found or already filled"}

            if msg.reply:
                await msg.respond(json.dumps(response_payload).encode('utf-8'))
            return

        # ---------------------------------------------------------
        # Case B: Handle Order Creation Requests (Market / Limit)
        # ---------------------------------------------------------
        client_id = data.get('client_id')
        side_str = data.get('side', 'buy').lower()
        quantity = float(data.get('quantity', 0.0))
        order_id = f"ord_{int(now.timestamp() * 1000)}"
        side = Side.BUY if side_str == 'buy' else Side.SELL

        if "market" in subject:
            price_logged = 0.0
            print(f"Processing MARKET order: {order_id} | {side_str} {quantity} {symbol}")
            
            market_order = MarketOrder(order_id, side, quantity)
            engine.add_order(market_order)
            
        elif "limit" in subject:
            price_logged = float(data.get('price', 0.0))
            print(f"Processing LIMIT order: {order_id} | {side_str} {quantity} {symbol} @ {price_logged}")
            
            limit_order = LimitOrder(order_id, side, price_logged, quantity)
            engine.add_order(limit_order)
        
        # Log Creation to kdb+ Tickerplant
        orders_payload = [
            kx.TimestampAtom(now),
            kx.SymbolAtom(symbol),
            kx.SymbolAtom(order_id),
            kx.SymbolAtom(side_str),
            kx.FloatAtom(price_logged),
            kx.FloatAtom(quantity),
            kx.SymbolAtom(client_id)
        ]
        tp('.u.upd', 'orders', orders_payload, wait=False)

        # Match Order Book and Track Executions
        results = engine.match()
        trades_executed = []
        
        if hasattr(results, 'trades') and results.trades:
            for trade in results.trades:
                trades_executed.append({
                    "trade_id": str(trade.trade_id),
                    "price": float(trade.price),
                    "size": float(trade.size)
                })
                
                taker_id = str(trade.incoming_order_id)
                maker_id = str(trade.book_order_id)
                trade_side_str = str(trade.side).lower()
                
                executions_payload = [
                    kx.TimestampAtom(now),
                    kx.SymbolAtom(symbol),
                    kx.SymbolAtom(str(trade.trade_id)),
                    kx.SymbolAtom(trade_side_str),
                    kx.FloatAtom(float(trade.price)),
                    kx.FloatAtom(float(trade.size)),
                    kx.SymbolAtom(taker_id),
                    kx.SymbolAtom(maker_id)
                ]
                tp('.u.upd', 'executions', executions_payload, wait=False)
                print(f"*** MATCH: {trade.price} | Size: {trade.size} ***")

        print_engine_book(symbol)

        response_payload = {
            "status": "success",
            "order_id": order_id,
            "client_id": client_id,
            "pair": symbol,
            "processed_at": str(now),
            "trades": trades_executed
        }
        
        if msg.reply:
            await msg.respond(json.dumps(response_payload).encode('utf-8'))

    except Exception as e:
        print(f"Error handling request: {e}")
        if msg.reply:
            error_response = {"status": "rejected", "error": str(e)}
            await msg.respond(json.dumps(error_response).encode('utf-8'))

async def main():
    nc = await nats.connect("nats://localhost:4222")
    
    with kx.SyncQConnection('localhost', 5000) as tp:
        print("Worker connected to NATS and kdb+ TP.")
        
        # Subscribe to all lifecycle routes under the 'orders' umbrella
        # This wildcard pattern captures orders.market, orders.limit, and orders.cancel
        await nc.subscribe("orders.*", cb=lambda msg: order_processor(msg, tp))
        print("Listening for transactions on 'orders.market', 'orders.limit', and 'orders.cancel'...")

        while True:
            await asyncio.sleep(1)

if __name__ == '__main__':
    try: 
        asyncio.run(main())
    except KeyboardInterrupt: 
        print("Matching engine stopped cleanly.")

