import asyncio
import json
import nats
from datetime import datetime

class MarketMakerAlgo:
    def __init__(self, nats_url="nats://localhost:4222"):
        self.nats_url = nats_url
        self.nc = None
        # Track IDs of active orders currently resting in the matching engine
        self.active_order_ids = set()
        self.client_id = "mm_algo_bot"

    async def connect(self):
        """Initializes connection to the NATS broker cluster."""
        self.nc = await nats.connect(self.nats_url)
        print(f"Algo Engine connected to NATS at {self.nats_url}")

    async def process_quote(self, msg):
        """Callback invoked whenever a fresh market quote event arrives from okx_sub.py."""
        try:
            # 1. Parse raw market data payload
            data = json.loads(msg.data.decode())
            clean_symbol = data.get('pair')
            
            # Extract bid/ask structures passed from our ingestion pipeline
            bids = data.get('bids', [])[:5]
            asks = data.get('asks', [])[:5]
            
            if not bids or not asks:
                return

            print(f"\n--- New Quote Event: {clean_symbol} | Triggering MM Logic ---")

            # 2. Cancel Previous Orders via NATS Request-Reply
            # Actively loop and request matching_engine.py to evict outstanding entries
            if self.active_order_ids:
                print(f"Purging {len(self.active_order_ids)} stale resting orders from the book...")
                
                # Copy to prevent modification runtime errors during the asynchronous iteration loop
                for oid in list(self.active_order_ids):
                    cancel_payload = {
                        "order_id": oid,
                        "pair": clean_symbol,
                        "client_id": self.client_id
                    }
                    try:
                        print(f"Sending CANCEL Request -> Order ID: {oid}")
                        # Issue blocking request over NATS network and wait for verification response
                        cancel_reply = await self.nc.request(
                            "orders.cancel", 
                            json.dumps(cancel_payload).encode('utf-8'), 
                            timeout=1.0
                        )
                        reply_data = json.loads(cancel_reply.data.decode())
                        print(f"Engine Cancellation Response: {reply_data.get('status')}")
                        
                    except asyncio.TimeoutError:
                        print(f"⚠️ Timeout: matching_engine failed to acknowledge cancel for {oid}")
                    except Exception as ex:
                        print(f"⚠️ Routing transmission error during cancel: {ex}")
                
                # Flush the tracked collection once complete
                self.active_order_ids.clear()

            # 3. Generate 5 Crossed/Flipped Limit Orders with 1% Markup Commission
            orders_to_send = []

            # Generate 5 Buy Limit Orders based on OKX Asks (Flipped side)
            # To buy safely, we subtract 1% commission from the asking price (bidding lower)
            for ask in asks[:5]:
                okx_ask_price = float(ask[0])
                okx_ask_size = float(ask[1])
                
                mm_buy_price = okx_ask_price * 0.99  # Add 1% commission buffer (discounted buy)
                orders_to_send.append({
                    "subject": "orders.limit",
                    "payload": {
                        "client_id": self.client_id,
                        "pair": clean_symbol,
                        "side": "buy",
                        "quantity": okx_ask_size,
                        "price": round(mm_buy_price, 2)
                    }
                })

            # Generate 5 Sell Limit Orders based on OKX Bids (Flipped side)
            # To sell profitably, we add 1% commission to the bid price (offering higher)
            for bid in bids[:5]:
                okx_bid_price = float(bid[0])
                okx_bid_size = float(bid[1])
                
                mm_sell_price = okx_bid_price * 1.01  # Add 1% commission buffer (marked-up sell)
                orders_to_send.append({
                    "subject": "orders.limit",
                    "payload": {
                        "client_id": self.client_id,
                        "pair": clean_symbol,
                        "side": "sell",
                        "quantity": okx_bid_size,
                        "price": round(mm_sell_price, 2)
                    }
                })

            # 4. Fire Requests Concurrently to matching_engine.py using Request-Reply Pattern
            for item in orders_to_send:
                subject = item["subject"]
                payload_bytes = json.dumps(item["payload"]).encode('utf-8')
                
                try:
                    print(f"Sending MM Order -> {item['payload']['side'].upper()} {item['payload']['quantity']} @ {item['payload']['price']}")
                    
                    # Request-Reply Pattern: Publish order and await acknowledgement response from matching_engine.py
                    response = await self.nc.request(subject, payload_bytes, timeout=1.0)
                    response_data = json.loads(response.data.decode())
                    
                    # Track newly created order IDs returned by matching_engine response payload
                    if response_data.get("status") == "success":
                        new_oid = response_data.get("order_id")
                        self.active_order_ids.add(new_oid)
                        
                except asyncio.TimeoutError:
                    print(f"⚠️ Timeout warning: matching_engine failed to acknowledge order on subject {subject}")
                except Exception as ex:
                    print(f"⚠️ Routing transmission error: {ex}")

        except Exception as e:
            print(f"Algo Processing Error: {e}")

    async def start(self):
        """Subscribes to market telemetry and enters keep-alive phase."""
        await self.connect()
        # Subscribe to market data feed published by okx_sub.py
        await self.nc.subscribe("quotes.*", cb=self.process_quote)
        print("Market Making Algo Microservice is active and listening for quotes...")
        
        while True:
            await asyncio.sleep(1)

if __name__ == '__main__':
    algo = MarketMakerAlgo()
    try:
        asyncio.run(algo.start())
    except KeyboardInterrupt:
        print("Algo container context killed.")

