import os
import json
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import nats
import pykx as kx

# ---------------------------------------------------------
# 1. Lifecycle & NATS Connection Management
# ---------------------------------------------------------
# We use FastAPI's lifespan to safely connect and disconnect from NATS
nats_client = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global nats_client
    # Retrieve NATS server address from environments, defaulting to local
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    try:
        # Establish connection to NATS broker
        nats_client = await nats.connect(nats_url)
        print(f"Successfully connected to NATS cluster at {nats_url}")
    except Exception as e:
        print(f"Failed to connect to NATS: {e}")
        raise e
    yield
    # Graceful teardown
    if nats_client:
        await nats_client.close()
        print("NATS connection closed safely.")

app = FastAPI(title="Trading API Gateway Microservice", lifespan=lifespan)

# ---------------------------------------------------------
# 2. Pydantic Models for Payload Validation
# ---------------------------------------------------------
class MarketOrder(BaseModel):
    client_id: str = Field(..., description="Unique ID for the client entity")
    pair: str = Field(..., description="Trading pair instrument, e.g., BTCUSD")
    side: str = Field(..., description="Order direction: 'buy' or 'sell'")
    quantity: float = Field(..., gt=0, description="Volume/Quantity to execute")

class LimitOrder(MarketOrder):
    price: float = Field(..., gt=0, description="Minimum/Maximum threshold execution price")

# ---------------------------------------------------------
# 3. Request-Reply Helper Utility
# ---------------------------------------------------------
async def request_reply_nats(subject: str, payload_dict: dict, timeout: float = 2.0) -> dict:
    """Serializes payloads, issues a request to NATS, and waits for a reply."""
    if not nats_client or not nats_client.is_connected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Upstream NATS broker unavailable"
        )

    try:
        # 1. Transform basic data types into structured, strict kdb+ types via PyKX dictionary
        # This standardizes memory alignments if the downstream service reads kdb+ native buffers
        kx_dict = kx.Dictionary({
            kx.SymbolAtom(k): (kx.SymbolAtom(v) if isinstance(v, str) else kx.FloatAtom(v))
            for k, v in payload_dict.items()
        })

        # 2. Convert PyKX objects down to a JSON-compatible format or pass Python-native fallback
        serialized_bytes = json.dumps(payload_dict).encode("utf-8")

        # 3. Publish via Request-Reply pattern and await downstream response
        response = await nats_client.request(subject, serialized_bytes, timeout=timeout)

        # 4. Decode returning payload from the downstream microservice
        return json.loads(response.data.decode("utf-8"))

    except nats.errors.TimeoutError:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail="Downstream order processing engine timed out"
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Messaging infrastructure error: {str(e)}"
        )

  # ---------------------------------------------------------
# 4. REST API Endpoint Router
# ---------------------------------------------------------
@app.post("/api/order/market", status_code=status.HTTP_202_ACCEPTED)
async def create_market_order(order: MarketOrder):
    """Submits a market execution order via NATS topic 'orders.market'."""
    payload = order.model_dump()
    reply = await request_reply_nats(subject="orders.market", payload_dict=payload)
    return reply

@app.post("/api/order/limit", status_code=status.HTTP_202_ACCEPTED)
async def create_limit_order(order: LimitOrder):
    """Submits a price-locked limit order via NATS topic 'orders.limit'."""
    payload = order.model_dump()
    reply = await request_reply_nats(subject="orders.limit", payload_dict=payload)
    return reply

@app.get("/api/account/position")
async def get_client_position(client_id: str, pair: str = None):
    """Fetches real-time position state and PnL metrics via NATS topic 'account.positions'."""
    payload = {"client_id": client_id}
    if pair:
        payload["pair"] = pair

    reply = await request_reply_nats(subject="account.positions", payload_dict=payload)
    return reply
