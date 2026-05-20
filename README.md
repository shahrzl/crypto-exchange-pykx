# crypto-exchange-pykx
A centralized crypto exchange based on KDBX and Python

# ⚡ Q-Stream Systems

An institutional-grade, low-latency automated market-making and order-matching platform. This event-driven architecture leverages **FastAPI** for client gateway access, **NATS** for high-throughput microservice communication, and a dual-node **kdb+/q (PyKX)** data engine for real-time streaming market depth and transactional persistence.

---

## 🏗️ System Architecture

```text
       [ OKX Exchange WebSockets ]
                   │
                   ▼ (Real-time Order Book Depth Arrays)
             +------------+


             | okx_sub.py |
             +------------+
                   │
                   ├───────────────────────────────┐
                   ▼ (JSON via NATS PubSub)        ▼ (PyKX Async .u.upd Pipeline)
           Topic: "quotes.BTCUSDT"          +-------------------------+
                   │                        |   tick.q (Port 5001)    |
                   ▼                        |                         |
       +─────────────────────────+          |  [quotes table]         |


       | market_making_algo.py   |          |  - Heavy Timeseries LOB |
       +─────────────────────────+          |    Market Depth Storage |
                   │                        +-------------------------+
                   │ (NATS Request-Reply Loop)
                   │ - Clears old orders via "orders.cancel"
                   │ - Places new orders via "orders.limit"
                   ▼
       +─────────────────────────+          +-------------------------+


       |   matching_engine.py    |          |   position_tracker.py   |
       +─────────────────────────+          +-------------------------+
                   │                                     ▲
                   │                                     │ (Vectorised q Queries)
                   ▼ (PyKX Ingestion API Stream)         │ - Calculates Net Volume
       +──────────────────────────────────────+          │ - Aggregates Realised PnL


       |        orders.q (Port 5000)          |          │
       |                                      |          │
       |  [orders table] (Keyed on orderID)   |──────────┘
       |  - IF side=`cancel -> UPSERT (Update) |
       |  - ELSE           -> INSERT (New)    |          ▼ (NATS Request-Reply)
       |                                      |   Topic: "account.positions"
       |  [executions table]                  |          ▲
       |  - Always         -> INSERT (Append) |          │
       +──────────────────────────────────────+    +------------+


                                                   | api_gw.py  |
                                                   +------------+
                                                         ▲
  [ Client REST Endpoints ] ─────────────────────────────┘
  - POST /api/order/market
  - POST /api/order/limit
  - GET  /api/account/position
```

---

## 🛠️ The Technology Stack

*   **API Gateway Router (`FastAPI`)**: Exposes structured public REST endpoints. It translates synchronous external client calls directly into internal asynchronous NATS event streams.
*   **Asynchronous Core Fabric (`NATS Broker`)**: Orchestrates communication across microservices using high-throughput PubSub channels and a deterministic **Request-Reply** handshake loop.
*   **Vector Engine & Storage (`kdb+/q`)**: Processes and stores data using two separate engines via **PyKX** wrappers to isolate performance bottlenecks:
    *   **`tick.q` (Port 5001)**: Dedicated to capturing high-frequency, multi-layer LOB depth frames from public exchanges.
    *   **`orders.q` (Port 5000)**: A keyed analytical database engine that logs order lifecycles and trades while computing real-time exposure.
*   **Matching Core (`Python & CCXT`)**: Connects to global liquidity streams via WebSockets and hosts an in-memory optimized electronic limit order book (LOB) engine supporting price-time priority matching.

---

## 📦 Python Dependencies

The platform requires a mix of web networking, real-time messaging, and high-performance financial data libraries. 

### 1. Unified Dependency List (`requirements.txt`)
Create a `requirements.txt` file in your project root with the following pinned packages:

```text
fastapi>=0.110.0
uvicorn>=0.28.0
pydantic>=2.6.0
nats-py>=2.7.0
pykx>=2.5.0
ccxt>=4.2.0
pandas>=2.2.0
```

### 2. Operational Breakdown of Libraries

*   **`fastapi` & `uvicorn`**: Handles the public REST gateway infrastructure. `uvicorn` acts as the lightning-fast ASGI server that runs the `api_gw.py` endpoint loop.
*   **`pydantic`**: Validates inbound data payloads for market, limit, and cancellation requests against strict typing structures before they hit the NATS network.
*   **`nats-py`**: The official Python client library for NATS. It manages low-latency, event-driven communications, wildcard subscriptions (`orders.*`), and synchronous Request-Reply tokens across all processing nodes.
*   **`pykx`**: The official, high-performance integration wrapper between Python and kdb+. It converts Python data structures into native vectorized q arrays with zero-copy efficiency and executes async IPC data streams (`.u.upd`).
*   **`ccxt`**: Provides a standardized, high-speed interface to connect directly to the OKX WebSocket network to ingest raw multi-layer order book arrays.
*   **`pandas`**: Required as an optimization layer for `pykx`. It converts kdb+ analytical tables returned from `orders.q` directly into Python dictionaries for the `/api/account/position` endpoint.

---

## 🧩 Core Microservices Breakdown

1.  **`api_gw.py`**: The public-facing entry point (Port 8000). Receives HTTP orders and queries, issues request tokens to the NATS bus, and waits for a reply to give clients instant confirmations.
2.  **`okx_sub.py`**: A dedicated market data feed ingestion agent. It aggregates live quotes from the exchange, passes flat structural arrays into `tick.q` (Port 5001), and publishes simplified depth objects to the NATS pipeline.
3.  **`market_making_algo.py`**: A low-latency algorithmic strategy bot. It tracks live quote events, automatically cancels outdated orders on the book via NATS `orders.cancel`, flips asset pricing directions, and structures a 5-layer deep market-maker layer embedded with a **1% commission framework** sent to NATS `orders.limit`.
4.  **`matching_engine.py`**: An in-memory matching engine that processes order creations and cancellations under the wildcard subscription `orders.*`. It maps records to `orders.q` (Port 5000) and executes matches against active liquidity spreads.
5.  **`position_tracker.py`**: An analytics service that subscribes to `account.positions` requests. It uses vectorized q expressions directly inside `orders.q` (Port 5000) to return precise metrics like net positions and realized profit-and-loss (PnL).

---

## 💾 Intelligent kdb+ Routing Logic

To guarantee structural transaction integrity without impacting overall system throughput, **`orders.q`** utilizes a custom routing design inside its ingestion framework (`.u.upd`):

*   **Keyed Upsert (`orders` table)**: The `orders` table is explicitly keyed by `orderID`. When a cancellation instruction carries a `` `cancel `` tag, the database performs an **in-place `upsert`** to overwrite the specific row state instead of creating a duplicate row.
*   **Append-Only Insert (`executions` table)**: Fills and execution clearings are immutable records. They bypass lookup maps and utilize traditional **`insert`** mechanics to achieve ultra-fast sequential data writes.

---

## 🚀 Getting Started

### Prerequisites
*   Python 3.10+
*   kdb+ Runtime Engine (`q`)
*   NATS Server

### Running the Infrastructure Locally

1. **Install Python Libraries**
   ```bash
   pip install -r requirements.txt
   ```

2. **Spin up NATS Broker & kdb+ Databases**
   ```bash
   # Start NATS server
   nats-server -p 4222
   
   # Start the transactional engine
   q orders.q -p 5000
   
   # Start the market data engine
   q tick.q -p 5001
   ```

3. **Boot the Python Microservices**
   ```bash
   python -m matching_engine
   python position_tracker.py
   python market_making_algo.py
   python okx_sub.py
   uvicorn api_gw:app --host 0.0.0.0 --port 8000
   ```

> ⚠️ **Note on kdb+ Integration**: To use `pykx` successfully, make sure your system environment variables point to your local kdb+ installation license keys (`QLIC`).
