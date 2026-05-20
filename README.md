# crypto-exchange-pykx
A centralized crypto exchange based on KDBX and Python

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
       +─────────────────────────+          +─────────────────────────+

       |   matching_engine.py    |          |   position_tracker.py   |
       +─────────────────────────+          +─────────────────────────+
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
