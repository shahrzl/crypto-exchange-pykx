# crypto-exchange-pykx
A centralized crypto exchange based on KDBX and Python

       [ OKX Exchange WebSockets ]
                   │
                   ▼ (Real-time Order Book Depth)
             +------------+

             | okx_sub.py |
             +------------+
                   │
                   ├───────────────────────────────┐
                   ▼ (JSON via NATS PubSub)        ▼ (PyKX Async .u.upd)
           Topic: "quotes.BTCUSDT"          +-------------------------+
                   │                        |  orders.q (Port 5000)   |
                   ▼                        |                         |
       +─────────────────────────+          |  [quotes table]         |

       | market_making_algo.py   |          |  - Multiplies Bids/Asks |
       +─────────────────────────+          |    by 0.99 / 1.01       |
                   │                        +-------------------------+
                   │ (NATS Request-Reply Loop)
                   │ - Clears old orders via "orders.cancel"
                   │ - Places new orders via "orders.limit"
                   ▼
       +─────────────────────────+          +─────────────────────────+

       |   matching_engine.py    |          |   position_tracker.py   |
       +─────────────────────────+          +─────────────────────────+
                   │                                     ▲
                   ├──────────────────────────────┐      │
                   ▼ (PyKX Ingestion API Stream)   │      │ (Vectorised q Queries)
       +──────────────────────────────────────+   │      │ - Calculates Net Volume

       |        orders.q (Port 5000)          |   │      │ - Aggregates Realised PnL
       |                                      |   │      │
       |  [orders table] (Keyed on orderID)   |   │      │
       |  - IF side=`cancel -> UPSERT (Update) |   │      │
       |  - ELSE           -> INSERT (New)    |   │      │
       |                                      |   │      │
       |  [executions table]                  |   │      │
       |  - Always         -> INSERT (Append) |   │      │
       +──────────────────────────────────────+   │      │
                                                  ▼      ▼ (NATS Request-Reply)
                                           Topic: "account.positions"
                                                  ▲
                                                  │
                                            +------------+
  [ Client REST Endpoints ] ──────────────> | api_gw.py  |
  - POST /api/order/market                  +------------+
  - POST /api/order/limit
  - GET  /api/account/position

