/ orders_current_state.q - Maintains the current state of orders by subscribing to orders_audit
/ This process will upsert based on orderID to keep only the latest state of each order.

/ --- 1. Schema ---

/ Current Orders Table - Keyed by orderID for efficient upserts
orders_current_state:`orderID x (
    time:`timestamp$();      / Last update timestamp
    sym:`symbol$();       / Trading pair (e.g., BTCUSD)
    orderID:`symbol$();   / Unique ID for the order (key)
    msgtype:`symbol$();   / Latest message type ('new', 'amend', 'cancel')
    ordtype:`symbol$();   / Order type ('market', 'limit')
    side:`symbol$();      / Order direction ('buy', 'sell') or 'cancel'
    price:`float$();      / Latest price (0.0 for market/cancel)
    size:`float$();       / Latest order quantity (0.0 for cancel)
    traderID:`symbol$()   / ID of the client or bot
    );

/ --- 2. Update Logic ---

.u.upd:{[t;x]
    / Only process updates for the 'orders_audit' table
    if[t=`orders_audit;
        / Convert the incoming list 'x' into a dictionary suitable for upserting
        / This assumes 'x' is in the same order as the table columns
        new_row:`time`sym`orderID`msgtype`ordtype`side`price`size`traderID!x;

        / Perform an upsert: update if orderID exists, insert if new
        orders_current_state upsert (`orderID; new_row);

        / Optional: Print for debugging
        0N!("Upserted orderID: ", string new_row`orderID, " | MsgType: ", string new_row`msgtype);
    ];
};

/ --- 3. Subscription ---

/ Connect to the tickerplant publishing orders_audit
/ This assumes the orders_audit tickerplant is running on localhost:5000
system "L :5000";

/ Subscribe to the 'orders_audit' table
.u.sub[`orders_audit];

\p 5002
show "orders_current_state.q loaded. Subscribing to orders_audit from :5000 and listening on :5001..."
