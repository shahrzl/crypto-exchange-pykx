/ orders.q - Tickerplant for Market Data, Orders, and Executions

/ --- 1. Schemas ---



/ Orders Table
orders_audit:([] 
    time:`timestamp$(); 
    sym:`symbol$(); 
    orderID:`symbol$(); 
    msgtype:`symbol$(); 
    ordtype:`symbol$(); 
    side:`symbol$(); 
    price:`float$(); 
    size:`float$(); 
    traderID:`symbol$()
    );

/ Executions Table
executions:([] 
    time:`timestamp$(); 
    sym:`symbol$(); 
    tradeID:`symbol$(); 
    side:`symbol$(); 
    price:`float$(); 
    size:`float$(); 
    takerID:`symbol$(); 
    makerID:`symbol$()
    );

/ --- 2. PubSub Logic ---

.u.w:enlist[`]!enlist[()]; / Handle tracking
.u.sub:{[t] .u.w[t]:distinct .u.w[t],.z.w};
.u.pub:{[t;x] {[h;t;x] (neg h)(`upd;t;x)}[;t;x] each .u.w[t]};

/ --- 3. Update Logic ---

.u.upd:{[t;x] 

    
    t insert enlist x; / Insert single record
    .u.pub[t;x]       / Broadcast to subscribers
    };

/ Clean up on disconnect
.z.pc:{[h] .u.w: {x except y}[;h] each .u.w};

\p 5000
show "orders.q loaded. TP listening on 5000..."

