/ orders.q - Tickerplant for Market Data, Orders, and Executions

/ --- 1. Schemas ---

/ Market Data Table (Flat 5-level depth)
quotes:([] 
    time:`timestamp$(); 
    sym:`symbol$(); 
    bid1:`float$(); bid2:`float$(); bid3:`float$(); bid4:`float$(); bid5:`float$();
    bsize1:`float$(); bsize2:`float$(); bsize3:`float$(); bsize4:`float$(); bsize5:`float$();
    ask1:`float$(); ask2:`float$(); ask3:`float$(); ask4:`float$(); ask5:`float$();
    asize1:`float$(); asize2:`float$(); asize3:`float$(); asize4:`float$(); asize5:`float$()
    );

/ Orders Table
orders:([] 
    time:`timestamp$(); 
    sym:`symbol$(); 
    orderID:`symbol$(); 
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
    / Apply 1% commission to quote price indices if table is 'quotes'
    if[t=`quotes; x[(2+til 5),12+til 5]*:10#0.99 1.01];
    
    t insert enlist x; / Insert single record
    .u.pub[t;x]       / Broadcast to subscribers
    };

/ Clean up on disconnect
.z.pc:{[h] .u.w: {x except y}[;h] each .u.w};

\p 5000
show "orders.q loaded. TP listening on 5000..."

