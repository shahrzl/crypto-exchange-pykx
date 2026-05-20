/ =========================================================================
/ ⚡ Q-STREAM MARKET DATA TICK ENGINE
/ =========================================================================

/ 1. Define Inbound Market Data Schema
/ Top 5 Bid/Ask prices and volumes tracked sequentially
quotes:([]
    time:`timestamp$();
    sym:`symbol$();
    bidPrice1:`float$(); bidPrice2:`float$(); bidPrice3:`float$(); bidPrice4:`float$(); bidPrice5:`float$();
    bidSize1:`float$();  bidSize2:`float$();  bidSize3:`float$();  bidSize4:`float$();  bidSize5:`float$();
    askPrice1:`float$(); askPrice2:`float$(); askPrice3:`float$(); askPrice4:`float$(); askPrice5:`float$();
    askSize1:`float$();  askSize2:`float$();  askSize3:`float$();  askSize4:`float$();  askSize5:`float$()
    );

/ --- 2. PubSub Framework Core ---

.u.w:enlist[`]!enlist[()];                              / Map table names to active network handle lists
.u.sub:{[t] .u.w[t]:distinct .u.w[t],.z.w};            / Register connection handles uniquely to tables
.u.pub:{[t;x] {[h;t;x] (neg h)(`upd;t;x)}[;t;x] each .u.w[t]}; / Async fan-out messaging loop 

/ --- 3. Dynamic Vector Mutation Ingestion Engine ---

.u.upd:{[t;x]
    / Intercept quotes stream to inject a 1% fee layer across all price indices on the fly
    if[t=`quotes; 
        / Indices 2-6 represent Bid Prices 1-5
        / Indices 12-16 represent Ask Prices 1-5
        / We multiply Bids by 0.99 (markdown) and Asks by 1.01 (markup)
        x[(2+til 5),12+til 5]*:10#0.99 1.01
    ];

    / Safe Ingestion Guard:
    / Check if first item is a single atom (like a single timestamp from PyKX). 
    / If yes, format it correctly as a row list matrix, otherwise insert raw vector structure.
    $[0>type first x; t insert enlist x; t insert x];
    
    / Broadcast the mutated array packet instantly out to all downstream real-time subscribers
    .u.pub[t;x]       
    };

/ 4. Session Lifespan Clean Up Callback
.z.pc:{[h] .u.w: {x except y}[;h] each .u.w};

/ --- 5. Initialisation Boot Sequence ---
\p 5001
show "⚡ tick.q loaded. Market Data Engine listening on Port 5001...";
