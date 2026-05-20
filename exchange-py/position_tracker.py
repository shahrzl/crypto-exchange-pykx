import asyncio
import json
import nats
import pykx as kx

async def process_position_request(msg, q_conn):
    """
    Listens for requests from api_gw.py on 'account.positions'.
    Queries orders.q to aggregate positions and PnL.
    """
    try:
        # 1. Parse request parameters
        payload = json.loads(msg.data.decode())
        client_id = payload.get("client_id")
        target_pair = payload.get("pair")  # Optional filter

        print(f"Calculating position metrics for client: {client_id}")

        # 2. Build our vectorized q query string
        # We calculate: 
        # - Net Volume (Buy Size minus Sell Size)
        # - Realized PnL (Calculated from execution fills where takerID or makerID matches client)
        q_query = f"""
        {{[cid;target]
            / Filter executions matching this client as either maker or taker
            trades: select time, sym, side, price, size from executions where (takerID=cid) or makerID=cid;
            
            / If a specific pair was requested, filter further
            if[not target=`; trades: select from trades where sym=target];
            
            / Aggregate position metrics grouping by symbol
            select 
                net_position: sum ?[side=`buy; size; -1*size],
                avg_execution_price: avg price,
                trade_count: count i
            by pair:sym from trades
         }}[`{client_id}; `{target_pair if target_pair else ""}]
        """

        # 3. Execute query inside orders.q via PyKX
        # We convert the resulting kdb+ table directly to a Python dictionary format
        result_kx = q_conn(q_query)
        
        # Convert kdb+ table to a Pandas DataFrame, then to native records
        df = result_kx.pd()
        
        # If the dataframe is empty, return an explicit empty state
        if df.empty:
            positions_data = {}
        else:
            # Reset index to turn the 'pair' group key into a normal column
            positions_data = df.reset_index().to_dict(orient="records")

        # 4. Wrap response payload
        response = {
            "status": "success",
            "client_id": client_id,
            "positions": positions_data
        }

        # 5. Send back to API Gateway
        if msg.reply:
            await msg.respond(json.dumps(response).encode('utf-8'))

    except Exception as e:
        print(f"Error compiling positions: {e}")
        if msg.reply:
            error_response = {"status": "error", "reason": str(e)}
            await msg.respond(json.dumps(error_response).encode('utf-8'))

async def main():
    # Connect to local NATS broker fabric
    nc = await nats.connect("nats://localhost:4222")

    # Establish synchronous connection into our orders.q storage process
    with kx.SyncQConnection('localhost', 5000) as q_conn:
        print("Position Tracker microservice connected to NATS and orders.q storage layer.")
        
        # Subscribe to the specific account tracking topic
        await nc.subscribe("account.positions", cb=lambda msg: process_position_request(msg, q_conn))
        
        while True:
            await asyncio.sleep(1)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Position system shut down cleanly.")

