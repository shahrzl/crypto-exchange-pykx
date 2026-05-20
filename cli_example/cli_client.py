import os
import sys
import requests
from google import genai
from google.genai import types

# Define our API Gateway base address configuration 
API_GW_URL = os.getenv("API_GW_URL", "http://localhost:8000")

# ---------------------------------------------------------
# 1. Native Python Tool Definitions (Function Calling)
# ---------------------------------------------------------

def place_market_order(client_id: str, pair: str, side: str, quantity: float) -> str:
    """
    Places a market execution order into the trading system.
    Args:
        client_id: Unique identifier for the trader entity (e.g. 'trader_1').
        pair: Target trading pair ticker instrument (e.g. 'BTCUSDT').
        side: Direction of transaction, must be 'buy' or 'sell'.
        quantity: The volume or quantity size to execute.
    """
    url = f"{API_GW_URL}/api/order/market"
    payload = {
        "client_id": client_id,
        "pair": pair.replace("/", ""), # Strip slash to match system expectations
        "side": side.lower(),
        "quantity": float(quantity)
    }
    try:
        response = requests.post(url, json=payload, timeout=5.0)
        return response.text
    except Exception as e:
        return f"Network routing connection error: {str(e)}"

def place_limit_order(client_id: str, pair: str, side: str, quantity: float, price: float) -> str:
    """
    Places a locked price threshold limit order into the matching book queue.
    Args:
        client_id: Unique identifier for the trader entity (e.g. 'trader_1').
        pair: Target trading pair ticker instrument (e.g. 'BTCUSDT').
        side: Direction of transaction, must be 'buy' or 'sell'.
        quantity: The volume or quantity size to allocate.
        price: Threshold execution price level.
    """
    url = f"{API_GW_URL}/api/order/limit"
    payload = {
        "client_id": client_id,
        "pair": pair.replace("/", ""),
        "side": side.lower(),
        "quantity": float(quantity),
        "price": float(price)
    }
    try:
        response = requests.post(url, json=payload, timeout=5.0)
        return response.text
    except Exception as e:
        return f"Network routing connection error: {str(e)}"

def check_account_positions(client_id: str, pair: str = None) -> str:
    """
    Queries real-time positions, trade metrics, and PnL sorted by trading pair.
    Args:
        client_id: Unique identifier for the trader entity (e.g. 'trader_1').
        pair: Optional trading pair filter token (e.g. 'BTCUSDT').
    """
    url = f"{API_GW_URL}/api/account/position"
    params = {"client_id": client_id}
    if pair:
        params["pair"] = pair.replace("/", "")
        
    try:
        response = requests.get(url, params=params, timeout=5.0)
        return response.text
    except Exception as e:
        return f"Network routing connection error: {str(e)}"


# ---------------------------------------------------------
# 2. Main Multi-Turn Interactive Execution Session
# ---------------------------------------------------------

def main():
    if not os.environ.get("GEMINI_API_KEY"):
        print("❌ Error: GEMINI_API_KEY environment variable not detected.")
        sys.exit(1)

    print("⚡ Q-Stream Intelligent Terminal Initializing...")
    
    # Instantiate the unified Google GenAI client structure
    client = genai.Client()
    
    # Declare instructions ensuring system parameters (like trader ID) persist between turns
    system_instruction = (
        "You are an executive institutional trading assistant interface for the Q-Stream platform. "
        "Your duty is to map conversational inputs to trade execution tools. "
        "Assume a default client_id of 'trader_1' unless explicit instructions are provided by the user. "
        "Keep technical parameter outputs clean, actionable, and visually conversational."
    )
    
    # Build list of available tools the model can call on demand
    trading_tools = [place_market_order, place_limit_order, check_account_positions]

    # Initialize a live state-managed multi-turn Chat Session
    chat = client.chats.create(
        model="gemini-2.5-flash",
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            tools=trading_tools, # Registers tools for automatic intercept resolution
            temperature=0.2      # Set low temperature for absolute precision
        )
    )

    print("🤖 AI Execution Agent Active. Type 'exit' to terminate session.\n")
    
    # Enter perpetual loop for terminal multi-turn chatbot experience
    while True:
        try:
            user_input = input("\033[94mUser 👤 > \033[0m")
            if user_input.strip().lower() in ['exit', 'quit']:
                print("Disconnecting CLI interface context...")
                break
                
            if not user_input.strip():
                continue

            # Transmit message over the dynamic state tracking stack
            response = chat.send_message(user_input)
            
            print(f"\033[92m🤖 Agent >\033[0m {response.text}\n")
            
        except KeyboardInterrupt:
            print("\nSession killed.")
            break
        except Exception as err:
            print(f"⚠️ Error encountered during chat iteration: {err}\n")

if __name__ == "__main__":
    main()
