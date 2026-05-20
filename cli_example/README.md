# 🤖 Intelligent CLI Client

An AI-driven, multi-turn conversational trading terminal designed for the Q-Stream platform. This client leverages the official **Google GenAI SDK** and its **Automatic Function Calling** engine to translate natural language user commands into structured execution payloads against the platform's REST gateways (`api_gw.py`).

---

## 🧠 How It Works in Detail

Instead of requiring rigid, syntax-heavy terminal flags or manual JSON structures, the client uses an advanced Large Language Model (**Gemini 2.5 Flash**) as an intelligent routing agent. 

```text
 [ User Natural Input ] 
         │
         ▼ (Conversational Stream)
 +────────────────────────────────────────────────────────────────────────+

 |                       Gemini 2.5 Flash Engine                         |
 |                                                                        |
 |  1. Intercepts Intent (e.g., "Buy 0.5 BTC at market")                  |
 |  2. Parses & Validates Entity Arguments against System Tools Schema   |
 |  3. Generates a Structured Function Call Token                         |
 +────────────────────────────────────────────────────────────────────────+
         │
         ▼ (Automatic Tool Execution Hook)
   Local Python Function Invoke (e.g., place_market_order())
         │
         ▼ (HTTP POST / GET Frame Transmission)
 +────────────────────────────────────────────────────────────────────────+

 |                        api_gw.py Gateway                               |
 +────────────────────────────────────────────────────────────────────────+
         │
         ▼ (Network Response Payload String)
   Returns Transaction Status Data JSON
         │
         ▼ (Context Feed Injection)
 +────────────────────────────────────────────────────────────────────────+

 |                       Gemini 2.5 Flash Engine                         |
 |                                                                        |
 |  4. Consumes raw JSON system telemetry response data                  |
 |  5. Formulates natural conversational feedback to user                 |
 +────────────────────────────────────────────────────────────────────────+
         │
         ▼
 [ Agent UI Response Summary ]
```

### 1. State Management & Multi-Turn Context Tracking
The interface initializes a persistent chat session object via `client.chats.create()`. This session keeps track of the entire conversation tree:
* **Context Preservation**: If you tell the agent *"I am trader_99"*, subsequent calls automatically pin `client_id="trader_99"` without forcing you to retype it.
* **Anaphoric Resolution**: The AI resolves contextual references. For example, if you ask *"Check my positions on BTCUSDT"*, and follow up with *"Sell all of it at market"*, the model tracks that *"it"* refers to your exact `BTCUSDT` position size.

### 2. Automatic Function Calling (Tool Binding)
Python code functions are bound directly to the AI runtime using the `tools` registration array. The model analyzes the native Python **Docstrings** and **Type Hints** at startup to automatically build an internal API schema wrapper:
* It reads arguments like `quantity: float` and maps them to strict primitive data validation masks.
* If a parameter is marked optional (e.g., `pair: str = None`), the model determines whether to prompt you for it or execute the tool omitting that component.
* **Low-Latency Determinism**: The `temperature` config is strictly pinned at `0.2` to ensure the AI behaves like a reliable router, avoiding creative drift or hallucinated inputs.

---

## 📦 Runtime Dependencies

To initialize this service component standalone, create a `requirements.txt` workspace layer:

```text
google-genai>=0.1.1
requests>=2.31.0
```

---

## 🛠️ Step-by-Step System Tool Mapping

The client registers three major operational helper tools mapped directly to your cluster network gateways:


| Helper Tool Interface | Extracted Parameters | Target API Gateway Routing Endpoint |
| :--- | :--- | :--- |
| **`place_market_order`** | `client_id`, `pair`, `side`, `quantity` | `POST /api/order/market` |
| **`place_limit_order`** | `client_id`, `pair`, `side`, `quantity`, `price` | `POST /api/order/limit` |
| **`check_account_positions`**| `client_id`, `pair` (optional) | `GET /api/account/position` |

---

## 🚀 Deployment & Operational Guide

### 1. Set Up Environment Variables
You must expose your API key token to authorize secure handshakes with Google's foundation endpoint systems, alongside your platform's base entry address link if it runs inside an external docker wrapper:

```bash
export GEMINI_API_KEY="AIzaSy..."
export API_GW_URL="http://localhost:8000"
```

### 2. Run the Interface Node
Boot up the command line interface framework tool:

```bash
python cli_client.py
```

### 3. Example Live Interaction Session

```text
⚡ Q-Stream Intelligent Terminal Initializing...
🤖 AI Execution Agent Active. Type 'exit' to terminate session.

User 👤 > Check my current exposure profile on BTCUSDT
🤖 Agent > Checking ledger records... Currently, client `trader_1` has no active positions recorded for asset pair BTCUSDT.

User 👤 > Place a buy limit order for 0.4 BTC at 62500.50
🤖 Agent > Sending transaction request... Order placement successful. Limit order ID `ord_1716382500` for 0.4 BTC at a price of \$62,500.50 has been successfully routed to the matching engine book.

User 👤 > What about ETH? Buy 5 of them at market price instead.
🤖 Agent > Initiating market sweep... Market order submitted successfully. 5.0 ETHUSDT has been executed at market under reference tracker `ord_1716382612`.

User 👤 > exit
Disconnecting CLI interface context...
```
