# 🚀 Guide: Wiring Your Own AI Agent into Agentic Delta Guard

This run-book details how to connect **any custom LangChain, AutoGen, or Python AI agent** directly to the Agentic Delta Guard streaming lakehouse pipeline.

---

## Architecture Flow

```
[Your Custom Agent]
       │
       ▼ (1. LangChain Event Callback)
[LangChainKafkaProducer]
       │
       ▼ (2. Topic: agent-events on localhost:9092)
[Apache Kafka KRaft Broker]
       │
       ▼ (3. Micro-Batch JVM Catalyst Evaluation)
[PySpark Streaming Gatekeeper]
   ┌───┴───────────────────────────────┐
   │                                   │
   ▼                                   ▼
[Bronze Delta Lake]            [Quarantine Delta Lake]
(Valid tools, budget OK)       (Disallowed tools, cost spikes, malformed JSON)
   │                                   │
   ▼                                   ▼
[dbt Gold Marts (DuckDB)]      [triage.py Auto-Contract Evolution]
```

---

## 3 Steps to Run

### Step 1: Ensure Kafka is Up
```powershell
docker compose up -d kafka
```

### Step 2: Start the Streaming Gatekeeper (Terminal 1)
```powershell
.\run_gatekeeper.ps1
```
The Gatekeeper runs continuously, consuming micro-batches from Kafka and splitting them into Bronze and Quarantine Delta tables.

### Step 3: Run Your Custom Agent (Terminal 2)
```powershell
.\.venv\Scripts\python.exe examples/run_custom_agent.py
```

---

## How to Integrate with Your Own Existing Agent Code

In your agent's Python code, add **three lines**:

```python
from delta_guard.langchain_adapter import (
    LangChainEventCallback,
    LangChainKafkaProducer,
)

# 1. Connect producer to Kafka
producer = LangChainKafkaProducer(bootstrap_servers="localhost:9092", topic="agent-events")

# 2. Instantiate callback with your agent's unique ID
callback = LangChainEventCallback(agent_id="my_custom_agent_id", kafka_producer=producer)

# 3. Attach callback to your LangChain tool or agent invocation
try:
    # Any LangChain Runnable, Chain, or Tool accepts 'callbacks':
    my_tool.invoke({"input": "query"}, config={"callbacks": [callback]})
finally:
    # Always flush before script exit!
    producer.flush(timeout=5.0)
    producer.close()
```

---

## How to Register Your Custom Tools in the Contract

By default, the contract ([configs/agent_contract.yaml](file:///c:/Users/kheza/Desktop/Data_Engineering/Agentic-Delta-Guard/configs/agent_contract.yaml)) only permits:
`["sql_query_executor", "vector_search", "web_scraper", "db_writer"]`

To allow your agent's custom tools, simply add them to `allowed_values`:

```yaml
schema:
  fields:
    - name: tool_name
      type: string
      nullable: false
      allowed_values:
        - sql_query_executor
        - vector_search
        - web_scraper
        - db_writer
        - my_custom_tool        # <--- Add your tool here
```

If your agent calls a tool that is **not** in this list, the Gatekeeper protects the lakehouse by routing that execution to `data/quarantine/agent_events` with the diagnostic tag:
`semantic_rule:tool_not_allowed`

---

## How to Inspect What Landed in the Lakehouse

You can inspect the Delta Lake storage immediately without starting Spark:

```powershell
# Check Bronze (Valid actions):
.\.venv\Scripts\python.exe examples/run_custom_agent.py --inspect-only

# Or run triage on quarantined events:
.\.venv\Scripts\python.exe src/delta_guard/triage.py
```
