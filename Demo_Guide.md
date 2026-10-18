# 🚀 Practical Guide: Utilizing Agentic Delta Guard with Your Own AI Agent

Welcome to **Agentic Delta Guard**! This guide provides a start-to-end walkthrough for anyone cloning this repository to run the streaming data lakehouse pipeline, wire their own autonomous AI agents (LangChain, AutoGen, or custom Python), and enforce data contracts in real-time.

---

## 🏗️ Architecture Overview

```
 [Your Custom AI Agent] (LangChain / AutoGen / Python)
          │
          ▼ (1. Structured JSON Telemetry via LangChainEventCallback)
 [Apache Kafka KRaft Broker] (localhost:9092 / 127.0.0.1:9092)
          │
          ▼ (2. Micro-Batch JVM Catalyst Contract Validation)
 [PySpark Streaming Gatekeeper]
   ┌──────┴──────────────────────────────┐
   │                                     │
   ▼ (Valid Tools, Budget OK)            ▼ (Banned Tools, Cost Spikes, Malformed)
 [Bronze Delta Lake]                   [Quarantine Delta Lake]
 (data/bronze/agent_events)            (data/quarantine/agent_events)
   │                                     │
   ▼                                     ▼
 [dbt Gold Analytical Marts]           [Incident Triage & Contract Evolution]
 (DuckDB delta_scan)                   (triage.py -> agent_contract_proposed.yaml)
```

---

## 📋 Prerequisites & One-Time Setup

1. **Python:** 3.10 or 3.11
2. **Java:** Java 11 or 17 (Required by PySpark. Java 21+ is unsupported by Spark).
   ```powershell
   java -version
   ```
3. **Docker Desktop:** Running (for Kafka broker).
   ```powershell
   docker --version
   ```

### Clone & Install Virtual Environment

```powershell
# Clone the repository and enter directory
git clone -b my-own-agent-demo https://github.com/laila-kz/Agentic-Delta-Guard.git
cd Agentic-Delta-Guard

# Create and activate virtual environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Install core and agent dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

---

## 🎬 Step-by-Step Execution: Start to End

### Step 1: Start the Kafka KRaft Broker

Launch Kafka without ZooKeeper in KRaft mode:

```powershell
docker compose up -d kafka
```

*Optional:* Launch Kafka UI to monitor topics in your browser at `http://localhost:8080`:
```powershell
docker compose up -d kafka-ui
```

---

### Step 2: Start the PySpark Streaming Gatekeeper (Terminal 1)

In your first terminal, launch the Gatekeeper. It will establish the streaming micro-batch consumer on Kafka topic `agent-events` and enforce the rules in `configs/agent_contract.yaml`:

```powershell
.\run_gatekeeper.ps1
```

**What you will see:**
- PySpark initializes using JVM Catalyst optimization and local Hadoop binaries (`.hadoop/bin/winutils.exe`).
- A streaming query `gatekeeper_stream` begins micro-batch evaluation every 5 seconds.
- Leave this terminal running in the background!

---

### Step 3: Run the Real Custom Agent Demo (Terminal 2)

In a second terminal, execute the custom agent demonstration:

```powershell
.\.venv\Scripts\python.exe examples/run_custom_agent.py
```

**What happens during this run:**
1. Connects a `LangChainEventCallback` directly to Kafka broker socket `127.0.0.1:9092`.
2. The agent executes 4 distinct tasks:
   - **Task 1 (`sql_query_executor`):** Valid tool, `$0.042` cost ➔ **Committed to Bronze Delta Lake**.
   - **Task 2 (`web_scraper`):** Valid tool, `$0.015` cost ➔ **Committed to Bronze Delta Lake**.
   - **Task 3 (`unauthorized_shell_exec`):** Rogue tool not in YAML contract ➔ **Isolated into Quarantine Delta Lake** (`semantic_rule:tool_not_allowed`).
   - **Task 4 (`expensive_financial_calc`):** Cost spike of `$154.20` (> `$50.00` limit) ➔ **Isolated into Quarantine Delta Lake** (`semantic_rule:cost_out_of_bounds`).
3. Flushes events to Kafka.
4. Waits for the PySpark micro-batch trigger (5s).
5. Automatically queries the Delta Lake storage via DuckDB and displays the audit breakdown:
   - **Bronze Table:** 2 clean events.
   - **Quarantine Table:** 2 isolated violations.

---

### Step 4: How to Wire YOUR OWN AI Agent

To connect your existing LangChain, AutoGen, or custom Python agent, you only need **three lines of code**:

```python
from delta_guard.langchain_adapter import (
    LangChainEventCallback,
    LangChainKafkaProducer,
)

# 1. Connect the telemetry producer to Kafka
producer = LangChainKafkaProducer(
    bootstrap_servers="127.0.0.1:9092", 
    topic="agent-events"
)

# 2. Instantiate the callback with your agent's unique ID
callback = LangChainEventCallback(
    agent_id="my_production_bot",
    kafka_producer=producer,
)

# 3. Attach the callback to any LangChain tool, chain, or agent invocation
try:
    # Any LangChain Runnable, Tool, or Chain accepts 'config={"callbacks": [...]}'
    my_tool.invoke({"input": "Fetch customer record"}, config={"callbacks": [callback]})
finally:
    # Always flush before script exit!
    producer.flush(timeout=5.0)
    producer.close()
```

---

### Step 5: Registering Your Custom Tools & Budget in the Contract

By default, [configs/agent_contract.yaml](configs/agent_contract.yaml) allows four standard tools:
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
```

To allow your agent's custom tools, simply add them to `allowed_values`:
```yaml
        - my_custom_api_tool       # <--- Add your tool here
```

You can also adjust the max cost limit per action:
```yaml
    - name: cost_usd
      type: float
      min: 0.0
      max: 100.0                   # <--- Increase or decrease spending limit
```

Any tool called by your agent that is **not** in this list is automatically prevented from corrupting your analytics lakehouse and is cleanly diverted into Quarantine.

---

### Step 6: Verifying Delta Lake Storage with DuckDB

You can audit your Delta Lake tables at any time without running PySpark:

```powershell
# Inspect the latest Bronze & Quarantine counts and records:
.\.venv\Scripts\python.exe examples/run_custom_agent.py --inspect-only
```

Or query directly with Python:
```python
import duckdb
con = duckdb.connect()
con.execute("INSTALL delta; LOAD delta;")

# Query valid Bronze events
con.execute("SELECT agent_id, tool_name, cost_usd, status FROM delta_scan('data/bronze/agent_events')").df()

# Query quarantined events and reasons
con.execute("SELECT action_id, quarantine_reason, cost_usd FROM delta_scan('data/quarantine/agent_events')").df()
```

---

### Step 7: Automated Incident Triage & Contract Evolution

When rogue agents or unapproved tools land in Quarantine, run the autonomous triage engine:

```powershell
.\.venv\Scripts\python.exe src/delta_guard/triage.py
```

**What it does:**
- Scans `data/quarantine/agent_events`.
- Clusters failures by root cause (e.g. `semantic_rule:tool_not_allowed`, `semantic_rule:cost_out_of_bounds`).
- Proposes a non-breaking candidate contract patch in [configs/agent_contract_proposed.yaml](configs/agent_contract_proposed.yaml) with automatic safety headroom!
- Logs incidents in `docs/INCIDENT_LOG.md`.

---

### Step 8: Build Gold Analytical Marts (dbt + DuckDB)

Agentic Delta Guard uses zero-JVM analytics: dbt models read raw Bronze Delta Lake files via DuckDB's `delta_scan` extension:

```powershell
# 1. Sync contract YAML variables into dbt
.\.venv\Scripts\python.exe scripts/sync_contract_to_dbt_vars.py

# 2. Build and test Gold marts
cd dbt_delta_guard
dbt deps --profiles-dir .
dbt build --profiles-dir .
cd ..
```

**What this produces:**
- `stg_agent_events`: Clean staging view over Bronze Delta Lake.
- `fct_agent_activity`: Aggregated agent actions and cost metrics.
- `dim_tool_efficiency`: Tool latency and cost efficiency mart.
- Automated data quality assertion tests.

---

### Step 9: Optional Shift-Left Pre-Flight Validation (MCP)

To prevent bad payloads from even reaching Kafka, cooperative agents can query our Model Context Protocol (MCP) server before execution:

```powershell
# Start the MCP server over stdio
.\.venv\Scripts\python.exe run_mcp_server.py
```

Your agent can call `check_contract({"tool_name": "...", "cost_usd": ...})` to receive pre-flight authorization and remediation hints.

---

## 🧹 State Reset (Clean Slate)

To wipe all data and start completely fresh:

```powershell
# 1. Stop Kafka
docker compose down

# 2. Remove local Delta Lake tables, checkpoints, and status
Remove-Item -Recurse -Force data\bronze, data\quarantine, checkpoints\gatekeeper, status.json -ErrorAction SilentlyContinue

# 3. Restart Kafka
docker compose up -d kafka
```

---

## 📚 Reference Links
- [Detailed Custom Agent Runbook](docs/MY_AGENT_RUNBOOK.md)
- [Real Agent Execution Proof & Audit Log](docs/reports/REAL_AGENT_DEMO_PROOF.md)
- [Active Data Contract](configs/agent_contract.yaml)