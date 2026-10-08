Step-by-Step Demo Recording Guide
This guide gives you a cinematic, structured script to record a 2 to 3 minute portfolio demo video.

🖥️ Recommended Screen Setup
Left Half of Screen: Split into 2 or 3 terminal panes (e.g., Windows Terminal or VS Code).
Right Half of Screen: Web Browser open to:
http://localhost:8080 (Kafka UI)
The generated benchmark/incident reports in your code editor.
📋 Demo Flow & Commands
mermaid
flowchart LR
    A[1. Infrastructure\nKafka KRaft] --> B[2. Streaming\nProducer + Gatekeeper + HUD]
    B --> C[3. Shift-Left\nMCP Pre-Flight Check]
    C --> D[4. Lakehouse & Quality\ndbt Run + Tests]
    D --> E[5. Automated Triage\nMTTR Benchmark]
Scene 1: Start Infrastructure (0:00 – 0:30)
Action: Show your terminal and spin up Kafka KRaft mode with Kafka UI.

powershell
# In Terminal 1:
docker compose up -d
Open browser to http://localhost:8080 (Kafka UI).
Show that Kafka KRaft mode started in < 3 seconds without ZooKeeper.
Scene 2: Live Streaming, Poison Injection & Quarantine HUD (0:30 – 1:15)
Action: Demonstrate the streaming gatekeeper separating valid records from poisoned records in real time.

Terminal 1 — Streaming Gatekeeper:
powershell
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python src/delta_guard/gatekeeper.py
Terminal 2 — Real-Time HUD (Dashboard):
powershell
python src/delta_guard/hud.py
Terminal 3 — Synthetic Producer (with 15% poison payloads):
powershell
python src/delta_guard/producer.py
What to highlight on camera:

Show the HUD updating live: valid events flowing to data/bronze and poisoned events (disallowed tools, negative cost, stale timestamps) routed into data/quarantine.
Explain: "The pipeline never crashed or halted on poison rows—valid rows were merged while invalid rows were safely quarantined."
Scene 3: Shift-Left MCP Contract Validation (1:15 – 1:50)
Action: Show that LLM agents can validate their payloads before sending them to Kafka.

powershell
# Run the live MCP interaction demo:
python examples/mcp_client_demo.py
# (or via alias: python scripts/record_mcp_session.py)
Open 

docs/reports/mcp_interaction_log.md
 in your editor.
Explain: "By implementing the Model Context Protocol (MCP), upstream autonomous agents query our schema contract and self-heal before producing bad data to the broker."
Scene 4: dbt Data Quality & Lakehouse Analytics (1:50 – 2:20)
Action: Build the Gold Layer marts and run all automated data quality checks.

powershell
cd dbt_delta_guard
dbt build --profiles-dir .
cd ..
What to highlight on camera:

All 3 models (stg_agent_events, fct_agent_activity, dim_tool_efficiency) created.
26/26 data tests passed with 0 errors and 0 warnings.
Scene 5: Single-Command End-to-End Test & Benchmarks (2:20 – 3:00)
Action: Run the automated master harness to show total test pass & engineering rigor.

powershell
python scripts/run_e2e_verification.py
What to highlight on camera:

Show the colored output passing all 5 stages in seconds:
Contract Sync ([OK])
Pytest Unit & Chaos Suite — 35 passed, 0 failures ([OK])
dbt Gold Models & Tests — 29/29 passed ([OK])
Quarantine Triage Benchmark — 100% MTTR Reduction ([OK])
Storage & Throughput Benchmark — 40,000+ events/sec ([OK])









Viewed README.md:1-120
Listed directory Agentic-Delta-Guard
Viewed README.md:120-315

Here is the complete run-book, start to finish:

---

## Agentic Delta Guard — Full Run-Book

### Prerequisites (one-time checks)
```powershell
java -version          # must be Java 11 or 17 — NOT 21+
docker --version       # Docker Desktop must be running (for Path B / Docker)
```
> Java 21+ breaks PySpark. If `java -version` shows 21, switch to 17 first.

---

### Phase 0 — Environment setup
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
```
**Expect:** No errors. `mcp`, `langchain-core`, `duckdb`, `pyspark`, `textual` all install. Takes ~2–3 minutes on first run.

---

### Phase 1 — Test suite (verify everything is green before running live)
```powershell
.\.venv\Scripts\python.exe -m pytest tests/ -v --tb=short
```
**Expect:** `38 passed, 2 skipped` in ~30–60s. The 2 skips are:
- `test_gatekeeper_survives_broker_restart` — needs live Docker Kafka
- `test_llm_triage_with_openai` — needs `OPENAI_API_KEY`

Both are correct and intentional. Any `FAILED` is a problem.

---

### Phase 2 — MCP wire-level tests (separate suite, stdio transport)
```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_mcp_stdio.py -v
```
**Expect:** `4 passed` in ~30s. Each test spawns `run_mcp_server.py` as a subprocess and talks JSON-RPC over stdio. Seeing the server log `Processing request of type CallToolRequest` in stderr is normal.

---

### Path A — Local Textual HUD (no Docker required)

#### Step A1 — Run the full local pipeline
```powershell
.\.venv\Scripts\python.exe run_pipeline.py
```
**Expect:** Three components start in the same terminal:
1. **Producer** — generates synthetic agent events (∼15% poisoned) and streams them
2. **PySpark Gatekeeper** — reads, validates, routes to Bronze/Quarantine Delta tables
3. **Textual HUD** — live terminal dashboard showing Kafka status, Bronze row count, Quarantine count, resource meters

The HUD is interactive — press `q` to quit cleanly.

> If PySpark fails with Hadoop/winutils errors, set `HADOOP_HOME` to `.\.hadoop` (already in `.env`).

---

#### Step A2 — Run triage on quarantined records (after pipeline has run)
```powershell
.\.venv\Scripts\python.exe src/delta_guard/triage.py
```
**Expect:** Reads `data/quarantine/agent_events`, clusters error signatures, and either:
- Prints a deterministic JSON diagnosis (no API keys needed)
- Or enriched LLM diagnosis if `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` is set

Writes `configs/agent_contract_proposed.yaml` and `docs/INCIDENT_LOG.md`.

---

#### Step A3 — Run dbt analytics models (Bronze → Gold marts)
```powershell
cd dbt_delta_guard
dbt deps --profiles-dir .
dbt build --profiles-dir .
```
**Expect:** dbt reads `data/bronze/agent_events` via `delta_scan`, runs staging and mart models in DuckDB, writes Gold layer. All models should show `OK` with row counts.

```powershell
dbt test --profiles-dir .   # optional: data quality tests
cd ..
```

---

#### Step A4 — MCP server (Gemini CLI / Claude Desktop integration)
```powershell
.\.venv\Scripts\python.exe run_mcp_server.py
```
**Expect:** Stays running silently, listening on stdin for JSON-RPC. You don't run this directly — your AI client (Gemini CLI, Claude Desktop) connects to it via `mcp_config.json`. Kill with `Ctrl+C`.

To exercise it from your own code:
```powershell
.\.venv\Scripts\python.exe examples/mcp_client_demo.py
```
**Expect:** Connects over stdio, calls all 7 tools, prints structured results, exits cleanly.

---

#### Step A5 — LangChain adapter demo
```powershell
.\.venv\Scripts\python.exe examples/langchain_demo.py
```
**Expect:** A real LangChain `AgentExecutor` runs with the `LangChainEventCallback` attached. Tool calls and LLM completions are translated into 9-field contract events and published to Kafka (or logged if Kafka is not up). No API key needed — the demo runs a mock LLM path.

---

#### Step A6 — Benchmark
```powershell
.\.venv\Scripts\python.exe src/delta_guard/benchmark.py
```
**Expect:** 5 runs × 100k events of in-process validation. Prints throughput (target: ~47k ev/s), P50/P95/P99 latencies, and poison quarantine rate (~14.9%). Takes ~30s.

---

### Path B — Full Docker Compose pipeline

```powershell
docker compose up -d
```
**Expect:** 5 containers start: Kafka (KRaft), Kafka UI, producer, gatekeeper, web console. Takes 30–60s for Kafka to be ready.

| What | Where |
|---|---|
| Live event console | http://localhost:8888 |
| Kafka topic browser | http://localhost:8080 |

```powershell
docker compose logs -f gatekeeper    # watch routing in real time
docker compose down                  # stop everything when done
```

---

### Quick orientation of what writes where

| Component | Writes to |
|---|---|
| `producer.py` | Kafka topic `agent-events` |
| `gatekeeper.py` | `data/bronze/agent_events/` (Delta) + `data/quarantine/agent_events/` (Delta) |
| `triage.py` | `configs/agent_contract_proposed.yaml` + `docs/INCIDENT_LOG.md` |
| `dbt run` | `data/bronze/dbt_delta_guard.duckdb` (Gold marts) |
| `propose_contract_patch` (MCP) | `configs/agent_contract_proposed.yaml` (or `tmp_path` in tests) |

Edited server.py
Edited server.py
Viewed server.py:14-41