# 🎬 Agentic Delta Guard — Demo & Run-Book Guide

A cinematic, step-by-step script for recording a portfolio demo video **and** a complete run-book for everyday use.

---

## 🖥️ Recommended Screen Setup

| Pane | Content |
|---|---|
| **Left — Terminal 1** | Gatekeeper / Main command |
| **Left — Terminal 2** | Producer / Agent runner |
| **Right — Browser** | Kafka UI (`http://localhost:8080`) or generated reports in editor |

---

## Prerequisites (one-time checks)

```powershell
java -version          # Must be Java 11 or 17 — NOT 21+
docker --version       # Docker Desktop must be running
```

> Java 21+ breaks PySpark. If `java -version` shows 21, switch to 17 first.

---

## Phase 0 — Environment Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

**Expect:** No errors. `mcp`, `langchain-core`, `duckdb`, `pyspark`, `textual` all install. ~2-3 minutes on first run.

---

## Phase 1 — Full Test Suite (green before running live)

```powershell
.\.venv\Scripts\python.exe -m pytest tests/ -v -m "not (chaos_docker or requires_docker)"
```

**Expect:** `56 passed, 1 deselected` in ~3-4 minutes. Covers:
- Contract validation, chaos infra, Delta clone isolation
- LangChain adapter + model pricing
- Real agent autonomous ReAct evaluation
- Oracle confusion matrix (TP/TN/FP/FN accounting)
- MCP server tools + stdio wire-level JSON-RPC

---

## Phase 2 — MCP Wire-Level Tests (separate stdio suite)

```powershell
.\.venv\Scripts\python.exe -m pytest tests/test_mcp_stdio.py -v
```

**Expect:** `4 passed` in ~15s. Each test spawns `run_mcp_server.py` as a subprocess and talks JSON-RPC over stdio. Seeing `Processing request of type CallToolRequest` in stderr is **normal**.

---

## Scene 1: Start Infrastructure (0:00 - 0:30)

Start Kafka KRaft mode and Kafka UI.

```powershell
docker compose up -d
```

Open browser: `http://localhost:8080` (Kafka UI)

**Highlight on camera:**
- Kafka started in less than 3 seconds with no ZooKeeper.
- Topic `agent-events` visible in Kafka UI.

---

## Scene 2: Live Streaming, Poison Injection & Quarantine HUD (0:30 - 1:15)

Demonstrate the streaming gatekeeper separating valid from poisoned records in real time.

```powershell
# Terminal 1 — Streaming Gatekeeper
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
.\.venv\Scripts\python.exe src/delta_guard/gatekeeper.py
```

```powershell
# Terminal 2 — Real-Time HUD (Dashboard)
.\.venv\Scripts\python.exe src/delta_guard/hud.py
```

```powershell
# Terminal 3 — Synthetic Producer (~15% poison payloads)
.\.venv\Scripts\python.exe src/delta_guard/producer.py
```

**Highlight on camera:**
- HUD updating live: valid events flowing to `data/bronze`, poisoned events (disallowed tools, negative cost, stale timestamps) routed to `data/quarantine`.
- "The pipeline never crashed or halted on poison rows — valid rows were merged while invalid rows were safely quarantined."

---

## Scene 3: Real Autonomous Agent Evaluation Matrix (1:15 - 1:55)

Show the one-command evaluation suite running 55 tasks across 11 adversarial scenarios.

```powershell
.\.venv\Scripts\python.exe scripts/run_real_agent_evaluation.py --scenarios all
```

**Highlight on camera:**
- 11 scenarios: A (valid baseline) + B1-B10 (adversarial poisons: null fields, cost overflow, stale timestamps, future skew, banned tools, truncated JSON, compound failures).
- Oracle confusion matrix showing 100.00% Routing Precision and 0.00% Breach Rate — zero poisoned events leaked into Bronze.
- Auto-generated report written to `docs/reports/real_agent_validation.md`.

---

## Scene 4: Shift-Left MCP Pre-Flight Validation (1:55 - 2:25)

Show that LLM agents can validate payloads before sending them to Kafka.

```powershell
.\.venv\Scripts\python.exe examples/mcp_client_demo.py
```

Open `docs/reports/mcp_interaction_log.md` in your editor.

**Highlight on camera:**
- `check_contract` tool returns pass/fail with precise remediation hints.
- `get_quarantine_summary` clusters error signatures from live Delta Lake.
- "Upstream agents query our schema contract and self-heal before producing bad data to the broker."

---

## Scene 5: dbt Data Quality & Lakehouse Analytics (2:25 - 2:55)

Build the Gold Layer analytical marts and run automated data quality checks.

```powershell
.\.venv\Scripts\python.exe scripts/sync_contract_to_dbt_vars.py

cd dbt_delta_guard
dbt deps --profiles-dir .
dbt build --profiles-dir .
cd ..
```

**Highlight on camera:**
- All 3 models (stg_agent_events, fct_agent_activity, dim_tool_efficiency) created.
- Contract thresholds (max_cost_usd, max_event_age_hours) auto-synced from YAML into dbt variables.

---

## Scene 6: Automated Incident Triage & Contract Proposals (2:55 - 3:20)

Demonstrate autonomous clustering and self-healing contract proposals.

```powershell
.\.venv\Scripts\python.exe src/delta_guard/triage.py
```

**Highlight on camera:**
- Clusters thousands of quarantine records into root-cause error signatures.
- Distinguishes genuine defects (negative cost, banned tools) from benign contract drift.
- Produces surgical patch proposal in `configs/agent_contract_proposed.yaml`.

---

## Scene 7: Throughput Benchmark (3:20 - 3:40)

```powershell
.\.venv\Scripts\python.exe src/delta_guard/benchmark.py
```

**Expect:**
```
Throughput (median of 5 runs): 41,431.70 events/sec
P50/P95/P99: 0.0043 / 0.0094 / 0.0125 ms
Quarantine rate: 14.9%
```

---

## Quick Orientation: What Writes Where

| Component | Writes to |
|---|---|
| `producer.py` | Kafka topic `agent-events` |
| `gatekeeper.py` | `data/bronze/agent_events/` (Delta) + `data/quarantine/agent_events/` (Delta) |
| `triage.py` | `configs/agent_contract_proposed.yaml` + `docs/INCIDENT_LOG.md` |
| `dbt run` | `data/bronze/dbt_delta_guard.duckdb` (Gold marts) |
| `run_real_agent_evaluation.py` | `runs/<run_id>/` (traces, manifests) + `docs/reports/real_agent_validation.md` |
| MCP `propose_contract_patch` | `configs/agent_contract_proposed.yaml` |

---

## Path B — Full Docker Compose Pipeline

```powershell
docker compose up -d
```

**Expect:** 5 containers start: Kafka (KRaft), Kafka UI, producer, gatekeeper, web console. Takes 30-60s.

| What | Where |
|---|---|
| Live event console | http://localhost:8888 |
| Kafka topic browser | http://localhost:8080 |

```powershell
docker compose logs -f gatekeeper    # watch routing in real time
docker compose down                  # stop everything when done
```

---

## State Reset (clean slate)

```powershell
docker compose down
Remove-Item -Recurse -Force data\bronze, data\quarantine, checkpoints\gatekeeper, status.json
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic agent-events
```