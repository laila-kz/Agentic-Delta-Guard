# 🛡️ Agentic Delta Guard

**A data-contract gate for AI-agent event streams: validates events against a YAML contract, quarantines violations without stalling the stream, and proposes YAML contract patches from the failures.**

[![CI](https://github.com/laila-kz/Agentic-Delta-Guard/actions/workflows/ci.yml/badge.svg)](https://github.com/laila-kz/Agentic-Delta-Guard/actions/workflows/ci.yml)
[![Engine](https://img.shields.io/badge/Engine-PySpark%203.5%20%7C%20Delta%20Lake-00A4E4?logo=apachespark)](https://delta.io/)
[![Quality](https://img.shields.io/badge/Quality-dbt%20%2B%20DuckDB-FF694B?logo=dbt)](https://getdbt.com)

> 🌐 **Interactive Simulation (no live pipeline):** [https://agentic-delta-guard.vercel.app](https://agentic-delta-guard.vercel.app)

---

## Live Demo State & Headline Metrics

The system provides two distinct monitoring interfaces depending on your execution path:
- **Path A Interface (Local Host Execution):** Real-time Textual Terminal HUD ([`src/delta_guard/hud.py`](src/delta_guard/hud.py)), launched via `python run_pipeline.py`.
- **Path B Interface (Docker Compose Stack):** Animated Web Console ([`console.html`](console.html) served by FastAPI [`status_server.py`](src/delta_guard/status_server.py) at `http://localhost:8888`), launched via `docker compose up -d`.

The terminal interface below reflects a **sustained 15-minute streaming run (4,355 total agent events)**:

![Real-Time Terminal HUD](docs/screenshots/hud_interface.png)

### Key Results & Engineering Guarantees
| Guarantee | Measured Value | Engineering Mechanism |
| :--- | :---: | :--- |
| **Stream Reliability** | **Zero Pipeline Halts** | Micro-batch Catalyst filtering diverts invalid payloads into Quarantine without stalling Bronze ingestion |
| **Defect Isolation** | **100% Contained (15.4% rate)** | **671 invalid records quarantined** across 4 poison signatures; **0 corrupt rows leaked** into Bronze |
| **Replay Deduplication** | **0 Duplicate Rows (100% dedupe)** | Composite key `(agent_id, session_id, action_id)` via Delta `MERGE INTO` drops duplicate deliveries |
| **Incident Remediation** | **< 1 sec Automated Proposal** | Deterministic triage diffs quarantined anomalies and writes candidate contract patch with 10% headroom |
| **Bronze Commits** | **3,684 Valid Rows** | Compliant agent events committed with full ACID guarantees and immediate dbt/DuckDB queryability |
| **Real Agent Guarding** | **100% Routing Precision (0 Breaches)** | 50 tasks across 11 scenarios (A, B1–B10) evaluated with Automated Oracle ([Report](docs/reports/real_agent_validation.md)) |

> 🚀 **One-Command Real Agent Evaluation Suite:**
> ```powershell
> python scripts/run_real_agent_evaluation.py --scenarios all
> ```

---

## Architecture

```mermaid
flowchart TD
    subgraph Layer0 ["Layer 0: Advisory Pre-Flight & Event Production"]
        LC[LangChain Agent Fleet] -->|1. Advisory Pre-Flight| MCP["MCP Server (run_mcp_server.py)\ncheck_contract tool"]
        LC -->|2. Async Emit| LCProd["LangChain Adapter\n(langchain_adapter.py)"]
        Synth["Synthetic Producer\n(producer.py ~15% poison)"] -->|Direct Emit| Kafka[(Kafka: agent-events\nlocalhost:9092 / kafka:29092)]
        LCProd -->|Kafka Publish| Kafka
    end

    subgraph Layer1 ["Layer 1: Hard Enforcement Gatekeeper"]
        Kafka --> Engine["PySpark Streaming Gatekeeper (gatekeeper.py)\nNative Catalyst Expressions in JVM"]
        Engine -->|Contract Valid| Bronze[("Bronze Delta Lake\n(data/bronze/agent_events)")]
        Engine -->|Contract Violations| Quarantine[("Quarantine Delta Lake\n(data/quarantine/agent_events)")]
    end

    subgraph Layer2 ["Layer 2: Analytics, Sandbox & Governance"]
        Bronze --> DBT["dbt Core + DuckDB\ndelta_scan reads _delta_log"]
        DBT --> Gold[("Gold Analytical Marts")]
        Bronze -.-> Sandbox["Delta SandboxGuard\n(sandbox_guard.py / clone utils)"]
        Quarantine --> Triage["Deterministic / LLM Triage Engine\n(triage.py)"]
        Triage --> Patch["Proposed Contract Patch\n(agent_contract_proposed.yaml)"]
    end

    subgraph Layer3 ["Layer 3: Observability (Path A vs Path B)"]
        Bronze & Quarantine -.-> HUD["Path A: Textual Terminal HUD (hud.py)\nLocal host execution"]
        Engine -.-> StatusFile["status.json"]
        StatusFile -.-> Web["Path B: FastAPI Server (status_server.py)\nWeb Console (console.html: 8888)"]
    end
```

> **Architectural Boundary — Advisory Pre-Flight vs. Hard Streaming Enforcement:**
> - **Layer 0 (Advisory Pre-Flight via MCP):** The Model Context Protocol server (`src/delta_guard/mcp/server.py`) exposes `check_contract` directly to agents before emission. This is strictly **advisory** — it gives cooperative agents instant feedback and self-healing hints to repair malformed calls, but cannot protect against rogue agents or out-of-band writes.
> - **Layer 1 (Hard Enforcement via PySpark Gatekeeper):** The streaming Gatekeeper (`src/delta_guard/gatekeeper.py`) is the **authoritative enforcement boundary**. Every Kafka event is validated natively inside the JVM via Catalyst column expressions. Non-compliant records are physically isolated into Quarantine; only valid events land in Bronze.

> **Medallion Architecture Justification (Bronze vs. Quarantine):**
> In traditional medallion architecture, Bronze is raw append-only ingestion, deferring validation to Silver. In autonomous AI agent streams, admitting unvetted payloads into Bronze introduces severe operational risks: invalid tool executions and schema drift pollute downstream analytics. Here, contract validation acts as an **ingestion gate**: compliant events land in **Bronze**, while violating events are segregated into a parallel **Quarantine Delta table**. Both tables preserve raw event data, but Quarantine insulates the analytical lineage from corrupt data without stalling the streaming pipeline.

> **Scope:** The deterministic validation path, quarantine routing, dbt/DuckDB analytics, and idempotent merge are implemented and tested. End-to-end Kafka-to-Delta routing in a live cluster and live LLM enrichment are opt-in and not exercised in CI. See [Limitations](#limitations) and [docs/CLAIMS.md](docs/CLAIMS.md) for verification evidence.

---

## What It Does

### 1. Event Contract (`configs/agent_contract.yaml`)

Every agent event is validated against a single YAML contract. A valid payload committed to Bronze:

```yaml
# Compliant event — committed to Bronze Delta table
agent_id:          "agent_008"
session_id:        "2352e973-51de-462d-bc81-a17804a8f8c5"
action_id:         "462cdc67-c7c3-4a6e-830f-4cfcec22a9a3"
timestamp:         "2026-09-22T21:49:03.157045+00:00"
tool_name:         "web_scraper"                          # must be one of allowed tools
execution_time_ms: 903
cost_usd:          0.183546                               # must be in [0.0, 50.0]
status:            "SUCCESS"
tool_args:         '{"query":"Or stuff generation style hour admit return.","limit":48}'
```

Contract rules enforced per micro-batch (all applied as native Catalyst expressions in `process_batch`):
- All nine fields present. In the PySpark streaming ingestion schema (`EVENT_SCHEMA`), all fields are parsed as nullable (`StructField(..., nullable=True)`) to protect the parser from dropping corrupt JSON; required field constraints (`agent_id`, `session_id`, `action_id` declared `nullable: false` in YAML) are enforced dynamically via Catalyst validation expressions (`missing_required_field:<name>`), routing nulls to Quarantine rather than failing parser execution.
- `tool_name` in `{sql_query_executor, vector_search, web_scraper, db_writer}` — the allowlist is read from `configs/agent_contract.yaml` at startup, and out-of-allowlist values are quarantined as `semantic_rule:tool_not_allowed` rather than reaching Bronze.
- `cost_usd` in `[0.0, 50.0]`.
- `timestamp` within a rolling 24-hour freshness window: `timestamp >= current_timestamp() - INTERVAL 24 HOURS` and `timestamp <= current_timestamp() + INTERVAL 5 MINUTES` (5-minute future-skew tolerance to absorb distributed agent clock drift).
- **Timezone handling:** The Spark session is explicitly pinned to UTC (`spark.sql.session.timeZone = "UTC"`). Timezone-less string timestamps are parsed as UTC without local time conversion.
- **Contract loading & versioning lifecycle:** The contract is loaded into memory once at Gatekeeper startup (`_load_contract_thresholds()`); dynamic hot-reloading is not supported, so updating the active contract requires restarting the stream. Ingested events and committed rows in Bronze/Quarantine **do not carry a `contract_version` column**. Promoting a patch from `configs/agent_contract_proposed.yaml` to the active contract requires manual verification, copying to `configs/agent_contract.yaml`, and restarting the Gatekeeper.

### 2. Quarantined Record

Records failing contract rules are appended to the Quarantine Delta table with an `error_summary` diagnostic string without failing the micro-batch:

```json
{
  "agent_id":          null,
  "session_id":        "c7593eba-38fa-4bd6-b88b-dc6dfe7a0a45",
  "action_id":         "0744c95d-2f67-4a58-acf4-37255dbb3c7c",
  "timestamp":         "2026-09-22T21:48:20.308360+00:00",
  "tool_name":         "db_writer",
  "execution_time_ms": 488,
  "cost_usd":          "0.031163",
  "status":            "SUCCESS",
  "tool_args":         "{\"query\":\"Small stock focus note.\",\"limit\":12}",
  "quarantined_at":    "2026-09-22 21:49:57.118049",
  "error_summary":     "missing_required_field:agent_id"
}
```

- **Multi-error rows & delimiter:** When an event violates multiple contract rules, all errors are joined into `error_summary` using the **`; ` (semicolon and space)** delimiter (`F.concat_ws("; ", F.col("errors"))`). The triage engine clusters on the exact concatenated string (`frame.groupby("error_summary")`), so composite multi-error rows form distinct combination signatures rather than decomposing into atomic rule counts.
- **DuckDB access:** DuckDB reads both Bronze and Quarantine tables directly via `delta_scan('../data/bronze/agent_events')`, querying the Delta `_delta_log` transaction log without requiring an active Spark session.

### 3. Proposed YAML Contract Patch

The triage engine clusters quarantine signatures, diffs observed patterns against the active contract, and writes proposed evolution patches to `configs/agent_contract_proposed.yaml`:

```yaml
# Generated patch — deterministic triage engine
# Quarantined records analysed: 221 across 6 error signatures

version: "1.1.0"
contract_id: agent_events_v1_proposed

semantic_rules:
  - id: cost_non_negative
    rule: "cost_usd >= 0.0 AND cost_usd <= 935.0"
    message: "cost_usd out of valid boundaries [0.0, 935.0]"
```

*Patch novelty & rule naming:*
- **Rule ID `cost_non_negative`:** The patch retains the rule ID `cost_non_negative` because it directly evolves the existing semantic rule from `configs/agent_contract.yaml` (which governs both the lower bound `>= 0.0` and the upper bound `<= 50.0` under that single rule ID).
- **The $935.0 ceiling (headroom buffer):** The triage engine evaluates high-cost quarantine events (e.g., 60 observed records reaching a maximum observed cost of $850.0) and widens the upper bound by applying a 10% headroom buffer: `round(max_observed * 1.1, 1)` ($850.0 \times 1.1 = \$935.0$).
- **Demonstration circularity:** In the synthetic demo workload, triage exhibits circular reasoning: `producer.py` injects synthetic high-cost poison ($850.0), and the triage engine observes those quarantined records and proposes relaxing the contract to $935.0 to accept them. In production, contract widening requires human validation to differentiate valid large-model workloads from runaway agent cost anomalies.
- **Defect isolation:** Stale timestamps, negative costs, and invalid types are identified as defects and isolated without proposing contract relaxation. If no valid evolution is supported, triage emits an explicit "no contract change recommended" result.

Run `python src/delta_guard/triage.py` (or `make triage`) to regenerate proposals against the current Quarantine table.

---

## Quick Start

### Core Tool & Environment Versions
- **Python:** 3.11 (tested on 3.11.9)
- **PySpark:** 3.5.1
- **Delta Lake:** 3.2.0 (`delta-spark==3.2.0`, `deltalake>=0.17.0`)
- **dbt & DuckDB:** `dbt-core~=1.8.0`, `dbt-duckdb~=1.8.0`, `duckdb>=1.0.0`
- **Apache Kafka:** Confluent cp-kafka 7.5.0 (KRaft mode)

### Ports & Services
| Port | Service | Scope / Purpose |
| :--- | :--- | :--- |
| `8888` | Web Console | Live visual event monitor (`http://localhost:8888`) |
| `8080` | Kafka UI | Web dashboard for Kafka cluster (`http://localhost:8080`) |
| `9092` | Kafka Broker (Host) | External plaintext listener for local host processes (`localhost:9092`) |
| `29092`| Kafka Broker (Docker) | Internal plaintext listener within Docker Compose network (`kafka:29092`) |

---

Two execution paths. Pick one.

### Path A — Textual Terminal HUD (local Python execution)

**Prerequisites:**
- **Docker Desktop** (running, so Kafka broker can be started or probed on port 9092)
- **Java 11 or 17** (PySpark 3.5 requires Java 11 or 17 on the host; Java 21+ is not supported due to Hadoop UGI compatibility)
- **Python 3.11**

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python run_pipeline.py
```

`run_pipeline.py` checks Kafka health (starting `kafka` and `kafka-ui` via Docker Compose if not already active), launches the Producer and PySpark Gatekeeper in the background, and runs the live Textual Terminal HUD in the foreground.

### Path B — Full streaming pipeline (Docker Compose + Spark)

**Prerequisites:**
- **Docker Desktop** (all runtimes including Java 17 and PySpark execute inside container images)

```powershell
docker compose up -d    # starts Kafka (KRaft), Kafka UI, producer, gatekeeper, and web console
# Live console  → http://localhost:8888
# Kafka UI      → http://localhost:8080
docker compose down     # stop all services
```

*Note on `make`:* `Makefile` targets are provided for Linux/macOS/WSL environments. On native Windows, run the PowerShell equivalents shown above or via [`run_gatekeeper.ps1`](run_gatekeeper.ps1).

### Building Analytics Marts (dbt + DuckDB)

Once the Gatekeeper commits micro-batches to the Bronze Delta Lake table, run dbt to build analytical staging models and gold marts:

```powershell
# 1. Sync contract thresholds from agent_contract.yaml to dbt_project.yml
python scripts/sync_contract_to_dbt_vars.py

# 2. Run dbt from dbt_delta_guard/ (required due to relative path ../data/bronze/agent_events)
cd dbt_delta_guard
dbt deps --profiles-dir .
dbt build --profiles-dir .
cd ..
```
*(Or run `make sync-contract` followed by `make dbt-run` / `make dbt-test` on Linux/macOS/WSL).*

---

## Real Agent Instrumentation (LangChain)

Instead of using the synthetic `producer.py` event generator, you can instrument any LangChain agent directly using the [`LangChainEventCallback`](src/delta_guard/langchain_adapter.py) adapter. The adapter hooks into LangChain's callback system (`on_tool_start`, `on_tool_end`, `on_tool_error`, `on_llm_end`, `on_agent_finish`) and translates real agent execution events into canonical 9-field Agentic Delta Guard contract events, publishing them asynchronously to the Kafka topic (`agent-events`).

> **Import Convention Note:** When running with `PYTHONPATH=src` (the standard convention used in `docker-compose.yml`, `run_pipeline.py`, and developer scripts), modules are imported as `delta_guard.<module>`. When running directly from repository root without setting `PYTHONPATH`, imports use `src.delta_guard.<module>`.

```python
# With PYTHONPATH=src (or sys.path.insert(0, "src")):
from delta_guard.langchain_adapter import create_langchain_producer

callback = create_langchain_producer(
    agent_id="langchain_agent_001",
    tool_name_mapping={
        "web_search": "web_scraper",      # mapped -> Bronze
        "sql": "sql_query_executor",       # mapped -> Bronze
        # "calculator" not mapped -> Quarantine (semantic_rule:tool_not_allowed)
    },
)

# Attach to any LangChain chain, AgentExecutor, or LLM
chain.invoke({"input": "Research quantum computing"}, config={"callbacks": [callback]})
```

Run the no-API-key demo locally:

```powershell
pip install -r requirements-dev.txt    # adds langchain-core
$env:PYTHONPATH = "src"
python examples/langchain_demo.py
```

> **Note:** This adapter publishes real LangChain callback events — tool calls, LLM completions, errors — to the same Kafka topic the Gatekeeper consumes. The synthetic `producer.py` remains for load testing; this adapter demonstrates contract enforcement against real agent behaviour. See [`examples/langchain_demo.py`](examples/langchain_demo.py) for a complete runnable example and [`src/delta_guard/langchain_adapter.py`](src/delta_guard/langchain_adapter.py) for the full implementation.

---

## Model Context Protocol (MCP) Server

Agentic Delta Guard exposes a native Model Context Protocol (MCP) server ([`run_mcp_server.py`](run_mcp_server.py) / [`src/delta_guard/mcp/server.py`](src/delta_guard/mcp/server.py)) over stdio, providing pre-flight contract validation and supervisor observability for LLM agents (compatible with Claude Desktop, Cursor, Gemini CLI, and FastMCP runners).

### Available Tools
1. **`check_contract(payload)`**: Pre-flight advisor. Validates an event dictionary before emitting to Kafka. Returns pass/fail with remediation hints for self-healing agents.
2. **`get_active_contract()`**: Schema discovery. Returns the full active YAML contract, allowed tools list, max cost per call, and freshness boundaries.
3. **`get_quarantine_summary(limit=5, agent_id=None)`**: Supervisor observability. Queries recent rejections from the Quarantine Delta table via DuckDB `delta_scan` and clusters error signatures.
4. **`inspect_bronze_lakehouse(limit=5, agent_id=None)`**: Lakehouse observability. Queries recent validated Bronze Delta events.
5. **`propose_contract_patch(rationale, proposed_change, authorization_token)`**: Autonomous governance. Writes proposed updates to `configs/agent_contract_proposed.yaml`. Gated by `MCP_PROPOSAL_TOKEN`.

### Running the MCP Server
```powershell
# Set token for patch proposal authorization (optional for read-only tools)
$env:MCP_PROPOSAL_TOKEN = "your-secret-token"

# Run server over stdio
python run_mcp_server.py
```

### Client Configuration (`mcp_config.json`)
Configure your MCP client (e.g., Claude Desktop or Cursor) to launch the server:
```json
{
  "mcpServers": {
    "agentic-delta-guard": {
      "command": "python",
      "args": [
        "run_mcp_server.py"
      ],
      "env": {
        "PYTHONPATH": ".",
        "MCP_PROPOSAL_TOKEN": "your-secret-token"
      }
    }
  }
}
```

---

## Benchmarks

Measured using [`src/delta_guard/benchmark.py`](src/delta_guard/benchmark.py) (median across 5 runs of 100,000 events + 1 discarded warmup run). Full report: [`docs/reports/storage_benchmark.md`](docs/reports/storage_benchmark.md).

| Metric | Measured Value | Scope & Test Environment |
| :--- | ---: | :--- |
| **Validation Throughput** | **41,431.70 ev/s** | Single-core Python in-process validation logic (`_validate_event`, 100k events/run, 5 runs + 1 warmup, no Kafka / Spark I/O overhead) |
| **P50 Latency** | 0.0043 ms | Per-event contract validation latency (median) |
| **P95 Latency** | 0.0094 ms | Per-event contract validation latency (median) |
| **P99 Latency** | 0.0125 ms | Per-event contract validation latency (median) |
| **Synthetic Poison Consistency** | 14.9%–15.0% | Verifies contract validator consistency against producer-injected anomalies (~15% target). Evaluates generator-to-validator rule alignment on synthetic data, not real-world classification accuracy. |

*Environment:* Intel64 Family 6 Model 166 (x86_64), Windows 10/11 host, Python 3.11.9, 100,000-event workload per run. Regenerated report artifacts in `docs/reports/storage_benchmark.md` confirm Linux CI test suite parity.

*Throughput varies with machine load (observed range: 13.9k–48.2k ev/s across 5 runs on a shared developer machine). The README reports the median of the most recent full run.*

> **Benchmark Scope Note:** `benchmark.py` measures in-process Python validation logic (`_validate_event`), whereas `gatekeeper.py` executes native Spark Catalyst column expressions in the JVM. Parity between the two implementations is verified via `tests/test_contract_validation.py::test_benchmark_validation_parity`. Real end-to-end streaming throughput across Kafka $\rightarrow$ PySpark micro-batches $\rightarrow$ Delta disk commits will be lower due to network serialization, Spark task scheduling, and filesystem I/O.

---

## Design Decisions

### Idempotent retry handling & streaming state

Distributed agents retrying tool executions generate duplicate events sharing the same composite key `(agent_id, session_id, action_id)`.

Three mechanisms provide resilience:

1. **Delta Lake `MERGE INTO`** — keyed on `(agent_id, session_id, action_id)`:
   - When not matched $\rightarrow$ insert new event row into Bronze.
   - When matched $\rightarrow$ **leave existing row unchanged** (`whenNotMatchedInsertAll()`), dropping duplicate deliveries.

2. **In-batch deduplication & watermarking behavior** — `dropDuplicates(["agent_id", "session_id", "action_id"])` removes duplicates within a micro-batch. Inside `foreachBatch`, the DataFrame is a static micro-batch; Spark Structured Streaming watermarks do not perform stateful state eviction across micro-batches inside `foreachBatch`. Cross-batch idempotency across streaming restarts is guaranteed downstream by the Delta `MERGE INTO`.

3. **Late and stale events** — Events older than 24 hours are routed to Quarantine with the tag `freshness:stale_timestamp`.

Verified in [`tests/test_chaos_infra.py`](tests/test_chaos_infra.py) under synthetic checkpoint wipes and retry storm bursts.

> *Edge Case & Scope:*
> - **Idempotent duplicate handling:** Handles identical retries (identical composite key and identical payload) by dropping duplicates downstream via Delta `whenNotMatchedInsertAll()`.
> - **Same-key with mutated content (mutation drop):** Because the merge is insert-only, if an agent retries an action with the **same composite key** `(agent_id, session_id, action_id)` but updated content (e.g. flipping `status` from `"FAILED"` to `"SUCCESS"` or updating execution cost), the existing Bronze record matches and is **left unchanged**. The state change is silently ignored. Supporting in-place updates requires `whenMatchedUpdateAll()` with ordering/version columns.
> - **Out-of-sequence events:** Does not reorder late out-of-sequence events with timestamps newer than stored state.

### PySpark Catalyst expression validation

Validation rules in `src/delta_guard/gatekeeper.py` are implemented using native PySpark DataFrame column expressions (`F.when`, `F.filter`, `F.array`), compiled via Catalyst whole-stage code generation directly inside the JVM (local[2] execution mode) with zero Python row-by-row UDF overhead.

### Bronze / Quarantine → dbt / DuckDB handoff

Bronze and Quarantine tables are committed as native Delta Lake tables by PySpark. The analytical dbt layer reads them using DuckDB's `delta` extension:

```sql
-- dbt_delta_guard/models/staging/stg_agent_events.sql
from delta_scan('../data/bronze/agent_events')
```

`delta_scan` reads the Delta `_delta_log` transaction log to discover active Parquet data files without starting a Spark session. The DuckDB profile (`dbt_delta_guard/profiles.yml`) uses `type: duckdb` locally and can be configured with `type: spark` for distributed deployments.

### Delta sandbox mutation testing

`src/delta_guard/sandbox_guard.py` executes mutation testing against an isolated copy of Bronze data. The portable default creates an **independent Delta snapshot** (copy fallback). Native Spark `SHALLOW CLONE` (zero-copy metadata cloning) requires a working Spark + Delta runtime with native Hadoop binaries (`winutils.exe` on Windows), and is executed when that environment is present.

### Deterministic triage; optional LLM mode

`src/delta_guard/triage.py` operates deterministically by default: it clusters quarantine records by `error_summary`, evaluates cost/freshness/schema-drift heuristics, and writes proposed YAML patches. When `OPENAI_API_KEY` or `ANTHROPIC_API_KEY` is provided, it calls the LLM provider for enriched diagnosis. **CI and automated tests always execute the deterministic fallback path without external network dependencies or API token costs.**

### Vercel console

The deployment at `https://agentic-delta-guard.vercel.app` is an **interactive simulation** of the gatekeeper's behavior using pre-rendered event particle animations. Vercel hosts static frontend assets; no live Kafka or Spark infrastructure runs on Vercel.

---

## Project Structure

<details>
<summary>Expand file table</summary>

| Path | Purpose |
| :--- | :--- |
| `configs/agent_contract.yaml` | Single source of truth — active data contract rules consumed by gatekeeper, dbt, and MCP |
| `configs/agent_contract_proposed.yaml` | Proposed contract evolution patch emitted by triage engine or MCP proposal tool |
| `Project_summary.md` | Comprehensive technical architecture audit, failure modes, and 100x scaling redesign |
| `src/delta_guard/gatekeeper.py` | PySpark Structured Streaming engine: enforces contract via Catalyst expressions into Bronze / Quarantine |
| `src/delta_guard/producer.py` | Synthetic agent event generator with ~15% injected poison records for testing |
| `src/delta_guard/langchain_adapter.py` | LangChain callback handler: converts agent execution events into canonical 9-field Kafka payloads |
| `src/delta_guard/mcp/` | Model Context Protocol server (`server.py`), validators, and advisory pre-flight tools |
| `src/delta_guard/triage.py` | Deterministic triage engine + optional LLM mode; clusters failures and generates contract patches |
| `src/delta_guard/sandbox_guard.py` | Mutation testing against Delta snapshots / shallow clones to test contract evolution safely |
| `src/delta_guard/delta_clone_utils.py` | Delta Lake shallow clone and snapshot copy utilities with portable fallback |
| `src/delta_guard/status_server.py` | FastAPI backend serving `status.json` and the web console interface |
| `src/delta_guard/hud.py` | Real-time Textual terminal dashboard for local pipeline monitoring (Path A) |
| `src/delta_guard/benchmark.py` | Reproducible in-process validation throughput and storage footprint benchmark |
| `run_pipeline.py` | Path A local runner: orchestrates Kafka health check, workers, and terminal HUD in one command |
| `run_mcp_server.py` | Stdio entrypoint for launching the Agentic Delta Guard MCP server |
| `scripts/` | Automation utilities: `sync_contract_to_dbt_vars.py`, `run_e2e_verification.py`, and test scripts |
| `console.html` / `index.html` | Animated HTML5/Canvas real-time streaming pipeline dashboard (Path B and Vercel simulation) |
| `dbt_delta_guard/` | dbt models + DuckDB profiles; staging reads Bronze via `delta_scan` |
| `tests/` | Pytest suite: contract validation, chaos/infrastructure failover, Delta clones, MCP stdio, and LangChain |
| `docs/CLAIMS.md` | Verification matrix mapping all claims to source files and tests |
| `docs/reports/` | Auto-generated benchmark, quality, and triage audit reports |

</details>

---

## Environment Variables & Operational Runbook

### Environment Variables

| Variable | Default | Purpose / Scope |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker host:port (`localhost:9092` on host, `kafka:29092` inside Docker) |
| `MCP_PROPOSAL_TOKEN` | *(unset)* | Authorization token required to invoke `propose_contract_patch` on MCP server |
| `OPENAI_API_KEY` | *(unset)* | OpenAI API key for LLM-assisted quarantine triage in `src/delta_guard/triage.py` |
| `ANTHROPIC_API_KEY` | *(unset)* | Anthropic API key alternative for LLM-assisted quarantine triage |
| `LIVE_LLM_TEST` | `0` | Set to `1` to run opt-in live LLM test (`test_llm_triage_with_openai`) in pytest |
| `CHAOS_DOCKER_TESTS` | `0` | Set to `1` to run opt-in Docker broker restart test (`test_gatekeeper_survives_broker_restart`) |
| `JAVA_HOME` | *(system default)* | JDK 11 or 17 home directory (required for host PySpark runs; Java 21+ unsupported) |
| `PYTHONPATH` | *(unset)* | Python import path; set to `src` or `.` depending on execution context |

### Topic Management & Partitions
- **Topic Name:** `agent-events`
- **Auto-creation:** Enabled via `KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"` in `docker-compose.yml` (creates topic with 1 partition, replication factor 1 upon first producer event).
- **Manual inspection & creation:**
  ```powershell
  # Inspect topic metadata and partitions
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --describe --topic agent-events

  # Manually create topic with custom partitions
  docker exec kafka kafka-topics --bootstrap-server localhost:9092 --create --topic agent-events --partitions 1 --replication-factor 1
  ```

### State Reset Runbook
To purge all pipeline state and restart with a clean slate:
```powershell
# 1. Stop streaming services
docker compose down

# 2. Delete Delta tables, checkpoints, and status file
Remove-Item -Recurse -Force data\bronze, data\quarantine, checkpoints\gatekeeper, status.json

# 3. Reset Kafka topic and offsets (or wipe volumes with docker compose down -v)
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --delete --topic agent-events
```
*(On Linux/WSL, run `make clean` to delete build artifacts and generated reports).*

---

## Test Suite

Install development dependencies first (includes `langchain-core` and `mcp` needed by adapter and stdio integration tests):

```powershell
pip install -r requirements.txt
pip install -r requirements-dev.txt
python -m pytest tests/ -v --tb=short
```

### Skipped tests

The test suite collects **61 test cases** — 56 passed, 1 deselected (chaos_docker/requires_docker env-gated tests) on a
Windows host (`.venv\Scripts\python.exe -m pytest tests/ -v -m "not (chaos_docker or requires_docker)"`, verified 2026-10-17).
This includes 4 wire-level MCP stdio integration tests (`tests/test_mcp_stdio.py`),
8 LangChain adapter tests (`tests/test_langchain_adapter.py`), 5 real agent evaluation tests
(`tests/real_agent/`), and contract, chaos, clone, and MCP server suites.
Env-gated tests below skip on Linux CI unless the relevant variable is set:

| Test | Module | Marker | Reason for Skip | How to Enable |
| :--- | :--- | :--- | :--- | :--- |
| `test_gatekeeper_survives_broker_restart` | `test_chaos_infra.py` | `requires_docker` | Requires active Docker Compose cluster | `CHAOS_DOCKER_TESTS=1` |
| `test_llm_triage_with_openai` | `test_contract_validation.py` | `requires_llm` | Requires OpenAI API key and explicit opt-in | `LIVE_LLM_TEST=1` + `OPENAI_API_KEY` |

---

## Limitations

- **Throughput is single-node validation logic:** 28,427.92 ev/s (median of 5 × 100k-event runs) measures in-process Python validation only (`_validate_event`). End-to-end Kafka-to-Delta streaming throughput is not claimed.
- **Validator independence:** `_validate_event` (Python benchmark) and the gatekeeper's Catalyst column expressions are independently tested against the same 11 fixtures and produce identical results; they are not a single shared implementation.
- **Late-event data loss (watermark drop):** Events arriving after the 10-minute streaming watermark are silently dropped by Spark — they are not routed to Quarantine and produce no quarantine record. This is true data loss, distinct from the 24-hour freshness rule that tags stale events with `freshness:stale_timestamp` and routes them to Quarantine. Verified by `TestWatermarkLateEventHandling`.
- **Unparseable JSON evidence loss:** If an event contains syntactically invalid JSON, Spark's `from_json` evaluates the entire struct to `NULL`. The Gatekeeper flags this as `missing_required_field:<name>` and writes a quarantine row where all event attributes are null. The raw Kafka string payload (`value`), topic, partition, and offset are discarded at parse time, resulting in permanent evidence loss (forensic analysis, deduplication, and automated re-drive are impossible).
- **Same-key retry state loss:** Retrying an action with the same composite key `(agent_id, session_id, action_id)` but mutated payload fields (e.g. `FAILED` $\rightarrow$ `SUCCESS`) is silently ignored by the insert-only Delta `MERGE INTO`.
- **Multi-error clustering opacity:** `error_summary` concatenates multiple violations with `; `. Triage groups by the exact composite string rather than decomposing multi-violation records into atomic rule frequencies.
- **Static contract lifecycle & unversioned rows:** Gatekeeper loads `agent_contract.yaml` only at startup; dynamic reloading is unsupported and committed rows carry no `contract_version` metadata.
- **Triage heuristic circularity:** The triage engine's automated rule relaxation widens the cost ceiling to $935.0 to accommodate the very synthetic anomalies ($850.0) injected to test contract violations.
- **LangChain adapter queue drop:** The background thread queue in `LangChainKafkaProducer` is bounded at 1,000 items (`_BACKGROUND_QUEUE_MAXSIZE = 1000`). Under Kafka backpressure or broker unavailability, subsequent events are dropped silently (`logger.debug`) via `queue.put_nowait()`.
- **LangChain cost estimation:** LangChain does not track execution costs natively. The adapter calculates `cost_usd` using a static token estimate (`$0.00001` per token), which is a demonstration approximation rather than real billing attribution.
- **MCP SQL injection vulnerability:** In `src/delta_guard/mcp/server.py`, the `agent_id` filter in `get_quarantine_summary` and `inspect_bronze_lakehouse` is interpolated directly into DuckDB SQL query strings (`WHERE agent_id = '{agent_id}'`) rather than using parameterized query arguments.
- **Status server permissive CORS:** `status_server.py` configures `allow_origins=["*"]` to enable local browser and `file://` testing, which is permissive for production deployment.
- **HUD silent read failure:** `_delta_rows()` in `hud.py` uses a bare `try...except Exception: return 0`. Any read lock, delta log desync, or deserialization error silently displays as 0 rows rather than reporting an error state.
- **dbt test predicate & naming inconsistencies:** The custom dbt test `assert_cost_anomaly.sql` hardcodes `where cost_usd > 1.00` rather than referencing `var('max_cost_usd')`, and earlier test names exhibited alignment drift noted in the project audit.
- **LLM triage untested in CI:** The deterministic path is exercised in CI. Live LLM enrichment is opt-in (`LIVE_LLM_TEST=1`).
- **Sandbox portability:** Portable `SandboxGuard` uses an independent Delta snapshot (copy fallback). Native shallow clone requires a Spark/Delta runtime with native Hadoop support.
- **Single-broker footprint:** Evaluated on a single Docker Compose KRaft broker.
- **MCP server authorization:** `run_mcp_server.py` is tested in `tests/test_mcp_server.py` with `MCP_PROPOSAL_TOKEN` gating; this is a local review workflow, not enterprise IAM.
- **Schema drift (silent field drop):** `from_json` is called with a fixed schema; unknown fields in incoming events are silently dropped rather than flagged. There is no policy for detecting or alerting on schema drift from producers.
- **No quarantine re-drive path:** Once a contract is updated (e.g. via a proposed patch), previously-quarantined records that would now pass the new rules are not replayed. They remain in the Quarantine table indefinitely with no automated backfill mechanism.
- **Small-file problem:** The Bronze table accumulates one Parquet file per micro-batch; a 1,159-row sample produced 403 files with no partitioning or `OPTIMIZE`/`ZORDER` compaction. This is a known production anti-pattern and would require a compaction job at scale.
- **Exactly-once gap:** `foreachBatch` provides at-least-once delivery — Kafka offsets are committed after the Delta write, but there is a narrow window between the offset commit and the Delta commit where a crash could replay an already-written batch. The Delta `MERGE INTO` keyed on `(agent_id, session_id, action_id)` prevents duplicate rows from landing in Bronze, but does not cover the Quarantine append path.


---

## License

[MIT](LICENSE)
