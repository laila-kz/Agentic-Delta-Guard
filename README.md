# 🛡️ Agentic Delta Guard
### Real-Time Governance, Contract Enforcement & Self-Healing Lakehouse for Autonomous AI Agents

[![Engine](https://img.shields.io/badge/Engine-PySpark%203.5%20%7C%20Delta%20Lake-00A4E4?logo=apachespark)](https://delta.io/)
[![Quality](https://img.shields.io/badge/Quality-dbt%20%2B%20DuckDB-FF694B?logo=dbt)](https://getdbt.com)
[![MTTR Reduction](https://img.shields.io/badge/Incident%20MTTR-94.8%25%20Reduction-brightgreen)](#-quantified-performance--benchmarks)
[![Throughput](https://img.shields.io/badge/Validation-4%2C520%20ev%2Fs-success)](#-quantified-performance--benchmarks)
[![Tests](https://img.shields.io/badge/Pytest%20Suite-100%25%20Passing-blue)](tests/)

---

## 🚨 The Problem: The "Silent Poison" in Multi-Agent Pipelines

As enterprises grant autonomous LLM agents (AutoGPT, CrewAI, LangGraph, custom tool-calling agents) write permissions to production data lakes, traditional batch validation fails:
1. **Schema Hallucinations & Drift:** Agents dynamically mutate tool outputs, injecting unexpected fields or invalid types.
2. **Cost & Rate Runaways:** Rogue agent loops blow through token and API budgets without warning.
3. **Pipeline Stalls:** One malformed record in a traditional streaming job crashes the entire micro-batch.

**Agentic Delta Guard** is a streaming data gateway and self-healing lakehouse that provides a zero-trust firewall for data lakes: validating streaming records in distributed memory, isolating non-conforming events to a quarantine Delta table, and autonomously diagnosing incidents using LLMs in **<90 seconds** (vs ~20 mins manual SRE triage).

---

## 📊 Quantified Performance & Benchmarks

All metrics verified on reproducible benchmarks ([`scripts/measure_triage_efficiency.py`](scripts/measure_triage_efficiency.py) and [`src/delta_guard/benchmark.py`](src/delta_guard/benchmark.py)):

| Performance Metric | Baseline / Naive Approach | Agentic Delta Guard | Impact |
| :--- | :--- | :--- | :--- |
| **Incident Triage MTTR** | 23.4 min (Manual SRE) | **1.2 min (Automated LLM)** | **⚡ 94.8% Faster MTTR** |
| **Validation Throughput** | 120 ev/s (Row-by-Row UDF) | **4,520 ev/s (Vectorized PySpark)** | **🚀 37.5x Throughput Gain** |
| **Stream Interruption** | Pipeline halted on error | **Zero Downtime (Delta Quarantine)** | **100% Ingestion Uptime** |
| **Triage Cost / 100 Incidents** | $4,680.00 (Engineering hours) | **$0.04 (LLM Triage API calls)** | **💰 99.99% Cost Reduction** |

Detailed reports available at [docs/reports/triage_benchmark.md](docs/reports/triage_benchmark.md) and [docs/reports/storage_benchmark.md](docs/reports/storage_benchmark.md).

---

## 🌐 Live Interactive Demo (`demo_app.py`)

Try the interactive governance gateway without running local Docker or Spark:

```powershell
# Run the interactive demo locally
pip install -r requirements-demo.txt
streamlit run demo_app.py
```

- **Live Stream Generator:** Simulate agent tool calls, inject schema drift or budget spikes, and watch the gateway make real-time routing decisions.
- **Automated Incident Triage:** Trigger the LLM triage engine against quarantined batches to produce root-cause diagnoses and YAML contract patches.
- **Contract Playground:** Inspect live constraints defined in `configs/agent_contract.yaml`.

---

## 💡 Architecture & Data Flow

```mermaid
flowchart LR
    subgraph Producers ["AI Agent Fleet"]
        A1[Research Agent] --> Kafka[(Kafka: agent-events)]
        A2[SQL Coder] --> Kafka
        A3[Rogue Crawler] --> Kafka
    end

    subgraph Gateway ["Vectorized PySpark Gatekeeper"]
        Kafka --> Engine{Vectorized Contract Engine}
        Engine -->|Contract Valid| Bronze[(Bronze Delta Lake)]
        Engine -->|Contract Violations| Quarantine[(Quarantine Delta Lake)]
    end

    subgraph Transformation ["Analytics & Operations"]
        Bronze --> DBT[dbt Core + DuckDB]
        DBT --> Gold[(Gold Analytics Marts)]
        Quarantine --> Triage[LLM Incident Triage Engine]
        Triage --> Patch[Proposed YAML Contract Patch]
    end

    Bronze -.-> HUD[Live Textual Terminal HUD]
    Quarantine -.-> HUD
    Kafka -.-> HUD
```

The complete architecture and failure handling are documented in [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

---

## 🧠 The Engineering Deep Dive: Solving Agent Retry Storms & Out-of-Order Merges

> **The Edge Case:** Distributed LLM agents running across intermittent networks experience HTTP timeouts and retry tool executions 5–10 seconds later with duplicate sequence IDs. Meanwhile, network latency causes older events to arrive *after* newer state updates.

### The Dual-Stage Architecture
1. **Bounded PySpark Watermark:** `.withWatermark("timestamp", "10 seconds")` evicts stale streaming state, preventing JVM memory leaks.
2. **Idempotent Delta Lake Merge:** The gatekeeper upserts records via conditional `MERGE INTO` keyed on `(agent_id, session_id, action_id)`:
   - If matched and `source.timestamp > target.timestamp` → Update in place.
   - If not matched → Insert new event.
   - If duplicate older event arrives → Silently drop without corrupting table state.

Verified via [`tests/test_chaos_infra.py`](tests/test_chaos_infra.py) under synthetic retry bursts.

---

## 🖥️ Real-Time Terminal HUD

The terminal dashboard monitors Kafka partition offsets, Delta ingestion throughput, and quarantine incident signals with a 2-second live refresh:

![Terminal HUD](docs/hud_screenshot.jpg)

---

## 📁 Project Structure

| Path | Purpose |
| :--- | :--- |
| `demo_app.py` | Interactive Streamlit web demo for cloud or local deployment |
| `src/delta_guard/gatekeeper.py` | Vectorized PySpark streaming engine with Delta Bronze & Quarantine routing |
| `src/delta_guard/producer.py` | Synthetic multi-agent event generator with deliberate error injection |
| `src/delta_guard/triage.py` | Deterministic and LLM-assisted quarantine root-cause clustering |
| `src/delta_guard/hud.py` | Real-time Textual operations dashboard |
| `src/delta_guard/sandbox_guard.py` | Zero-copy shallow clone sandbox mutation testing |
| `scripts/measure_triage_efficiency.py` | Reproducible MTTR & cost benchmark generator |
| `configs/agent_contract.yaml` | Single source of truth event contract & quality constraints |
| `dbt_delta_guard/` | dbt data models, staging views, and DuckDB analytical marts |
| `tests/test_chaos_infra.py` | Chaos resilience tests (checkpoint loss, broker outage, retry storms) |
| `docs/reports/` | Auto-generated benchmark, quality, and triage audit reports |

---

## 🚀 Quick Start

### Local Docker & Python Environment

```powershell
# 1. Setup isolated virtualenv
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1

# 2. Install dependencies
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install dbt-core dbt-duckdb pytest streamlit

# 3. Start Kafka & ZooKeeper
docker compose up -d

# 4. Launch streaming pipeline (separate terminals)
python src/delta_guard/producer.py        # Terminal 1: Event Producer
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
python src/delta_guard/gatekeeper.py      # Terminal 2: Streaming Gatekeeper
python src/delta_guard/hud.py             # Terminal 3: Real-Time HUD
```

Kafka UI is available at [http://localhost:8080](http://localhost:8080).

---

## 🧪 Test & Chaos Verification

```powershell
# Run the complete test suite
python -m pytest tests/ -v --tb=short

# Run the automated triage MTTR benchmark
python scripts/measure_triage_efficiency.py

# Run dbt data quality marts and tests
cd dbt_delta_guard
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## 📑 Verification Reports & Artifacts

- [`docs/reports/triage_benchmark.md`](docs/reports/triage_benchmark.md): Verified MTTR and cost efficiency report.
- [`docs/reports/storage_benchmark.md`](docs/reports/storage_benchmark.md): Delta storage footprint and compression benchmarks.
- [`docs/INCIDENT_LOG.md`](docs/INCIDENT_LOG.md): Autonomous incident diagnoses and proposed contract patches.
- [`docs/reports/quality_audit.md`](docs/reports/quality_audit.md): Automated dbt test summary.

---

## 🔍 Engineering Trade-Offs & Limitations

- **Single-Broker / Single-Worker Footprint:** Evaluated on a single Docker Compose broker; numbers represent single-node throughput ceiling.
- **Bronze/Silver Storage Boundary:** Bronze & Quarantine use native Delta Lake; Silver and Gold models execute in DuckDB/dbt Parquet for lightweight local analytics. (See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md#storage-layer-boundary-deliberate-trade-off)).
- **LLM Triage Testing:** Continuous CI uses deterministic fallback heuristics for speed and zero API cost; live LLM verification is opt-in via `LIVE_LLM_TEST=1`.
