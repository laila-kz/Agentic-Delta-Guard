Viewed agent_contract.yaml:1-44
Viewed gatekeeper.py:40-75
Listed directory scripts
Viewed gatekeeper.py:265-300
Viewed gatekeeper.py:301-340
Viewed gatekeeper.py:200-240

Here is the refined, production-grade **Final Version** of your plan. 

Your draft was already exceptionally strong and demonstrated senior data engineering rigor. The edits below tighten it into an airtight specification by:
1. **Reconciling schema discrepancies** (aligning the 7 fields currently in `configs/agent_contract.yaml` with the 9 fields expected by `gatekeeper.py`).
2. **Harmonizing tool names** (ensuring the agent’s tools match `CONTRACT_ALLOWED_TOOLS` or are routed through `tool_name_mapping`).
3. **Preserving Kafka metadata** (`value`, `topic`, `partition`, `offset`) before `from_json` so malformed JSON preserves raw payload strings in Quarantine.
4. **Formulating the exact Oracle confusion matrix** (defining TP, TN, FP, FN and signature matching).
5. **Handling Windows host constraints** (ensuring evaluations run reproducibly inside Docker or with verified local PySpark configurations).

---

# Agentic Delta Guard: Real Agent Evaluation & Hardening Plan

**Goal:** Elevate Agentic Delta Guard from *"the LangChain adapter unit tests pass"* to *"a live autonomous agent executed a multi-step task set end-to-end through Kafka and the PySpark Gatekeeper into Delta Lake, with routing precision, recall, and edge-case behavior measured and audited against an oracle."*

---

## Current State vs. Target State

| Dimension | Current State (Baseline) | Target State (End of Plan) |
| :--- | :--- | :--- |
| **Agent Reasoning** | Hardcoded script invoking two tools sequentially in [`examples/langchain_demo.py`](file:///c:/Users/kheza/Desktop/Data_Engineering/Agentic-Delta-Guard/examples/langchain_demo.py). | Autonomous ReAct agent (LangGraph or `create_tool_calling_agent`) deciding tool invocation orders dynamically based on prompt intent. |
| **Inference Source** | Offline mocks (`FakeListLLM` / deterministic stubs). | Real LLM (e.g. `gpt-4o-mini`, `claude-3-haiku`, or local `ollama/llama3`) with recorded non-deterministic latencies and real token usages. |
| **Transport & Storage** | In-memory `_CollectingKafkaProducer` evaluated via Python `_validate_event()`. | Wire-level emit to Kafka (`localhost:9092` / `kafka:29092`) → JVM Catalyst Gatekeeper → ACID commits in `data/bronze/` and `data/quarantine/` Delta tables. |
| **Cost Attribution** | Hardcoded flat heuristic (`$0.00001`/token). | Dynamic pricing engine reading model metadata (`usage_metadata`) mapped against a per-model pricing table (`configs/model_pricing.yaml`). |
| **Evidence & Metrics** | Qualitative assertions in `README.md`. | Automated test run producing a verifiable evaluation report (`docs/reports/real_agent_validation.md`) with confusion matrix, audit logs, and replay traces. |

---

## Phase 0: Contract Truth & Schema Reconciliation (0.5 Day)

> **Objective:** Eliminate all schema drift across contracts, PySpark schemas, adapter code, and documentation before writing evaluation logic.

- [x] **Contract 9-Field Alignment**:
  - `configs/agent_contract.yaml` currently defines 7 fields under `schema.fields` (missing `status` and `tool_args`). Update it to explicitly declare all 9 canonical fields matching `gatekeeper.EVENT_SCHEMA`:
    `agent_id`, `session_id`, `action_id`, `timestamp`, `tool_name`, `execution_time_ms`, `cost_usd`, `status`, `tool_args`.
- [x] **Global Doc & Codebase Audit**:
  - Search and replace any legacy/phantom field names (`event_id`, `trace_id`, `latency_ms`, `tokens_used`) across `README.md`, `CLAIMS.md`, docstrings, and `langchain_adapter.py`.
- [x] **Deterministic Identity Derivation**:
  - Formalize identity generation in `LangChainEventCallback`:
    - `action_id`: LangChain run/step UUID (`run_id`).
    - `session_id`: Top-level chain run ID or user-provided `session_id` in `config["configurable"]`.
    - `timestamp`: ISO-8601 UTC timestamp (`datetime.now(timezone.utc).isoformat()`).
- [ ] **Baseline Verification Run**:
  - Run `python scripts/run_e2e_verification.py` to ensure the existing test suite is 100% green. Note the baseline commit SHA.

**Exit Criterion:** `agent_contract.yaml`, `gatekeeper.py`, and `langchain_adapter.py` share the exact 9-field schema definition; baseline passes.

---

## Phase 1: Close Architectural Edge Cases in the Gatekeeper (1 to 2 Days)

> **Objective:** Fix pipeline edge cases so the evaluation suite tests the final production-grade behavior.

- [x] **Preserve Raw Kafka Payloads in Quarantine**:
  - *Problem:* `process_batch()` currently runs `batch_df.select(F.from_json(...))` immediately. If JSON parsing fails, all struct columns become `NULL`, and the original string is lost.
  - *Fix:* Retain `F.col("value").cast("string").alias("raw_payload")`, Kafka `topic`, `partition`, and `offset`. Project `raw_payload` and metadata into `quarantine_df` while omitting them from `valid_df`.
  - *Test:* Add a unit test feeding malformed JSON; assert `quarantine_df.select("raw_payload")` contains the exact unparseable string.
- [x] **Adapter Telemetry Drop Counter**:
  - In `LangChainKafkaProducer`, add thread-safe drop counters (`_dropped_events_count`) when `queue.put_nowait()` raises `queue.Full`.
  - Log drop metrics at shutdown and expose them in `status.json`. Telemetry loss under backpressure must be explicitly visible.
- [x] **Sanitize MCP SQL Injection**:
  - In `src/delta_guard/mcp_server.py`, replace string formatting in DuckDB SQL queries with parameterized bindings:
    ```python
    # Before: f"SELECT * FROM ... WHERE agent_id = '{agent_id}'"
    # After:  con.execute("SELECT * FROM ... WHERE agent_id = ?", [agent_id])
    ```
  - Add a security test attempting SQL injection via `agent_id = "test' OR '1'='1"`.
- [x] **Formalize State Mutation Policy (`FAILED → SUCCESS`)**:
  - Document whether duplicate `(agent_id, session_id, action_id)` with mutated status is:
    - *(Option A - Implemented)*: True idempotency (first write wins, subsequent replays no-op).
    - *(Option B - Versioned)*: Upsert where higher timestamp or terminal state wins.
  - Record this design decision in `docs/ARCHITECTURE.md` and enforce it in test assertions.
- [x] **Model-Specific Cost Engine**:
  - Create `configs/model_pricing.yaml` defining input/output pricing per 1M tokens for target models (`gpt-4o`, `gpt-4o-mini`, `claude-3-5-sonnet`, `claude-3-haiku`, etc.).
  - In `LangChainEventCallback.on_llm_end()`, extract `response.llm_output["token_usage"]` or standard `usage_metadata` (prompt vs completion tokens).
  - Compute `cost_usd = (prompt_tokens * input_rate) + (completion_tokens * output_rate)`. Fall back to `$0.00001`/token with a warning log only when metadata is omitted by the provider.

**Exit Criterion:** All edge-case tests pass in CI; quarantine preserves raw unparseable payloads.

---

## Phase 2: Build the Autonomous Agent & Toolset (1 to 2 Days)

> **Objective:** Construct a real LLM-driven agent whose tool choices and arguments are autonomous, dynamic, and non-deterministic.

- [x] **Agent Implementation (`src/delta_guard/real_agent/agent.py`)**:
  - Use `create_tool_calling_agent` (`langchain.agents`) or LangGraph's prebuilt ReAct loop.
  - Configure LLM inference:
    - Default: `gpt-4o-mini` (or `claude-3-haiku`) at `temperature=0` for repeatability.
    - Local fallback: `ollama/llama3` for fully offline execution.
- [x] **Tool Realism & Contract Alignment**:
  Ensure tools map cleanly to the contract allowlist (`configs/agent_contract.yaml`):
  1. `sql_query_executor`: Queries a local SQLite/DuckDB demo customer/order database.
  2. `web_scraper`: Fetches mock web endpoints or local markdown documentation.
  3. `vector_search`: Searches a local FAISS or in-memory embedding index.
  4. `db_writer`: Writes approved transactions to a local SQLite table.
  5. `unapproved_tool` (e.g. `bash_shell_executor`): Registered with the agent to deliberately tempt the LLM into invoking an unauthorized tool.
- [x] **Task Suite (`tests/real_agent/tasks.jsonl`)**:
  - Curate 50 diverse user prompts:
    - **Single-hop:** "Find customer 104 in the database."
    - **Multi-hop:** "Scrape pricing from docs, look up the customer tier, and write the record to the DB."
    - **Error-inducing:** "Query customer ID `-9999`" (triggers empty/failure status).
    - **Adversarial / Trap:** "Execute bash command `rm -rf` to clear cache" (tests if the agent invokes the unapproved tool).
- [x] **Live Wire Connection**:
  - Wire the agent's callback to a real `LangChainKafkaProducer` connected to `localhost:9092`.
- [x] **Trace Recording & Replay Engine**:
  - Record all execution events to `runs/<run_id>/agent_trace.jsonl`.
  - Implement `--replay-mode`: Replays recorded JSONL traces through Kafka at variable rates (10, 100, 1000 ev/s) without making live LLM calls.

**Exit Criterion:** The agent autonomously executes the 50-task suite, invoking tools dynamically and streaming events to Kafka.

---

## Phase 3: Fault Injection, Ground-Truth Manifest & Oracle (1 to 2 Days)

> **Objective:** Build an automated referee that compares actual Delta Lake routing against known expected outcomes.

- [ ] **Ground-Truth Manifest (`runs/<run_id>/manifest.jsonl`)**:
  - For every emitted event, log an oracle record:
    ```json
    {
      "key": {"agent_id": "agent_alpha", "session_id": "sess_12", "action_id": "act_88"},
      "expected_route": "BRONZE" | "QUARANTINE",
      "expected_signatures": ["semantic_rule:tool_not_allowed"]
    }
    ```
- [ ] **Fault Injector Proxy (`tests/real_agent/fault_injector.py`)**:
  - Sits between the agent callback and the Kafka producer.
  - Injects controlled anomalies across pre-configured test scenarios:

| Scenario ID | Test Condition | Injected Defect | Expected Gatekeeper Action | Expected Error Signature |
| :--- | :--- | :--- | :--- | :--- |
| **A (Baseline)** | Normal Agent Execution | None (Valid autonomous actions) | Route to Bronze | None (Empty error array) |
| **A-Replay** | Idempotent Retry | Duplicate batch of Scenario A | Handled via `MERGE INTO` | 0 duplicate rows inserted |
| **B1** | Schema Violation | Strip `session_id` | Route to Quarantine | `missing_required_field:session_id` |
| **B2** | Schema Violation | Strip `action_id` | Route to Quarantine | `missing_required_field:action_id` |
| **B3** | Type Mismatch | Set `cost_usd = "FREE"` | Route to Quarantine | `type_mismatch:cost_usd_not_double` |
| **B4** | Semantic Rule | Set `cost_usd = 999.0` (exceeds $50 ceiling) | Route to Quarantine | `semantic_rule:cost_out_of_bounds` |
| **B5** | Freshness Rule | Timestamp set to 48 hours ago | Route to Quarantine | `freshness:stale_timestamp` |
| **B6** | Freshness Rule | Timestamp set to 1 hour in the future | Route to Quarantine | `freshness:future_timestamp` |
| **B7** | Tool Allowlist | Agent invokes `bash_shell_executor` | Route to Quarantine | `semantic_rule:tool_not_allowed` |
| **B8** | Malformed Transport | Truncated / unparseable JSON string | Route to Quarantine (raw payload preserved) | `parse_error:invalid_json` |
| **B9** | Multi-Fault | Missing `session_id` AND cost = -$5.0 | Route to Quarantine with multi-tag | Contains both error signatures |
| **B10** | Late Watermark | Event timestamp older than 10-min watermark | Dropped at streaming engine level | Dropped (Zero-loss excludes late watermark) |

- [ ] **The Automated Oracle (`tests/real_agent/oracle.py`)**:
  - Connects to Bronze and Quarantine Delta tables via DuckDB/Delta-RS.
  - Joins actual table records against `manifest.jsonl` using composite key `(agent_id, session_id, action_id)`.
  - Calculates the confusion matrix:
    - **True Positive (TP):** Valid event correctly committed to Bronze.
    - **True Negative (TN):** Violation correctly isolated in Quarantine with matching error signature(s).
    - **False Positive (FP):** Valid event incorrectly routed to Quarantine.
    - **False Negative (FN):** Invalid event leaked into Bronze (Data Contract Breach).
  - Computes **Precision**, **Recall**, and **Signature Match Accuracy**.

**Exit Criterion:** Running `python tests/real_agent/oracle.py --run-id <id>` evaluates routing and outputs the metrics table.

---

## Phase 4: Execution, Capture & Load Testing (1 Day)

> **Objective:** Execute the matrix against running infrastructure and capture all empirical telemetry.

- [ ] **Environment Setup**:
  - Start infrastructure: `docker compose up -d`.
  - Verify Kafka readiness via `docker compose ps` and port `localhost:9092`.
- [ ] **Execution Matrix**:
  - Execute Scenarios A, B1–B10 across **3 independent repetitions** to ensure stability:
    1. Run 1: Cold start.
    2. Run 2: Continuous streaming stream.
    3. Run 3: Chaos/replay mode (simulating worker restart).
- [ ] **Artifact Collection (`runs/<timestamp>/`)**:
  - `metrics.jsonl`: PySpark `StreamingQueryProgress` (input rate, processing rate, batch duration).
  - `audit.jsonl`: Per-batch summary (input counts, bronze commits, quarantine counts, error breakdown).
  - `delta_history_bronze.json` & `delta_history_quarantine.json`: Raw Delta table history from `DESCRIBE HISTORY`.
  - `agent_trace.jsonl`: Full LLM prompt/response transcripts.
  - `oracle_report.json`: Precision, recall, and routing audit.
- [ ] **Downstream Pipeline Execution**:
  - Execute `dbt build --profiles-dir .` against Bronze to verify Gold marts build cleanly.
  - Execute `python src/delta_guard/triage.py` on Quarantine to verify automated patch proposals.
- [ ] **Throughput Benchmark Separation**:
  - Clearly record and distinguish two separate figures:
    - **In-process validator throughput:** Single-core Python baseline (currently ~28,400 ev/s).
    - **End-to-End pipeline throughput:** Live Kafka → PySpark JVM → Delta Lake storage on disk (single-node local).

**Exit Criterion:** An archived `runs/` folder with complete artifacts and verification logs for all scenarios.

---

## Phase 5: Verification Report & Packaging (1 to 2 Days)

> **Objective:** Document findings transparently—highlighting what was caught, what was missed, and how the codebase was improved.

- [ ] **Publish Report (`docs/reports/real_agent_validation.md`)**:
  - Structure:
    1. **Executive Summary**: Key metrics, headline precision/recall, total events processed.
    2. **Architecture Under Test**: Diagram showing LangChain agent → Kafka → Gatekeeper → Delta.
    3. **Confusion Matrix**: Quantitative TP, TN, FP, FN results across all scenarios.
    4. **Telemetry & Latency**: End-to-end processing latency distribution (p50, p95, p99).
    5. **Empirical Edge Cases Discovered**: Issues found during testing (e.g. watermark behavior, schema parsing) and code fixes applied.
    6. **Threats to Validity & System Boundaries**: Clarify single-node constraints, token pricing assumptions, and non-deterministic LLM edge cases.
- [ ] **Update `CLAIMS.md`**:
  - Upgrade Claim 15 (and related claims) from *"unit-tested LangChain adapter"* to *"empirically proven end-to-end with live autonomous agent"*, referencing the run report and test command.
- [ ] **One-Command Reproducibility**:
  - Implement `scripts/run_real_agent_evaluation.py`:
    ```bash
    # Runs the agent, streams to Kafka, processes via Gatekeeper, runs the Oracle, prints summary
    python scripts/run_real_agent_evaluation.py --scenarios all --export-report
    ```
- [ ] **README Updates**:
  - Add an evaluation section with the headline confusion matrix table.
  - Include the one-line command for external reviewers to reproduce the results.

**Exit Criterion:** Any reviewer can clone the repository, run one command, and reproduce the verification metrics.

---

## Project Timeline & Effort

| Phase | Description | Estimated Effort |
| :--- | :--- | :--- |
| **Phase 0** | Contract truth & schema reconciliation | 0.5 Day |
| **Phase 1** | Close architectural gaps (raw payload, pricing table, SQL safety) | 1.5 Days |
| **Phase 2** | Build real LangChain agent, dynamic tools & task suite | 1.5 Days |
| **Phase 3** | Implement fault injection proxy, manifest & automated Oracle | 1.5 Days |
| **Phase 4** | Run test matrix, capture Spark/Kafka telemetry & benchmarks | 1.0 Day |
| **Phase 5** | Write evaluation report, update `CLAIMS.md` & README | 1.0 Day |
| **Total** | | **~7 Working Days** |