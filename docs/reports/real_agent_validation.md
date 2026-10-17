# Agentic Delta Guard: Real Autonomous Agent Empirical Validation Report

**Suite Run Identifier:** `suite_real_agent_matrix_baseline`  
**Evaluation Mode:** `Autonomous ReAct Agent (Open Data Contract Standard)`  
**Storage Engine:** `Delta Lake (ACID Bronze & Quarantine Tables)`  
**Oracle Referee:** `Automated Confusion Matrix & Signature Match Referee`  

---

## 1. Executive Summary

This empirical evaluation validates the **Agentic Delta Guard** architecture end-to-end against an autonomous AI agent. Rather than using static mock dictionaries or scripted tests, the evaluation runs an autonomous ReAct agent ([`AutonomousReActAgent`](../../src/delta_guard/real_agent/agent.py)) that dynamically selects, invokes, and interprets tools against live SQLite databases, web scrapers, and vector indices.

Telemetry generated during tool execution is emitted in real-time, serialized to Open Data Contract Standard (ODCS) schema definitions ([`configs/agent_contract.yaml`](../../configs/agent_contract.yaml)), subjected to controlled fault injection across 11 scenarios, and routed into Delta Lake. The **Automated Oracle** independently validates routing precision, recall, and error signature accuracy against ground-truth manifests.

| Headline Metric | Measured Result | Production Target | Status |
| :--- | :--- | :--- | :--- |
| **Routing Precision** | **100.0%** | > 99.0% | **PASS** |
| **Routing Recall** | **100.0%** | 100.0% | **PASS** |
| **Signature Match Accuracy** | **100.0%** | 100.0% | **PASS** |
| **Data Contract Breach Rate** | **0.00%** | 0.00% (Zero-Breach) | **PASS** |
| **Total Scenarios Evaluated** | **11 Scenarios (A, B1–B10)** | 11 Scenarios | **PASS** |
| **Evaluation Task Suite** | **50 Autonomous Tasks** | 50 Tasks | **PASS** |

---

## 2. Quantitative Confusion Matrix

Every event emitted during agent task execution is paired with a ground-truth manifest verdict. The Automated Oracle joins the Delta Lake table records against the manifests using composite keys `(agent_id, session_id, action_id)`:

```text
                            Predicted Bronze       Predicted Quarantine
Actual Valid Event          TP = 5                 FP = 0
Actual Contract Violation   FN = 0 (Breach)        TN = 50
```

- **True Positives (TP = 5):** Valid autonomous agent actions correctly accepted and committed into Bronze Delta table.
- **True Negatives (TN = 50):** Deliberate schema, freshness, cost, and allowlist violations accurately intercepted and diverted into Quarantine Delta table.
- **False Positives (FP = 0):** Valid agent executions incorrectly routed to Quarantine (Zero false alarms).
- **False Negatives (FN = 0):** Invalid or corrupted agent actions leaking into Bronze (**Zero Contract Breaches**).

---

## 3. Scenario Matrix Breakdown

| Scenario ID | Category | Test Condition | Injected Defect | Target Route | Observed Result | Error Signature Verified |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **A** | Baseline | Normal Autonomous Agent | None (Valid actions) | BRONZE | 100% Bronze | None (Valid) |
| **B1** | Schema | Missing required field | `session_id = null` | QUARANTINE | 100% Quarantine | `missing_required_field:session_id` |
| **B2** | Schema | Missing required field | `action_id = null` | QUARANTINE | 100% Quarantine | `missing_required_field:action_id` |
| **B3** | Schema | Type mismatch | `cost_usd = "FREE"` | QUARANTINE | 100% Quarantine | `type_mismatch:cost_usd_not_double` |
| **B4** | Semantic | Cost bounds violation | `cost_usd = $999.00` (ceiling $50) | QUARANTINE | 100% Quarantine | `semantic_rule:cost_out_of_bounds` |
| **B5** | Freshness | Stale timestamp | Timestamp set to 48h ago | QUARANTINE | 100% Quarantine | `freshness:stale_timestamp` |
| **B6** | Freshness | Future clock skew | Timestamp set to +1h in future | QUARANTINE | 100% Quarantine | `freshness:future_timestamp` |
| **B7** | Governance | Unauthorized tool trap | Agent executes `bash_shell_executor` | QUARANTINE | 100% Quarantine | `semantic_rule:tool_not_allowed` |
| **B8** | Transport | Malformed JSON transport | Truncated byte payload | QUARANTINE | 100% Quarantine | `parse_error:invalid_json` |
| **B9** | Multi-Fault | Compound violations | Missing `session_id` AND cost = -$5.0 | QUARANTINE | 100% Quarantine | Multi-signature tag verified |
| **B10** | Watermark | Late streaming event | Timestamp older than 10-min window | DROPPED | 100% Dropped | Engine-level watermark drop |

---

## 4. Architectural Hardening & Edge Cases Resolved

1. **Host Virtual Network Binding (Windows & Docker):**
   - *Issue:* When Docker is running on Windows, Apache Spark's default hostname resolution queries `vEthernet` NetBIOS adapters, causing 60-second connection hangs during driver-executor registration.
   - *Hardening:* Pinned `spark.driver.host` and `spark.driver.bindAddress` strictly to `127.0.0.1`.
2. **Delta Lake Schema Evolution Globbing:**
   - *Issue:* DuckDB Parquet scanning threw `InvalidInputException` when reading historical Parquet files alongside newly evolved schemas containing `raw_payload` and `kafka_offset`.
   - *Hardening:* Enforced `union_by_name=true` in DuckDB's `read_parquet()` loader.
3. **Null-Key Deduplication Poisoning:**
   - *Issue:* Ingesting malformed events where `agent_id` or `session_id` was `null` caused standard Spark `dropDuplicates` to collapse multiple distinct failures into a single row.
   - *Hardening:* Split deduplication in the Gatekeeper to only collapse non-null keys, preserving all individual failure records in Quarantine.
4. **Local Spark Shuffle Parallelism:**
   - *Issue:* Default Spark shuffle partition count of 200 spawned 600 tasks for tiny 5-event test batches, bottlenecking local execution.
   - *Hardening:* Configured `spark.sql.shuffle.partitions = 2` for sub-second micro-batch completion.

---

## 5. One-Command Reproducibility

Any reviewer can reproduce the entire evaluation matrix across all 11 scenarios by executing a single command:

```powershell
python scripts/run_real_agent_evaluation.py --scenarios all
```
