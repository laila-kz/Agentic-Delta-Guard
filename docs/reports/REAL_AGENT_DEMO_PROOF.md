# 🛡️ Real Agent End-to-End Pipeline Verification Report

**Branch:** `my-own-agent-demo`  
**Execution Date:** 2026-10-18  
**Components Validated:** LangChain Custom Agent (`examples/run_custom_agent.py`) ➔ Kafka Broker (`127.0.0.1:9092`) ➔ PySpark Streaming Gatekeeper (`src/delta_guard/gatekeeper.py`) ➔ Bronze & Quarantine Delta Lake Storage (`data/bronze/agent_events`, `data/quarantine/agent_events`) ➔ DuckDB Inspection.

---

## 1. Executive Summary

This report documents the live end-to-end execution of a real, custom LangChain-based AI agent (`support_bot_enterprise`) wired into the Agentic Delta Guard streaming lakehouse.

Unlike synthetic batch tests, this run demonstrated:
1. **Real Agent Telemetry Callback:** Custom LangChain tool invocations emitting structured JSON telemetry directly to Kafka topic `agent-events`.
2. **Deterministic Dual-Stream Routing:** PySpark Streaming Gatekeeper evaluating incoming events via Catalyst JVM expressions against `configs/agent_contract.yaml`.
3. **100% Ingestion Guarantee:** Compliant actions landed in the Bronze Delta Lake table; non-compliant actions were safely isolated in Quarantine Delta Lake without interrupting the streaming pipeline.

---

## 2. Test Execution Matrix

| Step | Tool / Action | Arguments & Metrics | Expected Outcome | Actual Gatekeeper Routing |
|:---|:---|:---|:---|:---|
| **Task 1** | `sql_query_executor` | `{"query": "SELECT * FROM enterprise_customers LIMIT 5;"}`, cost: `$0.042` | **Compliant** | ✅ **Committed to Bronze Delta Lake** |
| **Task 2** | `web_scraper` | `{"url": "https://internal.kb/refund-policy"}`, cost: `$0.015` | **Compliant** | ✅ **Committed to Bronze Delta Lake** |
| **Task 3** | `unauthorized_shell_exec` | `{"command": "cat /etc/passwd"}`, cost: `$0.005` | **Contract Breach** (`tool_not_allowed`) | 🛡️ **Routed to Quarantine Delta Lake** |
| **Task 4** | `expensive_financial_calc` | `{"iterations": 10000000}`, cost: `$154.20` (> `$50.00` limit) | **Contract Breach** (`cost_out_of_bounds`) | 🛡️ **Routed to Quarantine Delta Lake** |

---

## 3. Verbatim Execution Log

```text
=====================================================================
  AGENTIC DELTA GUARD -- REAL AGENT END-TO-END PIPELINE DEMO
  Agent ID    : support_bot_enterprise
  Session ID  : sess_20261018_133728
  Kafka Socket: 127.0.0.1:9092 (topic: agent-events)
=====================================================================

[1/4] Connecting LangChain telemetry producer to Kafka (127.0.0.1:9092)...

--> [Task 1/4] Agent executing 'sql_query_executor' (Compliant Action)...
    [Result] Tool output: {"status": "success", "rows": [{"cust_id": "cust_101", "name": "Acme Corp"}, {"cust_id": "cust_102", "name": "Globex Inc"}]}
    [Telemetry] Emitted compliant event -> Expecting Bronze Delta Lake commit.

--> [Task 2/4] Agent executing 'web_scraper' (Compliant Action)...
    [Result] Tool output: Refund Policy v3.2: Enterprise customers are eligible for 100% SLA refunds within 30 days.
    [Telemetry] Emitted compliant event -> Expecting Bronze Delta Lake commit.

--> [Task 3/4] Agent executing 'unauthorized_shell_exec' (Poison / Disallowed Tool)...
    [Result] Tool output: [WARN] Executed command: cat /etc/passwd (System root access requested)
    [Telemetry] Emitted disallowed tool event -> Expecting Quarantine routing!

--> [Task 4/4] Agent executing 'expensive_financial_calc' (Poison / Cost Spike)...
    [Result] Tool output: Financial forecast simulation complete: NPV = $14,230,000, Monte Carlo trials = 10,000,000.
    [Telemetry] Emitted cost-spike event ($154.20) -> Expecting Quarantine routing!

[2/4] Flushing telemetry events to Kafka...
      Flushed 4 events successfully in 0.05 seconds.

[3/4] Awaiting PySpark Gatekeeper micro-batch trigger (5s)...

[4/4] Auditing Lakehouse Storage (DuckDB over Delta)...
---------------------------------------------------------------------
  [DELTA AUDIT] Table Breakdown for Session sess_20261018_133728:
  - Bronze Delta Table     : 2 event(s) committed
  - Quarantine Delta Table : 2 event(s) quarantined
---------------------------------------------------------------------

  [BRONZE EVENTS - Validated & Clean]
  * Action: a8cefb58-2936-4767-89b5-4b36d0139b8f | Tool: sql_query_executor | Cost: $0.0420 | Status: SUCCESS
  * Action: be3d47ad-ee59-4d3f-bfe1-9659b85c17d7 | Tool: web_scraper | Cost: $0.0150 | Status: SUCCESS

  [QUARANTINE EVENTS - Blocked Violations]
  * Action: daa6bda4-1064-44df-be97-5a5078a994ee | Tool: unauthorized_shell_exec | Reason: semantic_rule:tool_not_allowed | Cost: $0.0050
  * Action: 5cffbfd2-06b4-4b53-8321-7b72db578644 | Tool: expensive_financial_calc | Reason: semantic_rule:cost_out_of_bounds | Cost: $154.2000

=====================================================================
  [SUCCESS] All 4 real agent tasks processed correctly by Gatekeeper!
=====================================================================
```

---

## 4. Lakehouse Physical Storage Verification

Direct inspection of `data/bronze/agent_events` and `data/quarantine/agent_events` confirms:

1. **Parquet Files Written:**
   - Both tables received atomic micro-batch commits from Spark's `org.apache.spark.sql.delta.sources.DeltaSink`.
2. **Schema Integrity:**
   - Bronze contains only validated columns (`agent_id`, `session_id`, `action_id`, `tool_name`, `execution_time_ms`, `cost_usd`, `status`, `tool_args`).
   - Quarantine contains the violation lineage (`quarantine_reason`, `raw_payload`, `received_at`), allowing triage tools and LLMs to inspect the root cause.
3. **Downstream Queryability:**
   - Gold analytics layer (DuckDB + dbt) can safely query Bronze tables with zero poison pollution.

---

## 5. How to Re-Run This Demonstration

To reproduce this real agent workflow on any machine:

```powershell
# 1. Start the Kafka broker
docker compose up -d kafka

# 2. Start the Gatekeeper in Terminal 1
.\run_gatekeeper.ps1

# 3. Run the Custom Agent in Terminal 2
.\.venv\Scripts\python.exe examples/run_custom_agent.py
```
