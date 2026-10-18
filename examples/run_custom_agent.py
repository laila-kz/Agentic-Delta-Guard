"""
examples/run_custom_agent.py - Real Autonomous Agent Production Integration
=============================================================================

Demonstrates how to wire ANY real LangChain agent into the Agentic Delta Guard
streaming lakehouse pipeline:

    [Autonomous Agent]
           |
     (1) Shift-Left Pre-Flight Check (MCP / Contract Validator)
           |
     (2) Agent executes tools via LangChain callbacks
           |
     (3) LangChainKafkaProducer streams events to Kafka (topic: agent-events)
           |
     (4) PySpark Gatekeeper validates & routes:
           - Compliant records  --> Bronze Delta Lake (ACID append/merge)
           - Violations/Poisons --> Quarantine Delta Lake (zero-stall isolation)
           |
     (5) Instant local verification via DuckDB delta_scan

Usage:
    # 1. Start Kafka (if not already running):
    docker compose up -d kafka

    # 2. Start the PySpark Gatekeeper in Terminal 1:
    .\run_gatekeeper.ps1

    # 3. Run this script in Terminal 2:
    python examples/run_custom_agent.py

Options:
    python examples/run_custom_agent.py --inspect-only   # Query current lakehouse tables without emitting
    python examples/run_custom_agent.py --no-kafka       # Dry-run without publishing to Kafka
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

# Ensure src/ and project root are importable
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from langchain_core.tools import tool

from delta_guard.langchain_adapter import (
    LangChainEventCallback,
    LangChainKafkaProducer,
)
from delta_guard.mcp.validators.contract_validator import ContractValidator

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("custom_agent")

BANNER = """
=====================================================================
  AGENTIC DELTA GUARD -- REAL AGENT END-TO-END PIPELINE DEMO
  Agent ID    : support_bot_enterprise
  Session ID  : {session_id}
  Kafka Socket: localhost:9092 (topic: agent-events)
=====================================================================
"""


# ---------------------------------------------------------------------------
# 1. Define Real LangChain Tools
# ---------------------------------------------------------------------------

@tool
def sql_query_executor(query: str) -> str:
    """Execute a database query against customer and order records."""
    time.sleep(0.04)  # simulate database latency
    return json.dumps({
        "status": "success",
        "rows": [{"cust_id": "cust_101", "name": "Alice Corp", "balance_usd": 14250.00, "status": "active"}]
    })


@tool
def web_scraper(url: str) -> str:
    """Fetch return, refund, or service-level agreements from web documentation."""
    time.sleep(0.06)  # simulate HTTP network fetch
    return "Refund Policy v3.2: Enterprise customers are eligible for 100% SLA credits upon outage verification."


@tool
def unauthorized_shell_exec(command: str) -> str:
    """Administrative shell executor (UNAUTHORIZED by data contract)."""
    time.sleep(0.02)
    return "root:x:0:0:root:/root:/bin/bash"


@tool
def expensive_financial_calc(formula: str) -> str:
    """High-compute financial risk simulation tool (breaches contract cost ceiling)."""
    time.sleep(0.05)
    return "Risk Value at Risk (VaR 99%): $4,200,000"


# ---------------------------------------------------------------------------
# 2. Main Workflow: Pre-Flight Check -> Execution -> Lakehouse Verification
# ---------------------------------------------------------------------------

class _CollectingKafkaProducer(LangChainKafkaProducer):
    """Captures events into memory when running with --no-kafka."""
    def __init__(self) -> None:
        self.events: list[dict[str, Any]] = []
        self._sent_events_count = 0
        self._dropped_events_count = 0

    def send(self, event: dict[str, Any]) -> None:
        self.events.append(event)
        self._sent_events_count += 1

    def flush(self, timeout: float = 5.0) -> None:
        pass

    def close(self) -> None:
        pass


def run_agent_workflow(publish_to_kafka: bool = True) -> None:
    session_id = f"sess_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    print(BANNER.format(session_id=session_id))

    # Initialize telemetry producer
    if publish_to_kafka:
        print("[1/4] Connecting LangChain telemetry producer to Kafka (localhost:9092)...")
        producer = LangChainKafkaProducer(bootstrap_servers="localhost:9092", topic="agent-events")
    else:
        print("[1/4] Running in dry-run mode (--no-kafka active)...")
        producer = _CollectingKafkaProducer()

    callback = LangChainEventCallback(
        agent_id="support_bot_enterprise",
        kafka_producer=producer,
        session_id=session_id,
    )

    contract_path = PROJECT_ROOT / "configs" / "agent_contract.yaml"

    try:
        # -------------------------------------------------------------------
        # Action 1: Valid SQL Query (Expected -> Bronze Delta Lake)
        # -------------------------------------------------------------------
        print("\n--> [Task 1/4] Agent executing 'sql_query_executor' (Compliant Action)...")
        act1_id = f"act_{uuid.uuid4().hex[:8]}"
        res1 = sql_query_executor.invoke(
            {"query": "SELECT balance_usd FROM customers WHERE cust_id = 'cust_101'"},
            config={"callbacks": [callback], "run_name": "lookup_customer_balance"},
        )
        print(f"    [Result] Tool output: {res1[:65]}...")
        print("    [Telemetry] Emitted compliant event -> Expecting Bronze Delta Lake commit.")

        # -------------------------------------------------------------------
        # Action 2: Valid Web Scraper (Expected -> Bronze Delta Lake)
        # -------------------------------------------------------------------
        print("\n--> [Task 2/4] Agent executing 'web_scraper' (Compliant Action)...")
        act2_id = f"act_{uuid.uuid4().hex[:8]}"
        res2 = web_scraper.invoke(
            {"url": "https://internal.corp/policies/refunds"},
            config={"callbacks": [callback], "run_name": "fetch_refund_sla"},
        )
        print(f"    [Result] Tool output: {res2[:65]}...")
        print("    [Telemetry] Emitted compliant event -> Expecting Bronze Delta Lake commit.")

        # -------------------------------------------------------------------
        # Action 3: Shift-Left MCP Pre-Flight Verification on Rogue Tool
        # -------------------------------------------------------------------
        print("\n--> [Task 3/4] Agent attempting 'unauthorized_shell_exec' (Rogue Tool Call)...")
        rogue_payload = {
            "agent_id": "support_bot_enterprise",
            "session_id": session_id,
            "action_id": f"act_{uuid.uuid4().hex[:8]}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "unauthorized_shell_exec",
            "execution_time_ms": 25,
            "cost_usd": 0.05,
            "status": "SUCCESS",
            "tool_args": {"command": "cat /etc/passwd"},
        }

        # Shift-Left Advisory Pre-Flight Check (MCP Layer)
        validator = ContractValidator(contract_path)
        preflight_result = validator.validate(rogue_payload)
        print(f"    [Pre-Flight MCP Check] Allowed: {preflight_result['allowed']}")
        if not preflight_result["allowed"]:
            print(f"    [Pre-Flight MCP Warnings] Violations caught before commit: {preflight_result['violations']}")

        # Still execute tool through LangChain to demonstrate Gatekeeper Quarantine isolation
        res3 = unauthorized_shell_exec.invoke(
            {"command": "cat /etc/passwd"},
            config={"callbacks": [callback], "run_name": "attempt_unauthorized_shell"},
        )
        print(f"    [Result] Tool executed on host: {res3}")
        print("    [Telemetry] Ingestion Gatekeeper will route this event to Quarantine Delta Lake.")

        # -------------------------------------------------------------------
        # Action 4: Runaway Cost Spike (Breaches $50 Ceiling)
        # -------------------------------------------------------------------
        print("\n--> [Task 4/4] Agent executing tool with budget spike ($120.00 > $50.00 ceiling)...")
        # Direct emission simulating runaway LLM token cost spike
        cost_spike_event = {
            "agent_id": "support_bot_enterprise",
            "session_id": session_id,
            "action_id": f"act_{uuid.uuid4().hex[:8]}",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tool_name": "expensive_financial_calc",
            "execution_time_ms": 520,
            "cost_usd": 120.00,  # Exceeds $50 ceiling
            "status": "SUCCESS",
            "tool_args": json.dumps({"formula": "monte_carlo_var_99"}),
        }
        if producer:
            producer.send(cost_spike_event)
        print("    [Telemetry] Emitted cost violation ($120.00) -> Expecting Quarantine isolation.")

    finally:
        if producer:
            print("\n[3/4] Flushing background telemetry queue to Kafka broker...")
            producer.flush(timeout=5.0)
            producer.close()
            print("      [OK] Telemetry queue flushed cleanly.")

    # -----------------------------------------------------------------------
    # Step 5: Instant Lakehouse Storage Verification via DuckDB
    # -----------------------------------------------------------------------
    print("\n[4/4] Verifying Lakehouse Delta tables via DuckDB...")
    inspect_lakehouse(session_id)


def inspect_lakehouse(session_id: str | None = None) -> None:
    """Queries Bronze and Quarantine Delta Lake tables directly without Spark cluster."""
    try:
        import duckdb
    except ImportError:
        print("      [WARN] duckdb not installed. Install duckdb to run instant inspection.")
        return

    bronze_path = (PROJECT_ROOT / "data" / "bronze" / "agent_events").as_posix()
    quarantine_path = (PROJECT_ROOT / "data" / "quarantine" / "agent_events").as_posix()

    conn = duckdb.connect(":memory:")

    print("\n" + "=" * 68)
    print("  LAKEHOUSE STORAGE AUDIT (LIVE DELTA TABLES)")
    print("=" * 68)

    # 1. Inspect Bronze Table
    try:
        bronze_query = f"""
            SELECT agent_id, session_id, action_id, tool_name, cost_usd, status
            FROM read_parquet('{bronze_path}/**/*.parquet', union_by_name=true)
            WHERE agent_id = 'support_bot_enterprise'
            ORDER BY timestamp DESC
            LIMIT 5
        """
        bronze_df = conn.execute(bronze_query).df()
        print(f"\n[BRONZE DELTA LAKE] Compliant Agent Actions (Total sampled: {len(bronze_df)}):")
        if not bronze_df.empty:
            print(bronze_df.to_string(index=False))
        else:
            print("  (Waiting for PySpark Gatekeeper micro-batch commit...)")
    except Exception as exc:
        print(f"  [Bronze] No records yet or table initializing: {exc}")

    # 2. Inspect Quarantine Table
    try:
        quar_query = f"""
            SELECT agent_id, tool_name, cost_usd, error_summary, quarantined_at
            FROM read_parquet('{quarantine_path}/**/*.parquet', union_by_name=true)
            WHERE agent_id = 'support_bot_enterprise'
            ORDER BY quarantined_at DESC
            LIMIT 5
        """
        quar_df = conn.execute(quar_query).df()
        print(f"\n[QUARANTINE DELTA LAKE] Intercepted Rogue Actions (Total sampled: {len(quar_df)}):")
        if not quar_df.empty:
            print(quar_df.to_string(index=False))
        else:
            print("  (Waiting for PySpark Gatekeeper micro-batch commit...)")
    except Exception as exc:
        print(f"  [Quarantine] No records yet or table initializing: {exc}")

    print("=" * 68 + "\n")
    conn.close()


def main():
    parser = argparse.ArgumentParser(description="Real Agent Production Pipeline Integration Demo")
    parser.add_argument("--no-kafka", action="store_true", help="Run without sending events to Kafka broker")
    parser.add_argument("--inspect-only", action="store_true", help="Inspect live Delta Lake tables without executing tasks")
    args = parser.parse_args()

    if args.inspect_only:
        inspect_lakehouse()
    else:
        run_agent_workflow(publish_to_kafka=not args.no_kafka)


if __name__ == "__main__":
    main()
