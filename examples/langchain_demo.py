"""
examples/langchain_demo.py -- LangChain Adapter Demo for Agentic Delta Guard.

Demonstrates the LangChain callback adapter publishing real agent events to
the same Kafka topic consumed by the PySpark gatekeeper.

LangChain itself invokes the callback handler during tool and chain execution.

Two tools are defined:
  * web_scraper  (contract-allowed -> Bronze)
  * calculator   (not allowed     -> Quarantine via semantic_rule:tool_not_allowed)

Run:
    python examples/langchain_demo.py
"""
from __future__ import annotations

import json
import queue
import sys
import time
from pathlib import Path
from typing import Any, Dict, List

from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool

# Ensure src/ is importable when run from the project root
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from delta_guard.langchain_adapter import (
    CONTRACT_ALLOWED_TOOLS,
    LangChainEventCallback,
    LangChainKafkaProducer,
)
from delta_guard.benchmark import _validate_event


# ---------------------------------------------------------------------------
# Minimal in-memory Kafka producer (no real Kafka broker needed for demo)
# ---------------------------------------------------------------------------

class _CollectingKafkaProducer(LangChainKafkaProducer):
    """Captures events into a list instead of publishing to Kafka."""

    def __init__(self) -> None:
        self._queue: queue.Queue = queue.Queue()
        self._producer = None
        self._thread = None
        self.events: List[Dict[str, Any]] = []

    def send(self, event: Dict[str, Any]) -> None:
        self.events.append(event)

    def flush(self, timeout: float = 5.0) -> None:
        pass

    def close(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Real LangChain tool definitions using @tool
# ---------------------------------------------------------------------------

@tool
def web_scraper(url: str) -> str:
    """Fetches web page content."""
    time.sleep(0.012)  # simulate network latency
    return f"<html>Mock page for {url}</html>"


@tool
def calculator(expression: str) -> str:
    """Evaluates a math expression."""
    time.sleep(0.005)
    try:
        return str(eval(expression, {"__builtins__": {}}, {}))  # noqa: S307
    except Exception as exc:
        return f"Error: {exc}"


# ---------------------------------------------------------------------------
# Real LangChain execution via Runnable chain
# ---------------------------------------------------------------------------

def run_agent_workflow(inputs: Dict[str, Any]) -> Dict[str, Any]:
    """
    Workflow function invoked inside a LangChain Runnable chain.
    Tool executions pass the LangChain config so callbacks propagate.
    """
    config = inputs.get("config")

    # Tool call 1: web_scraper (contract-allowed)
    res_1 = web_scraper.invoke({"url": "https://example.com"}, config=config)

    # Tool call 2: calculator (NOT in contract allowlist)
    res_2 = calculator.invoke({"expression": "2 + 2"}, config=config)

    return {"web_result": res_1, "calc_result": res_2}


# ---------------------------------------------------------------------------
# Routing analysis
# ---------------------------------------------------------------------------

def analyse_routing(events: List[Dict[str, Any]]) -> None:
    """Print a routing summary matching the gatekeeper's Bronze / Quarantine split."""
    bronze = []
    quarantine = []

    for event in events:
        is_valid, error = _validate_event(event)
        tool_allowed = event.get("tool_name") in CONTRACT_ALLOWED_TOOLS
        if is_valid and tool_allowed:
            bronze.append(event)
        else:
            quarantine.append((event, error or "semantic_rule:tool_not_allowed"))

    print("\n" + "=" * 62)
    print("  AGENTIC DELTA GUARD -- LangChain Adapter Demo")
    print("=" * 62)
    print(f"  Total events emitted : {len(events)}")
    print(f"  -> Bronze  (valid)   : {len(bronze)}")
    print(f"  -> Quarantine (bad)  : {len(quarantine)}")
    print()

    if bronze:
        print("  BRONZE events:")
        for e in bronze:
            print(f"    tool={e['tool_name']:20s}  status={e['status']}  ms={e['execution_time_ms']}")

    if quarantine:
        print()
        print("  QUARANTINE events:")
        for e, reason in quarantine:
            print(f"    tool={e['tool_name']:20s}  reason={reason}")

    print("=" * 62)
    print()
    print("  NOTE: LangChain itself invoked the callback handler during")
    print("  chain and tool execution. Events were produced automatically.")
    print("=" * 62 + "\n")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    collecting_kafka = _CollectingKafkaProducer()

    callback = LangChainEventCallback(
        agent_id="langchain_demo_agent",
        kafka_producer=collecting_kafka,
        tool_name_mapping={},  # calculator will be quarantined
        strict=False,
    )

    print("\n[demo] Executing real LangChain Runnable sequence with tools...")

    chain = RunnableLambda(run_agent_workflow)
    chain.invoke(
        {"config": {"callbacks": [callback]}},
        config={"callbacks": [callback]},
    )

    analyse_routing(collecting_kafka.events)


if __name__ == "__main__":
    main()
