"""
tests/test_langchain_adapter.py -- Unit tests for the LangChain callback adapter.

All tests execute REAL LangChain tools, LLMs, or chains, verifying that
LangChain itself invokes the callback handler automatically.
All tests run without Kafka, LLM API keys, or network access.
"""
from __future__ import annotations

import json
import queue
import time
from typing import Any, Dict, Optional
from unittest.mock import patch

import pytest
from langchain_core.callbacks import BaseCallbackHandler
from langchain_core.language_models.fake import FakeListLLM
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from delta_guard.langchain_adapter import (
    CONTRACT_ALLOWED_TOOLS,
    LangChainEventCallback,
    LangChainKafkaProducer,
)
from delta_guard.benchmark import _validate_event


# ---------------------------------------------------------------------------
# Fixtures & Helpers
# ---------------------------------------------------------------------------

class _FakeKafkaProducer(LangChainKafkaProducer):
    """In-memory stand-in that records sent events instead of contacting Kafka."""

    def __init__(self) -> None:
        self._queue: queue.Queue = queue.Queue()
        self._producer = None
        self._thread = None
        self.events_sent: list = []

    def send(self, event: Dict[str, Any]) -> None:
        self.events_sent.append(event)

    def flush(self, timeout: float = 5.0) -> None:
        pass

    def close(self) -> None:
        pass


def _make_callback(
    tool_name_mapping: Optional[Dict[str, str]] = None,
    strict: bool = True,
    agent_id: str = "test_agent",
) -> tuple[LangChainEventCallback, _FakeKafkaProducer]:
    fake_producer = _FakeKafkaProducer()
    callback = LangChainEventCallback(
        agent_id=agent_id,
        kafka_producer=fake_producer,
        tool_name_mapping=tool_name_mapping,
        strict=strict,
    )
    return callback, fake_producer


# ---------------------------------------------------------------------------
# Test 1: LangChain invokes tool callback -> emits valid contract event
# ---------------------------------------------------------------------------

def test_adapter_emits_valid_contract_event():
    """LangChain tool invocation triggers callback & emits event passing _validate_event."""
    callback, fake_producer = _make_callback(
        tool_name_mapping={"web_search": "web_scraper"},
    )

    @tool
    def web_search(query: str) -> str:
        """Search tool."""
        time.sleep(0.01)
        return f"results for {query}"

    # LangChain itself executes the tool and invokes callback methods
    res = web_search.invoke({"query": "hello world"}, config={"callbacks": [callback]})
    assert res == "results for hello world"

    assert len(fake_producer.events_sent) == 1
    event = fake_producer.events_sent[0]

    # Check 9 canonical fields
    required_fields = {
        "agent_id", "session_id", "action_id", "timestamp",
        "tool_name", "execution_time_ms", "cost_usd", "status", "tool_args",
    }
    assert required_fields.issubset(event.keys()), f"Missing fields: {required_fields - set(event.keys())}"
    assert event["tool_name"] == "web_scraper"
    assert event["status"] == "SUCCESS"

    # Contract validation check
    is_valid, error = _validate_event(event)
    assert is_valid, f"Event failed contract validation: {error}\nEvent: {event}"


# ---------------------------------------------------------------------------
# Test 2: Tool name mapping via LangChain invocation
# ---------------------------------------------------------------------------

def test_adapter_maps_tool_name_via_mapping():
    """LangChain tool with mapped name translates to contract allowed tool."""
    callback, fake_producer = _make_callback(
        tool_name_mapping={"my_sql_tool": "sql_query_executor"},
    )

    @tool
    def my_sql_tool(query: str) -> str:
        """Executes SQL."""
        return "table data"

    my_sql_tool.invoke({"query": "SELECT * FROM users"}, config={"callbacks": [callback]})

    assert len(fake_producer.events_sent) == 1
    event = fake_producer.events_sent[0]
    assert event["tool_name"] == "sql_query_executor"
    assert event["tool_name"] in CONTRACT_ALLOWED_TOOLS


# ---------------------------------------------------------------------------
# Test 3: Unmapped tool (gatekeeper quarantine path)
# ---------------------------------------------------------------------------

def test_adapter_quarantines_unknown_tool():
    """Unmapped tool emits original name so gatekeeper quarantines it."""
    callback, fake_producer = _make_callback(strict=False)

    @tool
    def calculator(expression: str) -> str:
        """Math tool."""
        return "42"

    calculator.invoke({"expression": "6 * 7"}, config={"callbacks": [callback]})

    assert len(fake_producer.events_sent) == 1
    event = fake_producer.events_sent[0]
    assert event["tool_name"] == "calculator"
    assert event["tool_name"] not in CONTRACT_ALLOWED_TOOLS

    required = {"agent_id", "session_id", "action_id", "timestamp",
                "tool_name", "execution_time_ms", "cost_usd", "status", "tool_args"}
    assert required.issubset(event.keys())


# ---------------------------------------------------------------------------
# Test 4: Kafka down does NOT block LangChain execution
# ---------------------------------------------------------------------------

def test_adapter_does_not_block_on_kafka_down():
    """LangChain execution completes fast even when Kafka is unavailable."""
    with patch(
        "delta_guard.langchain_adapter.LangChainKafkaProducer._get_producer",
        return_value=None,
    ):
        real_kp = LangChainKafkaProducer(bootstrap_servers="bad_host:9999", queue_maxsize=10)
        callback = LangChainEventCallback(
            agent_id="test_agent",
            kafka_producer=real_kp,
        )

        @tool
        def fast_tool(x: str) -> str:
            """Fast tool docstring."""
            return x

        start = time.perf_counter()
        fast_tool.invoke({"x": "hello"}, config={"callbacks": [callback]})
        elapsed = time.perf_counter() - start

        assert elapsed < 1.0, f"send() blocked for {elapsed:.2f}s -- must not block agent"
        real_kp.close()


# ---------------------------------------------------------------------------
# Test 5: session_id is stable across multiple LangChain tools in a chain
# ---------------------------------------------------------------------------

def test_session_id_stable_across_tool_calls():
    """Multiple tools executed within a LangChain Runnable share session_id."""
    callback, fake_producer = _make_callback(
        tool_name_mapping={"search_tool": "web_scraper", "sql_tool": "sql_query_executor"},
    )

    @tool
    def search_tool(query: str) -> str:
        """Search tool docstring."""
        return "search res"

    @tool
    def sql_tool(query: str) -> str:
        """SQL tool docstring."""
        return "sql res"

    def chain_logic(inputs: dict) -> dict:
        cfg = inputs.get("config")
        r1 = search_tool.invoke({"query": "q1"}, config=cfg)
        r2 = sql_tool.invoke({"query": "q2"}, config=cfg)
        return {"r1": r1, "r2": r2}

    chain = RunnableLambda(chain_logic)
    chain.invoke({"config": {"callbacks": [callback]}}, config={"callbacks": [callback]})

    assert len(fake_producer.events_sent) == 2
    session_ids = {e["session_id"] for e in fake_producer.events_sent}
    assert len(session_ids) == 1, f"Expected 1 session_id, got: {session_ids}"


# ---------------------------------------------------------------------------
# Test 6: action_id uses LangChain run_id
# ---------------------------------------------------------------------------

def test_action_id_uses_langchain_run_id():
    """action_id matches the run_id created by LangChain."""
    callback, fake_producer = _make_callback(
        tool_name_mapping={"db_tool": "db_writer"},
    )

    @tool
    def db_tool(data: str) -> str:
        """DB tool docstring."""
        return "saved"

    db_tool.invoke({"data": "payload"}, config={"callbacks": [callback]})

    assert len(fake_producer.events_sent) == 1
    event = fake_producer.events_sent[0]
    assert len(event["action_id"]) > 0
    # Ensure action_id is a valid UUID string
    import uuid
    uuid_obj = uuid.UUID(event["action_id"])
    assert str(uuid_obj) == event["action_id"]


# ---------------------------------------------------------------------------
# Test 7: Tool error handling via LangChain
# ---------------------------------------------------------------------------

def test_adapter_captures_tool_error():
    """When a tool raises an error, LangChain invokes on_tool_error and event status is ERROR."""
    callback, fake_producer = _make_callback(
        tool_name_mapping={"buggy_tool": "vector_search"},
    )

    @tool
    def buggy_tool(param: str) -> str:
        """Buggy tool docstring."""
        raise ValueError("database connection lost")

    with pytest.raises(ValueError, match="database connection lost"):
        buggy_tool.invoke({"param": "test"}, config={"callbacks": [callback]})

    assert len(fake_producer.events_sent) == 1
    event = fake_producer.events_sent[0]
    assert event["status"] == "ERROR"
    assert "database connection lost" in event["tool_args"]


# ---------------------------------------------------------------------------
# Test 8: Callback inheritance check
# ---------------------------------------------------------------------------

def test_adapter_is_base_callback_handler_subclass():
    """LangChainEventCallback is a subclass of BaseCallbackHandler."""
    callback, _ = _make_callback()
    assert isinstance(callback, BaseCallbackHandler)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
