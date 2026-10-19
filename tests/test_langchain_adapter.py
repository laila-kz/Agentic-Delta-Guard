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
    _calculate_model_cost,
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
    session_id: Optional[str] = None,
) -> tuple[LangChainEventCallback, _FakeKafkaProducer]:
    fake_producer = _FakeKafkaProducer()
    callback = LangChainEventCallback(
        agent_id=agent_id,
        kafka_producer=fake_producer,
        tool_name_mapping=tool_name_mapping,
        strict=strict,
        session_id=session_id,
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


def test_session_id_explicit_and_metadata_propagation():
    """Explicit session_id and metadata['session_id'] propagate deterministically."""
    callback, fake_producer = _make_callback(
        tool_name_mapping={"dummy": "web_scraper"},
        session_id="explicit_session_123",
    )

    @tool
    def dummy(q: str) -> str:
        """Dummy docstring."""
        return "ok"

    dummy.invoke({"q": "test"}, config={"callbacks": [callback]})
    assert len(fake_producer.events_sent) == 1
    assert fake_producer.events_sent[0]["session_id"] == "explicit_session_123"

    # Also test chain metadata propagation
    callback2, fake_producer2 = _make_callback(
        tool_name_mapping={"dummy": "web_scraper"},
    )
    chain = RunnableLambda(lambda inp: dummy.invoke({"q": "test"}, config=inp.get("config")))
    chain.invoke(
        {"config": {"callbacks": [callback2]}},
        config={"callbacks": [callback2], "metadata": {"session_id": "meta_sess_456"}},
    )
    assert len(fake_producer2.events_sent) == 1
    assert fake_producer2.events_sent[0]["session_id"] == "meta_sess_456"


# ---------------------------------------------------------------------------
# Test 10: Producer queue drop counter tracks backpressure
# ---------------------------------------------------------------------------

def test_producer_queue_drop_counter():
    """Producer records dropped events when queue fills up."""
    producer = LangChainKafkaProducer(queue_maxsize=2)
    # Patch _get_producer to avoid real Kafka attempts during unit test
    producer._get_producer = lambda: None

    for i in range(10):
        producer.send({"agent_id": "test", "event_num": i})

    assert producer.sent_count + producer.dropped_count == 10
    assert producer.dropped_count > 0, "Expected events to be dropped with maxsize=2"


# ---------------------------------------------------------------------------
# Test 11: Per-model token pricing calculation
# ---------------------------------------------------------------------------

def test_model_specific_pricing():
    """Calculates cost based on model input/output rates per million tokens."""
    # gpt-4o-mini: input 0.15 / 1M, output 0.60 / 1M
    cost_mini = _calculate_model_cost("gpt-4o-mini", prompt_tokens=1_000_000, completion_tokens=1_000_000, total_tokens=2_000_000)
    assert abs(cost_mini - 0.75) < 1e-4

    # Default/unknown model: fallback per token ($0.00001)
    cost_default = _calculate_model_cost("unknown-llm", prompt_tokens=500, completion_tokens=500, total_tokens=1000)
    assert abs(cost_default - 0.01) < 1e-4


# ---------------------------------------------------------------------------
# Test 12: on_llm_end captures model usage metadata and attributes cost
# ---------------------------------------------------------------------------

def test_on_llm_end_model_cost_attribution():
    """Callback extracts usage_metadata and calculates attributed cost."""
    callback, fake_producer = _make_callback()

    class _MockResponse:
        def __init__(self):
            self.llm_output = {
                "model_name": "gpt-4o-mini",
                "token_usage": {"prompt_tokens": 1000, "completion_tokens": 500, "total_tokens": 1500},
            }

    import uuid
    callback.on_llm_end(_MockResponse(), run_id=uuid.uuid4())

    assert len(fake_producer.events_sent) == 1
    event = fake_producer.events_sent[0]
    assert event["tool_name"] == "vector_search"
    # 1000 * (0.15/1M) + 500 * (0.60/1M) = 0.00015 + 0.00030 = 0.00045
    assert abs(event["cost_usd"] - 0.00045) < 1e-5
    args = json.loads(event["tool_args"])
    assert args["model"] == "gpt-4o-mini"
    assert args["prompt_tokens"] == 1000


# =============================================================================
# Fix 10 — Unit tests: secret redaction and cost calculation correctness
# =============================================================================

from delta_guard.langchain_adapter import _redact_sensitive_data  # noqa: E402


class TestSecretRedaction:
    """Verify that _redact_sensitive_data masks secrets and preserves safe values.

    Note: _redact_sensitive_data operates on raw strings (JSON-serialized payloads).
    """

    # ── OpenAI / generic API keys ───────────────────────────────────────────

    def test_openai_api_key_is_redacted(self):
        payload = '{"api_key": "sk-abc1234567890abcdef"}'
        result = _redact_sensitive_data(payload)
        assert "sk-abc1234567890abcdef" not in result, "API key must be redacted"
        assert "REDACTED" in result

    def test_sk_key_pattern_redacted(self):
        payload = "Authorization header: sk-1234567890abcdefghij"
        result = _redact_sensitive_data(payload)
        assert "sk-1234567890abcdefghij" not in result
        assert "REDACTED" in result

    # ── Bearer tokens ───────────────────────────────────────────────────────

    def test_bearer_token_in_authorization_header(self):
        payload = '{"Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig"}'
        result = _redact_sensitive_data(payload)
        assert "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9" not in result, "Bearer token payload must be redacted"
        assert "REDACTED" in result

    # ── Passwords in JSON-style payloads ─────────────────────────────────────

    def test_password_field_is_redacted_in_json(self):
        payload = '{"password": "supersecret123"}'
        result = _redact_sensitive_data(payload)
        assert "supersecret123" not in result, "password field must be redacted"
        assert "REDACTED" in result

    def test_secret_field_is_redacted_in_json(self):
        payload = '{"secret": "topsecretvalue"}'
        result = _redact_sensitive_data(payload)
        assert "topsecretvalue" not in result
        assert "REDACTED" in result

    def test_access_token_is_redacted_in_json(self):
        payload = '{"access_token": "ghp_someGithubToken123"}'
        result = _redact_sensitive_data(payload)
        assert "ghp_someGithubToken123" not in result
        assert "REDACTED" in result

    # ── PII — email addresses ────────────────────────────────────────────────

    def test_email_address_is_redacted(self):
        payload = '{"user": "alice@example.com"}'
        result = _redact_sensitive_data(payload)
        assert "alice@example.com" not in result, "Email address must be redacted"
        assert "REDACTED_EMAIL" in result

    # ── Safe values are preserved ────────────────────────────────────────────

    def test_ordinary_sql_query_preserved(self):
        payload = '{"query": "SELECT * FROM customers", "limit": 10}'
        result = _redact_sensitive_data(payload)
        assert "SELECT * FROM customers" in result
        assert "limit" in result

    def test_url_without_credentials_preserved(self):
        payload = '{"url": "https://example.com/api/data", "topic": "refund_policy"}'
        result = _redact_sensitive_data(payload)
        assert "https://example.com/api/data" in result
        assert "refund_policy" in result

    def test_empty_string_returns_empty(self):
        assert _redact_sensitive_data("") == ""

    def test_none_returns_none(self):
        """None input should be returned as-is without crashing."""
        # _redact_sensitive_data returns early on falsy input
        result = _redact_sensitive_data(None)  # type: ignore[arg-type]
        assert result is None

    def test_plain_text_no_secrets_preserved(self):
        payload = "The agent completed the task successfully with 3 results."
        result = _redact_sensitive_data(payload)
        assert result == payload


class TestCostCalculationCorrectness:
    """Verify _calculate_model_cost propagates real costs, does not clamp, and
    returns sentinel values when token usage is missing."""

    def test_gpt4o_mini_cost(self):
        cost = _calculate_model_cost("gpt-4o-mini", prompt_tokens=1000, completion_tokens=500, total_tokens=1500)
        # gpt-4o-mini: $0.15/1M input + $0.60/1M output
        expected = (1000 / 1_000_000) * 0.15 + (500 / 1_000_000) * 0.60
        assert abs(cost - expected) < 1e-5, f"Expected ~{expected}, got {cost}"

    def test_gpt4o_cost(self):
        cost = _calculate_model_cost("gpt-4o", prompt_tokens=2000, completion_tokens=800, total_tokens=2800)
        expected = (2000 / 1_000_000) * 2.50 + (800 / 1_000_000) * 10.00
        assert abs(cost - expected) < 1e-5, f"Expected ~{expected}, got {cost}"

    def test_large_token_count_is_not_clamped(self):
        """No $50 cap: a very expensive call must propagate its real cost."""
        cost = _calculate_model_cost("gpt-4o", prompt_tokens=10_000_000, completion_tokens=5_000_000, total_tokens=15_000_000)
        assert cost > 50.0, "Cost should exceed $50 without artificial clamping"

    def test_zero_tokens_returns_zero(self):
        cost = _calculate_model_cost("gpt-4o-mini", prompt_tokens=0, completion_tokens=0, total_tokens=0)
        assert cost == 0.0

    def test_unknown_model_uses_fallback_rate(self):
        """An unrecognised model uses the default per-token fallback, not $0.00."""
        cost = _calculate_model_cost("some-unknown-future-model", prompt_tokens=500, completion_tokens=200, total_tokens=700)
        # Should use fallback_per_token rate: positive and non-zero for positive tokens
        assert cost > 0.0, f"Unknown model must use fallback rate, got {cost}"

    def test_cost_is_deterministic(self):
        """Same inputs must always produce the same output."""
        cost1 = _calculate_model_cost("gpt-4o-mini", prompt_tokens=300, completion_tokens=100, total_tokens=400)
        cost2 = _calculate_model_cost("gpt-4o-mini", prompt_tokens=300, completion_tokens=100, total_tokens=400)
        assert cost1 == cost2

    def test_cost_is_non_negative_for_valid_tokens(self):
        """Positive token counts must yield non-negative cost."""
        cost = _calculate_model_cost("gpt-4o-mini", prompt_tokens=100, completion_tokens=50, total_tokens=150)
        assert cost >= 0.0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
