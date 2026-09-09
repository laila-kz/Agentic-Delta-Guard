#!/usr/bin/env python
"""
test_validator.py
Unit tests and timing benchmarks for ContractValidator & InFlightContractValidator.
"""

import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from src.delta_guard.mcp.validators.contract_validator import (
    ContractValidator,
    InFlightContractValidator,
)


def test_validator():
    print("=" * 70)
    print("🛡️  TESTING CONTRACT VALIDATOR (SUB-5MS PRE-FLIGHT)")
    print("=" * 70)

    validator = ContractValidator()

    # Test 1: Valid payload
    print("\n[Test 1] Valid payload")
    valid_payload = {
        "agent_id": "agent_001",
        "session_id": "sess_100",
        "action_id": "act_200",
        "tool_name": "sql_query_executor",
        "execution_time_ms": 250,
        "cost_usd": 0.05,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool_args": {"query": "SELECT 1;"},
    }
    res = validator.validate(valid_payload)
    print(f"  Allowed: {res['allowed']}")
    print(f"  Violations: {res['violations']}")
    assert res["allowed"] is True

    # Test 2: Invalid payload (unauthorized tool & high cost)
    print("\n[Test 2] Unauthorized tool & excessive cost")
    invalid_payload = {
        "agent_id": "agent_001",
        "session_id": "sess_100",
        "action_id": "act_200",
        "tool_name": "arbitrary_code_exec",
        "execution_time_ms": 50,
        "cost_usd": 150.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    res = validator.validate(invalid_payload)
    print(f"  Allowed: {res['allowed']}")
    print(f"  Violations: {res['violations']}")
    print(f"  Hints: {res['remediation_hints']}")
    assert res["allowed"] is False
    assert any("UNAUTHORIZED_TOOL" in v for v in res["violations"])
    assert any("COST_LIMIT_BREACH" in v for v in res["violations"])

    # Test 3: Stale and future timestamps
    print("\n[Test 3] Stale timestamp (>24h ago)")
    stale_payload = {
        "agent_id": "agent_001",
        "session_id": "sess_100",
        "action_id": "act_200",
        "tool_name": "vector_search",
        "execution_time_ms": 100,
        "cost_usd": 0.01,
        "timestamp": "2020-01-01T00:00:00Z",
    }
    res = validator.validate(stale_payload)
    print(f"  Allowed: {res['allowed']}")
    print(f"  Violations: {res['violations']}")
    assert res["allowed"] is False
    assert any("STALE_TIMESTAMP" in v for v in res["violations"])

    # Test 4: Missing required fields
    print("\n[Test 4] Missing required fields")
    missing_payload = {
        "tool_name": "vector_search",
        "cost_usd": 0.01,
    }
    res = validator.validate(missing_payload)
    print(f"  Allowed: {res['allowed']}")
    print(f"  Violations: {res['violations']}")
    assert res["allowed"] is False
    assert any("MISSING_REQUIRED_FIELD" in v for v in res["violations"])

    # Test 5: Latency Benchmark (<5ms target)
    print("\n[Test 5] Latency Benchmark (1,000 iterations)")
    t0 = time.perf_counter()
    iterations = 1000
    for _ in range(iterations):
        validator.validate(valid_payload)
    total_ms = (time.perf_counter() - t0) * 1000
    avg_ms = total_ms / iterations
    print(f"  1,000 evaluations total : {total_ms:.2f} ms")
    print(f"  Average latency         : {avg_ms:.4f} ms per validation")
    assert avg_ms < 5.0, f"Latency target breach: {avg_ms:.4f}ms > 5.0ms"

    print("\n" + "=" * 70)
    print(f"✅ ALL VALIDATOR TESTS PASSED! Average Latency: {avg_ms:.4f} ms (< 5ms SLA)")
    print("=" * 70)


if __name__ == "__main__":
    test_validator()
