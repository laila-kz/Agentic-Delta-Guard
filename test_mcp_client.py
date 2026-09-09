#!/usr/bin/env python
"""
test_mcp_client.py
Direct test runner for Agentic Delta Guard MCP tools.
Verifies all MCP server endpoints without requiring an active stdio connection.
"""

from datetime import datetime, timezone
import json
import os
import sys
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

from src.delta_guard.mcp.server import (
    check_contract,
    get_active_contract,
    get_quarantine_summary,
    get_system_status,
    inspect_bronze_lakehouse,
    propose_contract_patch,
    run_health_check,
)


def test_mcp_tools():
    print("=" * 70)
    print("🛡️  TESTING AGENTIC DELTA GUARD MCP SERVER TOOLS")
    print("=" * 70)

    # Test 1: Check contract (Valid Payload)
    print("\n[1] check_contract (Valid Payload)")
    valid_payload = {
        "agent_id": "agent_001",
        "session_id": "sess_abc_123",
        "action_id": "act_def_456",
        "tool_name": "sql_query_executor",
        "execution_time_ms": 150,
        "cost_usd": 0.045,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool_args": {"query": "SELECT count(*) FROM users;"},
    }
    res_raw = check_contract(valid_payload)
    res = json.loads(res_raw)
    print(f"  Allowed     : {res.get('allowed')}")
    print(f"  Message     : {res.get('message')}")
    assert res.get("allowed") is True, f"Expected allowed=True, got {res}"

    # Test 2: Check contract (Invalid Payload - Unauthorized Tool & Cost Breach)
    print("\n[2] check_contract (Invalid Payload - Tool & Cost Breach)")
    invalid_payload = {
        "agent_id": "agent_002",
        "session_id": "sess_xyz_789",
        "action_id": "act_uvw_012",
        "tool_name": "admin_root_shell",
        "execution_time_ms": 90,
        "cost_usd": 999.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    res_raw = check_contract(invalid_payload)
    res = json.loads(res_raw)
    print(f"  Allowed     : {res.get('allowed')}")
    print(f"  Violations  : {res.get('violations')}")
    print(f"  Hints       : {res.get('remediation_hints')}")
    assert res.get("allowed") is False, f"Expected allowed=False, got {res}"
    assert len(res.get("violations", [])) >= 2

    # Test 3: Get Active Contract
    print("\n[3] get_active_contract()")
    res_raw = get_active_contract()
    res = json.loads(res_raw)
    print(f"  Version     : {res.get('contract_version')}")
    print(f"  Allowed Tools: {res.get('allowed_tools')}")
    print(f"  Max Cost    : ${res.get('max_cost_per_call')}")
    assert "contract" in res
    assert res.get("max_cost_per_call") == 50.0

    # Test 4: Operational System Status
    print("\n[4] get_system_status()")
    res_raw = get_system_status()
    res = json.loads(res_raw)
    print(f"  Overall Health : {res.get('overall_health')}")
    print(f"  Components     : {list(res.get('components', {}).keys())}")

    # Test 5: Comprehensive Health Check
    print("\n[5] run_health_check()")
    res_raw = run_health_check()
    res = json.loads(res_raw)
    print(f"  Status   : {res.get('status')}")
    print(f"  Passed   : {res.get('passed')}")
    print(f"  Failed   : {res.get('failed')}")
    for c in res.get("checks", []):
        print(f"    - {c.get('name')}: {c.get('status')} ({c.get('detail')})")
    assert res.get("failed") == 0

    # Test 6: Quarantine Summary
    print("\n[6] get_quarantine_summary(limit=3)")
    res_raw = get_quarantine_summary(limit=3)
    res = json.loads(res_raw)
    print(f"  Status   : {res.get('status')}")
    print(f"  Message  : {res.get('message')}")

    # Test 7: Inspect Bronze Lakehouse
    print("\n[7] inspect_bronze_lakehouse(limit=3)")
    res_raw = inspect_bronze_lakehouse(limit=3)
    res = json.loads(res_raw)
    print(f"  Status   : {res.get('status')}")

    # Test 8: Propose Contract Patch
    print("\n[8] propose_contract_patch()")
    os.environ.setdefault("MCP_PROPOSAL_TOKEN", "local-test-token")
    res_raw = propose_contract_patch(
        rationale="Autonomous supervisor authorizing new tool 'web_researcher' and higher threshold",
        proposed_change={"add_allowed_tool": "web_researcher", "increase_max_cost": 75.0},
        authorization_token=os.environ["MCP_PROPOSAL_TOKEN"],
    )
    res = json.loads(res_raw)
    print(f"  Status   : {res.get('status')}")
    print(f"  Artifact : {res.get('artifact_path')}")
    print(f"  Changes  : {res.get('changes_applied')}")
    assert res.get("status") == "PROPOSAL_CREATED"

    print("\n" + "=" * 70)
    print("✅ ALL MCP TOOLS PASSED DIRECT EXECUTION VALIDATION!")
    print("=" * 70)


if __name__ == "__main__":
    test_mcp_tools()