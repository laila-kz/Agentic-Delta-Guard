#!/usr/bin/env python
"""
Test the MCP server tools directly (without running the server)
"""

import json
from src.delta_guard.mcp.server import (
    check_contract,
    get_active_contract,
    get_quarantine_summary,
    get_system_status,
    run_health_check,
    propose_contract_patch
)

def test_mcp_tools():
    print("=" * 60)
    print("TESTING MCP TOOLS")
    print("=" * 60)
    
    # Test 1: Check contract (valid)
    print("\n1. check_contract (valid payload)")
    valid_payload = {
        "agent_id": "agent_001",
        "tool_name": "search_tool",
        "cost_usd": 2.50,
        "timestamp": "2026-09-07T12:00:00Z",
        "payload": {"query": "test search"}
    }
    result = check_contract(valid_payload)
    print(json.loads(result)["message"])
    
    # Test 2: Check contract (invalid)
    print("\n2. check_contract (invalid payload)")
    invalid_payload = {
        "agent_id": "agent_001",
        "tool_name": "admin_shell_exec",
        "cost_usd": 65.0,
        "timestamp": "2026-09-07T12:00:00Z"
    }
    result = check_contract(invalid_payload)
    data = json.loads(result)
    print(f"Allowed: {data['allowed']}")
    print(f"Violations: {len(data['violations'])}")
    for v in data['violations'][:2]:
        print(f"  - {v}")
    
    # Test 3: Get active contract
    print("\n3. get_active_contract()")
    result = get_active_contract()
    data = json.loads(result)
    print(f"Contract version: {data['contract'].get('version', 'unknown')}")
    print(f"Allowed tools: {len(data['contract'].get('allowed_tools', []))}")
    
    # Test 4: System status
    print("\n4. get_system_status()")
    result = get_system_status()
    data = json.loads(result)
    print(f"Overall health: {data['overall_health']}")
    for component, status in data['components'].items():
        print(f"  - {component}: {status.get('status', 'unknown')}")
    
    # Test 5: Health check
    print("\n5. run_health_check()")
    result = run_health_check()
    data = json.loads(result)
    print(f"Health status: {data['status']}")
    print(f"Passed: {data['summary']['passed']}")
    print(f"Failed: {data['summary']['failed']}")
    
    # Test 6: Quarantine summary
    print("\n6. get_quarantine_summary()")
    result = get_quarantine_summary(limit=3)
    data = json.loads(result)
    print(f"Status: {data.get('status', 'unknown')}")
    if data.get('status') == 'quarantine_active':
        print(f"Records: {data.get('total_records_in_sample', 0)}")
        print(f"Error signatures: {data.get('error_signatures', {})}")
    
    # Test 7: Propose contract patch
    print("\n7. propose_contract_patch()")
    result = propose_contract_patch(
        rationale="New agent needs access to file_search tool",
        proposed_change={"add_allowed_tool": "file_search"}
    )
    data = json.loads(result)
    print(f"Status: {data['status']}")
    if data['status'] == 'PROPOSAL_CREATED':
        print(f"Artifact: {data['artifact_path']}")

if __name__ == "__main__":
    test_mcp_tools()