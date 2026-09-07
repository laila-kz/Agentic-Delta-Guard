#!/usr/bin/env python
"""
Test the contract validator
"""

import sys
from pathlib import Path

# Add src directory to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from delta_guard.mcp.validators.contract_validator import ContractValidator

def test_validator():
    validator = ContractValidator()
    
    # Test 1: Valid payload
    print("Test 1: Valid payload")
    valid_payload = {
        "agent_id": "agent_001",
        "tool_name": "search_tool",
        "cost_usd": 2.50,
        "timestamp": "2026-09-07T12:00:00Z",
        "payload": {"query": "test search"}
    }
    result = validator.validate(valid_payload)
    print(f"  Allowed: {result['allowed']}")
    print(f"  Violations: {result['violations']}")
    print(f"  Hints: {result['remediation_hints'][:2]}\n")
    
    # Test 2: Invalid payload (unauthorized tool, high cost)
    print("Test 2: Invalid payload")
    invalid_payload = {
        "agent_id": "agent_001",
        "tool_name": "admin_shell_exec",
        "cost_usd": 65.0,
        "timestamp": "2026-09-07T12:00:00Z"
    }
    result = validator.validate(invalid_payload)
    print(f"  Allowed: {result['allowed']}")
    print(f"  Violations: {result['violations']}")
    print(f"  Hints: {result['remediation_hints'][:2]}\n")
    
    # Test 3: Very old timestamp
    print("Test 3: Stale timestamp")
    stale_payload = {
        "agent_id": "agent_001",
        "tool_name": "search_tool",
        "cost_usd": 0.50,
        "timestamp": "2026-09-01T12:00:00Z"  # 6 days ago
    }
    result = validator.validate(stale_payload)
    print(f"  Allowed: {result['allowed']}")
    print(f"  Violations: {result['violations']}")
    print(f"  Hints: {result['remediation_hints'][:2]}\n")

if __name__ == "__main__":
    test_validator()
