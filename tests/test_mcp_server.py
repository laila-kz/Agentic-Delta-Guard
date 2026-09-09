"""
tests/test_mcp_server.py
Pytest suite for the Model Context Protocol (MCP) server tools and validator.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
import pytest

from src.delta_guard.mcp.server import (
    check_contract,
    get_active_contract,
    get_quarantine_summary,
    get_system_status,
    inspect_bronze_lakehouse,
    propose_contract_patch,
    run_health_check,
)
from src.delta_guard.mcp.validators.contract_validator import (
    ContractValidator,
    InFlightContractValidator,
)


@pytest.fixture
def sample_valid_event():
    return {
        "agent_id": "test_agent_001",
        "session_id": "sess_unit_test",
        "action_id": "act_unit_test",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tool_name": "sql_query_executor",
        "execution_time_ms": 150,
        "cost_usd": 0.05,
        "tool_args": {"query": "SELECT 1;"},
    }


def test_validator_valid_payload(sample_valid_event):
    validator = ContractValidator()
    result = validator.validate(sample_valid_event)
    assert result["allowed"] is True
    assert len(result["violations"]) == 0


def test_validator_unauthorized_tool(sample_valid_event):
    validator = ContractValidator()
    sample_valid_event["tool_name"] = "unauthorized_malicious_tool"
    result = validator.validate(sample_valid_event)
    assert result["allowed"] is False
    assert any("UNAUTHORIZED_TOOL" in v for v in result["violations"])
    assert len(result["remediation_hints"]) > 0


def test_validator_cost_limit_breach(sample_valid_event):
    validator = ContractValidator()
    sample_valid_event["cost_usd"] = 75.0  # Cap is 50.0
    result = validator.validate(sample_valid_event)
    assert result["allowed"] is False
    assert any("COST_LIMIT_BREACH" in v for v in result["violations"])


def test_validator_negative_cost(sample_valid_event):
    validator = ContractValidator()
    sample_valid_event["cost_usd"] = -5.0
    result = validator.validate(sample_valid_event)
    assert result["allowed"] is False
    assert any("NEGATIVE_COST" in v for v in result["violations"])


def test_validator_stale_timestamp(sample_valid_event):
    validator = ContractValidator()
    sample_valid_event["timestamp"] = "2020-01-01T00:00:00Z"
    result = validator.validate(sample_valid_event)
    assert result["allowed"] is False
    assert any("STALE_TIMESTAMP" in v for v in result["violations"])


def test_validator_missing_fields():
    validator = ContractValidator()
    result = validator.validate({"tool_name": "sql_query_executor"})
    assert result["allowed"] is False
    assert any("MISSING_REQUIRED_FIELD" in v for v in result["violations"])


def test_mcp_check_contract_tool(sample_valid_event):
    response = json.loads(check_contract(sample_valid_event))
    assert response["allowed"] is True
    assert "Safe to emit" in response["message"]


def test_mcp_get_active_contract_tool():
    response = json.loads(get_active_contract())
    assert "contract" in response
    assert "allowed_tools" in response
    assert response["max_cost_per_call"] == 50.0


def test_mcp_get_quarantine_summary_tool():
    response = json.loads(get_quarantine_summary(limit=5))
    assert "status" in response
    assert "records" in response


def test_mcp_inspect_bronze_lakehouse_tool():
    response = json.loads(inspect_bronze_lakehouse(limit=5))
    assert "status" in response
    assert "records" in response


def test_mcp_propose_contract_patch_tool(tmp_path, monkeypatch):
    monkeypatch.setenv("MCP_PROPOSAL_TOKEN", "test-proposal-token")
    proposal = {
        "add_allowed_tool": "data_extractor_v2",
        "increase_max_cost": 80.0,
    }
    response = json.loads(propose_contract_patch(
        "Adding specialized data extractor",
        proposal,
        authorization_token="test-proposal-token",
    ))
    assert response["status"] == "PROPOSAL_CREATED"
    assert len(response["changes_applied"]) >= 2
    assert "triage_context" in response
    assert Path(response["artifact_path"]).exists()


def test_mcp_propose_contract_patch_requires_authorization(monkeypatch):
    monkeypatch.delenv("MCP_PROPOSAL_TOKEN", raising=False)
    response = json.loads(propose_contract_patch("audit", {"add_allowed_tool": "x"}))
    assert response["status"] == "AUTHORIZATION_REQUIRED"


def test_mcp_get_system_status_tool():
    response = json.loads(get_system_status())
    assert "overall_health" in response
    assert "components" in response


def test_mcp_run_health_check_tool():
    response = json.loads(run_health_check())
    assert response["status"] in ["healthy", "degraded"]
    assert response["passed"] >= 2
