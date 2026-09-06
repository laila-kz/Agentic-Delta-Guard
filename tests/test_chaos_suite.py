import os
from pathlib import Path

import duckdb
import pandas as pd
import pytest
import yaml
from deltalake import DeltaTable, write_deltalake

from src.delta_guard.sandbox_guard import SandboxGuard
from src.delta_guard.triage import LLMTriageEngine


REQUIRED_GOLD_COLUMNS = {
    "tool_name",
    "total_invocations",
    "unique_agents_using_tool",
    "total_cost_usd",
    "avg_cost_per_invocation_usd",
    "avg_latency_ms",
    "p95_latency_ms",
    "successful_executions",
    "failed_executions",
    "success_rate_pct",
}


def _gold_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "tool_name": ["sql_query_executor", "vector_search"],
            "total_invocations": [20, 10],
            "unique_agents_using_tool": [3, 2],
            "total_cost_usd": [0.06, 0.025],
            "avg_cost_per_invocation_usd": [0.003, 0.0025],
            "avg_latency_ms": [120.0, 90.0],
            "p95_latency_ms": [220.0, 160.0],
            "successful_executions": [19, 10],
            "failed_executions": [1, 0],
            "success_rate_pct": [95.0, 100.0],
        }
    )


def _quarantine_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "agent_id": ["agent-1", "agent-2", "agent-1"],
            "session_id": ["session-1", "session-2", "session-3"],
            "action_id": ["action-1", "action-2", "action-3"],
            "timestamp": ["2026-09-06 10:00:00"] * 3,
            "tool_name": ["sql_query_executor", "vector_search", "sql_query_executor"],
            "execution_time_ms": [100, 200, 300],
            "cost_usd": ["bad-value", "-1.0", "51.0"],
            "status": ["FAILED", "FAILED", "FAILED"],
            "tool_args": ["{}", "{}", "{}"],
            "quarantined_at": ["2026-09-06 10:01:00"] * 3,
            "error_summary": [
                "type_mismatch:cost_usd_not_double",
                "semantic_rule:cost_out_of_bounds",
                "semantic_rule:cost_out_of_bounds",
            ],
        }
    )


@pytest.fixture
def setup_test_lakehouse(tmp_path):
    """Create temporary Bronze, Quarantine, Gold, and Sandbox Delta tables."""
    paths = {
        "root": tmp_path,
        "bronze": tmp_path / "bronze",
        "quarantine": tmp_path / "quarantine",
        "gold": tmp_path / "gold",
        "sandbox": tmp_path / "sandbox",
    }
    write_deltalake(str(paths["bronze"]), _gold_data(), mode="overwrite")
    write_deltalake(str(paths["quarantine"]), _quarantine_data(), mode="overwrite")
    write_deltalake(str(paths["gold"]), _gold_data(), mode="overwrite")
    write_deltalake(str(paths["sandbox"]), _gold_data(), mode="overwrite")
    return paths


def test_bronze_lakehouse_integrity(setup_test_lakehouse):
    bronze = setup_test_lakehouse["bronze"]
    table = DeltaTable(str(bronze))
    assert table.to_pandas().shape[0] == 2
    assert REQUIRED_GOLD_COLUMNS.issubset(table.to_pandas().columns)


def test_quarantine_error_clustering(setup_test_lakehouse):
    paths = setup_test_lakehouse
    engine = LLMTriageEngine(
        quarantine_path=paths["quarantine"],
        contract_path=Path("configs/agent_contract.yaml").resolve(),
        incident_log_path=paths["root"] / "INCIDENT_LOG.md",
        proposed_contract_path=paths["root"] / "proposed.yaml",
    )

    clusters = engine.cluster_error_signatures(engine.load_quarantine_records())

    assert len(clusters) == 2
    assert clusters[0]["signature"] == "semantic_rule:cost_out_of_bounds"
    assert clusters[0]["count"] == 2
    assert clusters[0]["affected_agents"] == ["agent-1", "agent-2"]


def test_sandbox_shallow_clone_isolation(setup_test_lakehouse):
    paths = setup_test_lakehouse
    guard = SandboxGuard(paths["gold"], paths["sandbox"])
    original_gold = DeltaTable(str(paths["gold"])).to_pandas()
    guard.create_shallow_clone()
    mutation = _gold_data().iloc[[0]].assign(tool_name="web_scraper")

    guard.run_sandbox_mutation(mutation)

    gold_after = DeltaTable(str(paths["gold"])).to_pandas()
    sandbox_after = DeltaTable(str(paths["sandbox"])).to_pandas()
    assert len(gold_after) == len(original_gold)
    assert len(sandbox_after) == len(original_gold) + 1
    assert "web_scraper" not in set(gold_after["tool_name"])


def test_sandbox_assertion_rejection_on_poison(setup_test_lakehouse):
    paths = setup_test_lakehouse
    guard = SandboxGuard(paths["gold"], paths["sandbox"])
    guard.create_shallow_clone()
    poison = _gold_data().iloc[[0]].copy()
    poison.loc[poison.index[0], "tool_name"] = None
    poison.loc[poison.index[0], "total_cost_usd"] = -1.0
    poison.loc[poison.index[0], "success_rate_pct"] = 150.0

    guard.run_sandbox_mutation(poison)
    assertions = guard.run_comprehensive_assertions()

    assert assertions["passed"] is False
    assert assertions["checks"]["tool_name_not_null"] is False
    assert assertions["checks"]["costs_within_bounds"] is False
    assert assertions["checks"]["success_rates_valid"] is False
    assert guard.promote_sandbox_to_production() is False


@pytest.mark.skipif(
    not os.getenv("OPENAI_API_KEY"),
    reason="OPENAI_API_KEY is not configured",
)
def test_llm_triage_with_openai(setup_test_lakehouse, monkeypatch):
    paths = setup_test_lakehouse
    engine = LLMTriageEngine(
        quarantine_path=paths["quarantine"],
        contract_path=Path("configs/agent_contract.yaml").resolve(),
        incident_log_path=paths["root"] / "INCIDENT_LOG.md",
        proposed_contract_path=paths["root"] / "proposed.yaml",
    )
    monkeypatch.setattr(
        engine,
        "_call_llm",
        lambda prompt: {
            "summary": "OpenAI diagnosis received.",
            "findings": ["Cost values require review."],
            "schema_drift": {},
            "metrics": {},
            "recommended_patches": [],
        },
    )

    diagnosis = engine.generate_llm_diagnosis(
        engine.load_quarantine_records(),
        engine.cluster_error_signatures(),
    )

    assert diagnosis["provider"] == "openai_or_anthropic"
    assert diagnosis["summary"] == "OpenAI diagnosis received."


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
