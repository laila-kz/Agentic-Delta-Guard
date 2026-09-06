import pandas as pd

from src.delta_guard.sandbox_guard import SandboxGuard


def mutation_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "tool_name": ["db_writer", "web_scraper", "vector_search"],
            "total_invocations": [8, 12, 4],
            "unique_agents_using_tool": [1, 2, 1],
            "total_cost_usd": [0.032, 0.075, 0.01],
            "avg_cost_per_invocation_usd": [0.004, 0.00625, 0.0025],
            "avg_latency_ms": [88.0, 135.0, 72.0],
            "p95_latency_ms": [110.0, 250.0, 95.0],
            "successful_executions": [8, 11, 4],
            "failed_executions": [0, 1, 0],
            "success_rate_pct": [100.0, 91.67, 100.0],
        }
    )


def test_valid_mutation(tmp_path):
    guard = SandboxGuard(
        gold_path=tmp_path / "gold",
        sandbox_path=tmp_path / "sandbox",
    )
    guard.create_shallow_clone()
    guard.run_sandbox_mutation(mutation_data())

    assertions = guard.run_comprehensive_assertions()

    assert assertions["passed"] is True
    assert assertions["checks"]["positive_row_growth"] is True
    assert guard.promote_sandbox_to_production() is True


def test_invalid_mutation(tmp_path):
    guard = SandboxGuard(
        gold_path=tmp_path / "gold",
        sandbox_path=tmp_path / "sandbox",
    )
    guard.create_shallow_clone()
    invalid = mutation_data()
    invalid.loc[0, "tool_name"] = None
    invalid.loc[1, "total_cost_usd"] = -1.0
    invalid.loc[2, "success_rate_pct"] = 150.0
    guard.run_sandbox_mutation(invalid)

    assertions = guard.run_comprehensive_assertions()

    assert assertions["passed"] is False
    assert assertions["checks"]["tool_name_not_null"] is False
    assert assertions["checks"]["costs_within_bounds"] is False
    assert assertions["checks"]["success_rates_valid"] is False
    assert guard.promote_sandbox_to_production() is False


if __name__ == "__main__":
    import pytest

    raise SystemExit(pytest.main([__file__, "-v"]))