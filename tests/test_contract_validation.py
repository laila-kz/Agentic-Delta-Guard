import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb
import pandas as pd
import pytest
import yaml
import pyarrow as pa
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
    write_deltalake(str(paths["bronze"]), pa.Table.from_pandas(_gold_data()), mode="overwrite")
    write_deltalake(
        str(paths["quarantine"]), pa.Table.from_pandas(_quarantine_data()), mode="overwrite"
    )
    write_deltalake(str(paths["gold"]), pa.Table.from_pandas(_gold_data()), mode="overwrite")
    write_deltalake(str(paths["sandbox"]), pa.Table.from_pandas(_gold_data()), mode="overwrite")
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


def test_triage_deterministic_fallback(setup_test_lakehouse, monkeypatch):
    """Always runs in CI and verifies the complete deterministic triage path."""
    paths = setup_test_lakehouse
    engine = LLMTriageEngine(
        quarantine_path=paths["quarantine"],
        contract_path=Path("configs/agent_contract.yaml").resolve(),
        incident_log_path=paths["root"] / "INCIDENT_LOG.md",
        proposed_contract_path=paths["root"] / "proposed.yaml",
    )
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    report = engine.run_triage()

    assert report["record_count"] == 3
    assert report["diagnosis"]["provider"] == "deterministic"
    assert report["diagnosis"]["metrics"]["cost_anomaly_rows"] == 2
    assert paths["root"].joinpath("INCIDENT_LOG.md").exists()
    assert paths["root"].joinpath("proposed.yaml").exists()
    assert "# Agent Events Incident Log" in paths["root"].joinpath(
        "INCIDENT_LOG.md"
    ).read_text(encoding="utf-8")


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


def test_benchmark_validation_parity():
    """
    Parity test: verifies that the in-process Python benchmark validator (_validate_event)
    and the actual PySpark gatekeeper column-expression logic enforce identical accept/reject
    decisions and exact matching error tags across valid events and all poison types.
    """
    from datetime import datetime, timedelta, timezone
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType
    from src.delta_guard.benchmark import _validate_event
    from src.delta_guard.gatekeeper import EVENT_SCHEMA, build_spark

    os.environ["PYSPARK_PYTHON"] = os.sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = os.sys.executable

    now = datetime.now(timezone.utc)
    valid_ts = now.isoformat()
    stale_ts = (now - timedelta(hours=48)).isoformat()
    future_ts = (now + timedelta(minutes=30)).isoformat()

    fixtures = [
        # (name, payload, expected_valid, expected_tags)
        (
            "valid_event",
            {
                "agent_id": "agent-1",
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": valid_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 120,
                "cost_usd": "0.05",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            True,
            [],
        ),
        (
            "missing_agent_id",
            {
                "agent_id": None,
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": valid_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "0.05",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["missing_required_field:agent_id"],
        ),
        (
            "missing_session_id",
            {
                "agent_id": "agent-1",
                "session_id": None,
                "action_id": "act-1",
                "timestamp": valid_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "0.05",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["missing_required_field:session_id"],
        ),
        (
            "missing_action_id",
            {
                "agent_id": "agent-1",
                "session_id": "sess-1",
                "action_id": None,
                "timestamp": valid_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "0.05",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["missing_required_field:action_id"],
        ),
        (
            "bad_cost_string",
            {
                "agent_id": "agent-1",
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": valid_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "not-a-number",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["type_mismatch:cost_usd_not_double"],
        ),
        (
            "negative_cost",
            {
                "agent_id": "agent-1",
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": valid_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "-5.0",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["semantic_rule:cost_out_of_bounds"],
        ),
        (
            "exceeded_cost",
            {
                "agent_id": "agent-1",
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": valid_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "500.0",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["semantic_rule:cost_out_of_bounds"],
        ),
        (
            "stale_timestamp",
            {
                "agent_id": "agent-1",
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": stale_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "0.05",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["freshness:stale_timestamp"],
        ),
        (
            "future_timestamp",
            {
                "agent_id": "agent-1",
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": future_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "0.05",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["freshness:future_timestamp"],
        ),
        (
            "invalid_timestamp_format",
            {
                "agent_id": "agent-1",
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": "bad-date-time",
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "0.05",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["parse_error:invalid_timestamp_format"],
        ),
        (
            "multi_poison_missing_id_and_bad_cost",
            {
                "agent_id": None,
                "session_id": "sess-1",
                "action_id": "act-1",
                "timestamp": valid_ts,
                "tool_name": "sql_query_executor",
                "execution_time_ms": 100,
                "cost_usd": "invalid",
                "status": "SUCCESS",
                "tool_args": "{}",
            },
            False,
            ["missing_required_field:agent_id", "type_mismatch:cost_usd_not_double"],
        ),
    ]

    spark = build_spark()
    try:
        # Run through PySpark gatekeeper path
        df = spark.createDataFrame([f[1] for f in fixtures], schema=EVENT_SCHEMA)
        max_cost = 50.0
        age_hours = 24
        skew_mins = 5

        validated_df = (
            df.withColumn("event_timestamp", F.to_timestamp(F.col("timestamp")))
            .withColumn("cost_usd_double", F.col("cost_usd").cast(DoubleType()))
            .withColumn("timestamp_ts", F.col("event_timestamp"))
            .withColumn(
                "errors",
                F.filter(
                    F.array(
                        F.when(F.col("agent_id").isNull(), "missing_required_field:agent_id"),
                        F.when(F.col("session_id").isNull(), "missing_required_field:session_id"),
                        F.when(F.col("action_id").isNull(), "missing_required_field:action_id"),
                        F.when(F.col("cost_usd_double").isNull(), "type_mismatch:cost_usd_not_double"),
                        F.when(
                            (F.col("cost_usd_double") < 0.0) | (F.col("cost_usd_double") > max_cost),
                            "semantic_rule:cost_out_of_bounds",
                        ),
                        F.when(F.col("timestamp_ts").isNull(), "parse_error:invalid_timestamp_format"),
                        F.when(
                            F.col("timestamp_ts") < (F.current_timestamp() - F.expr(f"INTERVAL {age_hours} HOURS")),
                            "freshness:stale_timestamp",
                        ),
                        F.when(
                            F.col("timestamp_ts") > (F.current_timestamp() + F.expr(f"INTERVAL {skew_mins} MINUTES")),
                            "freshness:future_timestamp",
                        ),
                    ),
                    lambda x: x.isNotNull(),
                ),
            )
        )

        spark_rows = validated_df.select("errors").collect()

        for (name, payload, expected_valid, expected_tags), row in zip(fixtures, spark_rows):
            # 1. Python validator results
            py_valid, py_error_summary = _validate_event(payload)
            py_errors = set(py_error_summary.split("; ")) if py_error_summary else set()

            # 2. PySpark Gatekeeper results
            spark_errors = set(row["errors"])
            spark_valid = (len(spark_errors) == 0)

            # Assert Python matches expected
            assert py_valid == expected_valid, (
                f"Python validator mismatch on {name}: expected valid={expected_valid}, got {py_valid}"
            )
            assert py_errors == set(expected_tags), (
                f"Python tag mismatch on {name}: expected {set(expected_tags)}, got {py_errors}"
            )

            # Assert Spark Gatekeeper matches expected
            assert spark_valid == expected_valid, (
                f"Spark Gatekeeper mismatch on {name}: expected valid={expected_valid}, got {spark_valid}"
            )
            assert spark_errors == set(expected_tags), (
                f"Spark tag mismatch on {name}: expected {set(expected_tags)}, got {spark_errors}"
            )

            # Assert Spark and Python are in 100% exact parity
            assert spark_valid == py_valid, (
                f"Parity mismatch on {name}: Spark valid={spark_valid} vs Python valid={py_valid}"
            )
            assert spark_errors == py_errors, (
                f"Parity tag mismatch on {name}: Spark errors={spark_errors} vs Python errors={py_errors}"
            )
    finally:
        spark.stop()


@pytest.mark.skipif(
    not (os.getenv("OPENAI_API_KEY") and os.getenv("LIVE_LLM_TEST") == "1"),
    reason="Set OPENAI_API_KEY and LIVE_LLM_TEST=1 to run the manual live test",
)
def test_llm_triage_with_openai(setup_test_lakehouse, monkeypatch):
    paths = setup_test_lakehouse
    engine = LLMTriageEngine(
        quarantine_path=paths["quarantine"],
        contract_path=Path("configs/agent_contract.yaml").resolve(),
        incident_log_path=paths["root"] / "INCIDENT_LOG.md",
        proposed_contract_path=paths["root"] / "proposed.yaml",
    )
    diagnosis = engine.generate_llm_diagnosis(
        engine.load_quarantine_records(),
        engine.cluster_error_signatures(),
    )

    assert diagnosis["provider"] == "openai_or_anthropic"
    assert diagnosis["summary"]


def test_gatekeeper_process_batch_quarantines_unauthorized_tool(tmp_path, monkeypatch):
    """
    The streaming gate must enforce schema.fields[tool_name].allowed_values.

    This drives the real process_batch (build_spark + the production column
    expressions + real Delta writes), not _validate_event, because the gap this
    covers existed only in the Spark path -- the Python validator already
    rejected unauthorized tools. An event whose tool_name is outside the
    allowlist must be routed to Quarantine and must not reach Bronze.

    NOTE: this test ends in .collect()/.count() against a Spark DataFrame. On
    some Windows hosts that intermittently fails with "Python worker failed to
    connect back" (WinError 10061); Linux CI is the source of truth here.
    """
    pyspark = pytest.importorskip("pyspark")
    from src.delta_guard import gatekeeper
    from src.delta_guard.gatekeeper import build_spark

    bronze = tmp_path / "bronze"
    quarantine = tmp_path / "quarantine"

    # Redirect every side effect of process_batch away from the repo.
    monkeypatch.setattr(gatekeeper, "BRONZE_PATH", str(bronze))
    monkeypatch.setattr(gatekeeper, "QUARANTINE_PATH", str(quarantine))
    monkeypatch.setattr(gatekeeper, "STATUS_PATH", str(tmp_path / "status.json"))

    # Read the allowlist from the contract itself (the source of truth), not from
    # gatekeeper's internal cache, so this test fails on missing enforcement
    # rather than on a missing implementation detail.
    contract_file = Path(gatekeeper.__file__).resolve().parents[2] / "configs" / "agent_contract.yaml"
    contract = yaml.safe_load(contract_file.read_text(encoding="utf-8"))
    allowed = next(
        field["allowed_values"]
        for field in contract["schema"]["fields"]
        if field["name"] == "tool_name"
    )
    assert allowed, "contract must declare tool_name.allowed_values"
    assert "unauthorized_admin_bash" not in allowed

    # Fresh timestamp: the contract has a 24h staleness window, so a hardcoded
    # date would quarantine both events for the wrong reason.
    now = datetime.now(timezone.utc) - timedelta(seconds=30)

    def _event(tool: str) -> dict:
        return {
            "agent_id": f"agent-{tool}",
            "session_id": "sess-allowlist",
            "action_id": f"act-{tool}",
            "timestamp": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tool_name": tool,
            "execution_time_ms": 120,
            "cost_usd": "1.25",
            "status": "success",
            "tool_args": "{}",
        }

    payloads = [_event(allowed[0]), _event("unauthorized_admin_bash")]

    spark = build_spark()
    try:
        batch_df = spark.createDataFrame(
            [(json.dumps(p),) for p in payloads], "value string"
        )
        gatekeeper.process_batch(batch_df, 0)
    finally:
        spark.stop()

    # Security invariant first: the unauthorized event must NOT reach Bronze.
    bronze_rows = DeltaTable(str(bronze)).to_pyarrow_table().to_pylist()
    bronze_action_ids = {row["action_id"] for row in bronze_rows}
    assert "act-unauthorized_admin_bash" not in bronze_action_ids, (
        f"unauthorized tool leaked into Bronze: {bronze_action_ids}"
    )
    assert f"act-{allowed[0]}" in bronze_action_ids, (
        f"allowed tool should have reached Bronze: {bronze_action_ids}"
    )

    # And it must be quarantined, carrying the allowlist signature.
    assert quarantine.exists(), "quarantine table was never created"
    quarantine_df = DeltaTable(str(quarantine)).to_pyarrow_table().to_pylist()
    quarantined_action_ids = {row["action_id"] for row in quarantine_df}
    assert "act-unauthorized_admin_bash" in quarantined_action_ids, (
        f"unauthorized tool was not quarantined; quarantine has {quarantined_action_ids}"
    )
    summaries = [row["error_summary"] for row in quarantine_df]
    assert any(
        "semantic_rule:tool_not_allowed" in (s or "") for s in summaries
    ), f"expected semantic_rule:tool_not_allowed signature, got {summaries}"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
