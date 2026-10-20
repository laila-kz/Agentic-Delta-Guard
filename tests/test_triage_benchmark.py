"""
tests/test_triage_benchmark.py
Comprehensive unit and integration test suite for scripts/measure_triage_efficiency.py.
Tests live-data mode, synthetic mode, empty/missing quarantine handling, schema validation,
actual perf_counter timing measurements, and honest cost reporting.
"""

from pathlib import Path
import subprocess
import sys
import pytest
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from scripts.measure_triage_efficiency import (
    validate_quarantine_schema,
    simulate_manual_triage,
    generate_synthetic_incidents,
    benchmark_automated_triage,
    load_live_quarantine_table,
    MANUAL_ESTIMATED_MINUTES_PER_INCIDENT,
    MANUAL_ENGINEER_HOURLY_RATE_USD,
)


class TestSchemaValidation:
    def test_valid_schema(self):
        df = pd.DataFrame([
            {
                "action_id": "act-1",
                "tool_name": "db_writer",
                "cost_usd": 10.0,
                "timestamp": "2026-09-07T10:00:00Z",
                "error_summary": "cost_out_of_bounds",
            }
        ])
        is_valid, missing, warnings = validate_quarantine_schema(df)
        assert is_valid is True
        assert len(missing) == 0
        assert len(warnings) == 0

    def test_missing_required_column(self):
        df = pd.DataFrame([
            {
                "action_id": "act-1",
                # missing tool_name
                "cost_usd": 10.0,
                "timestamp": "2026-09-07T10:00:00Z",
            }
        ])
        is_valid, missing, warnings = validate_quarantine_schema(df)
        assert is_valid is False
        assert "tool_name" in missing

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        is_valid, missing, warnings = validate_quarantine_schema(df)
        assert is_valid is False
        assert len(missing) > 0


class TestManualBaselineEstimates:
    def test_manual_baseline_labeled_as_estimate(self):
        result = simulate_manual_triage(10)
        assert result["is_measured"] is False
        assert "Estimated" in result["method"]
        assert result["incidents"] == 10
        assert result["avg_time_min"] == MANUAL_ESTIMATED_MINUTES_PER_INCIDENT
        expected_cost = MANUAL_ESTIMATED_MINUTES_PER_INCIDENT * (MANUAL_ENGINEER_HOURLY_RATE_USD / 60.0)
        assert result["cost_per_incident_usd"] == round(expected_cost, 2)


class TestSyntheticIncidents:
    def test_generate_synthetic_count(self):
        count = 15
        df = generate_synthetic_incidents(count)
        assert len(df) == count
        is_valid, missing, _ = validate_quarantine_schema(df)
        assert is_valid is True
        assert len(missing) == 0


class TestBenchmarkTimingAndCost:
    def test_deterministic_timing_and_zero_cost(self):
        df = generate_synthetic_incidents(10)
        contract_path = PROJECT_ROOT / "configs" / "agent_contract.yaml"

        result = benchmark_automated_triage(
            records=df,
            contract_path=contract_path,
            use_llm=False,
            data_source_label="Test Synthetic",
        )

        assert result["is_measured"] is True
        assert result["incidents"] == 10
        # Check high-resolution perf_counter timing
        durs = result["durations"]
        assert durs["clustering_sec"] > 0
        assert durs["deterministic_sec"] > 0
        assert durs["total_measured_sec"] > 0
        assert result["avg_time_sec"] > 0
        # No artificial floor (total_measured_sec must NOT be artificially forced to >= 1.2)
        assert durs["total_measured_sec"] < 1.0  # 10 records locally take a few milliseconds

        # Cost transparency: deterministic compute must be $0.00
        assert result["cost_per_incident_usd"] == 0.0
        assert result["total_cost_usd"] == 0.0
        assert "Deterministic execution" in result["cost_methodology"]

    def test_empty_dataset_raises_value_error(self):
        contract_path = PROJECT_ROOT / "configs" / "agent_contract.yaml"
        with pytest.raises(ValueError, match="Cannot benchmark automated triage on an empty dataset"):
            benchmark_automated_triage(
                records=pd.DataFrame(),
                contract_path=contract_path,
            )


class TestLiveTableHandling:
    def test_missing_table_raises_file_not_found(self, tmp_path):
        missing_dir = tmp_path / "non_existent_quarantine"
        contract_path = PROJECT_ROOT / "configs" / "agent_contract.yaml"
        with pytest.raises(FileNotFoundError, match="Quarantine table directory does not exist"):
            load_live_quarantine_table(
                quarantine_path=missing_dir,
                contract_path=contract_path,
            )

    def test_empty_table_raises_value_error(self, tmp_path):
        empty_dir = tmp_path / "empty_quarantine"
        empty_dir.mkdir(parents=True)
        contract_path = PROJECT_ROOT / "configs" / "agent_contract.yaml"
        with pytest.raises(ValueError, match="contains 0 records"):
            load_live_quarantine_table(
                quarantine_path=empty_dir,
                contract_path=contract_path,
            )


class TestCliIntegration:
    def test_cli_synthetic_mode_success(self):
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "measure_triage_efficiency.py"),
            "--synthetic",
            "--count",
            "5",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0
        assert "SYNTHETIC SIMULATION" in result.stdout
        assert "Generated 5 normalized synthetic incident records" in result.stdout
        assert "Benchmark report written to:" in result.stdout

    def test_cli_missing_quarantine_exits_nonzero(self, tmp_path):
        fake_path = tmp_path / "fake_quarantine"
        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "measure_triage_efficiency.py"),
            "--quarantine-path",
            str(fake_path),
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 1
        assert "Benchmark Failed:" in result.stdout
        # Must not silently fall back to synthetic
        assert "Generated 10 normalized synthetic incident records" not in result.stdout

    def test_cli_live_quarantine_mode_success(self):
        live_path = PROJECT_ROOT / "data" / "quarantine" / "agent_events"
        if not live_path.exists():
            pytest.skip("Live quarantine Delta table not present in workspace")

        cmd = [
            sys.executable,
            str(PROJECT_ROOT / "scripts" / "measure_triage_efficiency.py"),
            "--count",
            "10",
        ]
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            cwd=str(PROJECT_ROOT),
        )
        assert result.returncode == 0
        assert "LIVE DATA BENCHMARK" in result.stdout
        assert "Successfully loaded" in result.stdout
        assert "Benchmark report written to:" in result.stdout
