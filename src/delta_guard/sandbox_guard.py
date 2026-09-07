"""
sandbox_guard.py — Delta sandbox and post-execution assertion suite.

The sandbox is an isolated Delta snapshot using zero-copy shallow cloning
when running on Spark/Delta, or independent copies when using DuckDB.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import duckdb
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

try:
    from delta_clone_utils import DeltaCloneUtils
    HAS_DELTA_CLONE_UTILS = True
except ImportError:
    HAS_DELTA_CLONE_UTILS = False


PROJECT_ROOT = Path(__file__).resolve().parents[2]


class SandboxGuard:
    REQUIRED_COLUMNS = {
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

    def __init__(
        self,
        gold_path: str | Path = "data/gold/agent_analytics",
        sandbox_path: str | Path = "data/gold/agent_sandbox",
    ) -> None:
        self.gold_path = self._resolve_path(gold_path)
        self.sandbox_path = self._resolve_path(sandbox_path)

        if self.gold_path == self.sandbox_path:
            raise ValueError("gold_path and sandbox_path must be different")

        self._bootstrap_gold_table()

    @staticmethod
    def _resolve_path(path: str | Path) -> Path:
        resolved = Path(path)
        if not resolved.is_absolute():
            resolved = PROJECT_ROOT / resolved
        return resolved.resolve()

    @staticmethod
    def _table_exists(path: Path) -> bool:
        return (path / "_delta_log").is_dir()

    def create_shallow_clone(self, use_delta_api: bool = True) -> str:
        """
        Create an isolated snapshot of the Gold table.

        When running on Spark/Delta with use_delta_api=True, creates a zero-copy
        shallow clone using Delta Lake's clone API. Otherwise falls back to
        copying data via pandas/write_deltalake (compatible with DuckDB).

        Args:
            use_delta_api: If True, attempt to use Delta's native clone API.
                          Falls back to copy method if Delta not available.

        Returns:
            Path to the sandbox table

        See Also:
            docs/DELTA_TIME_TRAVEL_AND_CLONING.md for shallow clone details
        """
        self._bootstrap_gold_table()
        self.sandbox_path.parent.mkdir(parents=True, exist_ok=True)

        if use_delta_api and HAS_DELTA_CLONE_UTILS:
            try:
                clone_utils = DeltaCloneUtils()
                result = clone_utils.create_shallow_clone(
                    source_table_path=str(self.gold_path),
                    target_table_path=str(self.sandbox_path),
                    replace=True,
                )
                print(f"✓ Shallow clone created (zero-copy, {result['clone_duration_seconds']:.2f}s)")
                return str(self.sandbox_path)
            except Exception as e:
                # Fall back to copy method
                print(f"Note: Delta API unavailable ({e}), using copy method")

        # Fallback: Copy via pandas/deltalake (works with DuckDB)
        source_df = DeltaTable(str(self.gold_path)).to_pandas()
        write_deltalake(
            str(self.sandbox_path),
            pa.Table.from_pandas(source_df, preserve_index=False),
            mode="overwrite",
            schema_mode="overwrite",
        )

        return str(self.sandbox_path)

    def _bootstrap_gold_table(self) -> None:
        if self._table_exists(self.gold_path):
            return

        self.gold_path.parent.mkdir(parents=True, exist_ok=True)
        write_deltalake(
            str(self.gold_path),
            pa.Table.from_pandas(self._create_sample_gold_data(), preserve_index=False),
            mode="overwrite",
        )

    @staticmethod
    def _create_sample_gold_data() -> pd.DataFrame:
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

    def run_sandbox_mutation(self, experimental_df: pd.DataFrame) -> None:
        if not self._table_exists(self.sandbox_path):
            self.create_shallow_clone()

        missing_columns = self.REQUIRED_COLUMNS - set(experimental_df.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(f"Mutation is missing required columns: {missing}")

        if experimental_df.empty:
            raise ValueError("Mutation must contain at least one row")

        existing_columns = DeltaTable(str(self.sandbox_path)).to_pandas().columns.tolist()
        mutation = experimental_df.reindex(columns=existing_columns).reset_index(drop=True)
        write_deltalake(
            str(self.sandbox_path),
            pa.Table.from_pandas(mutation, preserve_index=False),
            mode="append",
        )

    @staticmethod
    def _delta_scan_sql(path: Path) -> str:
        escaped_path = str(path).replace("'", "''")
        return f"delta_scan('{escaped_path}')"

    def run_comprehensive_assertions(self) -> dict[str, Any]:
        """Compare Gold and Sandbox and return promotion-readiness metrics."""
        if not self._table_exists(self.gold_path):
            raise FileNotFoundError(f"Gold Delta table does not exist: {self.gold_path}")
        if not self._table_exists(self.sandbox_path):
            raise FileNotFoundError(
                f"Sandbox Delta table does not exist: {self.sandbox_path}"
            )

        gold_table = self._delta_scan_sql(self.gold_path)
        sandbox_table = self._delta_scan_sql(self.sandbox_path)
        connection = duckdb.connect()
        try:
            gold_schema_rows = connection.execute(
                f"describe select * from {gold_table}"
            ).fetchall()
            sandbox_schema_rows = connection.execute(
                f"describe select * from {sandbox_table}"
            ).fetchall()
            gold_row_count = connection.execute(
                f"select count(*) from {gold_table}"
            ).fetchone()[0]
            sandbox_row_count = connection.execute(
                f"select count(*) from {sandbox_table}"
            ).fetchone()[0]
            cost_stats = connection.execute(
                f"""
                select
                    min(total_cost_usd),
                    avg(total_cost_usd),
                    max(total_cost_usd),
                    stddev_pop(total_cost_usd)
                from {sandbox_table}
                """
            ).fetchone()
            low_success_count = connection.execute(
                f"select count(*) from {sandbox_table} "
                "where success_rate_pct < 50.0"
            ).fetchone()[0]
            null_tool_name_count = connection.execute(
                f"select count(*) from {sandbox_table} where tool_name is null"
            ).fetchone()[0]
            invalid_success_rate_count = connection.execute(
                f"select count(*) from {sandbox_table} "
                "where success_rate_pct < 0.0 or success_rate_pct > 100.0"
            ).fetchone()[0]
        finally:
            connection.close()

        gold_schema = {row[0]: row[1] for row in gold_schema_rows}
        sandbox_schema = {row[0]: row[1] for row in sandbox_schema_rows}
        schema_differences = {
            "missing_from_sandbox": sorted(set(gold_schema) - set(sandbox_schema)),
            "extra_in_sandbox": sorted(set(sandbox_schema) - set(gold_schema)),
            "type_mismatches": sorted(
                column
                for column in set(gold_schema) & set(sandbox_schema)
                if gold_schema[column] != sandbox_schema[column]
            ),
        }
        checks = {
            "schema_matches": not any(schema_differences.values()),
            "positive_row_growth": sandbox_row_count > gold_row_count,
            "tool_name_not_null": null_tool_name_count == 0,
            "costs_within_bounds": cost_stats[0] is not None
            and cost_stats[0] >= 0.0
            and cost_stats[2] <= 50.0,
            "no_low_success_rates": low_success_count == 0,
            "success_rates_valid": invalid_success_rate_count == 0,
        }

        return {
            "passed": all(checks.values()),
            "checks": checks,
            "schema_differences": schema_differences,
            "row_counts": {
                "gold": gold_row_count,
                "sandbox": sandbox_row_count,
                "growth": sandbox_row_count - gold_row_count,
            },
            "cost_statistics": {
                "min": cost_stats[0],
                "avg": cost_stats[1],
                "max": cost_stats[2],
                "stddev": cost_stats[3],
            },
            "low_success_rate_rows": low_success_count,
            "null_tool_name_rows": null_tool_name_count,
            "invalid_success_rate_rows": invalid_success_rate_count,
            "evaluated_at": datetime.now(timezone.utc).isoformat(),
        }

    def run_post_execution_assertions(self) -> dict[str, Any]:
        if not self._table_exists(self.sandbox_path):
            raise FileNotFoundError(
                f"Sandbox Delta table does not exist: {self.sandbox_path}"
            )

        df = DeltaTable(str(self.sandbox_path)).to_pandas()
        checks: dict[str, bool] = {}

        checks["required_columns_present"] = self.REQUIRED_COLUMNS.issubset(df.columns)
        if not checks["required_columns_present"]:
            return {
                "passed": False,
                "checked_at": datetime.now(timezone.utc).isoformat(),
                "checks": checks,
                "row_count": len(df),
                "errors": ["Required columns are missing"],
            }

        checks["has_rows"] = not df.empty
        checks["tool_name_not_null"] = df["tool_name"].notna().all()
        checks["tool_name_unique"] = not df["tool_name"].duplicated().any()
        checks["non_negative_invocations"] = (
            (df["total_invocations"] >= 0).all()
            and (df["successful_executions"] >= 0).all()
            and (df["failed_executions"] >= 0).all()
        )
        checks["costs_within_bounds"] = (
            (df["total_cost_usd"] >= 0).all()
            & (df["total_cost_usd"] <= 50).all()
        )
        checks["success_rates_valid"] = (
            (df["success_rate_pct"] >= 0).all()
            & (df["success_rate_pct"] <= 100).all()
        )

        errors = [
            name for name, passed in checks.items() if not passed
        ]

        return {
            "passed": not errors,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "checks": checks,
            "row_count": len(df),
            "errors": errors,
        }

    def promote_sandbox_to_production(self) -> bool:
        assertions = self.run_comprehensive_assertions()
        if not assertions["passed"]:
            return False

        sandbox_df = DeltaTable(str(self.sandbox_path)).to_pandas()
        write_deltalake(
            str(self.gold_path),
            pa.Table.from_pandas(sandbox_df, preserve_index=False),
            mode="overwrite",
            schema_mode="overwrite",
        )
        return True


if __name__ == "__main__":
    print("=" * 60)
    print("SANDBOX GUARD DEMO")
    print("=" * 60)

    guard = SandboxGuard()
    guard.create_shallow_clone()

    new_data = pd.DataFrame(
        {
            "tool_name": ["db_writer", "sql_query_executor"],
            "total_invocations": [8, 25],
            "unique_agents_using_tool": [1, 3],
            "total_cost_usd": [0.032, 0.075],
            "avg_cost_per_invocation_usd": [0.004, 0.003],
            "avg_latency_ms": [88.0, 135.0],
            "p95_latency_ms": [110.0, 250.0],
            "successful_executions": [8, 24],
            "failed_executions": [0, 1],
            "success_rate_pct": [100.0, 96.0],
        }
    )

    guard.run_sandbox_mutation(new_data)
    print("Assertions:", guard.run_post_execution_assertions())
    print("Promoted:", guard.promote_sandbox_to_production())

    print("\n" + "=" * 60)
    print("DEMO COMPLETE")
    print("=" * 60)