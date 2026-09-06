"""
sandbox_guard.py — Delta sandbox and post-execution assertion suite.

The sandbox is an isolated Delta snapshot. It is not a zero-copy shallow clone:
writing the snapshot creates independent Delta files.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from deltalake import DeltaTable, write_deltalake


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

    def create_shallow_clone(self) -> str:
        """
        Create an isolated snapshot of the Gold table.

        Despite the historical method name, this is not a zero-copy Delta clone.
        """
        self._bootstrap_gold_table()
        self.sandbox_path.parent.mkdir(parents=True, exist_ok=True)

        source_df = DeltaTable(str(self.gold_path)).to_pandas()
        write_deltalake(
            str(self.sandbox_path),
            source_df,
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
            self._create_sample_gold_data(),
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

        write_deltalake(
            str(self.sandbox_path),
            experimental_df,
            mode="overwrite",
            schema_mode="overwrite",
        )

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
        assertions = self.run_post_execution_assertions()
        if not assertions["passed"]:
            return False

        sandbox_df = DeltaTable(str(self.sandbox_path)).to_pandas()
        write_deltalake(
            str(self.gold_path),
            sandbox_df,
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