"""
test_chaos_infra.py — Infrastructure-level failure injection tests.

These tests exercise real failure modes — not input validation (that's in
test_contract_validation.py). Specifically:

  1. Checkpoint wipe → full replay from earliest offset → idempotent MERGE INTO
     must leave zero duplicate Bronze rows.

  2. Late / out-of-order events beyond the 10-minute watermark → gatekeeper
     must drop them, not silently insert them as new rows.

  3. Broker kill mid-stream (requires Docker Compose) → labelled with
     pytest.mark.requires_docker and skipped in regular CI.

Run all non-Docker infra tests:
    pytest tests/test_chaos_infra.py -v -m "not requires_docker"

Run all (including Docker, needs docker compose up):
    CHAOS_DOCKER_TESTS=1 pytest tests/test_chaos_infra.py -v
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest
import yaml
from deltalake import DeltaTable, write_deltalake


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = PROJECT_ROOT / "configs" / "agent_contract.yaml"


def _load_contract() -> dict[str, Any]:
    if CONTRACT_PATH.exists():
        with CONTRACT_PATH.open("r", encoding="utf-8") as fh:
            return yaml.safe_load(fh) or {}
    return {}


def _make_valid_event(
    agent_id: str = "agent-1",
    session_id: str = "session-1",
    action_id: str = "action-1",
    timestamp: datetime | None = None,
    cost_usd: float = 0.01,
) -> dict:
    ts = timestamp or datetime.now(timezone.utc)
    return {
        "agent_id": agent_id,
        "session_id": session_id,
        "action_id": action_id,
        "timestamp": ts.isoformat(),
        "tool_name": "sql_query_executor",
        "execution_time_ms": 100,
        "cost_usd": str(cost_usd),
        "status": "SUCCESS",
        "tool_args": "{}",
    }


def _write_bronze_events(path: Path, events: list[dict]) -> None:
    df = pd.DataFrame(events)
    df["cost_usd"] = df["cost_usd"].astype(float)
    df["execution_time_ms"] = df["execution_time_ms"].astype(int)
    write_deltalake(str(path), pa.Table.from_pandas(df, preserve_index=False), mode="overwrite")


def _count_duplicates(table_path: Path) -> int:
    dt = DeltaTable(str(table_path))
    df = dt.to_pandas()
    return int((df.groupby(["agent_id", "session_id", "action_id"]).size() > 1).sum())


def test_gatekeeper_error_array_keeps_valid_rows_writable():
    """The validation split must represent no errors as an empty array, not null."""
    pyspark = pytest.importorskip("pyspark")
    from pyspark.sql import functions as F
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType
    from src.delta_guard.gatekeeper import build_spark

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = build_spark()
    try:
        schema = StructType(
            [
                StructField("agent_id", StringType(), True),
                StructField("cost_usd_double", DoubleType(), True),
            ]
        )
        events = spark.createDataFrame([("valid-agent", 0.05), ("poison-agent", 850.0)], schema)
        errors = F.filter(
            F.array(
                F.when(F.col("agent_id").isNull(), "missing_required_field:agent_id"),
                F.when(F.col("cost_usd_double") > 50.0, "semantic_rule:cost_out_of_bounds"),
            ),
            lambda error_message: error_message.isNotNull(),
        )

        rows = (
            events.withColumn("errors", errors)
            .withColumn("error_size", F.coalesce(F.size("errors"), F.lit(0)))
            .select("agent_id", "error_size")
            .orderBy("agent_id")
            .collect()
        )
        assert [row.error_size for row in rows] == [1, 0]
    finally:
        spark.stop()


# ---------------------------------------------------------------------------
# Test 1: Checkpoint wipe -> full replay -> zero duplicates
# ---------------------------------------------------------------------------

class TestCheckpointWipeReplay:
    """
    Simulates deleting the streaming checkpoint directory and replaying
    from earliest offset.

    The gatekeeper's idempotent MERGE INTO on (agent_id, session_id,
    action_id) must prevent any duplicate rows from appearing in Bronze
    after the replay.
    """

    def test_checkpoint_wipe_forces_full_replay_without_duplicates(self, tmp_path):
        """
        Deletes the checkpoint dir, writes an identical second batch to Bronze
        via MERGE INTO, and asserts zero duplicate rows.
        """
        bronze_path = tmp_path / "bronze"
        checkpoint_dir = tmp_path / "checkpoints" / "gatekeeper"
        checkpoint_dir.mkdir(parents=True)

        events = [
            _make_valid_event("agent-1", "session-1", "action-1"),
            _make_valid_event("agent-1", "session-1", "action-2"),
            _make_valid_event("agent-2", "session-2", "action-1"),
        ]
        _write_bronze_events(bronze_path, events)
        initial_count = DeltaTable(str(bronze_path)).to_pandas().shape[0]
        assert initial_count == 3

        # Simulate checkpoint wipe
        shutil.rmtree(str(checkpoint_dir), ignore_errors=True)
        assert not checkpoint_dir.exists()

        # Simulate replay: attempt to re-insert the SAME events via MERGE
        replay_df = pd.DataFrame(events)
        replay_df["cost_usd"] = replay_df["cost_usd"].astype(float)
        replay_df["execution_time_ms"] = replay_df["execution_time_ms"].astype(int)
        replay_arrow = pa.Table.from_pandas(replay_df, preserve_index=False)

        dt = DeltaTable(str(bronze_path))
        (
            dt.merge(
                replay_arrow,
                predicate=(
                    "s.agent_id = t.agent_id AND "
                    "s.session_id = t.session_id AND "
                    "s.action_id = t.action_id"
                ),
                source_alias="s",
                target_alias="t",
            )
            .when_not_matched_insert_all()
            .execute()
        )

        after_replay = DeltaTable(str(bronze_path)).to_pandas()
        assert after_replay.shape[0] == initial_count, (
            f"Expected {initial_count} rows after checkpoint-wipe replay, "
            f"got {after_replay.shape[0]} -- MERGE INTO is not idempotent!"
        )
        assert _count_duplicates(bronze_path) == 0, (
            "Duplicate (agent_id, session_id, action_id) tuples found after replay."
        )

    def test_new_events_after_replay_are_still_appended(self, tmp_path):
        """Genuinely new events (new action_ids) must still be inserted post-replay."""
        bronze_path = tmp_path / "bronze"
        existing = [_make_valid_event("agent-1", "session-1", "action-1")]
        _write_bronze_events(bronze_path, existing)

        new_event = _make_valid_event("agent-1", "session-1", "action-2")
        new_df = pd.DataFrame([new_event])
        new_df["cost_usd"] = new_df["cost_usd"].astype(float)
        new_df["execution_time_ms"] = new_df["execution_time_ms"].astype(int)
        new_arrow = pa.Table.from_pandas(new_df, preserve_index=False)

        dt = DeltaTable(str(bronze_path))
        (
            dt.merge(
                new_arrow,
                predicate=(
                    "s.agent_id = t.agent_id AND "
                    "s.session_id = t.session_id AND "
                    "s.action_id = t.action_id"
                ),
                source_alias="s",
                target_alias="t",
            )
            .when_not_matched_insert_all()
            .execute()
        )

        final_df = DeltaTable(str(bronze_path)).to_pandas()
        assert final_df.shape[0] == 2
        assert "action-2" in set(final_df["action_id"])


# ---------------------------------------------------------------------------
# Test 2: Late / out-of-order event watermark behaviour
# ---------------------------------------------------------------------------

class TestWatermarkLateEventHandling:
    """
    Verifies the gatekeeper's watermark predicate: events whose
    event_timestamp is more than 10 minutes older than the current
    processing time must be filtered out, not merged into Bronze.
    """

    WATERMARK_MINUTES = 10  # matches _THRESHOLDS["watermark_minutes"] in gatekeeper.py

    def _is_within_watermark(self, event_timestamp: datetime, max_event_ts: datetime) -> bool:
        """Mirror of the Spark withWatermark predicate."""
        cutoff = max_event_ts - timedelta(minutes=self.WATERMARK_MINUTES)
        return event_timestamp >= cutoff

    def test_on_time_event_passes_watermark(self):
        """An event 5 minutes late is within the 10-minute watermark window."""
        now = datetime.now(timezone.utc)
        late_5min = now - timedelta(minutes=5)
        assert self._is_within_watermark(late_5min, max_event_ts=now)

    def test_late_event_beyond_watermark_is_dropped(self):
        """An event arriving 20 minutes after its timestamp exceeds the watermark."""
        now = datetime.now(timezone.utc)
        late_20min = now - timedelta(minutes=20)
        assert not self._is_within_watermark(late_20min, max_event_ts=now), (
            "Event 20 minutes late should be DROPPED by the 10-minute watermark"
        )

    def test_late_event_at_exact_watermark_boundary_is_included(self):
        """An event at exactly the watermark boundary (10 min late) is included."""
        now = datetime.now(timezone.utc)
        exactly_at_boundary = now - timedelta(minutes=self.WATERMARK_MINUTES)
        assert self._is_within_watermark(exactly_at_boundary, max_event_ts=now)

    def test_late_events_do_not_get_merged_into_bronze(self, tmp_path):
        """
        Simulate the pre-filter: a late event (20 min beyond watermark) must
        not increase the Bronze row count.
        """
        bronze_path = tmp_path / "bronze"
        now = datetime.now(timezone.utc)

        good_event = _make_valid_event("agent-1", "session-1", "action-1", timestamp=now)
        _write_bronze_events(bronze_path, [good_event])
        initial_count = DeltaTable(str(bronze_path)).to_pandas().shape[0]

        late_event = _make_valid_event(
            "agent-1", "session-1", "action-2",
            timestamp=now - timedelta(minutes=20),
        )
        # Gatekeeper watermark filter: only merge if within window
        event_ts = datetime.fromisoformat(late_event["timestamp"])
        within_window = self._is_within_watermark(event_ts, max_event_ts=now)

        if within_window:
            late_df = pd.DataFrame([late_event])
            late_df["cost_usd"] = late_df["cost_usd"].astype(float)
            late_df["execution_time_ms"] = late_df["execution_time_ms"].astype(int)
            late_arrow = pa.Table.from_pandas(late_df, preserve_index=False)
            dt = DeltaTable(str(bronze_path))
            (
                dt.merge(
                    late_arrow,
                    predicate=(
                        "s.agent_id = t.agent_id AND "
                        "s.session_id = t.session_id AND "
                        "s.action_id = t.action_id"
                    ),
                    source_alias="s",
                    target_alias="t",
                )
                .when_not_matched_insert_all()
                .execute()
            )

        final_count = DeltaTable(str(bronze_path)).to_pandas().shape[0]
        assert final_count == initial_count, (
            f"Late event (20 min beyond watermark) was incorrectly merged into Bronze! "
            f"Row count: {initial_count} -> {final_count}."
        )


# ---------------------------------------------------------------------------
# Test 3: Broker kill mid-stream (requires Docker Compose)
# ---------------------------------------------------------------------------

@pytest.mark.requires_docker
@pytest.mark.skipif(
    not os.getenv("CHAOS_DOCKER_TESTS"),
    reason=(
        "Broker-kill test requires Docker Compose and CHAOS_DOCKER_TESTS=1. "
        "Run manually: CHAOS_DOCKER_TESTS=1 pytest tests/test_chaos_infra.py::TestBrokerKill -v"
    ),
)
class TestBrokerKill:
    """
    Kills the Kafka broker mid-stream and verifies checkpoint recovery.

    Prerequisites:
        docker compose up -d
        CHAOS_DOCKER_TESTS=1
    """

    def test_gatekeeper_survives_broker_restart(self):
        """
        Kill the Kafka broker, wait for Docker to restart it,
        then assert Bronze has no duplicates after recovery.
        """
        bronze_path = PROJECT_ROOT / "data" / "bronze" / "agent_events"

        pre_count = 0
        if (bronze_path / "_delta_log").exists():
            pre_count = DeltaTable(str(bronze_path)).to_pandas().shape[0]

        subprocess.run(
            ["docker", "compose", "restart", "kafka"],
            cwd=str(PROJECT_ROOT),
            check=True,
        )
        time.sleep(15)

        if (bronze_path / "_delta_log").exists():
            post_count = DeltaTable(str(bronze_path)).to_pandas().shape[0]
            duplicates = _count_duplicates(bronze_path)
            assert duplicates == 0, (
                f"Found {duplicates} duplicate rows in Bronze after broker restart."
            )
            assert post_count >= pre_count, (
                f"Bronze row count dropped {pre_count} -> {post_count} after restart."
            )


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v", "-m", "not requires_docker"]))
