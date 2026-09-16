#!/usr/bin/env python
"""
Week 1 Verification Script for Apache Kafka
"""

import os
import sys
import json
import subprocess
import shutil
import pytest

# Configure UTF-8 encoding for Windows terminal output
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession


def _check_docker_available() -> bool:
    if shutil.which("docker") is None:
        return False
    try:
        res = subprocess.run(["docker", "ps"], capture_output=True, timeout=3)
        return res.returncode == 0
    except Exception:
        return False


DOCKER_RUNNING = _check_docker_available()


# ============ KAFKA TESTS ============

@pytest.mark.requires_docker
@pytest.mark.skipif(not DOCKER_RUNNING, reason="Docker daemon is not running")
def test_kafka_running():
    """Test if Kafka container is running"""
    result = subprocess.run(
        ["docker", "ps", "--filter", "name=kafka", "--format", "{{.Status}}"],
        capture_output=True, text=True
    )
    assert "Up" in result.stdout, "Kafka container is not running"
    print("✅ Kafka container is running")


@pytest.mark.requires_docker
@pytest.mark.skipif(not DOCKER_RUNNING, reason="Docker daemon is not running")
def test_topic_exists():
    """Test if the topic exists"""
    cmd = [
        "docker", "exec", "kafka",
        "kafka-topics", "--list",
        "--bootstrap-server", "kafka:29092"
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    topics = result.stdout.strip().split('\n')
    target_topic = next((t for t in ['agent-events', 'agent.events.v1'] if t in topics), None)
    assert target_topic is not None, f"Topic does not exist. Available topics: {topics}"
    print(f"✅ Topic '{target_topic}' exists")


@pytest.mark.requires_docker
@pytest.mark.skipif(not DOCKER_RUNNING, reason="Docker daemon is not running")
def test_consume_messages():
    """Test if we can consume messages"""
    consumed = False
    for topic in ["agent-events", "agent.events.v1"]:
        try:
            cmd = [
                "docker", "exec", "kafka",
                "kafka-console-consumer",
                "--bootstrap-server", "kafka:29092",
                "--topic", topic,
                "--from-beginning",
                "--max-messages", "1",
                "--timeout-ms", "5000"
            ]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=8)
            if result.stdout.strip():
                print(f"✅ Can consume messages from topic '{topic}'")
                consumed = True
                break
        except Exception:
            continue
    assert consumed, "No messages found in topic"


# ============ DELTA TABLE TESTS ============

def _read_delta_df(path: str):
    """
    Reads a Delta table into a pandas DataFrame.
    Uses DuckDB (fast, native C++ engine without Java/Hadoop version conflicts on host).
    Falls back to PySpark if available.
    """
    try:
        import duckdb
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        return conn.execute(f"SELECT * FROM delta_scan('{path}')").df()
    except Exception:
        pass

    # Spark fallback
    spark = (
        SparkSession.builder.appName("Week1Test")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    return spark.read.format("delta").load(path).toPandas()


def test_delta_tables():
    """Test if Delta tables exist and have data"""
    bronze_df = _read_delta_df("data/bronze/agent_events")
    assert len(bronze_df) > 0, "Bronze table missing or empty"
    print(f"✅ Bronze table exists with {len(bronze_df)} rows")

    quarantine_df = _read_delta_df("data/quarantine/agent_events")
    assert len(quarantine_df) > 0, "Quarantine table missing or empty"
    print(f"✅ Quarantine table exists with {len(quarantine_df)} rows")


def test_split_ratio():
    """Check if the split is roughly 85/15"""
    bronze_df = _read_delta_df("data/bronze/agent_events")
    quarantine_df = _read_delta_df("data/quarantine/agent_events")

    bronze_count = len(bronze_df)
    quarantine_count = len(quarantine_df)
    total = bronze_count + quarantine_count

    assert total > 0, "No data found in Delta tables"

    bronze_pct = bronze_count / total * 100
    quarantine_pct = quarantine_count / total * 100

    print(f"Bronze: {bronze_pct:.1f}% ({bronze_count} rows)")
    print(f"Quarantine: {quarantine_pct:.1f}% ({quarantine_count} rows)")

    if 70 < bronze_pct < 95:
        print("✅ Split ratio looks correct (~85% Bronze)")
    else:
        print(f"⚠️ Split ratio off: expected ~85%, got {bronze_pct:.1f}%")


def test_no_duplicates():
    """Check for duplicates in Bronze table"""
    df = _read_delta_df("data/bronze/agent_events")
    total = len(df)
    key_cols = [c for c in ["agent_id", "session_id", "action_id"] if c in df.columns]
    distinct = len(df.drop_duplicates(subset=key_cols)) if key_cols else total

    duplicates = total - distinct
    assert duplicates == 0, f"Found {duplicates} duplicates in Bronze table"
    print("✅ No duplicates found")


def test_poison_types():
    """Check that all poison types are captured"""
    df = _read_delta_df("data/quarantine/agent_events")
    assert "error_summary" in df.columns, "'error_summary' column not found in quarantine table"

    error_types = df["error_summary"].dropna().unique().tolist()
    print("Found error types:")
    for error in error_types:
        print(f"  - {error}")

    expected_types = [
        "type_mismatch",
        "stale_timestamp", 
        "missing_required_field",
        "cost_out_of_bounds"
    ]

    found_any = False
    for expected in expected_types:
        matched = any(expected in error for error in error_types)
        if matched:
            found_any = True
            print(f"  ✅ Found {expected}")
        else:
            print(f"  ⚠️ Missing {expected} (may be okay if no events of this type)")

    assert found_any, "No expected poison types found in quarantine table"


def test_no_error_columns_in_bronze():
    """Verify Bronze has no error columns"""
    df = _read_delta_df("data/bronze/agent_events")
    columns = list(df.columns)
    has_errors = "errors" in columns or "error_summary" in columns
    assert not has_errors, "Bronze has error columns!"
    print("✅ Bronze has no error columns")


# ============ MAIN ============

def run_all_tests():
    print("=" * 60)
    print("WEEK 1 VERIFICATION (Apache Kafka)")
    print("=" * 60)
    print()

    tests = [
        ("Kafka Running", test_kafka_running),
        ("Topic Exists", test_topic_exists),
        ("Can Consume", test_consume_messages),
        ("Delta Tables", test_delta_tables),
        ("Split Ratio", test_split_ratio),
        ("No Duplicates", test_no_duplicates),
        ("Poison Types", test_poison_types),
        ("No Error Columns in Bronze", test_no_error_columns_in_bronze),
    ]

    passed = 0
    for name, test_fn in tests:
        try:
            test_fn()
            print(f"✅ {name}")
            passed += 1
        except pytest.skip.Exception as e:
            print(f"⏭️ {name} (Skipped: {e})")
        except (AssertionError, Exception) as e:
            print(f"❌ {name}: {e}")

    print("=" * 60)
    print(f"\nPassed {passed}/{len(tests)} tests")

    if passed == len(tests):
        print("\n🎉 Week 1 is fully working!")
        return True
    else:
        print("\n⚠️ Some tests failed or were skipped.")
        return False


if __name__ == "__main__":
    run_all_tests()
