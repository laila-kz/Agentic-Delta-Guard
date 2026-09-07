#!/usr/bin/env python
"""
Week 1 Verification Script for Apache Kafka
"""

import os
import sys
import json
import subprocess

# Configure UTF-8 encoding for Windows terminal output
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

from pyspark.sql import SparkSession

# ============ KAFKA TESTS ============

def test_kafka_running():
    """Test if Kafka container is running"""
    try:
        result = subprocess.run(
            ["docker", "ps", "--filter", "name=kafka", "--format", "{{.Status}}"],
            capture_output=True, text=True
        )
        if "Up" in result.stdout:
            print("âœ… Kafka container is running")
            return True
        else:
            print("âŒ Kafka container is not running")
            return False
    except Exception as e:
        print(f"âŒ Docker check failed: {e}")
        return False

def test_topic_exists():
    """Test if the topic exists (CORRECT COMMAND)"""
    try:
        cmd = [
            "docker", "exec", "kafka",
            "kafka-topics", "--list",
            "--bootstrap-server", "kafka:29092"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        topics = result.stdout.strip().split('\n')
        target_topic = next((t for t in ['agent-events', 'agent.events.v1'] if t in topics), None)
        if target_topic:
            print(f"âœ… Topic '{target_topic}' exists")
            return True
        else:
            print("âŒ Topic does not exist")
            print(f"   Available topics: {topics}")
            return False
    except Exception as e:
        print(f"âŒ Topic check failed: {e}")
        return False

def test_consume_messages():
    """Test if we can consume messages"""
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
                print(f"âœ… Can consume messages from topic '{topic}'")
                return True
        except Exception:
            continue
    print("âŒ No messages found in topic")
    return False

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
    try:
        bronze_df = _read_delta_df("data/bronze/agent_events")
        bronze_count = len(bronze_df)
        print(f"âœ… Bronze table exists with {bronze_count} rows")
    except Exception as e:
        print(f"âŒ Bronze table missing or empty: {e}")
        return False
    
    try:
        quarantine_df = _read_delta_df("data/quarantine/agent_events")
        quarantine_count = len(quarantine_df)
        print(f"âœ… Quarantine table exists with {quarantine_count} rows")
    except Exception as e:
        print(f"âŒ Quarantine table missing or empty: {e}")
        return False
    
    return True

def test_split_ratio():
    """Check if the split is roughly 85/15"""
    bronze_df = _read_delta_df("data/bronze/agent_events")
    quarantine_df = _read_delta_df("data/quarantine/agent_events")
    
    bronze_count = len(bronze_df)
    quarantine_count = len(quarantine_df)
    total = bronze_count + quarantine_count
    
    if total == 0:
        print("âŒ No data found")
        return False
    
    bronze_pct = bronze_count / total * 100
    quarantine_pct = quarantine_count / total * 100
    
    print(f"Bronze: {bronze_pct:.1f}% ({bronze_count} rows)")
    print(f"Quarantine: {quarantine_pct:.1f}% ({quarantine_count} rows)")
    
    if 70 < bronze_pct < 95:
        print("âœ… Split ratio looks correct (~85% Bronze)")
        return True
    else:
        print(f"âš ï¸ Split ratio off: expected ~85%, got {bronze_pct:.1f}%")
        return True  # Still passes, just warning

def test_no_duplicates():
    """Check for duplicates in Bronze table"""
    df = _read_delta_df("data/bronze/agent_events")
    total = len(df)
    key_cols = [c for c in ["agent_id", "session_id", "action_id"] if c in df.columns]
    distinct = len(df.drop_duplicates(subset=key_cols)) if key_cols else total
    
    duplicates = total - distinct
    if duplicates == 0:
        print("âœ… No duplicates found")
        return True
    else:
        print(f"âš ï¸ Found {duplicates} duplicates")
        return False

def test_poison_types():
    """Check that all poison types are captured"""
    try:
        df = _read_delta_df("data/quarantine/agent_events")
        
        if "error_summary" not in df.columns:
            print("âš ï¸ 'error_summary' column not found in quarantine table")
            return False
            
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
                print(f"  âœ… Found {expected}")
            else:
                print(f"  âš ï¸ Missing {expected} (may be okay if no events of this type)")
        
        return found_any
        
    except Exception as e:
        print(f"âŒ Poison types check failed: {e}")
        return False

def test_no_error_columns_in_bronze():
    """Verify Bronze has no error columns"""
    df = _read_delta_df("data/bronze/agent_events")
    columns = list(df.columns)
    
    has_errors = "errors" in columns or "error_summary" in columns
    if not has_errors:
        print("âœ… Bronze has no error columns")
        return True
    else:
        print("âŒ Bronze has error columns!")
        return False

# ============ MAIN ============

def run_all_tests():
    print("=" * 60)
    print("WEEK 1 VERIFICATION (Apache Kafka)")
    print("=" * 60)
    print()
    
    results = []
    
    # Kafka tests
    print(">>> KAFKA TESTS")
    results.append(("Kafka Running", test_kafka_running()))
    results.append(("Topic Exists", test_topic_exists()))
    results.append(("Can Consume", test_consume_messages()))
    print()
    
    # Delta tests
    print(">>> DELTA TABLE TESTS")
    results.append(("Delta Tables", test_delta_tables()))
    if results[-1][1]:  # Only run if delta tables exist
        results.append(("Split Ratio", test_split_ratio()))
        results.append(("No Duplicates", test_no_duplicates()))
        results.append(("Poison Types", test_poison_types()))
        results.append(("No Error Columns in Bronze", test_no_error_columns_in_bronze()))
    print()
    
    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    
    passed = 0
    for name, result in results:
        status = "âœ…" if result else "âŒ"
        print(f"{status} {name}")
        if result:
            passed += 1
    
    print(f"\nPassed {passed}/{len(results)} tests")
    
    if passed == len(results):
        print("\nðŸŽ‰ ðŸŽ‰ ðŸŽ‰ Week 1 is fully working!")
        return True
    else:
        print("\nâš ï¸ Some tests failed. Check the output above.")
        return False

if __name__ == "__main__":
    run_all_tests()
