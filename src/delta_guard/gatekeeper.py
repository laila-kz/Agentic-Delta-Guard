"""
gatekeeper.py — Distributed PySpark Structured Streaming Gatekeeper.

Enforces ODCS data contract rules on worker nodes natively via DataFrame column
expressions. Zero Driver memory bottlenecks.
"""
import json
import os
import socket
import sys
import tempfile
import time
import yaml

# ── HADOOP_HOME self-heal (Windows / PySpark) ────────────────────────────────
# On Windows PySpark needs winutils.exe + hadoop.dll.  We bundle them in
# <project_root>/.hadoop/bin.  Override HADOOP_HOME here — before any PySpark
# import — so the JVM always finds the *correct* path for THIS project, even
# when a stale HADOOP_HOME from a different project is set in the environment.
_PROJECT_ROOT = os.path.dirname(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
)
_HADOOP_HOME = os.path.join(_PROJECT_ROOT, ".hadoop")
if os.path.isdir(_HADOOP_HOME):
    os.environ["HADOOP_HOME"] = _HADOOP_HOME
    _hadoop_bin = os.path.join(_HADOOP_HOME, "bin")
    # Prepend hadoop bin to PATH so the JVM's DLL loader finds hadoop.dll
    os.environ["PATH"] = _hadoop_bin + os.pathsep + os.environ.get("PATH", "")
if sys.platform == "win32":
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
# ─────────────────────────────────────────────────────────────────────────────

# ── Java Version Requirement ─────────────────────────────────────────────────
# PySpark 3.5 requires Java 11 or Java 17. Java 21+ is NOT supported because
# Hadoop's UserGroupInformation calls Subject.getSubject() which was removed.
# Use run_gatekeeper.ps1 to launch with Java 17, or set JAVA_HOME manually:
#   $env:JAVA_HOME = "C:\Program Files\Microsoft\jdk-17.0.x.x-hotspot"

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

# Running on your HOST MACHINE (outside Docker) → use localhost:9092
# Running INSIDE a Docker container             → use kafka:29092
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "agent-events"
CONTRACT_PATH = "configs/agent_contract.yaml"
BRONZE_PATH = "data/bronze/agent_events"
QUARANTINE_PATH = "data/quarantine/agent_events"
CHECKPOINT_PATH = "checkpoints/gatekeeper"
STATUS_PATH = os.path.join(_PROJECT_ROOT, "status.json")

EVENT_SCHEMA = StructType(
    [
        StructField("agent_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("action_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("tool_name", StringType(), True),
        StructField("execution_time_ms", IntegerType(), True),
        StructField("cost_usd", StringType(), True),  # string to catch cast mismatches
        StructField("status", StringType(), True),
        StructField("tool_args", StringType(), True),
    ]
)


def _load_contract_thresholds() -> dict:
    """
    Load cost and freshness thresholds from agent_contract.yaml.

    This is the single source of truth for contract bounds — both the
    PySpark gatekeeper and the dbt tests (via sync_contract_to_dbt_vars.py)
    derive their limits from this file.

    Returns a dict with keys:
        max_cost_usd          (float, default 50.0)
        max_event_age_hours   (int,   default 24)
        max_future_skew_mins  (int,   default 5)
        watermark_minutes     (int,   default 10)
        allowed_tools         (list[str], from schema.fields[tool_name].allowed_values)
    """
    import re

    defaults = {
        "max_cost_usd": 50.0,
        "max_event_age_hours": 24,
        "max_future_skew_mins": 5,
        "watermark_minutes": 10,
        "allowed_tools": [],
    }

    contract_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        CONTRACT_PATH,
    )
    if not os.path.exists(contract_file):
        return defaults

    with open(contract_file, "r", encoding="utf-8") as fh:
        contract = yaml.safe_load(fh) or {}

    for field in contract.get("schema", {}).get("fields", []):
        if field.get("name") == "tool_name" and field.get("allowed_values"):
            defaults["allowed_tools"] = list(field["allowed_values"])

    for rule in contract.get("semantic_rules", []):
        rule_text = rule.get("rule", "")
        # Extract max cost bound: cost_usd <= <value>
        m = re.search(r"cost_usd\s*<=\s*([\d.]+)", rule_text)
        if m:
            defaults["max_cost_usd"] = float(m.group(1))
        # Extract freshness window: INTERVAL <N> HOURS
        m = re.search(r"INTERVAL\s+(\d+)\s+HOURS", rule_text)
        if m:
            defaults["max_event_age_hours"] = int(m.group(1))
        # Extract future skew: INTERVAL <N> MINUTES
        m = re.search(r"INTERVAL\s+(\d+)\s+MINUTES", rule_text)
        if m:
            defaults["max_future_skew_mins"] = int(m.group(1))

    return defaults


# Load thresholds at module import time so they are consistent across batches
_THRESHOLDS = _load_contract_thresholds()

# ── Live status accumulator ──────────────────────────────────────────────────
# Written to status.json once per batch commit so the console can poll it.
_STATUS: dict = {
    "bronze_count": 0,
    "quarantine_count": 0,
    "throughput_eps": 0.0,
    "per_signature_counts": {},
    "patch_count": 0,
    "last_patch": None,
    "kafka_online": True,
    "updated_at": "",
}
_batch_times: list = []   # epoch timestamps of recent batch completions


def _write_status() -> None:
    """Atomically write _STATUS to status.json via tmp→rename."""
    _STATUS["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    # Throughput: batches completed in the last 60 s, normalised to events/s
    _batch_times.append(time.time())
    cutoff = time.time() - 60.0
    while _batch_times and _batch_times[0] < cutoff:
        _batch_times.pop(0)
    # rough ev/s: average events per batch × batches per second
    total = _STATUS["bronze_count"] + _STATUS["quarantine_count"]
    batches_per_sec = len(_batch_times) / 60.0
    _STATUS["throughput_eps"] = round(
        (total / max(len(_batch_times), 1)) * batches_per_sec, 2
    )
    # Kafka liveness: probe configured bootstrap server
    try:
        first_broker = BOOTSTRAP_SERVERS.split(",")[0].strip()
        if ":" in first_broker:
            host, port_str = first_broker.split(":", 1)
            port = int(port_str)
        else:
            host, port = first_broker, 9092
        with socket.create_connection((host, port), timeout=0.3):
            _STATUS["kafka_online"] = True
    except OSError:
        _STATUS["kafka_online"] = False
    tmp = STATUS_PATH + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(_STATUS, fh)
    os.replace(tmp, STATUS_PATH)
# ─────────────────────────────────────────────────────────────────────────────


from delta import configure_spark_with_delta_pip

def build_spark() -> SparkSession:
    spark_local_dir = os.path.join(_PROJECT_ROOT, "scratch", "spark_temp").replace("\\", "/")
    os.makedirs(spark_local_dir, exist_ok=True)

    builder = (
        SparkSession.builder.appName("AgenticDeltaGuard-Gatekeeper")
        .master("local[2]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.local.dir", spark_local_dir)
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # Pin session timezone to UTC so current_timestamp() matches the producer's
        # timezone.utc timestamps.  Without this, on a non-UTC host the freshness
        # window check compares UTC producer timestamps against local wall-time,
        # falsely routing valid events to Quarantine as stale or future.
        .config("spark.sql.session.timeZone", "UTC")
        # Disable Hadoop NativeIO on Windows to avoid UnsatisfiedLinkError on
        # NativeIO$Windows.access0 caused by JNI signature mismatch between the
        # bundled hadoop-client-api-3.3.4.jar and the local hadoop.dll.
        # With native IO disabled Spark falls back to pure-Java FileSystem APIs.
        .config("spark.hadoop.io.nativeio.enabled", "false")
        # Force the local filesystem to use the non-FileContext implementation
        # so that AbstractFileContextBasedCheckpointFileManager is never invoked
        # (it is the direct caller of NativeIO$Windows.access0).
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
    )
    # Check if local pre-downloaded jars exist in /workspace/jars
    jars_dir = "/workspace/jars"
    if os.path.exists(jars_dir) and os.listdir(jars_dir):
        jar_files = [os.path.join(jars_dir, f) for f in os.listdir(jars_dir) if f.endswith(".jar")]
        if jar_files:
            builder = builder.config("spark.jars", ",".join(jar_files))
            return builder.getOrCreate()

    extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"]
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()


def process_batch(batch_df: DataFrame, batch_id: int):
    """
    Validate a micro-batch of Kafka events against the agent contract.

    Contract thresholds are loaded from agent_contract.yaml at startup so
    there is a single source of truth shared with the dbt tests.

    Watermarking (10 minutes) is applied via event_timestamp before
    deduplication so out-of-order events beyond the watermark are dropped
    rather than silently merged as new rows.
    """
    max_cost = _THRESHOLDS["max_cost_usd"]
    age_hours = _THRESHOLDS["max_event_age_hours"]
    skew_mins = _THRESHOLDS["max_future_skew_mins"]
    watermark_mins = _THRESHOLDS["watermark_minutes"]
    allowed_tools = _THRESHOLDS["allowed_tools"]

    # 1. Parse JSON payload while preserving raw wire bytes & Kafka metadata
    has_kafka_meta = "topic" in batch_df.columns and "offset" in batch_df.columns
    base_df = batch_df.withColumn("raw_payload", F.col("value").cast("string"))
    if has_kafka_meta:
        base_df = (
            base_df
            .withColumn("kafka_topic", F.col("topic"))
            .withColumn("kafka_partition", F.col("partition"))
            .withColumn("kafka_offset", F.col("offset").cast(StringType()))
        )
    else:
        base_df = (
            base_df
            .withColumn("kafka_topic", F.lit(None).cast(StringType()))
            .withColumn("kafka_partition", F.lit(None).cast(IntegerType()))
            .withColumn("kafka_offset", F.lit(None).cast(StringType()))
        )

    parsed_df = base_df.withColumn(
        "payload", F.from_json(F.col("raw_payload"), EVENT_SCHEMA)
    ).select(
        "raw_payload",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "payload",
        "payload.*",
    )

    # 2. Parse event timestamp early for validation and chronological tracking.
    #    Note: Intra-batch deduplication operates on this micro-batch DataFrame,
    #    while end-to-end cross-batch idempotency is guaranteed by Delta Lake's ACID MERGE INTO.
    parsed_with_ts = parsed_df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("timestamp")),
    )
    watermarked_df = parsed_with_ts.withWatermark(
        "event_timestamp", f"{watermark_mins} minutes"
    )

    # 3. Deduplicate within micro-batch on the idempotency key (valid keys only)
    valid_keys_df = watermarked_df.filter(
        F.col("agent_id").isNotNull()
        & F.col("session_id").isNotNull()
        & F.col("action_id").isNotNull()
    ).dropDuplicates(["agent_id", "session_id", "action_id"])

    null_keys_df = watermarked_df.filter(
        F.col("agent_id").isNull()
        | F.col("session_id").isNull()
        | F.col("action_id").isNull()
    )

    deduplicated_df = valid_keys_df.unionByName(null_keys_df)

    # 4. Add validation metadata and cast numerical fields safely
    validated_df = (
        deduplicated_df
        .withColumn("cost_usd_double", F.col("cost_usd").cast(DoubleType()))
        .withColumn("timestamp_ts", F.col("event_timestamp"))
        .withColumn(
            "errors",
            F.filter(
                F.array(
                    F.when(F.col("payload").isNull(), "parse_error:invalid_json"),
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
                    *(
                        [
                            F.when(
                                F.col("tool_name").isNotNull()
                                & ~F.col("tool_name").isin(allowed_tools),
                                "semantic_rule:tool_not_allowed",
                            )
                        ]
                        if allowed_tools
                        else []
                    ),
                ),
                lambda error_message: error_message.isNotNull(),
            ),
        )
    )

    # 5. Distributed Split: Valid vs Quarantine (executes on workers)
    valid_df = (
        validated_df.filter(F.coalesce(F.size(F.col("errors")), F.lit(0)) == 0)
        .withColumn("cost_usd", F.col("cost_usd_double"))
        .withColumn("timestamp", F.col("timestamp_ts"))
        .drop(
            "payload",
            "cost_usd_double",
            "timestamp_ts",
            "errors",
            "event_timestamp",
            "raw_payload",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
        )
    )

    quarantine_df = (
        validated_df.filter(F.coalesce(F.size(F.col("errors")), F.lit(0)) > 0)
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("error_summary", F.concat_ws("; ", F.col("errors")))
        .withColumn(
            "quarantine_id",
            F.coalesce(
                F.when(
                    F.col("action_id").isNotNull() & (F.col("action_id") != "") & F.col("agent_id").isNotNull(),
                    F.concat_ws("#", F.col("agent_id"), F.coalesce(F.col("session_id"), F.lit("unknown")), F.col("action_id")),
                ),
                F.when(
                    F.col("kafka_offset").isNotNull(),
                    F.concat_ws("#", F.coalesce(F.col("kafka_topic"), F.lit("topic")), F.coalesce(F.col("kafka_partition").cast("string"), F.lit("0")), F.col("kafka_offset")),
                ),
                F.sha2(F.coalesce(F.col("raw_payload"), F.lit("empty")), 256),
            ),
        )
        .drop("payload", "cost_usd_double", "timestamp_ts", "event_timestamp")
    )

    # 6. Distributed Delta Writes
    valid_count = valid_df.count()
    quarantine_count = quarantine_df.count()

    if valid_count > 0:
        merge_into_bronze(batch_df.sparkSession, valid_df)

    if quarantine_count > 0:
        merge_into_quarantine(batch_df.sparkSession, quarantine_df)
        # Accumulate per-signature error counts for the console
        sig_rows = (
            quarantine_df
            .select(F.explode(F.col("errors")).alias("sig"))
            .groupBy("sig")
            .count()
            .collect()
        )
        for row in sig_rows:
            sig = row["sig"]
            _STATUS["per_signature_counts"][sig] = (
                _STATUS["per_signature_counts"].get(sig, 0) + row["count"]
            )

    # 7. Update live status file (once per batch, not per event)
    _STATUS["bronze_count"] += valid_count
    _STATUS["quarantine_count"] += quarantine_count
    print(
        f"[gatekeeper] batch={batch_id} "
        f"valid={valid_count} quarantined={quarantine_count} "
        f"total_bronze={_STATUS['bronze_count']} total_q={_STATUS['quarantine_count']}"
    )
    _write_status()


def merge_into_bronze(spark: SparkSession, valid_df: DataFrame):
    if not DeltaTable.isDeltaTable(spark, BRONZE_PATH):
        valid_df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
        return

    bronze_table = DeltaTable.forPath(spark, BRONZE_PATH)
    (
        bronze_table.alias("target")
        .merge(
            valid_df.alias("source"),
            """
            target.agent_id = source.agent_id AND 
            target.session_id = source.session_id AND 
            target.action_id = source.action_id
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def merge_into_quarantine(spark: SparkSession, quarantine_df: DataFrame):
    """
    Idempotent Delta MERGE for quarantine table to prevent duplicate violation records on replay.
    Uses quarantine_id (or fallback composite key) to ensure identical invalid actions aren't duplicated.
    """
    if not DeltaTable.isDeltaTable(spark, QUARANTINE_PATH):
        quarantine_df.write.format("delta").mode("append").option("mergeSchema", "true").save(QUARANTINE_PATH)
        return

    quarantine_table = DeltaTable.forPath(spark, QUARANTINE_PATH)
    target_columns = quarantine_table.toDF().columns
    if "quarantine_id" in target_columns:
        merge_cond = "target.quarantine_id = source.quarantine_id"
    else:
        merge_cond = """
            (target.action_id IS NOT NULL AND target.action_id = source.action_id AND target.agent_id = source.agent_id)
            OR (target.raw_payload IS NOT NULL AND target.raw_payload = source.raw_payload)
        """

    (
        quarantine_table.alias("target")
        .merge(quarantine_df.alias("source"), merge_cond)
        .whenNotMatchedInsertAll()
        .execute()
    )


def main():
    import argparse
    from pathlib import Path
    import shutil

    parser = argparse.ArgumentParser(description="Distributed PySpark Gatekeeper")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Process all available batches and exit cleanly (availableNow=True)",
    )
    parser.add_argument(
        "--reset-checkpoint",
        action="store_true",
        help="Clear checkpoint directory to recover from Kafka restarts",
    )
    args = parser.parse_args()

    checkpoint_dir = Path(_PROJECT_ROOT) / CHECKPOINT_PATH
    if args.reset_checkpoint and checkpoint_dir.exists():
        shutil.rmtree(checkpoint_dir, ignore_errors=True)
        print(f"[gatekeeper] Cleared stale checkpoint directory: {checkpoint_dir}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[gatekeeper] Contract thresholds loaded: {_THRESHOLDS}")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    writer = (
        raw_stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
    )

    if args.once:
        print("[gatekeeper] Processing all available events in Kafka (availableNow=True)...")
        query = writer.trigger(availableNow=True).start()
        query.awaitTermination()
        print("[gatekeeper] Micro-batch execution completed successfully.")
    else:
        print("[gatekeeper] Distributed PySpark Gatekeeper running — press Ctrl+C to stop")
        query = writer.trigger(processingTime="5 seconds").start()
        query.awaitTermination()


if __name__ == "__main__":
    main()
