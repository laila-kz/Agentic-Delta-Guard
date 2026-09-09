"""
gatekeeper.py — Distributed PySpark Structured Streaming Gatekeeper.

Enforces ODCS data contract rules on worker nodes natively via DataFrame column
expressions. Zero Driver memory bottlenecks.
"""
import os
import yaml

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
BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = "agent-events"
CONTRACT_PATH = "configs/agent_contract.yaml"
BRONZE_PATH = "data/bronze/agent_events"
QUARANTINE_PATH = "data/quarantine/agent_events"
CHECKPOINT_PATH = "checkpoints/gatekeeper"

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
    """
    import re

    defaults = {
        "max_cost_usd": 50.0,
        "max_event_age_hours": 24,
        "max_future_skew_mins": 5,
        "watermark_minutes": 10,
    }

    contract_file = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        CONTRACT_PATH,
    )
    if not os.path.exists(contract_file):
        return defaults

    with open(contract_file, "r", encoding="utf-8") as fh:
        contract = yaml.safe_load(fh) or {}

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


from delta import configure_spark_with_delta_pip


def build_spark() -> SparkSession:
    builder = (
        SparkSession.builder.appName("AgenticDeltaGuard-Gatekeeper")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
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
    if batch_df.isEmpty():
        return

    max_cost = _THRESHOLDS["max_cost_usd"]
    age_hours = _THRESHOLDS["max_event_age_hours"]
    skew_mins = _THRESHOLDS["max_future_skew_mins"]
    watermark_mins = _THRESHOLDS["watermark_minutes"]

    # 1. Parse JSON payload
    parsed_df = batch_df.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("payload")
    ).select("payload.*")

    # 2. Parse event timestamp early so we can apply watermarking.
    #    withWatermark() tells Spark to drop state for events older than
    #    <watermark_mins> minutes relative to the max seen event_timestamp.
    #    Events arriving beyond this window are dropped, not duplicated.
    parsed_with_ts = parsed_df.withColumn(
        "event_timestamp",
        F.to_timestamp(F.col("timestamp")),
    )
    watermarked_df = parsed_with_ts.withWatermark(
        "event_timestamp", f"{watermark_mins} minutes"
    )

    # 3. Deduplicate within the watermark window on the idempotency key
    deduplicated_df = watermarked_df.dropDuplicates(
        ["agent_id", "session_id", "action_id"]
    )

    # 4. Add validation metadata and cast numerical fields safely
    validated_df = (
        deduplicated_df
        .withColumn("cost_usd_double", F.col("cost_usd").cast(DoubleType()))
        .withColumn("timestamp_ts", F.col("event_timestamp"))
        .withColumn(
            "errors",
            F.array_remove(
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
                None,
            ),
        )
    )

    # 5. Distributed Split: Valid vs Quarantine (executes on workers)
    valid_df = (
        validated_df.filter(F.size(F.col("errors")) == 0)
        .withColumn("cost_usd", F.col("cost_usd_double"))
        .withColumn("timestamp", F.col("timestamp_ts"))
        .drop("cost_usd_double", "timestamp_ts", "errors", "event_timestamp")
    )

    quarantine_df = (
        validated_df.filter(F.size(F.col("errors")) > 0)
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("error_summary", F.concat_ws("; ", F.col("errors")))
        .drop("cost_usd_double", "timestamp_ts", "event_timestamp")
    )

    # 6. Distributed Delta Writes
    if not valid_df.isEmpty():
        merge_into_bronze(batch_df.sparkSession, valid_df)

    if not quarantine_df.isEmpty():
        (
            quarantine_df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(QUARANTINE_PATH)
        )


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


def main():
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[gatekeeper] Contract thresholds loaded: {_THRESHOLDS}")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "earliest")
        .load()
    )

    query = (
        raw_stream.writeStream.foreachBatch(process_batch)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(processingTime="10 seconds")
        .start()
    )

    print("[gatekeeper] Distributed PySpark Gatekeeper running — press Ctrl+C to stop")
    query.awaitTermination()


if __name__ == "__main__":
    main()
