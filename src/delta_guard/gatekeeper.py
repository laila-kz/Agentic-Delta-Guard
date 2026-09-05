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

    extra_packages = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"]
    return configure_spark_with_delta_pip(builder, extra_packages=extra_packages).getOrCreate()


def process_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.isEmpty():
        return

    # 1. Parse JSON payload
    parsed_df = batch_df.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("payload")
    ).select("payload.*")

    # 2. Add validation metadata & cast numerical fields safely
    validated_df = (
        parsed_df.withColumn("cost_usd_double", F.col("cost_usd").cast(DoubleType()))
        .withColumn("timestamp_ts", F.to_timestamp(F.col("timestamp")))
        .withColumn(
            "errors",
            F.array_remove(
                F.array(
                    F.when(F.col("agent_id").isNull(), "missing_required_field:agent_id"),
                    F.when(F.col("session_id").isNull(), "missing_required_field:session_id"),
                    F.when(F.col("action_id").isNull(), "missing_required_field:action_id"),
                    F.when(F.col("cost_usd_double").isNull(), "type_mismatch:cost_usd_not_double"),
                    F.when(
                        (F.col("cost_usd_double") < 0.0) | (F.col("cost_usd_double") > 50.0),
                        "semantic_rule:cost_out_of_bounds",
                    ),
                    F.when(F.col("timestamp_ts").isNull(), "parse_error:invalid_timestamp_format"),
                    F.when(
                        F.col("timestamp_ts") < (F.current_timestamp() - F.expr("INTERVAL 24 HOURS")),
                        "freshness:stale_timestamp",
                    ),
                    F.when(
                        F.col("timestamp_ts") > (F.current_timestamp() + F.expr("INTERVAL 5 MINUTES")),
                        "freshness:future_timestamp",
                    ),
                ),
                None,
            ),
        )
    )

    # 3. Distributed Split: Valid vs Quarantine (Executes on Workers)
    valid_df = (
        validated_df.filter(F.size(F.col("errors")) == 0)
        .withColumn("cost_usd", F.col("cost_usd_double"))
        .withColumn("timestamp", F.col("timestamp_ts"))
        .drop("cost_usd_double", "timestamp_ts", "errors")
    )

    quarantine_df = (
        validated_df.filter(F.size(F.col("errors")) > 0)
        .withColumn("quarantined_at", F.current_timestamp())
        .withColumn("error_summary", F.concat_ws("; ", F.col("errors")))
        .drop("cost_usd_double", "timestamp_ts")
    )

    # 4. Distributed Delta Writes
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
