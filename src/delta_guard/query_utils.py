"""
query_utils.py — Helper functions to query Bronze and Quarantine Delta tables.
"""
from pyspark.sql import SparkSession


def _spark():
    return (
        SparkSession.builder.appName("QueryUtils")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .getOrCreate()
    )


def show_bronze():
    df = _spark().read.format("delta").load("data/bronze/agent_events")
    print(f"\n=== Bronze Delta Table (Total Rows: {df.count()}) ===")
    df.orderBy("timestamp", ascending=False).show(10, truncate=False)


def show_quarantine():
    df = _spark().read.format("delta").load("data/quarantine/agent_events")
    print(f"\n=== Quarantine Delta Table (Total Rows: {df.count()}) ===")
    df.select("agent_id", "tool_name", "error_summary", "quarantined_at").show(
        10, truncate=False
    )


if __name__ == "__main__":
    show_bronze()
    show_quarantine()
