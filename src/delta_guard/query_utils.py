"""
query_utils.py — Helper functions to query Bronze and Quarantine Delta tables.
Supports fast DuckDB query on host machine without Java/Spark conflicts, with PySpark fallback.
"""
import sys

# Configure UTF-8 encoding for Windows terminal output
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def _read_delta(path: str):
    try:
        import duckdb
        conn = duckdb.connect()
        conn.execute("INSTALL delta; LOAD delta;")
        return conn.execute(f"SELECT * FROM delta_scan('{path}')").df()
    except Exception:
        pass

    from pyspark.sql import SparkSession
    spark = (
        SparkSession.builder.appName("QueryUtils")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
        .getOrCreate()
    )
    return spark.read.format("delta").load(path).toPandas()


def show_bronze():
    df = _read_delta("data/bronze/agent_events")
    print(f"\n=== Bronze Delta Table (Total Rows: {len(df)}) ===")
    if "timestamp" in df.columns:
        df = df.sort_values(by="timestamp", ascending=False)
    print(df.head(10).to_string(index=False))


def show_quarantine():
    df = _read_delta("data/quarantine/agent_events")
    print(f"\n=== Quarantine Delta Table (Total Rows: {len(df)}) ===")
    cols = [c for c in ["agent_id", "cost_usd", "error_summary", "quarantined_at"] if c in df.columns]
    print(df[cols].head(10).to_string(index=False))


if __name__ == "__main__":
    show_bronze()
    show_quarantine()
