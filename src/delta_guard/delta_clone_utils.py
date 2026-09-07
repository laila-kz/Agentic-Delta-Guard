"""
delta_clone_utils.py — Zero-copy shallow cloning and time travel utilities for Delta Lake.

This module provides production-ready shallow clone and time travel capabilities
using the Delta Lake 3.x APIs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


class DeltaCloneUtils:
    """Utilities for creating shallow clones and time-travel queries on Delta tables."""

    def __init__(self, spark: Optional[SparkSession] = None):
        """
        Initialize DeltaCloneUtils.

        Args:
            spark: SparkSession. If None, will create a new session.
        """
        if spark is None:
            self.spark = (
                SparkSession.builder.appName("delta_clone_utils")
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate()
            )
        else:
            self.spark = spark

    def create_shallow_clone(
        self,
        source_table_path: str | Path,
        target_table_path: str | Path,
        replace: bool = False,
    ) -> dict:
        """
        Create a zero-copy shallow clone of a Delta table.

        Shallow clones share data files with the source table, so they:
        - Consume minimal storage (only metadata)
        - Are independent (modifications don't affect source)
        - Support full ACID semantics
        - Are ideal for sandboxing and testing

        Args:
            source_table_path: Path to source Delta table
            target_table_path: Path to target shallow clone
            replace: If True, replace existing target table

        Returns:
            dict with clone metadata (duration, source rows, target rows, etc.)
        """
        source_path = str(Path(source_table_path).resolve())
        target_path = str(Path(target_table_path).resolve())

        logger.info(f"Creating shallow clone: {source_path} → {target_path}")
        start_time = datetime.now()

        try:
            # Use Delta Lake clone API
            source_delta = DeltaTable.forPath(self.spark, source_path)
            source_delta.clone(
                target=target_path,
                isShallow=True,
                replace=replace,
            )

            # Gather metrics
            target_delta = DeltaTable.forPath(self.spark, target_path)
            source_count = source_delta.toDF().count()
            target_count = target_delta.toDF().count()
            clone_duration = (datetime.now() - start_time).total_seconds()

            result = {
                "status": "success",
                "source_path": source_path,
                "target_path": target_path,
                "source_row_count": source_count,
                "target_row_count": target_count,
                "clone_duration_seconds": clone_duration,
                "clone_type": "shallow",
                "timestamp": datetime.now().isoformat(),
            }

            logger.info(
                f"Shallow clone created in {clone_duration:.2f}s. "
                f"Rows: {source_count} → {target_count}"
            )
            return result

        except Exception as e:
            logger.error(f"Failed to create shallow clone: {e}")
            raise

    def create_deep_clone(
        self,
        source_table_path: str | Path,
        target_table_path: str | Path,
        replace: bool = False,
    ) -> dict:
        """
        Create a deep clone of a Delta table (independent copy of all data).

        Deep clones:
        - Are completely independent from source
        - Consume full disk space (copy of all data)
        - Are ideal for data snapshots and archives
        - Support full ACID semantics

        Args:
            source_table_path: Path to source Delta table
            target_table_path: Path to target deep clone
            replace: If True, replace existing target table

        Returns:
            dict with clone metadata (duration, source rows, target rows, disk size, etc.)
        """
        source_path = str(Path(source_table_path).resolve())
        target_path = str(Path(target_table_path).resolve())

        logger.info(f"Creating deep clone: {source_path} → {target_path}")
        start_time = datetime.now()

        try:
            # Use Delta Lake clone API with isShallow=False
            source_delta = DeltaTable.forPath(self.spark, source_path)
            source_delta.clone(
                target=target_path,
                isShallow=False,
                replace=replace,
            )

            # Gather metrics
            target_delta = DeltaTable.forPath(self.spark, target_path)
            source_count = source_delta.toDF().count()
            target_count = target_delta.toDF().count()
            clone_duration = (datetime.now() - start_time).total_seconds()

            result = {
                "status": "success",
                "source_path": source_path,
                "target_path": target_path,
                "source_row_count": source_count,
                "target_row_count": target_count,
                "clone_duration_seconds": clone_duration,
                "clone_type": "deep",
                "timestamp": datetime.now().isoformat(),
            }

            logger.info(
                f"Deep clone created in {clone_duration:.2f}s. "
                f"Rows: {source_count} → {target_count}"
            )
            return result

        except Exception as e:
            logger.error(f"Failed to create deep clone: {e}")
            raise

    def read_at_version(
        self,
        table_path: str | Path,
        version: int,
    ) -> pd.DataFrame:
        """
        Read a Delta table at a specific version (time travel).

        Args:
            table_path: Path to Delta table
            version: Version number to read

        Returns:
            pandas DataFrame of the table at the specified version
        """
        table_path = str(Path(table_path).resolve())
        logger.info(f"Reading {table_path} at version {version}")

        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            df = delta_table.toDF().select("*").where(f"dbt_version = {version}")
            return df.toPandas()
        except Exception as e:
            logger.error(f"Failed to read table at version {version}: {e}")
            raise

    def read_at_timestamp(
        self,
        table_path: str | Path,
        timestamp: str | datetime,
    ) -> pd.DataFrame:
        """
        Read a Delta table at a specific point in time (time travel).

        Args:
            table_path: Path to Delta table
            timestamp: ISO format timestamp string or datetime object

        Returns:
            pandas DataFrame of the table at the specified timestamp
        """
        table_path = str(Path(table_path).resolve())
        if isinstance(timestamp, datetime):
            timestamp_str = timestamp.isoformat()
        else:
            timestamp_str = timestamp

        logger.info(f"Reading {table_path} at timestamp {timestamp_str}")

        try:
            # Use Spark SQL with time travel syntax
            df = self.spark.sql(
                f"SELECT * FROM delta.`{table_path}` TIMESTAMP AS OF '{timestamp_str}'"
            )
            return df.toPandas()
        except Exception as e:
            logger.error(f"Failed to read table at timestamp {timestamp_str}: {e}")
            raise

    def get_table_history(
        self,
        table_path: str | Path,
        limit: int = 10,
    ) -> pd.DataFrame:
        """
        Get the commit history of a Delta table.

        Args:
            table_path: Path to Delta table
            limit: Maximum number of history records to return

        Returns:
            pandas DataFrame with version, timestamp, operation, and user info
        """
        table_path = str(Path(table_path).resolve())
        logger.info(f"Fetching history for {table_path} (limit: {limit})")

        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            history_df = delta_table.history(limit=limit)
            return history_df.toPandas()
        except Exception as e:
            logger.error(f"Failed to get table history: {e}")
            raise

    def restore_to_version(
        self,
        table_path: str | Path,
        version: int,
    ) -> dict:
        """
        Restore a Delta table to a specific version.

        WARNING: This overwrites the current table state.

        Args:
            table_path: Path to Delta table
            version: Version number to restore to

        Returns:
            dict with restoration metadata
        """
        table_path = str(Path(table_path).resolve())
        logger.warning(f"Restoring {table_path} to version {version}")

        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            delta_table.restoreToVersion(version)

            result = {
                "status": "success",
                "table_path": table_path,
                "restored_to_version": version,
                "timestamp": datetime.now().isoformat(),
            }

            logger.info(f"Table restored to version {version}")
            return result

        except Exception as e:
            logger.error(f"Failed to restore to version {version}: {e}")
            raise

    def get_table_details(
        self,
        table_path: str | Path,
    ) -> dict:
        """
        Get detailed metadata about a Delta table.

        Args:
            table_path: Path to Delta table

        Returns:
            dict with schema, row count, version, creation time, etc.
        """
        table_path = str(Path(table_path).resolve())

        try:
            delta_table = DeltaTable.forPath(self.spark, table_path)
            detail = delta_table.detail()
            detail_row = detail.collect()[0]

            return {
                "table_path": table_path,
                "format": "delta",
                "row_count": delta_table.toDF().count(),
                "size_bytes": detail_row.get("sizeInBytes"),
                "num_files": detail_row.get("numFiles"),
                "created_at": detail_row.get("createdAt"),
                "last_modified": detail_row.get("lastModified"),
                "version": self._get_latest_version(table_path),
            }

        except Exception as e:
            logger.error(f"Failed to get table details: {e}")
            raise

    @staticmethod
    def _get_latest_version(table_path: str) -> int:
        """Get the latest version number of a Delta table."""
        from delta import DeltaLog

        delta_log = DeltaLog.forTable(SparkSession.getActiveSession(), table_path)
        return delta_log.lastCommittedVersionId


if __name__ == "__main__":
    # Example usage
    logging.basicConfig(level=logging.INFO)

    clone_utils = DeltaCloneUtils()

    # Example: Create shallow clone
    # result = clone_utils.create_shallow_clone(
    #     source_table_path="data/gold/agent_analytics",
    #     target_table_path="data/sandbox/agent_analytics_sandbox",
    #     replace=True,
    # )
    # print(result)
