"""Delta Lake production governance, optimization, and time-travel utilities.

Provides reusable operations for:
- Storage optimization (`OPTIMIZE` with multi-dimensional `Z-ORDER BY`)
- Safe file retention pruning (`VACUUM`)
- Table history auditing
- ACID Time Travel (point-in-time querying and rollbacks)
- Change Data Feed (CDF) operations
"""

from __future__ import annotations

import os
from typing import List, Optional, Union
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession


def is_path_target(target: str) -> bool:
    """Determine if a target identifier is a directory path or a catalog table name."""
    return (
        "/" in target
        or "\\" in target
        or target.startswith(".")
        or target.startswith("dbfs:")
        or target.startswith("s3:")
        or target.startswith("abfss:")
    )


def get_delta_table(spark: SparkSession, target: str) -> DeltaTable:
    """Retrieve a DeltaTable instance by either table path or catalog table name."""
    if is_path_target(target):
        return DeltaTable.forPath(spark, target)
    return DeltaTable.forName(spark, target)


def optimize_table(
    spark: SparkSession,
    target: str,
    zorder_cols: Optional[List[str]] = None,
    partition_filter: Optional[str] = None,
) -> DataFrame:
    """Optimize table file storage layout and optionally cluster using Z-ORDER.

    Compacts small files into uniform target file sizes (default 1GB on Databricks)
    and clusters data spatially on high-cardinality join and filter columns.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    target : str
        Delta table path or registered table name.
    zorder_cols : Optional[List[str]]
        List of columns to apply multi-dimensional Z-Ordering to.
    partition_filter : Optional[str]
        Optional partition predicate to restrict optimization (e.g. "order_date >= '2026-01-01'").

    Returns
    -------
    DataFrame
        Optimization metrics summary (files added, removed, bytes compacted).
    """
    delta_table = get_delta_table(spark, target)
    opt = delta_table.optimize()

    if partition_filter:
        opt = opt.where(partition_filter)

    if zorder_cols and len(zorder_cols) > 0:
        return opt.executeZOrderBy(*zorder_cols)
    else:
        return opt.executeCompaction()


def vacuum_table(
    spark: SparkSession,
    target: str,
    retention_hours: float = 168.0,
    check_retention_duration: bool = True,
) -> DataFrame:
    """Remove historical data files no longer referenced by the Delta transaction log.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    target : str
        Delta table path or registered table name.
    retention_hours : float
        Retention threshold in hours (default: 168 hours / 7 days).
    check_retention_duration : bool
        If False, disables the safety check preventing retention < 168h (useful for testing).

    Returns
    -------
    DataFrame
        Vacuum result DataFrame.
    """
    if not check_retention_duration:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    delta_table = get_delta_table(spark, target)
    res = delta_table.vacuum(retention_hours)

    if not check_retention_duration:
        spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "true")

    return res


def get_table_history(
    spark: SparkSession,
    target: str,
    limit: int = 20,
) -> DataFrame:
    """Retrieve the audit log and commit history of a Delta table.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    target : str
        Delta table path or registered table name.
    limit : int
        Maximum number of historical commits to return.

    Returns
    -------
    DataFrame
        Commit history DataFrame sorted in descending chronological order.
    """
    delta_table = get_delta_table(spark, target)
    return delta_table.history(limit)


def read_version_as_of(
    spark: SparkSession,
    target: str,
    version: int,
) -> DataFrame:
    """Query a Delta table at a specific historical commit version using Time Travel.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    target : str
        Delta table path or registered table name.
    version : int
        Zero-indexed Delta commit version.

    Returns
    -------
    DataFrame
        Snapshot of the table at the specified version.
    """
    if is_path_target(target):
        return (
            spark.read.format("delta")
            .option("versionAsOf", version)
            .load(target)
        )
    return (
        spark.read.format("delta")
        .option("versionAsOf", version)
        .table(target)
    )


def read_timestamp_as_of(
    spark: SparkSession,
    target: str,
    timestamp_str: str,
) -> DataFrame:
    """Query a Delta table at a specific point in time using Time Travel.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    target : str
        Delta table path or registered table name.
    timestamp_str : str
        ISO-8601 formatted timestamp string (e.g., '2026-10-10 12:00:00').

    Returns
    -------
    DataFrame
        Snapshot of the table at the specified timestamp.
    """
    if is_path_target(target):
        return (
            spark.read.format("delta")
            .option("timestampAsOf", timestamp_str)
            .load(target)
        )
    return (
        spark.read.format("delta")
        .option("timestampAsOf", timestamp_str)
        .table(target)
    )


def restore_table(
    spark: SparkSession,
    target: str,
    to_version: Optional[int] = None,
    to_timestamp: Optional[str] = None,
) -> DataFrame:
    """Roll back a Delta table state to an earlier commit version or timestamp.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    target : str
        Delta table path or registered table name.
    to_version : Optional[int]
        Target version to roll back to.
    to_timestamp : Optional[str]
        Target ISO timestamp to roll back to.

    Returns
    -------
    DataFrame
        Restoration metrics.
    """
    delta_table = get_delta_table(spark, target)

    if to_version is not None:
        return delta_table.restoreToVersion(to_version)
    elif to_timestamp is not None:
        return delta_table.restoreToTimestamp(to_timestamp)
    else:
        raise ValueError("Must specify either `to_version` or `to_timestamp` to restore.")


def enable_change_data_feed(
    spark: SparkSession,
    target: str,
) -> None:
    """Enable Delta Change Data Feed (CDF) for granular row-level CDC auditing."""
    if is_path_target(target):
        spark.sql(f"ALTER TABLE delta.`{target}` SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
    else:
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
