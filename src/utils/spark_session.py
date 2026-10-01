"""Spark session builder and runtime configuration utility.

Provides a unified factory function to obtain a Delta-enabled SparkSession
configured with production-ready distributed performance settings (AQE,
dynamic partition coalescing, skew join optimization, and Delta catalogs)
both on local development workstations and within Databricks runtimes.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, Optional
import yaml

from pyspark.sql import SparkSession


def load_pipeline_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load the pipeline YAML configuration file.

    Parameters
    ----------
    config_path : Optional[str]
        Path to pipeline_config.yaml. If omitted, searches default locations.

    Returns
    -------
    Dict[str, Any]
        Parsed configuration dictionary.
    """
    if config_path is None:
        # Check standard relative paths
        base_dir = Path(__file__).resolve().parent.parent.parent
        config_path = str(base_dir / "configs" / "pipeline_config.yaml")

    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def is_databricks_runtime() -> bool:
    """Detect whether the current process is running inside a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ or "DB_CLUSTER_ID" in os.environ


def get_spark_session(
    app_name: Optional[str] = None,
    master: Optional[str] = None,
    extra_configs: Optional[Dict[str, str]] = None,
    config_path: Optional[str] = None,
) -> SparkSession:
    """Build or retrieve a Delta-enabled SparkSession with performance optimizations.

    Parameters
    ----------
    app_name : Optional[str]
        Application name for the Spark UI and logs.
    master : Optional[str]
        Spark master URL (defaults to 'local[*]' for local execution).
    extra_configs : Optional[Dict[str, str]]
        Additional Spark configuration key-value pairs to set.
    config_path : Optional[str]
        Path to pipeline_config.yaml.

    Returns
    -------
    SparkSession
        Configured SparkSession instance.
    """
    try:
        pipeline_cfg = load_pipeline_config(config_path)
    except Exception:
        pipeline_cfg = {}

    spark_cfg = pipeline_cfg.get("spark_engine", {})
    resolved_app_name = (
        app_name
        or spark_cfg.get("app_name")
        or "EnterpriseECommerceLakehouse"
    )

    builder = SparkSession.builder.appName(resolved_app_name)

    # Detect Databricks vs Local environment
    in_databricks = is_databricks_runtime()

    if not in_databricks:
        # Local Spark defaults
        resolved_master = master or "local[*]"
        builder = builder.master(resolved_master)

        # Delta Catalog configuration
        builder = (
            builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # Memory and warehouse defaults
            .config("spark.driver.memory", "4g")
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
            .config(
                "spark.driver.extraJavaOptions",
                "-Dfile.encoding=UTF-8 -XX:+IgnoreUnrecognizedVMOptions "
                "--add-opens=java.base/java.lang=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
                "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED "
                "--add-opens=java.base/java.io=ALL-UNNAMED "
                "--add-opens=java.base/java.net=ALL-UNNAMED "
                "--add-opens=java.base/java.nio=ALL-UNNAMED "
                "--add-opens=java.base/util=ALL-UNNAMED "
                "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
            )
        )

    # Shuffle and Adaptive Query Execution (AQE) tuning
    shuffle_partitions = str(spark_cfg.get("shuffle_partitions", 8))
    aqe_cfg = spark_cfg.get("adaptive_query_execution", {})
    aqe_enabled = str(aqe_cfg.get("enabled", True)).lower()
    coalesce_enabled = str(aqe_cfg.get("coalesce_partitions_enabled", True)).lower()
    skew_join_enabled = str(aqe_cfg.get("skew_join_enabled", True)).lower()

    builder = (
        builder.config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.sql.adaptive.enabled", aqe_enabled)
        .config("spark.sql.adaptive.coalescePartitions.enabled", coalesce_enabled)
        .config("spark.sql.adaptive.skewJoin.enabled", skew_join_enabled)
        # Delta production governance
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    )

    # Apply user-specified overrides
    if extra_configs:
        for k, v in extra_configs.items():
            builder = builder.config(k, v)

    if in_databricks:
        spark = builder.getOrCreate()
    else:
        # Import delta pip helper when running locally
        try:
            from delta import configure_spark_with_delta_pip
            spark = configure_spark_with_delta_pip(builder).getOrCreate()
        except ImportError:
            spark = builder.getOrCreate()

    # Set log level to WARN to reduce excessive console noise
    spark.sparkContext.setLogLevel("WARN")

    return spark
