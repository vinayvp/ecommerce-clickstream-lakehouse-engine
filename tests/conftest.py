"""Pytest fixtures for Delta Lake and PySpark testing."""

import os
import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession configured with Delta Lake extensions."""
    warehouse_dir = tempfile.mkdtemp(prefix="spark_warehouse_")
    builder = (
        SparkSession.builder.appName("PyTestDeltaLakehouse")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", warehouse_dir)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )

    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    spark_session.sparkContext.setLogLevel("ERROR")

    yield spark_session

    spark_session.stop()
    if os.path.exists(warehouse_dir):
        shutil.rmtree(warehouse_dir, ignore_errors=True)


@pytest.fixture
def temp_delta_dir():
    """Function-scoped temporary directory for isolated Delta table tests."""
    temp_dir = tempfile.mkdtemp(prefix="delta_test_")
    yield temp_dir
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir, ignore_errors=True)
