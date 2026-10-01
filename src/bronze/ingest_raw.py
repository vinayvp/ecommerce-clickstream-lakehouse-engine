"""Bronze Layer Raw Ingestion Engine.

Ingests raw CDC mutation records (Orders, Customers) and append-only event
logs (Clickstream, Product catalog) into raw Delta Lake tables.

Enforces:
1. Append-only ingestion (`delta.appendOnly = true`).
2. Auditing metadata enrichment (`_ingest_timestamp`, `_source_file`).
3. Schema-drift rescue handling (`_rescued_data` column for unparseable records).
4. Schema evolution support (`mergeSchema = true`).
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Optional
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from src.utils.spark_session import get_spark_session, load_pipeline_config


# Standard Raw Schemas with Rescued Data support
RAW_ORDERS_CDC_SCHEMA = StructType([
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("order_status", StringType(), True),
    StructField("order_amount", DoubleType(), True),
    StructField("currency", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("payment_method", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("_change_seq", IntegerType(), True),
    StructField("op_type", StringType(), True),  # 'INSERT', 'UPDATE', 'DELETE'
    StructField("_rescued_data", StringType(), True),
])

RAW_CUSTOMERS_CDC_SCHEMA = StructType([
    StructField("customer_id", StringType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("country", StringType(), True),
    StructField("city", StringType(), True),
    StructField("signup_date", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("_change_seq", IntegerType(), True),
    StructField("op_type", StringType(), True),
    StructField("_rescued_data", StringType(), True),
])

RAW_CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("device_type", StringType(), True),
    StructField("traffic_source", StringType(), True),
    StructField("event_timestamp", StringType(), True),
    StructField("_rescued_data", StringType(), True),
])

RAW_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category_id", StringType(), True),
    StructField("category_name", StringType(), True),
    StructField("unit_price", DoubleType(), True),
    StructField("brand", StringType(), True),
    StructField("_rescued_data", StringType(), True),
])


def ingest_file_to_bronze(
    spark: SparkSession,
    source_path: str,
    target_delta_path: str,
    schema: StructType,
    file_format: str = "json",
    table_properties: Optional[Dict[str, str]] = None,
) -> DataFrame:
    """Ingest raw files into a Bronze append-only Delta table with audit columns.

    Parameters
    ----------
    spark : SparkSession
        Configured Spark session.
    source_path : str
        Source filesystem path.
    target_delta_path : str
        Target Bronze Delta table path.
    schema : StructType
        Expected raw schema including `_rescued_data`.
    file_format : str
        Input format ('json', 'parquet', 'csv').
    table_properties : Optional[Dict[str, str]]
        Delta table properties (e.g. 'delta.appendOnly').

    Returns
    -------
    DataFrame
        Ingested Bronze DataFrame.
    """
    reader = (
        spark.read.format(file_format)
        .option("columnNameOfCorruptRecord", "_rescued_data")
        .schema(schema)
    )

    df_raw = reader.load(source_path)

    # If _rescued_data column was not populated by the reader, create it as null string
    if "_rescued_data" not in df_raw.columns:
        df_raw = df_raw.withColumn("_rescued_data", F.lit(None).cast(StringType()))

    # Add Bronze ingestion metadata
    df_bronze = (
        df_raw.withColumn("_ingest_timestamp", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )

    # Write to Bronze Delta table (append-only)
    writer = (
        df_bronze.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
    )

    props = table_properties or {
        "delta.appendOnly": "true",
        "delta.autoOptimize.optimizeWrite": "true",
    }
    for k, v in props.items():
        writer = writer.option(k, v)

    writer.save(target_delta_path)

    return df_bronze


def run_bronze_ingestion(
    spark: Optional[SparkSession] = None,
    config_path: Optional[str] = None,
) -> Dict[str, int]:
    """Execute full Bronze layer ingestion for all data sources.

    Parameters
    ----------
    spark : Optional[SparkSession]
        Existing SparkSession or None to create one.
    config_path : Optional[str]
        Path to pipeline_config.yaml.

    Returns
    -------
    Dict[str, int]
        Row count ingested per table.
    """
    config = load_pipeline_config(config_path)
    env = config.get("environment", "local")
    paths = config.get("paths", {}).get(env, {})

    raw_paths = paths.get("raw_data", {})
    bronze_base = paths.get("bronze", "data/lakehouse/bronze")
    table_props = config.get("medallion", {}).get("bronze", {}).get("table_properties", {})

    if spark is None:
        spark = get_spark_session(app_name="BronzeIngestionEngine", config_path=config_path)

    counts: Dict[str, int] = {}

    tasks = [
        ("orders_cdc", raw_paths.get("orders_cdc"), f"{bronze_base}/bronze_orders_cdc", RAW_ORDERS_CDC_SCHEMA, "json"),
        ("customers_cdc", raw_paths.get("customers_cdc"), f"{bronze_base}/bronze_customers_cdc", RAW_CUSTOMERS_CDC_SCHEMA, "json"),
        ("clickstream", raw_paths.get("clickstream"), f"{bronze_base}/bronze_clickstream", RAW_CLICKSTREAM_SCHEMA, "json"),
        ("products", raw_paths.get("products"), f"{bronze_base}/bronze_products", RAW_PRODUCTS_SCHEMA, "json"),
    ]

    for name, src, dst, schema, fmt in tasks:
        if src and Path(src).exists() and any(Path(src).iterdir()):
            df = ingest_file_to_bronze(
                spark=spark,
                source_path=src,
                target_delta_path=dst,
                schema=schema,
                file_format=fmt,
                table_properties=table_props,
            )
            count = df.count()
            counts[name] = count
            print(f"[Bronze] Successfully ingested {count} records into {dst}")
        else:
            print(f"[Bronze] Skipping {name}: source path {src} does not exist or is empty.")
            counts[name] = 0

    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion")
    parser.add_argument("--config", type=str, default=None, help="Path to pipeline_config.yaml")
    args = parser.parse_args()

    spark_sess = get_spark_session(app_name="BronzeCLI", config_path=args.config)
    run_bronze_ingestion(spark=spark_sess, config_path=args.config)
