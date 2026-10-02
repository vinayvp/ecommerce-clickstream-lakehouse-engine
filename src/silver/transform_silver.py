"""Silver Layer Conformed, Cleansed, and CDC Upsert Engine.

Implements enterprise data governance:
1. Strict schema validation, type casting, and data quality assertions.
2. In-batch CDC deduplication using window rank over sequence numbers.
3. Delta Lake ACID MERGE INTO with support for INSERT, UPDATE, and DELETE.
4. Deterministic handling of out-of-order and late-arriving CDC records
   ensuring stale events cannot overwrite committed state.
5. Quarantine routing for non-compliant records.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window

from src.utils.spark_session import get_spark_session, load_pipeline_config


def cleanse_and_validate_orders(
    df_raw: DataFrame,
    valid_statuses: Optional[List[str]] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Validate, cast, and conform Bronze Orders CDC records.

    Separates records into conforming valid data and quarantined invalid data.

    Parameters
    ----------
    df_raw : DataFrame
        Raw bronze orders DataFrame.
    valid_statuses : Optional[List[str]]
        Set of allowable order_status values.

    Returns
    -------
    Tuple[DataFrame, DataFrame]
        (valid_orders_df, quarantined_orders_df)
    """
    statuses = valid_statuses or [
        "PENDING", "CONFIRMED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"
    ]

    cleansed = (
        df_raw.withColumn("order_id", F.trim(F.col("order_id")))
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("order_status", F.upper(F.trim(F.col("order_status"))))
        .withColumn("order_amount", F.col("order_amount").cast("double"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("currency", F.coalesce(F.upper(F.trim(F.col("currency"))), F.lit("USD")))
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("_change_seq", F.coalesce(F.col("_change_seq").cast("int"), F.lit(0)))
        .withColumn("op_type", F.coalesce(F.upper(F.trim(F.col("op_type"))), F.lit("INSERT")))
        .withColumn("order_date", F.to_date(F.col("event_timestamp")))
    )

    # Data Quality Rules
    dq_cond = (
        F.col("order_id").isNotNull()
        & (F.length(F.col("order_id")) > 0)
        & F.col("customer_id").isNotNull()
        & (F.col("order_amount") >= 0.0)
        & (F.col("quantity") >= 0)
        & F.col("order_status").isin(statuses)
        & F.col("event_timestamp").isNotNull()
        & F.col("op_type").isin(["INSERT", "UPDATE", "DELETE"])
    )

    valid_df = cleansed.filter(dq_cond)
    quarantine_df = cleansed.filter(~dq_cond).withColumn(
        "_quarantine_reason",
        F.when(F.col("order_id").isNull(), "Null order_id")
        .when(F.col("customer_id").isNull(), "Null customer_id")
        .when(F.col("order_amount") < 0.0, "Negative order_amount")
        .when(~F.col("order_status").isin(statuses), "Invalid order_status")
        .otherwise("Malformed CDC payload"),
    )

    return valid_df, quarantine_df


def cleanse_and_validate_customers(df_raw: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Validate, cast, and conform Bronze Customers CDC records.

    Parameters
    ----------
    df_raw : DataFrame
        Raw bronze customers DataFrame.

    Returns
    -------
    Tuple[DataFrame, DataFrame]
        (valid_customers_df, quarantined_customers_df)
    """
    cleansed = (
        df_raw.withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("first_name", F.trim(F.col("first_name")))
        .withColumn("last_name", F.trim(F.col("last_name")))
        .withColumn("email", F.lower(F.trim(F.col("email"))))
        .withColumn("country", F.upper(F.trim(F.col("country"))))
        .withColumn("city", F.trim(F.col("city")))
        .withColumn("signup_date", F.to_date(F.col("signup_date")))
        .withColumn("customer_segment", F.coalesce(F.upper(F.trim(F.col("customer_segment"))), F.lit("STANDARD")))
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("_change_seq", F.coalesce(F.col("_change_seq").cast("int"), F.lit(0)))
        .withColumn("op_type", F.coalesce(F.upper(F.trim(F.col("op_type"))), F.lit("INSERT")))
    )

    dq_cond = (
        F.col("customer_id").isNotNull()
        & (F.length(F.col("customer_id")) > 0)
        & F.col("email").contains("@")
        & F.col("op_type").isin(["INSERT", "UPDATE", "DELETE"])
    )

    valid_df = cleansed.filter(dq_cond)
    quarantine_df = cleansed.filter(~dq_cond).withColumn(
        "_quarantine_reason",
        F.when(F.col("customer_id").isNull(), "Null customer_id")
        .when(~F.col("email").contains("@"), "Invalid email structure")
        .otherwise("Malformed customer CDC"),
    )

    return valid_df, quarantine_df


def cleanse_and_validate_clickstream(df_raw: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """Validate, cast, and conform Bronze Clickstream event logs.

    Parameters
    ----------
    df_raw : DataFrame
        Raw bronze clickstream DataFrame.

    Returns
    -------
    Tuple[DataFrame, DataFrame]
        (valid_clickstream_df, quarantined_clickstream_df)
    """
    valid_event_types = ["VIEW_PAGE", "ADD_TO_CART", "BEGIN_CHECKOUT", "PURCHASE_COMPLETED"]

    cleansed = (
        df_raw.withColumn("event_id", F.trim(F.col("event_id")))
        .withColumn("session_id", F.trim(F.col("session_id")))
        .withColumn("user_id", F.trim(F.col("user_id")))
        .withColumn("event_type", F.upper(F.trim(F.col("event_type"))))
        .withColumn("product_id", F.trim(F.col("product_id")))
        .withColumn("page_url", F.trim(F.col("page_url")))
        .withColumn("device_type", F.coalesce(F.upper(F.trim(F.col("device_type"))), F.lit("UNKNOWN")))
        .withColumn("traffic_source", F.coalesce(F.upper(F.trim(F.col("traffic_source"))), F.lit("DIRECT")))
        .withColumn("event_timestamp", F.to_timestamp(F.col("event_timestamp")))
        .withColumn("event_date", F.to_date(F.col("event_timestamp")))
    )

    dq_cond = (
        F.col("event_id").isNotNull()
        & F.col("session_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("event_type").isin(valid_event_types)
        & F.col("event_timestamp").isNotNull()
    )

    valid_df = cleansed.filter(dq_cond)
    quarantine_df = cleansed.filter(~dq_cond).withColumn(
        "_quarantine_reason",
        F.when(F.col("event_id").isNull(), "Null event_id")
        .when(F.col("session_id").isNull(), "Null session_id")
        .when(~F.col("event_type").isin(valid_event_types), "Invalid event_type")
        .otherwise("Malformed clickstream record"),
    )

    return valid_df, quarantine_df


def deduplicate_in_batch(
    df: DataFrame,
    primary_key: str,
    sequence_col: str = "_change_seq",
    timestamp_col: str = "event_timestamp",
) -> DataFrame:
    """Deduplicate records within the current batch keeping the highest sequence/timestamp.

    Parameters
    ----------
    df : DataFrame
        Incoming batch DataFrame with potential intra-batch duplicate keys.
    primary_key : str
        Business key column name.
    sequence_col : str
        CDC change sequence number column.
    timestamp_col : str
        Event timestamp column for secondary tie-breaking.

    Returns
    -------
    DataFrame
        Batch with unique primary keys.
    """
    window = Window.partitionBy(primary_key).orderBy(
        F.col(sequence_col).desc(),
        F.col(timestamp_col).desc(),
    )

    return (
        df.withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


def merge_cdc_into_delta(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_path: str,
    primary_key: str,
    sequence_col: str = "_change_seq",
    op_col: str = "op_type",
    partition_cols: Optional[List[str]] = None,
) -> None:
    """Execute an ACID MERGE INTO operation on a Delta table.

    Handles:
    - INSERT: When key is not matched and op_type != 'DELETE'.
    - UPDATE: When key is matched, op_type != 'DELETE', and source sequence >= target sequence.
    - DELETE: When key is matched, op_type == 'DELETE', and source sequence >= target sequence.
    - Late-arriving / Out-of-order rejection: Ignores mutations where source sequence < target sequence.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    source_df : DataFrame
        Deduplicated incoming batch.
    target_table_path : str
        Filesystem path or catalog name of the Silver Delta table.
    primary_key : str
        Unique business key column name.
    sequence_col : str
        Sequence number / timestamp column used for out-of-order resolution.
    op_col : str
        CDC operation flag ('INSERT', 'UPDATE', 'DELETE').
    partition_cols : Optional[List[str]]
        Partition columns to use if table is being initialized.
    """
    # Exclude metadata columns not intended for target Silver table
    cols_to_exclude = {"_ingest_timestamp", "_source_file", "_rescued_data"}
    cleaned_source = source_df.drop(*[c for c in cols_to_exclude if c in source_df.columns])

    # Deduplicate inside this batch first to avoid Delta merge runtime error
    batch_latest = deduplicate_in_batch(
        df=cleaned_source,
        primary_key=primary_key,
        sequence_col=sequence_col,
    )

    # Check if target Delta table already exists
    table_exists = DeltaTable.isDeltaTable(spark, target_table_path)

    if not table_exists:
        # Initialize table from first batch, excluding any initial DELETE requests
        init_df = batch_latest.filter(F.col(op_col) != "DELETE")
        writer = init_df.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_table_path)
        return

    # Target table exists: perform ACID MERGE INTO
    target_delta = DeltaTable.forPath(spark, target_table_path)

    (
        target_delta.alias("target")
        .merge(
            batch_latest.alias("source"),
            f"target.{primary_key} = source.{primary_key}",
        )
        # Condition 1: Out-of-order check for DELETE
        .whenMatchedDelete(
            condition=(
                f"source.{op_col} = 'DELETE' "
                f"AND source.{sequence_col} >= target.{sequence_col}"
            )
        )
        # Condition 2: Out-of-order check for UPDATE
        .whenMatchedUpdateAll(
            condition=(
                f"source.{op_col} != 'DELETE' "
                f"AND source.{sequence_col} >= target.{sequence_col}"
            )
        )
        # Condition 3: Not matched -> INSERT (skip DELETE operations)
        .whenNotMatchedInsertAll(
            condition=f"source.{op_col} != 'DELETE'"
        )
        .execute()
    )


def append_clickstream_into_silver(
    spark: SparkSession,
    source_df: DataFrame,
    target_table_path: str,
    primary_key: str = "event_id",
    partition_cols: Optional[List[str]] = None,
) -> None:
    """Idempotently insert clickstream records into Silver, preventing duplicate events.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    source_df : DataFrame
        Cleaned clickstream DataFrame.
    target_table_path : str
        Target Silver Delta table path.
    primary_key : str
        Deduplication primary key (event_id).
    partition_cols : Optional[List[str]]
        Partition columns (e.g. ['event_date']).
    """
    cols_to_exclude = {"_ingest_timestamp", "_source_file", "_rescued_data"}
    cleaned_source = source_df.drop(*[c for c in cols_to_exclude if c in source_df.columns])

    # Deduplicate in batch
    window = Window.partitionBy(primary_key).orderBy(F.col("event_timestamp").desc())
    batch_deduped = (
        cleaned_source.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if not DeltaTable.isDeltaTable(spark, target_table_path):
        writer = batch_deduped.write.format("delta").mode("overwrite")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(target_table_path)
        return

    # Idempotent merge on event_id: insert only if not already present
    target_delta = DeltaTable.forPath(spark, target_table_path)
    (
        target_delta.alias("target")
        .merge(
            batch_deduped.alias("source"),
            f"target.{primary_key} = source.{primary_key}",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_silver_transformations(
    spark: Optional[SparkSession] = None,
    config_path: Optional[str] = None,
) -> Dict[str, int]:
    """Execute full Silver layer transformation, cleansing, and CDC merge.

    Parameters
    ----------
    spark : Optional[SparkSession]
        Existing SparkSession or None.
    config_path : Optional[str]
        Path to pipeline_config.yaml.

    Returns
    -------
    Dict[str, int]
        Row count in each silver table after transformation.
    """
    config = load_pipeline_config(config_path)
    env = config.get("environment", "local")
    paths = config.get("paths", {}).get(env, {})

    bronze_base = paths.get("bronze", "data/lakehouse/bronze")
    silver_base = paths.get("silver", "data/lakehouse/silver")

    if spark is None:
        spark = get_spark_session(app_name="SilverTransformEngine", config_path=config_path)

    counts: Dict[str, int] = {}

    # 1. Transform Orders CDC
    bronze_orders_path = f"{bronze_base}/bronze_orders_cdc"
    silver_orders_path = f"{silver_base}/silver_orders"
    if Path(bronze_orders_path).exists() and DeltaTable.isDeltaTable(spark, bronze_orders_path):
        raw_orders = spark.read.format("delta").load(bronze_orders_path)
        valid_orders, quarantine_orders = cleanse_and_validate_orders(raw_orders)

        # Write quarantine table if bad records exist
        q_count = quarantine_orders.count()
        if q_count > 0:
            quarantine_orders.write.format("delta").mode("append").save(
                f"{silver_base}/quarantine_orders"
            )
            print(f"[Silver] Quarantined {q_count} invalid order records.")

        merge_cdc_into_delta(
            spark=spark,
            source_df=valid_orders,
            target_table_path=silver_orders_path,
            primary_key="order_id",
            sequence_col="_change_seq",
            op_col="op_type",
            partition_cols=["order_date"],
        )
        silver_count = spark.read.format("delta").load(silver_orders_path).count()
        counts["silver_orders"] = silver_count
        print(f"[Silver] silver_orders successfully merged: {silver_count} active records.")

    # 2. Transform Customers CDC
    bronze_cust_path = f"{bronze_base}/bronze_customers_cdc"
    silver_cust_path = f"{silver_base}/silver_customers"
    if Path(bronze_cust_path).exists() and DeltaTable.isDeltaTable(spark, bronze_cust_path):
        raw_cust = spark.read.format("delta").load(bronze_cust_path)
        valid_cust, quarantine_cust = cleanse_and_validate_customers(raw_cust)

        q_count = quarantine_cust.count()
        if q_count > 0:
            quarantine_cust.write.format("delta").mode("append").save(
                f"{silver_base}/quarantine_customers"
            )
            print(f"[Silver] Quarantined {q_count} invalid customer records.")

        merge_cdc_into_delta(
            spark=spark,
            source_df=valid_cust,
            target_table_path=silver_cust_path,
            primary_key="customer_id",
            sequence_col="_change_seq",
            op_col="op_type",
        )
        silver_count = spark.read.format("delta").load(silver_cust_path).count()
        counts["silver_customers"] = silver_count
        print(f"[Silver] silver_customers successfully merged: {silver_count} active records.")

    # 3. Transform Clickstream
    bronze_clicks_path = f"{bronze_base}/bronze_clickstream"
    silver_clicks_path = f"{silver_base}/silver_clickstream"
    if Path(bronze_clicks_path).exists() and DeltaTable.isDeltaTable(spark, bronze_clicks_path):
        raw_clicks = spark.read.format("delta").load(bronze_clicks_path)
        valid_clicks, quarantine_clicks = cleanse_and_validate_clickstream(raw_clicks)

        q_count = quarantine_clicks.count()
        if q_count > 0:
            quarantine_clicks.write.format("delta").mode("append").save(
                f"{silver_base}/quarantine_clickstream"
            )

        append_clickstream_into_silver(
            spark=spark,
            source_df=valid_clicks,
            target_table_path=silver_clicks_path,
            primary_key="event_id",
            partition_cols=["event_date"],
        )
        silver_count = spark.read.format("delta").load(silver_clicks_path).count()
        counts["silver_clickstream"] = silver_count
        print(f"[Silver] silver_clickstream successfully merged: {silver_count} records.")

    # 4. Transform Products Catalog
    bronze_prod_path = f"{bronze_base}/bronze_products"
    silver_prod_path = f"{silver_base}/silver_products"
    if Path(bronze_prod_path).exists() and DeltaTable.isDeltaTable(spark, bronze_prod_path):
        raw_prod = spark.read.format("delta").load(bronze_prod_path)
        cleaned_prod = (
            raw_prod.filter(F.col("product_id").isNotNull())
            .withColumn("product_id", F.trim(F.col("product_id")))
            .withColumn("product_name", F.trim(F.col("product_name")))
            .withColumn("category_id", F.trim(F.col("category_id")))
            .withColumn("category_name", F.trim(F.col("category_name")))
            .withColumn("unit_price", F.col("unit_price").cast("double"))
            .withColumn("brand", F.trim(F.col("brand")))
            .drop("_ingest_timestamp", "_source_file", "_rescued_data")
        )
        cleaned_prod.write.format("delta").mode("overwrite").save(silver_prod_path)
        silver_count = spark.read.format("delta").load(silver_prod_path).count()
        counts["silver_products"] = silver_count
        print(f"[Silver] silver_products conformed: {silver_count} records.")

    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Layer Transformation")
    parser.add_argument("--config", type=str, default=None, help="Path to pipeline_config.yaml")
    args = parser.parse_args()

    spark_sess = get_spark_session(app_name="SilverCLI", config_path=args.config)
    run_silver_transformations(spark=spark_sess, config_path=args.config)
