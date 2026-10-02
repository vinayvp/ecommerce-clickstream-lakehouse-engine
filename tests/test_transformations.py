"""Unit and integration tests for Medallion transformations and CDC logic."""

import os
import pytest
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.bronze.ingest_raw import (
    RAW_ORDERS_CDC_SCHEMA,
    ingest_file_to_bronze,
)
from src.silver.transform_silver import (
    cleanse_and_validate_clickstream,
    cleanse_and_validate_customers,
    cleanse_and_validate_orders,
    deduplicate_in_batch,
    merge_cdc_into_delta,
)
from src.gold.build_gold_marts import (
    build_daily_customer_ltv,
    build_retention_funnel,
)


def test_bronze_ingestion_metadata(spark, temp_delta_dir):
    source_json = os.path.join(temp_delta_dir, "raw_orders.json")
    target_delta = os.path.join(temp_delta_dir, "bronze_orders")

    raw_data = [
        {"order_id": "ORD-1", "customer_id": "CUST-1", "order_amount": 100.0, "order_status": "DELIVERED", "event_timestamp": "2026-10-01 10:00:00"}
    ]
    df = spark.createDataFrame(raw_data)
    df.write.json(source_json)

    bronze_df = ingest_file_to_bronze(
        spark=spark,
        source_path=source_json,
        target_delta_path=target_delta,
        schema=RAW_ORDERS_CDC_SCHEMA,
    )

    assert "_ingest_timestamp" in bronze_df.columns
    assert "_source_file" in bronze_df.columns
    assert "_rescued_data" in bronze_df.columns
    assert bronze_df.count() == 1


def test_silver_cleanse_and_quarantine_orders(spark):
    sample_records = [
        # Valid order
        ("ORD-1", "CUST-1", "PROD-1", "DELIVERED", 150.0, "USD", 2, "CREDIT_CARD", "2026-10-01 12:00:00", 1, "INSERT", None),
        # Malformed: Negative amount
        ("ORD-2", "CUST-2", "PROD-1", "DELIVERED", -20.0, "USD", 1, "CREDIT_CARD", "2026-10-01 12:00:00", 1, "INSERT", None),
        # Malformed: Null customer_id
        ("ORD-3", None, "PROD-1", "PENDING", 50.0, "USD", 1, "CREDIT_CARD", "2026-10-01 12:00:00", 1, "INSERT", None),
        # Malformed: Invalid status
        ("ORD-4", "CUST-4", "PROD-1", "UNKNOWN_STATUS", 50.0, "USD", 1, "CREDIT_CARD", "2026-10-01 12:00:00", 1, "INSERT", None),
    ]
    cols = [
        "order_id", "customer_id", "product_id", "order_status", "order_amount",
        "currency", "quantity", "event_timestamp", "_change_seq", "op_type", "_rescued_data"
    ]
    df_raw = spark.createDataFrame(sample_records, schema=RAW_ORDERS_CDC_SCHEMA)

    valid_df, quarantine_df = cleanse_and_validate_orders(df_raw)

    assert valid_df.count() == 1
    assert valid_df.first()["order_id"] == "ORD-1"

    assert quarantine_df.count() == 3
    q_reasons = [r["_quarantine_reason"] for r in quarantine_df.collect()]
    assert "Negative order_amount" in q_reasons
    assert "Null customer_id" in q_reasons
    assert "Invalid order_status" in q_reasons


def test_silver_cdc_merge_lifecycle(spark, temp_delta_dir):
    silver_path = os.path.join(temp_delta_dir, "silver_orders_lifecycle")

    cols = [
        "order_id", "customer_id", "product_id", "order_status", "order_amount",
        "currency", "quantity", "event_timestamp", "_change_seq", "op_type", "order_date"
    ]

    # Batch 1: Initial INSERT of 2 orders
    batch1 = [
        ("ORD-100", "CUST-1", "PROD-1", "PENDING", 100.0, "USD", 1, "2026-10-01 10:00:00", 1, "INSERT", "2026-10-01"),
        ("ORD-200", "CUST-2", "PROD-2", "PENDING", 200.0, "USD", 2, "2026-10-01 10:00:00", 1, "INSERT", "2026-10-01"),
    ]
    df_b1 = spark.createDataFrame(batch1, cols)
    merge_cdc_into_delta(spark, df_b1, silver_path, primary_key="order_id")

    res_b1 = spark.read.format("delta").load(silver_path).collect()
    assert len(res_b1) == 2

    # Batch 2: UPDATE ORD-100 to DELIVERED, and DELETE ORD-200
    batch2 = [
        ("ORD-100", "CUST-1", "PROD-1", "DELIVERED", 100.0, "USD", 1, "2026-10-01 15:00:00", 2, "UPDATE", "2026-10-01"),
        ("ORD-200", "CUST-2", "PROD-2", "CANCELLED", 200.0, "USD", 2, "2026-10-01 16:00:00", 2, "DELETE", "2026-10-01"),
    ]
    df_b2 = spark.createDataFrame(batch2, cols)
    merge_cdc_into_delta(spark, df_b2, silver_path, primary_key="order_id")

    res_b2 = spark.read.format("delta").load(silver_path).collect()
    assert len(res_b2) == 1
    assert res_b2[0]["order_id"] == "ORD-100"
    assert res_b2[0]["order_status"] == "DELIVERED"
    assert res_b2[0]["_change_seq"] == 2


def test_silver_cdc_out_of_order_rejection(spark, temp_delta_dir):
    silver_path = os.path.join(temp_delta_dir, "silver_orders_ooo")

    cols = [
        "order_id", "customer_id", "product_id", "order_status", "order_amount",
        "currency", "quantity", "event_timestamp", "_change_seq", "op_type", "order_date"
    ]

    # Current state is at _change_seq = 5
    state = [
        ("ORD-999", "CUST-1", "PROD-1", "DELIVERED", 300.0, "USD", 3, "2026-10-01 18:00:00", 5, "UPDATE", "2026-10-01"),
    ]
    df_state = spark.createDataFrame(state, cols)
    merge_cdc_into_delta(spark, df_state, silver_path, primary_key="order_id")

    # Late arriving packet with _change_seq = 3 arrives now (stale network packet)
    late_arriving = [
        ("ORD-999", "CUST-1", "PROD-1", "PENDING", 300.0, "USD", 3, "2026-10-01 12:00:00", 3, "UPDATE", "2026-10-01"),
    ]
    df_late = spark.createDataFrame(late_arriving, cols)
    merge_cdc_into_delta(spark, df_late, silver_path, primary_key="order_id")

    # Verify that the stale record was rejected and state remains DELIVERED at sequence 5
    res = spark.read.format("delta").load(silver_path).first()
    assert res["order_status"] == "DELIVERED"
    assert res["_change_seq"] == 5


def test_silver_cdc_in_batch_deduplication(spark):
    cols = ["order_id", "_change_seq", "event_timestamp", "order_status"]
    data = [
        ("ORD-1", 1, "2026-10-01 10:00:00", "PENDING"),
        ("ORD-1", 2, "2026-10-01 11:00:00", "CONFIRMED"),
        ("ORD-1", 3, "2026-10-01 12:00:00", "DELIVERED"),
        ("ORD-2", 1, "2026-10-01 10:00:00", "PENDING"),
    ]
    df = spark.createDataFrame(data, cols)

    deduped = deduplicate_in_batch(df, primary_key="order_id")

    assert deduped.count() == 2
    ord1 = deduped.filter(F.col("order_id") == "ORD-1").first()
    assert ord1["_change_seq"] == 3
    assert ord1["order_status"] == "DELIVERED"


def test_gold_customer_ltv(spark):
    orders_data = [
        ("ORD-1", "CUST-1", 100.0, 1, "DELIVERED", "2026-10-01 10:00:00", "2026-10-01"),
        ("ORD-2", "CUST-1", 150.0, 2, "DELIVERED", "2026-10-01 14:00:00", "2026-10-01"),
        ("ORD-3", "CUST-1", 50.0, 1, "CANCELLED", "2026-10-01 16:00:00", "2026-10-01"),
    ]
    orders_cols = ["order_id", "customer_id", "order_amount", "quantity", "order_status", "event_timestamp", "order_date"]
    orders_df = spark.createDataFrame(orders_data, orders_cols)

    cust_data = [
        ("CUST-1", "Jane", "Doe", "jane@example.com", "US", "New York", "2026-01-01", "VIP")
    ]
    cust_cols = ["customer_id", "first_name", "last_name", "email", "country", "city", "signup_date", "customer_segment"]
    cust_df = spark.createDataFrame(cust_data, cust_cols)

    ltv_mart = build_daily_customer_ltv(spark, orders_df, cust_df)
    row = ltv_mart.first()

    assert row["total_orders"] == 3
    assert row["completed_orders"] == 2
    assert row["cancelled_orders"] == 1
    assert row["total_revenue_usd"] == 250.0
    assert row["avg_order_value_usd"] == 125.0
    assert row["customer_segment"] == "VIP"


def test_gold_retention_funnel(spark):
    clicks_data = [
        # Session 1 completes all steps
        ("EVT-1", "SESS-1", "USER-1", "VIEW_PAGE", "2026-10-01", "DESKTOP", "DIRECT"),
        ("EVT-2", "SESS-1", "USER-1", "ADD_TO_CART", "2026-10-01", "DESKTOP", "DIRECT"),
        ("EVT-3", "SESS-1", "USER-1", "BEGIN_CHECKOUT", "2026-10-01", "DESKTOP", "DIRECT"),
        ("EVT-4", "SESS-1", "USER-1", "PURCHASE_COMPLETED", "2026-10-01", "DESKTOP", "DIRECT"),
        # Session 2 drops off after cart
        ("EVT-5", "SESS-2", "USER-2", "VIEW_PAGE", "2026-10-01", "DESKTOP", "DIRECT"),
        ("EVT-6", "SESS-2", "USER-2", "ADD_TO_CART", "2026-10-01", "DESKTOP", "DIRECT"),
    ]
    clicks_cols = ["event_id", "session_id", "user_id", "event_type", "event_date", "device_type", "traffic_source"]
    clicks_df = spark.createDataFrame(clicks_data, clicks_cols)

    funnel_mart = build_retention_funnel(spark, clicks_df)
    row = funnel_mart.first()

    assert row["total_sessions"] == 2
    assert row["view_sessions"] == 2
    assert row["cart_sessions"] == 2
    assert row["checkout_sessions"] == 1
    assert row["purchase_sessions"] == 1
    assert row["view_to_cart_rate_pct"] == 100.0
    assert row["cart_to_checkout_rate_pct"] == 50.0
    assert row["overall_conversion_rate_pct"] == 50.0
