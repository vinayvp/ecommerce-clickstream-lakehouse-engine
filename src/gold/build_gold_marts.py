"""Gold Layer Dimensional Modeling and Analytical Mart Engine.

Builds high-performance analytical KPI marts optimized for downstream BI:
1. Daily Customer Lifetime Value Mart (`gold_daily_customer_ltv`)
   - Employs broadcast hash join against Customer dimension.
2. Category Sales Velocity Mart (`gold_category_sales_velocity`)
   - Uses broadcast hash join against Product dimension and integrates salting
     to eliminate partition skew on hot category/product keys.
3. User Conversion & Retention Funnel Mart (`gold_retention_funnel`)
   - Multi-stage session funnel from clickstream events to order completion.
4. Storage Compaction and Spatial Clustering:
   - Applies OPTIMIZE with multi-dimensional Z-ORDER BY to eradicate small files
     and enable file skipping on high-cardinality BI filter columns.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, List, Optional
from delta.tables import DeltaTable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from src.utils.delta_utils import optimize_table
from src.utils.skew_handler import execute_salted_aggregation
from src.utils.spark_session import get_spark_session, load_pipeline_config


def build_daily_customer_ltv(
    spark: SparkSession,
    orders_df: DataFrame,
    customers_df: DataFrame,
) -> DataFrame:
    """Build Customer Lifetime Value (LTV) dimensional aggregation mart.

    Uses an explicit broadcast hash join (`F.broadcast`) on the customers dimension
    to avoid shuffling the large orders fact stream.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    orders_df : DataFrame
        Silver Orders fact DataFrame.
    customers_df : DataFrame
        Silver Customers dimension DataFrame.

    Returns
    -------
    DataFrame
        Enriched customer LTV mart.
    """
    # Order-level customer aggregates
    order_metrics = orders_df.groupBy("customer_id", "order_date").agg(
        F.count("order_id").alias("total_orders"),
        F.sum(F.when(F.col("order_status") == "DELIVERED", 1).otherwise(0)).alias("completed_orders"),
        F.sum(F.when(F.col("order_status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_orders"),
        F.sum(F.when(F.col("order_status") != "CANCELLED", F.col("order_amount")).otherwise(0.0)).alias("total_revenue_usd"),
        F.sum(F.col("quantity")).alias("total_units_purchased"),
        F.min("event_timestamp").alias("first_order_ts"),
        F.max("event_timestamp").alias("last_order_ts"),
    )

    # Broadcast join with small customer dimension
    dim_cust = customers_df.select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "country",
        "city",
        "signup_date",
        "customer_segment",
    )

    enriched = order_metrics.join(
        F.broadcast(dim_cust),
        on="customer_id",
        how="inner",
    )

    # Calculated metrics
    final_mart = (
        enriched.withColumn("report_date", F.col("order_date"))
        .withColumn(
            "avg_order_value_usd",
            F.when(
                F.col("completed_orders") > 0,
                F.round(F.col("total_revenue_usd") / F.col("completed_orders"), 2),
            ).otherwise(0.0),
        )
        .withColumn(
            "recency_days",
            F.datediff(F.col("report_date"), F.to_date(F.col("last_order_ts"))),
        )
        .withColumn("total_revenue_usd", F.round(F.col("total_revenue_usd"), 2))
    )

    return final_mart


def build_category_sales_velocity(
    spark: SparkSession,
    orders_df: DataFrame,
    products_df: DataFrame,
    use_salting: bool = False,
    num_salts: int = 8,
) -> DataFrame:
    """Build Category Sales Velocity and Product Performance mart.

    Integrates explicit broadcast hash joins for product catalog lookup and
    optional salting to mitigate key skew on viral/hot categories.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    orders_df : DataFrame
        Silver Orders fact DataFrame.
    products_df : DataFrame
        Silver Products catalog dimension DataFrame.
    use_salting : bool
        Whether to invoke two-stage salted aggregation.
    num_salts : int
        Number of salt partitions if salting is active.

    Returns
    -------
    DataFrame
        Category sales velocity analytical mart.
    """
    dim_prod = products_df.select(
        "product_id",
        "category_id",
        "category_name",
        "brand",
        "unit_price",
    )

    # Join orders fact with broadcasted product dimension
    orders_enriched = orders_df.join(
        F.broadcast(dim_prod),
        on="product_id",
        how="inner",
    )

    if use_salting:
        # Salting pattern for skewed category aggregations
        stage1_aggs = {
            "total_orders": ("count", "order_id"),
            "total_units_sold": ("sum", "quantity"),
            "gross_merchandise_value_gmv": ("sum", "order_amount"),
            "cancelled_orders": ("sum", "is_cancelled"),
        }
        orders_for_salt = orders_enriched.withColumn(
            "is_cancelled",
            F.when(F.col("order_status") == "CANCELLED", 1).otherwise(0),
        )
        salted_mart = execute_salted_aggregation(
            df=orders_for_salt,
            group_cols=["category_id", "category_name", "order_date"],
            stage1_aggs=stage1_aggs,
            num_salts=num_salts,
        )
        # Calculate derived metrics
        final_mart = (
            salted_mart.withColumn(
                "cancellation_rate_pct",
                F.round(
                    (F.col("cancelled_orders") / F.nullif(F.col("total_orders"), F.lit(0))) * 100.0,
                    2,
                ),
            )
            .withColumn(
                "sales_velocity_units_per_hour",
                F.round(F.col("total_units_sold") / 24.0, 2),
            )
            .withColumn("gross_merchandise_value_gmv", F.round(F.col("gross_merchandise_value_gmv"), 2))
        )
    else:
        # Standard aggregation
        final_mart = orders_enriched.groupBy("category_id", "category_name", "order_date").agg(
            F.count("order_id").alias("total_orders"),
            F.sum("quantity").alias("total_units_sold"),
            F.round(F.sum("order_amount"), 2).alias("gross_merchandise_value_gmv"),
            F.countDistinct("customer_id").alias("distinct_buyers"),
            F.round(F.avg("unit_price"), 2).alias("avg_unit_price"),
            F.round(
                (
                    F.sum(F.when(F.col("order_status") == "CANCELLED", 1).otherwise(0))
                    / F.nullif(F.count("order_id"), F.lit(0))
                )
                * 100.0,
                2,
            ).alias("cancellation_rate_pct"),
            F.round(F.sum("quantity") / 24.0, 2).alias("sales_velocity_units_per_hour"),
        )

    return final_mart


def build_retention_funnel(
    spark: SparkSession,
    clickstream_df: DataFrame,
) -> DataFrame:
    """Build User Conversion & Retention Funnel analytical mart.

    Models multi-touch user progression:
    VIEW_PAGE -> ADD_TO_CART -> BEGIN_CHECKOUT -> PURCHASE_COMPLETED.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    clickstream_df : DataFrame
        Silver Clickstream event stream.

    Returns
    -------
    DataFrame
        Funnel conversion metrics segmented by device and acquisition channel.
    """
    # 1. Session-level step flags
    session_steps = clickstream_df.groupBy(
        "session_id", "user_id", "event_date", "device_type", "traffic_source"
    ).agg(
        F.max(F.when(F.col("event_type") == "VIEW_PAGE", 1).otherwise(0)).alias("has_view"),
        F.max(F.when(F.col("event_type") == "ADD_TO_CART", 1).otherwise(0)).alias("has_cart"),
        F.max(F.when(F.col("event_type") == "BEGIN_CHECKOUT", 1).otherwise(0)).alias("has_checkout"),
        F.max(F.when(F.col("event_type") == "PURCHASE_COMPLETED", 1).otherwise(0)).alias("has_purchase"),
    )

    # 2. Rollup by date, device, and channel
    funnel_summary = session_steps.groupBy("event_date", "device_type", "traffic_source").agg(
        F.countDistinct("session_id").alias("total_sessions"),
        F.countDistinct("user_id").alias("unique_visitors"),
        F.sum("has_view").alias("view_sessions"),
        F.sum("has_cart").alias("cart_sessions"),
        F.sum("has_checkout").alias("checkout_sessions"),
        F.sum("has_purchase").alias("purchase_sessions"),
    )

    # 3. Conversion percentages and step-to-step drop-offs
    funnel_mart = (
        funnel_summary.withColumn(
            "view_to_cart_rate_pct",
            F.round(
                (F.col("cart_sessions") / F.nullif(F.col("view_sessions"), F.lit(0))) * 100.0,
                2,
            ),
        )
        .withColumn(
            "cart_to_checkout_rate_pct",
            F.round(
                (F.col("checkout_sessions") / F.nullif(F.col("cart_sessions"), F.lit(0))) * 100.0,
                2,
            ),
        )
        .withColumn(
            "checkout_to_purchase_rate_pct",
            F.round(
                (F.col("purchase_sessions") / F.nullif(F.col("checkout_sessions"), F.lit(0))) * 100.0,
                2,
            ),
        )
        .withColumn(
            "overall_conversion_rate_pct",
            F.round(
                (F.col("purchase_sessions") / F.nullif(F.col("total_sessions"), F.lit(0))) * 100.0,
                2,
            ),
        )
    )

    return funnel_mart


def save_and_optimize_gold_mart(
    spark: SparkSession,
    df: DataFrame,
    target_table_path: str,
    partition_cols: Optional[List[str]] = None,
    zorder_cols: Optional[List[str]] = None,
) -> None:
    """Write DataFrame as an optimized Delta Lake table and perform Z-Ordering.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    df : DataFrame
        Calculated analytical mart.
    target_table_path : str
        Target storage path for the Gold Delta table.
    partition_cols : Optional[List[str]]
        Partitioning columns (e.g. ['order_date']).
    zorder_cols : Optional[List[str]]
        Clustering columns for multi-dimensional spatial sorting.
    """
    # Coalesce / repartition dynamically via AQE, write Delta table
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(target_table_path)

    # Delta Lake storage compaction and Z-ORDER BY
    if zorder_cols and len(zorder_cols) > 0:
        try:
            print(f"[Gold] Running OPTIMIZE and Z-ORDER BY {zorder_cols} on {target_table_path}...")
            optimize_table(spark, target_table_path, zorder_cols=zorder_cols)
        except Exception as e:
            print(f"[Gold] Warning: OPTIMIZE failed or skipped on {target_table_path}: {e}")


def run_gold_marts(
    spark: Optional[SparkSession] = None,
    config_path: Optional[str] = None,
) -> Dict[str, int]:
    """Execute full Gold layer mart computation and storage optimization.

    Parameters
    ----------
    spark : Optional[SparkSession]
        Existing SparkSession or None.
    config_path : Optional[str]
        Path to pipeline_config.yaml.

    Returns
    -------
    Dict[str, int]
        Row count in each Gold analytical mart.
    """
    config = load_pipeline_config(config_path)
    env = config.get("environment", "local")
    paths = config.get("paths", {}).get(env, {})

    silver_base = paths.get("silver", "data/lakehouse/silver")
    gold_base = paths.get("gold", "data/lakehouse/gold")

    if spark is None:
        spark = get_spark_session(app_name="GoldMartsEngine", config_path=config_path)

    counts: Dict[str, int] = {}

    silver_orders_path = f"{silver_base}/silver_orders"
    silver_cust_path = f"{silver_base}/silver_customers"
    silver_prod_path = f"{silver_base}/silver_products"
    silver_clicks_path = f"{silver_base}/silver_clickstream"

    # Mart 1: Daily Customer Lifetime Value
    if (
        Path(silver_orders_path).exists()
        and DeltaTable.isDeltaTable(spark, silver_orders_path)
        and Path(silver_cust_path).exists()
        and DeltaTable.isDeltaTable(spark, silver_cust_path)
    ):
        orders_df = spark.read.format("delta").load(silver_orders_path)
        cust_df = spark.read.format("delta").load(silver_cust_path)

        ltv_mart = build_daily_customer_ltv(spark, orders_df, cust_df)
        ltv_path = f"{gold_base}/gold_daily_customer_ltv"
        save_and_optimize_gold_mart(
            spark=spark,
            df=ltv_mart,
            target_table_path=ltv_path,
            partition_cols=["report_date"],
            zorder_cols=["customer_id"],
        )
        c = spark.read.format("delta").load(ltv_path).count()
        counts["gold_daily_customer_ltv"] = c
        print(f"[Gold] gold_daily_customer_ltv generated: {c} records.")

    # Mart 2: Category Sales Velocity
    if (
        Path(silver_orders_path).exists()
        and DeltaTable.isDeltaTable(spark, silver_orders_path)
        and Path(silver_prod_path).exists()
        and DeltaTable.isDeltaTable(spark, silver_prod_path)
    ):
        orders_df = spark.read.format("delta").load(silver_orders_path)
        prod_df = spark.read.format("delta").load(silver_prod_path)

        velocity_mart = build_category_sales_velocity(
            spark, orders_df, prod_df, use_salting=False
        )
        velocity_path = f"{gold_base}/gold_category_sales_velocity"
        save_and_optimize_gold_mart(
            spark=spark,
            df=velocity_mart,
            target_table_path=velocity_path,
            partition_cols=["order_date"],
            zorder_cols=["category_id"],
        )
        c = spark.read.format("delta").load(velocity_path).count()
        counts["gold_category_sales_velocity"] = c
        print(f"[Gold] gold_category_sales_velocity generated: {c} records.")

    # Mart 3: Retention and Conversion Funnel
    if Path(silver_clicks_path).exists() and DeltaTable.isDeltaTable(spark, silver_clicks_path):
        clicks_df = spark.read.format("delta").load(silver_clicks_path)

        funnel_mart = build_retention_funnel(spark, clicks_df)
        funnel_path = f"{gold_base}/gold_retention_funnel"
        save_and_optimize_gold_mart(
            spark=spark,
            df=funnel_mart,
            target_table_path=funnel_path,
            partition_cols=["event_date"],
            zorder_cols=["device_type", "traffic_source"],
        )
        c = spark.read.format("delta").load(funnel_path).count()
        counts["gold_retention_funnel"] = c
        print(f"[Gold] gold_retention_funnel generated: {c} records.")

    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gold Layer Marts Builder")
    parser.add_argument("--config", type=str, default=None, help="Path to pipeline_config.yaml")
    args = parser.parse_args()

    spark_sess = get_spark_session(app_name="GoldCLI", config_path=args.config)
    run_gold_marts(spark=spark_sess, config_path=args.config)
