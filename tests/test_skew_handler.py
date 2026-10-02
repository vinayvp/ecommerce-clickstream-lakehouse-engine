"""Unit tests for salting and data skew mitigation utility."""

import pytest
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.utils.skew_handler import (
    add_salt_column,
    detect_key_skew,
    execute_salted_aggregation,
    salted_join,
)


def test_add_salt_column(spark):
    data = [(f"KEY_{i % 5}", float(i)) for i in range(100)]
    df = spark.createDataFrame(data, ["key", "value"])

    num_salts = 4
    salted_df = add_salt_column(df, num_salts=num_salts, salt_col_name="_salt_id", seed=123)

    assert "_salt_id" in salted_df.columns
    distinct_salts = [r["_salt_id"] for r in salted_df.select("_salt_id").distinct().collect()]
    for s in distinct_salts:
        assert 0 <= s < num_salts


def test_detect_key_skew(spark):
    # Skewed dataset: KEY_HOT has 80 rows, others have 2 rows each
    skewed_data = [("KEY_HOT", 1.0)] * 80 + [(f"KEY_{i}", 1.0) for i in range(10)]
    df_skewed = spark.createDataFrame(skewed_data, ["key", "val"])

    is_skewed, top_df, metrics = detect_key_skew(
        df_skewed, key_col="key", top_n=5, skew_factor_threshold=3.0
    )

    assert is_skewed is True
    assert metrics["max_count"] == 80
    assert metrics["skew_factor"] > 3.0

    # Uniform dataset: each key has exactly 5 rows
    uniform_data = [(f"KEY_{i}", 1.0) for i in range(10) for _ in range(5)]
    df_uniform = spark.createDataFrame(uniform_data, ["key", "val"])

    is_uniform_skewed, _, metrics_uniform = detect_key_skew(
        df_uniform, key_col="key", top_n=5, skew_factor_threshold=3.0
    )
    assert is_uniform_skewed is False
    assert metrics_uniform["skew_factor"] == 1.0


def test_execute_salted_aggregation_matches_standard(spark):
    # Create dataset with skewed keys and metrics
    data = [
        ("HOT_KEY", 10.0, 1),
        ("HOT_KEY", 20.0, 2),
        ("HOT_KEY", 30.0, 3),
        ("HOT_KEY", 40.0, 4),
        ("COLD_KEY_1", 5.0, 1),
        ("COLD_KEY_2", 15.0, 2),
    ]
    df = spark.createDataFrame(data, ["category", "amount", "qty"])

    # 1. Standard Aggregation
    std_df = df.groupBy("category").agg(
        F.sum("amount").alias("total_amount"),
        F.sum("qty").alias("total_qty"),
        F.min("amount").alias("min_amount"),
        F.max("amount").alias("max_amount"),
    )
    std_results = {r["category"]: r.asDict() for r in std_df.collect()}

    # 2. Salted Aggregation
    stage1_aggs = {
        "total_amount": ("sum", "amount"),
        "total_qty": ("sum", "qty"),
        "min_amount": ("min", "amount"),
        "max_amount": ("max", "amount"),
    }
    salted_df = execute_salted_aggregation(
        df=df,
        group_cols=["category"],
        stage1_aggs=stage1_aggs,
        num_salts=4,
    )
    salted_results = {r["category"]: r.asDict() for r in salted_df.collect()}

    # Verify identical counts and mathematical equivalence
    assert len(std_results) == len(salted_results)
    for cat, std_row in std_results.items():
        assert cat in salted_results
        salted_row = salted_results[cat]
        assert std_row["total_amount"] == pytest.approx(salted_row["total_amount"])
        assert std_row["total_qty"] == pytest.approx(salted_row["total_qty"])
        assert std_row["min_amount"] == pytest.approx(salted_row["min_amount"])
        assert std_row["max_amount"] == pytest.approx(salted_row["max_amount"])


def test_salted_join_matches_standard_join(spark):
    # Large fact table
    fact_data = [
        ("HOT_KEY", 100),
        ("HOT_KEY", 200),
        ("HOT_KEY", 300),
        ("KEY_B", 400),
    ]
    fact_df = spark.createDataFrame(fact_data, ["join_key", "metric"])

    # Dimension table
    dim_data = [
        ("HOT_KEY", "Popular Dimension"),
        ("KEY_B", "Regular Dimension"),
        ("KEY_C", "Unused Dimension"),
    ]
    dim_df = spark.createDataFrame(dim_data, ["join_key", "dim_name"])

    # Standard Join
    std_joined = fact_df.join(dim_df, on="join_key", how="inner")
    std_count = std_joined.count()

    # Salted Join
    salted_joined = salted_join(
        large_df=fact_df,
        small_df=dim_df,
        join_key="join_key",
        join_type="inner",
        num_salts=3,
    )
    salted_count = salted_joined.count()

    assert std_count == salted_count
    assert set(std_joined.columns) == set(salted_joined.columns)
