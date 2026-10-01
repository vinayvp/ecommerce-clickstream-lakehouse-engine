"""Data skew mitigation utilities using key salting.

In distributed data processing, extreme key skew (hot keys) causes severe
straggler tasks and out-of-memory (OOM) errors because all records for a hot
key are hashed to a single Spark reducer partition.

This module provides production-grade utilities to:
1. Detect data skew across partitions and key distributions.
2. Add uniform random salt columns to distribute hot keys.
3. Perform two-stage salted aggregations.
4. Perform salted joins by replicating dimension keys across salt buckets.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple, Union
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame


def add_salt_column(
    df: DataFrame,
    num_salts: int = 8,
    salt_col_name: str = "_salt_id",
    seed: Optional[int] = 42,
) -> DataFrame:
    """Add a random integer salt column bounded between 0 and num_salts - 1.

    Parameters
    ----------
    df : DataFrame
        Input Spark DataFrame.
    num_salts : int
        Number of salt partitions/buckets (default 8).
    salt_col_name : str
        Name of the created salt column.
    seed : Optional[int]
        Random seed for deterministic behavior in testing.

    Returns
    -------
    DataFrame
        DataFrame containing the new salt column.
    """
    if num_salts <= 1:
        return df.withColumn(salt_col_name, F.lit(0))

    if seed is not None:
        rand_expr = F.rand(seed=seed)
    else:
        rand_expr = F.rand()

    return df.withColumn(salt_col_name, F.floor(rand_expr * num_salts).cast("int"))


def detect_key_skew(
    df: DataFrame,
    key_col: str,
    top_n: int = 10,
    skew_factor_threshold: float = 3.0,
) -> Tuple[bool, DataFrame, Dict[str, float]]:
    """Analyze key frequency distribution and identify presence of key skew.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame.
    key_col : str
        The join or group-by key column to analyze.
    top_n : int
        Number of top keys to inspect.
    skew_factor_threshold : float
        Ratio of max key count to average key count above which skew is flagged.

    Returns
    -------
    Tuple[bool, DataFrame, Dict[str, float]]
        (is_skewed, top_keys_df, metrics_dict)
    """
    freq_df = (
        df.groupBy(key_col)
        .agg(F.count(F.lit(1)).alias("_key_count"))
        .orderBy(F.desc("_key_count"))
    )

    stats = freq_df.select(
        F.max("_key_count").alias("max_count"),
        F.avg("_key_count").alias("avg_count"),
        F.count(F.lit(1)).alias("distinct_keys"),
        F.stddev("_key_count").alias("stddev_count"),
    ).first()

    if not stats or stats["avg_count"] is None or stats["avg_count"] == 0:
        return False, freq_df.limit(top_n), {"skew_factor": 1.0, "max_count": 0, "avg_count": 0}

    max_c = float(stats["max_count"])
    avg_c = float(stats["avg_count"])
    skew_factor = max_c / avg_c if avg_c > 0 else 1.0

    is_skewed = skew_factor >= skew_factor_threshold

    metrics = {
        "skew_factor": round(skew_factor, 2),
        "max_count": max_c,
        "avg_count": round(avg_c, 2),
        "distinct_keys": int(stats["distinct_keys"] or 0),
    }

    return is_skewed, freq_df.limit(top_n), metrics


def execute_salted_aggregation(
    df: DataFrame,
    group_cols: Union[str, List[str]],
    stage1_aggs: Dict[str, Tuple[str, str]],
    num_salts: int = 8,
    salt_col_name: str = "_salt_id",
) -> DataFrame:
    """Execute a two-stage aggregation to eliminate partition skew on hot keys.

    Stage 1: Groups by `[*group_cols, _salt_id]` and computes partial aggregations,
             spreading hot keys across `num_salts` partitions.
    Stage 2: Groups by `group_cols` and computes the final aggregated metrics.

    Parameters
    ----------
    df : DataFrame
        Input DataFrame with skewed keys.
    group_cols : Union[str, List[str]]
        The logical grouping keys (e.g., 'category_id' or ['customer_id', 'date']).
    stage1_aggs : Dict[str, Tuple[str, str]]
        Mapping of output column name -> (operation, source_column_name).
        Supported operations: 'sum', 'count', 'min', 'max'.
    num_salts : int
        Number of salt partitions to spread each key across.
    salt_col_name : str
        Intermediate salt column name.

    Returns
    -------
    DataFrame
        Aggregated DataFrame with mathematically exact results, free of skew.
    """
    if isinstance(group_cols, str):
        cols = [group_cols]
    else:
        cols = list(group_cols)

    salted_df = add_salt_column(df, num_salts=num_salts, salt_col_name=salt_col_name)

    # Stage 1 expressions
    s1_exprs: List[Column] = []
    # Stage 2 expressions
    s2_exprs: List[Column] = []

    for out_name, (op, src_col) in stage1_aggs.items():
        op_lower = op.lower()
        partial_col = f"_{out_name}_partial"

        if op_lower == "sum":
            s1_exprs.append(F.sum(src_col).alias(partial_col))
            s2_exprs.append(F.sum(partial_col).alias(out_name))
        elif op_lower == "count":
            s1_exprs.append(F.count(src_col).alias(partial_col))
            s2_exprs.append(F.sum(partial_col).alias(out_name))
        elif op_lower == "min":
            s1_exprs.append(F.min(src_col).alias(partial_col))
            s2_exprs.append(F.min(partial_col).alias(out_name))
        elif op_lower == "max":
            s1_exprs.append(F.max(src_col).alias(partial_col))
            s2_exprs.append(F.max(partial_col).alias(out_name))
        else:
            raise ValueError(f"Unsupported aggregation operation for salting: {op}")

    # Stage 1: Group by business keys + salt
    stage1_df = salted_df.groupBy(*(cols + [salt_col_name])).agg(*s1_exprs)

    # Stage 2: Final group by business keys only
    stage2_df = stage1_df.groupBy(*cols).agg(*s2_exprs)

    return stage2_df


def salted_join(
    large_df: DataFrame,
    small_df: DataFrame,
    join_key: str,
    join_type: str = "inner",
    num_salts: int = 8,
    salt_col_name: str = "_salt_id",
) -> DataFrame:
    """Perform a salted join between a large skewed table and another table.

    1. Adds a random salt [0, num_salts - 1] to the large skewed table.
    2. Explodes the other table with an array containing all salt values [0, ..., num_salts - 1].
    3. Joins on both `join_key` and `_salt_id`, distributing each hot key across `num_salts` partitions.
    4. Drops the salt column from the resulting DataFrame.

    Parameters
    ----------
    large_df : DataFrame
        Large fact DataFrame exhibiting key skew.
    small_df : DataFrame
        Dimension or smaller fact DataFrame to join against.
    join_key : str
        Common join column name present in both DataFrames.
    join_type : str
        Spark SQL join type ('inner', 'left', etc.).
    num_salts : int
        Number of salt partitions.
    salt_col_name : str
        Intermediate salt column name.

    Returns
    -------
    DataFrame
        Joined DataFrame with skew eliminated.
    """
    # 1. Salt large DataFrame
    salted_large = add_salt_column(
        large_df, num_salts=num_salts, salt_col_name=salt_col_name
    )

    # 2. Replicate smaller DataFrame across all salt values
    salt_array = F.array([F.lit(i) for i in range(num_salts)])
    replicated_small = small_df.withColumn(salt_col_name, F.explode(salt_array))

    # 3. Join on (join_key, salt_col_name)
    joined = salted_large.join(
        replicated_small,
        on=[join_key, salt_col_name],
        how=join_type,
    ).drop(salt_col_name)

    return joined
