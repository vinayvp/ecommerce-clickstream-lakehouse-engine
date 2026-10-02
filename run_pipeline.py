"""Master End-to-End Lakehouse Pipeline Runner.

Executes Medallion Architecture progression:
Bronze (Raw Ingest) -> Silver (Conform, CDC Merge) -> Gold (Aggregated Marts & Z-Order).
"""

from __future__ import annotations

import argparse
import sys
import time

from src.bronze.ingest_raw import run_bronze_ingestion
from src.silver.transform_silver import run_silver_transformations
from src.gold.build_gold_marts import run_gold_marts
from src.utils.spark_session import get_spark_session


def run_pipeline(config_path: str = "configs/pipeline_config.yaml") -> None:
    """Execute end-to-end Medallion pipeline."""
    start_time = time.time()
    print("=" * 80)
    print("STARTING ENTERPRISE E-COMMERCE & CLICKSTREAM LAKEHOUSE ENGINE")
    print("=" * 80)

    spark = get_spark_session(app_name="LakehouseOrchestrator", config_path=config_path)

    try:
        # Layer 1: Bronze
        print("\n>>> [1/3] EXECUTING BRONZE LAYER (RAW INGESTION)...")
        b_start = time.time()
        bronze_results = run_bronze_ingestion(spark=spark, config_path=config_path)
        print(f"Bronze layer completed in {time.time() - b_start:.2f}s: {bronze_results}")

        # Layer 2: Silver
        print("\n>>> [2/3] EXECUTING SILVER LAYER (CONFORMING, CDC MERGE, DEDUPLICATION)...")
        s_start = time.time()
        silver_results = run_silver_transformations(spark=spark, config_path=config_path)
        print(f"Silver layer completed in {time.time() - s_start:.2f}s: {silver_results}")

        # Layer 3: Gold
        print("\n>>> [3/3] EXECUTING GOLD LAYER (ANALYTICAL MARTS, BROADCAST JOINS, Z-ORDER)...")
        g_start = time.time()
        gold_results = run_gold_marts(spark=spark, config_path=config_path)
        print(f"Gold layer completed in {time.time() - g_start:.2f}s: {gold_results}")

        print("\n" + "=" * 80)
        print(f"LAKEHOUSE PIPELINE RUN FINISHED IN {time.time() - start_time:.2f} SECONDS")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Lakehouse Pipeline Runner")
    parser.add_argument("--config", type=str, default="configs/pipeline_config.yaml")
    args = parser.parse_args()

    run_pipeline(args.config)
