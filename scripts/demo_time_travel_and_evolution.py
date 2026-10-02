"""Demonstration of Delta Lake ACID Governance, Time Travel, and Schema Evolution.

Showcases:
1. ACID Transactions & Commit History auditing.
2. Point-in-time Time Travel queries (versionAsOf).
3. Rollback of accidental/bad state mutations via Delta RESTORE.
4. Strict Schema Enforcement failure vs Controlled Schema Evolution (mergeSchema).
5. Storage compaction and multi-dimensional clustering via OPTIMIZE Z-ORDER BY.
"""

from __future__ import annotations

import os
import shutil
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import Row
import pyspark.sql.functions as F

from src.utils.delta_utils import (
    get_table_history,
    optimize_table,
    read_version_as_of,
    restore_table,
)
from src.utils.spark_session import get_spark_session


def demo_acid_and_time_travel(spark) -> None:
    print("\n" + "=" * 70)
    print("DEMO 1: DELTA ACID TRANSACTIONS, TIME TRAVEL & RESTORE ROLLBACK")
    print("=" * 70)

    demo_dir = "/tmp/lakehouse_demo/time_travel"
    if Path(demo_dir).exists():
        shutil.rmtree(demo_dir)

    # 1. Initial State (Version 0)
    initial_data = [
        ("CUST-101", "Alice", "STANDARD", 150.0),
        ("CUST-102", "Bob", "PREMIUM", 450.0),
        ("CUST-103", "Charlie", "VIP", 1200.0),
    ]
    df_v0 = spark.createDataFrame(initial_data, ["customer_id", "name", "segment", "lifetime_spend"])
    df_v0.write.format("delta").mode("overwrite").save(demo_dir)
    print("\n[Commit V0] Initialized table:")
    spark.read.format("delta").load(demo_dir).show()

    # 2. Legitimate Mutation (Version 1)
    df_v1_update = spark.createDataFrame(
        [("CUST-101", "Alice", "PREMIUM", 350.0)],
        ["customer_id", "name", "segment", "lifetime_spend"],
    )
    from delta.tables import DeltaTable
    dt = DeltaTable.forPath(spark, demo_dir)
    dt.alias("t").merge(
        df_v1_update.alias("s"),
        "t.customer_id = s.customer_id",
    ).whenMatchedUpdateAll().execute()
    print("\n[Commit V1] Legitimate update applied (Alice promoted to PREMIUM):")
    spark.read.format("delta").load(demo_dir).show()

    # 3. Bad State Mutation / Erroneous Query (Version 2)
    print("\n[Simulating Disaster] Erroneous batch sets all segments to 'MALICIOUS_CORRUPTION' and spend to 0...")
    spark.read.format("delta").load(demo_dir).withColumn(
        "segment", F.lit("MALICIOUS_CORRUPTION")
    ).withColumn("lifetime_spend", F.lit(0.0)).write.format("delta").mode("overwrite").save(demo_dir)
    print("\n[Commit V2] Corrupted table state:")
    spark.read.format("delta").load(demo_dir).show()

    # 4. Audit Table History
    print("\n[Delta Governance] Inspecting Delta Transaction Log History:")
    history_df = get_table_history(spark, demo_dir, limit=10)
    history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

    # 5. Time Travel Query (Point-in-Time Snapshot)
    print("\n[Time Travel] Querying historical state as of Version 1 (before corruption):")
    v1_snapshot = read_version_as_of(spark, demo_dir, version=1)
    v1_snapshot.show()

    # 6. Restore / Rollback State
    print("\n[Rollback] Executing Delta RESTORE to Version 1...")
    restore_res = restore_table(spark, demo_dir, to_version=1)
    restore_res.show(truncate=False)

    print("\n[Restored State] Verified table contents after rollback to Version 1:")
    spark.read.format("delta").load(demo_dir).show()


def demo_schema_enforcement_and_evolution(spark) -> None:
    print("\n" + "=" * 70)
    print("DEMO 2: STRICT SCHEMA ENFORCEMENT VS CONTROLLED SCHEMA EVOLUTION")
    print("=" * 70)

    demo_dir = "/tmp/lakehouse_demo/schema_evolution"
    if Path(demo_dir).exists():
        shutil.rmtree(demo_dir)

    # 1. Base table schema
    base_data = [("PROD-1", "Laptop", 999.99), ("PROD-2", "Mouse", 29.99)]
    df_base = spark.createDataFrame(base_data, ["product_id", "product_name", "price"])
    df_base.write.format("delta").mode("overwrite").save(demo_dir)
    print("\n[Initial Table] Schema: [product_id, product_name, price]")
    spark.read.format("delta").load(demo_dir).printSchema()

    # 2. Attempt to append with new column without mergeSchema (Strict Enforcement)
    new_data = [("PROD-3", "Keyboard", 79.99, "Electronics", "Apex")]
    df_new = spark.createDataFrame(new_data, ["product_id", "product_name", "price", "category", "brand"])

    print("\n[Test Strict Enforcement] Attempting append without 'mergeSchema' option...")
    try:
        df_new.write.format("delta").mode("append").save(demo_dir)
        print("Unexpected success!")
    except Exception as e:
        print(">>> SUCCESS: Delta blocked unexpected schema drift!")
        print(f"Exception snippet: {str(e)[:160]}...")

    # 3. Controlled Schema Evolution with .option("mergeSchema", "true")
    print("\n[Controlled Schema Evolution] Appending with .option('mergeSchema', 'true')...")
    df_new.write.format("delta").mode("append").option("mergeSchema", "true").save(demo_dir)
    print("Schema evolved successfully! New table schema:")
    spark.read.format("delta").load(demo_dir).printSchema()
    print("Table contents with backward-compatible NULL backfills:")
    spark.read.format("delta").load(demo_dir).show()


def demo_compaction_and_zorder(spark) -> None:
    print("\n" + "=" * 70)
    print("DEMO 3: STORAGE COMPACTION & MULTI-DIMENSIONAL Z-ORDER BY")
    print("=" * 70)

    demo_dir = "/tmp/lakehouse_demo/zorder_test"
    if Path(demo_dir).exists():
        shutil.rmtree(demo_dir)

    # Generate multiple small commits to simulate the "small files problem"
    print("\n[Simulating Small Files] Generating 5 small appends...")
    for i in range(5):
        chunk = [
            (f"ID-{i * 10 + j}", f"USER-{j}", 100.0 * (j + 1))
            for j in range(5)
        ]
        df_chunk = spark.createDataFrame(chunk, ["record_id", "user_id", "metric"])
        df_chunk.repartition(3).write.format("delta").mode("append").save(demo_dir)

    print("\n[Executing OPTIMIZE Z-ORDER BY] Clustering on user_id...")
    opt_metrics = optimize_table(spark, demo_dir, zorder_cols=["user_id"])
    opt_metrics.show(truncate=False)


def run_governance_demos() -> None:
    spark = get_spark_session(app_name="DeltaGovernanceDemo")
    try:
        demo_acid_and_time_travel(spark)
        demo_schema_enforcement_and_evolution(spark)
        demo_compaction_and_zorder(spark)
        print("\nAll Delta Lake Governance demonstrations passed successfully!")
    finally:
        spark.stop()


if __name__ == "__main__":
    run_governance_demos()
