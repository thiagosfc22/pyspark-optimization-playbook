"""Step 1 — Measure before you touch anything.

The most expensive optimization is the one you do to the wrong stage. Before
changing a single line, read the plan and watch the Spark UI. The plan tells
you what Spark *intends* to do; the UI tells you what actually hurt.

Run:  python demos/01_measure.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from common import banner, count_shuffles, data_path, get_spark, timed


def demo(spark):
    tx = spark.read.parquet(data_path("transactions"))
    dim = spark.read.parquet(data_path("customers"))

    q = (
        tx.join(dim, "customer_id")
        .groupBy("city")
        .agg(F.sum("amount").alias("total"))
    )

    banner("q.explain('formatted') — read this FIRST")
    q.explain("formatted")

    print(f"\nShuffles (Exchange nodes) in the plan: {count_shuffles(q)}")

    with timed("full query"):
        q.collect()

    print(
        "\nWhile a job runs, open the Spark UI at http://localhost:4040\n"
        "  • Stages tab   → the longest bar is your bottleneck; start there\n"
        "  • one task ≫ its siblings (e.g. 10×) → data skew (see 03_skew)\n"
        "  • 'Spill (disk)' > 0 → not enough memory; tune partitions/broadcast\n"
        "  • many tiny tasks → shuffle.partitions too high for this data size"
    )


if __name__ == "__main__":
    spark = get_spark("01-measure")
    spark.sparkContext.setLogLevel("ERROR")
    demo(spark)
    input("\nPress Enter to exit (keeps the Spark UI alive to explore)... ")
    spark.stop()
