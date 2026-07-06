"""Step 6 — Cache only with reuse.

Spark is lazy: a DataFrame is a recipe, not a result. Every action re-runs the
whole recipe from the source. If you trigger three actions on the same derived
DataFrame, you pay for it three times — unless you cache it, materializing it
once so later actions read from memory.

The trap is caching out of habit. If a DataFrame is used *once*, ``cache()``
only adds a materialization step and eats memory another stage could have used.
Cache is for reuse, not for comfort.

Run:  python demos/06_cache.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from common import banner, data_path, get_spark, timed


def demo(spark):
    tx = spark.read.parquet(data_path("transactions"))

    # A non-trivial derived frame we're going to hit several times.
    base = (
        tx.filter(F.col("amount") > 100)
        .withColumn("tax", F.col("amount") * 0.1)
        .withColumn("net", F.col("amount") - F.col("tax"))
    )

    banner("three actions, WITHOUT cache — 'base' is recomputed each time")
    with timed("no cache — 3 actions"):
        base.count()
        base.agg(F.sum("amount")).collect()
        base.agg(F.sum("net")).collect()

    banner("three actions, WITH cache — computed once, then read from memory")
    base.cache()
    base.count()  # first action materializes the cache
    with timed("cached — 3 actions"):
        base.count()
        base.agg(F.sum("amount")).collect()
        base.agg(F.sum("net")).collect()
    base.unpersist()

    print(
        "\nAnti-pattern: if you only used 'base' once, cache() would be pure\n"
        "overhead. Cache when a frame is reused across ≥2 actions AND it's\n"
        "expensive to recompute. Always unpersist() when you're done with it."
    )


if __name__ == "__main__":
    spark = get_spark("06-cache")
    spark.sparkContext.setLogLevel("ERROR")
    demo(spark)
    spark.stop()
