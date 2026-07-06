"""Step 3 — Hunt the skew.

Skew is when one key holds a wildly disproportionate share of the rows. Because
Spark groups a key into a single partition, that one partition's task runs long
after every other core has gone idle — the classic "99% done, stuck forever".

Two fixes, both shown here:

* **AQE skew join** (Spark 3+): Spark detects an oversized partition at runtime
  and splits it automatically. Free, but only for joins, and only when enabled.
* **Salting** (manual, two-stage aggregation): spread the hot key across N
  synthetic sub-keys, aggregate the partials, then fold them together. This is
  the tool when AQE can't help — and knowing *why* it works is the point.

Run:  python demos/03_skew.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from common import banner, data_path, get_spark, timed

SALT = 16


def demo(spark):
    skew = spark.read.parquet(data_path("skewed"))  # customer 0 ≈ 50% of rows

    banner("the skew — one key dwarfs the rest")
    skew.groupBy("customer_id").count().orderBy(F.desc("count")).show(5)

    # --- naive: the hot key's partition becomes a straggler -------------------
    with timed("naive groupBy (skewed)"):
        skew.groupBy("customer_id").agg(F.sum("amount").alias("total")).count()

    # --- salted two-stage aggregation -----------------------------------------
    # Stage 1: (customer_id, salt) spreads customer 0 across SALT partitions.
    # Stage 2: fold the SALT partials back into one total per customer.
    with timed("salted groupBy"):
        (
            skew.withColumn("salt", (F.rand() * SALT).cast("int"))
            .groupBy("customer_id", "salt").agg(F.sum("amount").alias("partial"))
            .groupBy("customer_id").agg(F.sum("partial").alias("total"))
            .count()
        )

    # Correctness check: salting must not change a single total.
    plain = {r["customer_id"]: r["total"] for r in
             skew.groupBy("customer_id").agg(F.sum("amount").alias("total")).collect()}
    salted = {r["customer_id"]: r["total"] for r in (
        skew.withColumn("salt", (F.rand() * SALT).cast("int"))
        .groupBy("customer_id", "salt").agg(F.sum("amount").alias("partial"))
        .groupBy("customer_id").agg(F.sum("partial").alias("total"))
    ).collect()}
    assert plain.keys() == salted.keys()
    assert all(abs(plain[k] - salted[k]) < 1e-6 for k in plain)
    print("\n✓ salted totals match the naive totals exactly")

    print(
        "\nFor skewed *joins*, prefer AQE — it splits the hot partition for free:\n"
        "  spark.conf.set('spark.sql.adaptive.skewJoin.enabled', 'true')\n"
        "Reach for salting when AQE can't see the skew (e.g. heavy aggregations)."
    )


if __name__ == "__main__":
    spark = get_spark("03-skew")
    spark.sparkContext.setLogLevel("ERROR")
    demo(spark)
    spark.stop()
