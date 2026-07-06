"""Step 4 — Read less data.

The fastest pipeline is the one that never reads the rows it doesn't need.
Columnar formats (Parquet/Delta) give you two levers for free, if you let them:

* **Partition pruning** — the table is partitioned by ``dt``; filtering on ``dt``
  lets Spark skip whole directories at *file-listing* time. It never opens them.
* **Predicate pushdown** — a filter on a regular column (``amount``) is handed
  down into the Parquet reader, which uses row-group min/max stats to skip
  chunks without decoding them.

Both show up in the plan. Learn to spot ``PartitionFilters`` and
``PushedFilters`` and you can see waste before you run anything.

Run:  python demos/04_read_less.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from common import banner, data_path, get_spark, timed


def demo(spark):
    tx = spark.read.parquet(data_path("transactions"))  # partitioned by dt

    banner("PARTITION PRUNING — filter on the partition column (dt)")
    one_day = tx.filter(F.col("dt") == "2026-01-05")
    one_day.explain()  # look for PartitionFilters: [..., (dt = 2026-01-05)]

    banner("PREDICATE PUSHDOWN — filter on a data column (amount)")
    pricey = tx.filter(F.col("amount") > 999)
    pricey.explain()  # look for PushedFilters: [GreaterThan(amount, 999.0)]

    banner("timings — one partition vs the whole table")
    with timed("scan ALL 30 days"):
        tx.agg(F.sum("amount")).collect()
    with timed("scan ONE day (pruned)"):
        one_day.agg(F.sum("amount")).collect()

    print(
        "\nRule of thumb: filter on partition columns as early as possible, and\n"
        "partition by what you actually filter on (usually a date). Over-\n"
        "partitioning (e.g. by a high-cardinality id) creates millions of tiny\n"
        "files and is its own performance problem."
    )


if __name__ == "__main__":
    spark = get_spark("04-read-less")
    spark.sparkContext.setLogLevel("ERROR")
    demo(spark)
    spark.stop()
