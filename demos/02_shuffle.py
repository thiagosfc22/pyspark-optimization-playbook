"""Step 2 — Kill the shuffle.

A join normally forces Spark to shuffle *both* sides across the network so that
matching keys land on the same partition (a sort-merge join). If one side is
small, you can skip all of that: broadcast the small side to every executor and
join locally, with zero shuffle of the big side.

The catch shown here: Spark only auto-broadcasts when it *knows* a side is
small (``autoBroadcastJoinThreshold``, 10 MB by default). Data coming from a
source without good statistics often slips past that, silently falling back to
a full shuffle. The ``broadcast()`` hint is you telling Spark what you know.

Run:  python demos/02_shuffle.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from common import banner, count_shuffles, data_path, get_spark, timed


def demo(spark):
    # Force the "Spark doesn't know the dim is small" scenario so the naive
    # path is an honest sort-merge join instead of a silent auto-broadcast.
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    tx = spark.read.parquet(data_path("transactions"))
    dim = spark.read.parquet(data_path("customers"))

    naive = (
        tx.join(dim, "customer_id")
        .groupBy("city").agg(F.sum("amount").alias("total"))
    )
    optimized = (
        tx.join(broadcast(dim), "customer_id")
        .groupBy("city").agg(F.sum("amount").alias("total"))
    )

    banner("NAIVE — sort-merge join (shuffles both sides)")
    naive.explain()
    print(f"shuffles in plan: {count_shuffles(naive)}")

    banner("OPTIMIZED — broadcast join (big side never moves)")
    optimized.explain()
    print(f"shuffles in plan: {count_shuffles(optimized)}")

    banner("timings")
    with timed("naive (sort-merge)"):
        naive.collect()
    with timed("broadcast join"):
        optimized.collect()

    # Same numbers out — the optimization must never change the answer.
    assert sorted(r["total"] for r in naive.collect()) == \
           sorted(r["total"] for r in optimized.collect())
    print("\n✓ identical results — faster AND correct")


if __name__ == "__main__":
    spark = get_spark("02-shuffle")
    spark.sparkContext.setLogLevel("ERROR")
    demo(spark)
    spark.stop()
