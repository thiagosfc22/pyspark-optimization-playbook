"""Step 7 — Go incremental (the biggest saver, and the least questioned waste).

Most "slow pipeline" tickets aren't a Spark tuning problem at all — they're a
pipeline that reprocesses the entire history every run when only today's slice
changed. Reprocessing everything is a tax nobody questions because it's the
default shape of a naive job.

This demo makes the waste a number: a full recompute scans all 30 days; an
incremental run scans only the newest partition. Same fresh output, a fraction
of the work.

In production on Delta Lake this is a ``MERGE INTO`` keyed on the record's
natural key, which also makes the job idempotent — re-running never duplicates.
The sketch is at the bottom of this file and in the README.

Run:  python demos/07_incremental.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pyspark.sql import functions as F

from common import banner, data_path, get_spark, timed


def demo(spark):
    tx = spark.read.parquet(data_path("transactions"))
    latest = "2026-01-30"  # the only partition that "changed" since last run

    banner("FULL reprocess — touches every day, every run")
    with timed("full recompute (all days)"):
        tx.groupBy("dt").agg(F.sum("amount").alias("total")).count()
    rows_full = tx.count()

    banner("INCREMENTAL — only the new partition")
    with timed("incremental (1 day)"):
        (
            tx.filter(F.col("dt") == latest)
            .groupBy("dt").agg(F.sum("amount").alias("total"))
            .count()
        )
    rows_incr = tx.filter(F.col("dt") == latest).count()

    print(
        f"\nrows scanned:  full = {rows_full:,}   incremental = {rows_incr:,}"
        f"   → {rows_full / rows_incr:.0f}× less data"
    )
    print(
        "\nProduction shape on Delta Lake (idempotent, keyed on a natural key):\n"
        "    delta_target.alias('t').merge(\n"
        "        new_data.alias('s'), 't.id = s.id')\\\n"
        "      .whenMatchedUpdateAll()\\\n"
        "      .whenNotMatchedInsertAll()\\\n"
        "      .execute()\n"
        "Re-running the same batch changes nothing — safe to retry."
    )


if __name__ == "__main__":
    spark = get_spark("07-incremental")
    spark.sparkContext.setLogLevel("ERROR")
    demo(spark)
    spark.stop()
