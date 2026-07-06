"""Tests that hold each optimization to two standards:

1. **Correct** — the optimized version returns the *same* result as the naive
   one. A faster query that changes the answer is worse than useless.
2. **Cheaper** — it actually removes work (fewer shuffles), not just moves it.

These run on tiny in-memory data so the suite is fast and needs no ./data.
"""
import os
import sys

import pytest
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common import count_shuffles, get_spark


@pytest.fixture(scope="session")
def spark():
    s = get_spark("tests", shuffle_partitions=4)
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


@pytest.fixture(scope="session")
def data(spark):
    dim = (
        spark.range(100)
        .withColumnRenamed("id", "customer_id")
        .withColumn("city", (F.col("customer_id") % 5).cast("string"))
    )
    fact = (
        spark.range(10_000)
        .withColumn("customer_id", F.col("id") % 100)
        .withColumn("amount", (F.col("id") % 50).cast("double"))
    )
    return fact, dim


def test_broadcast_is_correct_and_cheaper(spark, data):
    fact, dim = data
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

    naive = fact.join(dim, "customer_id").groupBy("city").agg(F.sum("amount").alias("t"))
    opt = fact.join(broadcast(dim), "customer_id").groupBy("city").agg(F.sum("amount").alias("t"))

    as_dict = lambda df: {r["city"]: r["t"] for r in df.collect()}
    assert as_dict(naive) == as_dict(opt)                 # correct
    assert count_shuffles(opt) < count_shuffles(naive)    # cheaper


def test_salting_preserves_every_total(spark):
    # customer 0 holds half the rows — the same skew shape as the demo.
    df = (
        spark.range(10_000)
        .withColumn("k", F.when(F.col("id") % 2 == 0, F.lit(0)).otherwise(F.col("id") % 100))
        .withColumn("v", (F.col("id") % 10).cast("double"))
    )
    plain = {r["k"]: r["t"] for r in df.groupBy("k").agg(F.sum("v").alias("t")).collect()}
    salted = {r["k"]: r["t"] for r in (
        df.withColumn("salt", (F.rand() * 8).cast("int"))
        .groupBy("k", "salt").agg(F.sum("v").alias("p"))
        .groupBy("k").agg(F.sum("p").alias("t"))
    ).collect()}

    assert plain.keys() == salted.keys()
    for k in plain:
        assert plain[k] == pytest.approx(salted[k])


def test_native_matches_udf(spark, data):
    from pyspark.sql.types import DoubleType

    fact, _ = data

    @F.udf(DoubleType())
    def with_tax(a):
        return a * 1.1

    via_udf = fact.select(F.sum(with_tax("amount")).alias("t")).first()["t"]
    via_native = fact.select(F.sum(F.col("amount") * 1.1).alias("t")).first()["t"]
    assert via_udf == pytest.approx(via_native)
