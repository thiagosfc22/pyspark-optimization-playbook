"""Generate the synthetic datasets every demo runs against.

Deterministic on purpose: the "randomness" comes from arithmetic on a row id,
not from a random seed, so every run — and every test — sees identical data.

Three tables, each chosen to make one lesson visible:

* ``customers``    — small dimension, the kind you want to broadcast.
* ``transactions`` — large fact, partitioned by date (for pruning demos).
* ``skewed``       — same shape, but one "hot" customer owns ~50% of the rows.
"""
import sys

from pyspark.sql import functions as F

from common import get_spark, data_path

FACT_ROWS = 5_000_000
N_CUSTOMERS = 50_000
N_DAYS = 30
CITIES = ["SP", "RJ", "BH", "POA", "REC"]


def build(spark, fact_rows: int = FACT_ROWS) -> None:
    # --- dimension: small enough to broadcast ---------------------------------
    dim = (
        spark.range(N_CUSTOMERS)
        .withColumnRenamed("id", "customer_id")
        .withColumn("name", F.concat(F.lit("customer_"), F.col("customer_id")))
        .withColumn(
            "city",
            F.element_at(
                F.array(*[F.lit(c) for c in CITIES]),
                (F.col("customer_id") % len(CITIES) + 1).cast("int"),
            ),
        )
    )
    dim.write.mode("overwrite").parquet(data_path("customers"))

    # --- fact: large, partitioned by date -------------------------------------
    fact = (
        spark.range(fact_rows)
        .withColumn("customer_id", F.col("id") % N_CUSTOMERS)
        .withColumn("amount", (F.col("id") % 1000).cast("double") + F.lit(0.99))
        .withColumn("day", (F.col("id") % N_DAYS).cast("int"))
        .withColumn("dt", F.date_add(F.lit("2026-01-01"), F.col("day")))
        .drop("day")
    )
    fact.write.mode("overwrite").partitionBy("dt").parquet(data_path("transactions"))

    # --- skewed: customer 0 holds ~half of all rows ---------------------------
    skew = (
        spark.range(fact_rows)
        .withColumn(
            "customer_id",
            F.when(F.col("id") % 2 == 0, F.lit(0)).otherwise(F.col("id") % N_CUSTOMERS),
        )
        .withColumn("amount", (F.col("id") % 1000).cast("double"))
    )
    skew.write.mode("overwrite").parquet(data_path("skewed"))


if __name__ == "__main__":
    rows = int(sys.argv[1]) if len(sys.argv) > 1 else FACT_ROWS
    spark = get_spark("generate-data")
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Generating {rows:,} fact rows into ./data ...")
    build(spark, rows)
    print("Done. Tables: customers, transactions (partitioned by dt), skewed.")
    spark.stop()
