"""Step 5 — Avoid Python UDFs (and when you can't, vectorize them).

A plain Python UDF forces every row to cross the JVM↔Python boundary, one at a
time, serialized and deserialized on the way. Spark also can't see inside it, so
it can't optimize through it. The same logic as a native column expression stays
in the JVM and runs vectorized.

When you genuinely need Python (a library with no Spark equivalent), a
``pandas_udf`` is the middle path: Apache Arrow ships whole batches across the
boundary instead of row-by-row, so you keep Python but pay a fraction of the
cost. That Arrow bridge is exactly why PyArrow shows up in every data-engineering
job description.

Expected order, slowest to fastest:  python UDF ≫ pandas_udf > native.

Run:  python demos/05_avoid_udfs.py
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType

from common import banner, data_path, get_spark, timed


def demo(spark):
    df = spark.read.parquet(data_path("transactions")).select("amount")

    # 1) Python UDF — row-by-row across the JVM↔Python boundary.
    @F.udf(DoubleType())
    def with_tax_udf(a):
        return a * 1.1

    # 2) pandas_udf — Python, but Arrow ships whole batches (vectorized).
    @pandas_udf(DoubleType())
    def with_tax_pandas(a: pd.Series) -> pd.Series:
        return a * 1.1

    banner("same computation, three ways")
    with timed("python UDF"):
        df.select(with_tax_udf("amount").alias("t")).agg(F.sum("t")).collect()
    with timed("pandas_udf (Arrow)"):
        df.select(with_tax_pandas("amount").alias("t")).agg(F.sum("t")).collect()
    with timed("native expression"):
        df.select((F.col("amount") * 1.1).alias("t")).agg(F.sum("t")).collect()

    print(
        "\nTakeaway: reach for native column functions first (F.*, arithmetic,\n"
        "when/otherwise). Only drop to pandas_udf when the logic truly has no\n"
        "native equivalent — and never a plain Python UDF in a hot path."
    )


if __name__ == "__main__":
    spark = get_spark("05-avoid-udfs")
    spark.sparkContext.setLogLevel("ERROR")
    demo(spark)
    spark.stop()
