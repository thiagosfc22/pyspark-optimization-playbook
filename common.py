"""Shared helpers: a local SparkSession and a tiny benchmarking harness.

Everything here exists so the demos can be self-contained and *measurable*:
each demo runs a naive version and an optimized version through the same
harness, so the difference is a number you can read, not a claim you have to
trust.
"""
from __future__ import annotations

import os
import sys
import time
from contextlib import contextmanager

# Pin the executor's Python to the one running this script. Without it, Spark
# picks up whatever `python3` is first on PATH for its workers — often a
# different minor version than the driver — and every Python UDF dies with
# PYTHON_VERSION_MISMATCH. Set before any SparkSession is built.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

from pyspark.sql import DataFrame, SparkSession  # noqa: E402

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))


def data_path(name: str) -> str:
    """Absolute path to a dataset under ./data, independent of the cwd."""
    return os.path.join(REPO_ROOT, "data", name)


def get_spark(app_name: str = "pyspark-optimization-playbook",
              shuffle_partitions: int = 8) -> SparkSession:
    """A small local SparkSession tuned for laptop-scale demos.

    Two deliberate choices:

    * ``spark.sql.shuffle.partitions`` defaults to 200. On a laptop that just
      spawns hundreds of near-empty tasks and buries the signal you're trying
      to see, so we turn it down.
    * AQE (Adaptive Query Execution) is left ON — it's the Spark 3+ default and
      several demos are precisely about what it does and does *not* fix for you.
    """
    return (
        SparkSession.builder
        .master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


@contextmanager
def timed(label: str):
    """Wall-clock a block and print it.

    Spark is lazy, so you must trigger an action (``.count()``, ``.collect()``,
    a write) *inside* the block — otherwise you're only timing how fast Python
    builds a query plan, which is always instant and always a lie.
    """
    start = time.perf_counter()
    yield
    print(f"  ⏱  {label:<30} {time.perf_counter() - start:6.2f}s")


def count_shuffles(df: DataFrame) -> int:
    """Count Exchange (shuffle) nodes in a DataFrame's physical plan.

    A shuffle moves data across the network to regroup it by key — serialize,
    write to disk, send, read back, deserialize. It is the single most
    expensive thing most pipelines do, so counting Exchanges is the cheapest
    possible proxy for "how much is this plan going to hurt".

    Heuristic, not gospel: with AQE some Exchanges are rewritten at runtime.
    Read it as a direction, and confirm the shape with ``df.explain()``.
    """
    plan = df._jdf.queryExecution().executedPlan().toString()
    return plan.count("Exchange")


def banner(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)
