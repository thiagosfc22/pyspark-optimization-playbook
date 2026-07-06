# PySpark Optimization Playbook

[🇧🇷 Português](README.pt-BR.md) · **🇬🇧 English**

A slow Spark job almost never means "we need a bigger cluster." It means the job
is doing work it doesn't have to — usually a shuffle it could have skipped, a
partition it could have pruned, or a full history it reprocesses every night.

This repo is a **runnable checklist**. Each step is a small, self-contained demo
that runs a *naive* version and an *optimized* version through the same
benchmark harness, so the difference is a number you can read — not a claim you
have to trust. Every optimization also ships with a test asserting it returns
the **same result** as the naive path, because a faster query that changes the
answer is worse than useless.

The order matters. Work top-down: measure first, and don't tune step 5 before
you've ruled out steps 1–4.

## The checklist

| # | Step | The one idea | Demo |
|---|------|-------------|------|
| 1 | **Measure first** | Read the plan and the Spark UI before changing a line. The longest stage is your bottleneck; guessing wastes the tuning. | [`01_measure.py`](demos/01_measure.py) |
| 2 | **Kill the shuffle** | A shuffle moves data across the network to regroup by key — the most expensive thing most jobs do. Broadcast the small side of a join and the big side never moves. | [`02_shuffle.py`](demos/02_shuffle.py) |
| 3 | **Hunt the skew** | One hot key = one giant partition = one straggler task. Let AQE split it, or salt the key into N sub-keys and aggregate in two stages. | [`03_skew.py`](demos/03_skew.py) |
| 4 | **Read less data** | The fastest read is the one that never happens. Partition pruning and predicate pushdown skip files and row-groups before decoding them. | [`04_read_less.py`](demos/04_read_less.py) |
| 5 | **Avoid Python UDFs** | A Python UDF serializes every row across the JVM↔Python boundary and blocks optimization. Use native functions; when you can't, `pandas_udf` + Arrow ships batches instead of rows. | [`05_avoid_udfs.py`](demos/05_avoid_udfs.py) |
| 6 | **Cache only with reuse** | Spark recomputes a DataFrame on every action. Cache when a frame is reused across ≥2 actions — never out of habit; that just burns memory. | [`06_cache.py`](demos/06_cache.py) |
| 7 | **Go incremental** | The biggest saver and the least questioned waste: process only what changed, not the whole history. On Delta, a keyed `MERGE INTO` makes it idempotent too. | [`07_incremental.py`](demos/07_incremental.py) |

<p align="center">
  <img src="docs/optimization-checklist.png" width="480" alt="The seven steps, in order: measure first, kill the shuffle, hunt the skew, read less data, avoid Python UDFs, cache only with reuse, go incremental.">
  <br><em>The checklist, in order — the order is the method: measure before you tune, and rule out cheap wins before expensive ones.</em>
</p>

## How to read a demo

Every demo prints the physical plan for both versions and their wall-clock
times. Learn to spot these in the plan output — they're where the cost hides:

- **`Exchange`** → a shuffle. Fewer is better.
- **`BroadcastHashJoin`** vs **`SortMergeJoin`** → did the small side get broadcast, or is Spark shuffling both?
- **`PartitionFilters: [...]`** → partition pruning kicked in; whole directories skipped.
- **`PushedFilters: [...]`** → the filter reached the Parquet reader instead of running after a full scan.

## Running it

Spark 3.5 runs on **Java 8, 11, or 17** — not on newer JVMs (the Security
Manager it relies on was removed in Java 24+). Check with `java -version` and
point `JAVA_HOME` at a 17 if needed.

```bash
pip install -r requirements.txt

python generate_data.py          # builds ./data (~5M rows; pass a number to change)
python demos/02_shuffle.py       # run any step
pytest -q                        # correctness + "actually cheaper" checks
```

Each demo starts a local `SparkSession`; while one runs, the Spark UI is at
<http://localhost:4040> (step 1 keeps it open so you can explore).

## Where this comes from

These are the moves I actually reach for, in order, on a medallion pipeline
(bronze → silver → gold) running on Databricks: raw data lands in S3, a PySpark
job dedups and cleans it into silver, and another rolls it up into gold. The
lesson that saved the most wall-clock there was #7 — the silver dedup was doing
a full scan every run until it became an incremental merge on the natural key.

The datasets here are synthetic and deterministic so anyone can clone, run, and
see the same numbers.
