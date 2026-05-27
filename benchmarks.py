#!/usr/bin/env python3
import time
import csv
import os
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark import SparkContext

RESULTS_DIR = os.path.expanduser("~/benchmark_results")
os.makedirs(RESULTS_DIR, exist_ok=True)

PLATFORM = os.environ.get("BENCH_PLATFORM", "k8s")  # set to "yarn" when running on YARN
RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S")
CSV_FILE = os.path.join(RESULTS_DIR, f"benchmark_{PLATFORM}_{RUN_ID}.csv")

CSV_HEADERS = [
    "run_id", "platform", "benchmark", "executor_count",
    "data_size", "start_time", "end_time", "duration_seconds",
    "init_duration_seconds", "result_summary"
]


def init_csv():
    with open(CSV_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(CSV_HEADERS)
    print(f"\n{'='*60}")
    print(f" Results will be saved to: {CSV_FILE}")
    print(f" Platform: {PLATFORM} | Run ID: {RUN_ID}")
    print(f"{'='*60}\n")


def log_result(benchmark, executor_count, data_size, start, end, init_dur, summary):
    duration = end - start
    row = [
        RUN_ID, PLATFORM, benchmark, executor_count,
        data_size, datetime.fromtimestamp(start).isoformat(),
        datetime.fromtimestamp(end).isoformat(),
        round(duration, 3), round(init_dur, 3), summary
    ]
    with open(CSV_FILE, "a", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(row)
    print(f"{benchmark} | {executor_count} executors | "
          f"{round(duration, 2)}s | {summary}")


def create_spark(app_name, num_executors=2):
    """Create a SparkSession and measure initialization time."""
    t0 = time.time()
    spark = (SparkSession.builder
             .appName(f"Bench_{app_name}_{RUN_ID}")
             .config("spark.executor.instances", str(num_executors))
             .getOrCreate())
    init_time = time.time() - t0
    return spark, init_time

# cpu intensive - pi estimation
def bench_cpu(num_executors=2, num_samples=500_000_000):
    """
    Pure CPU workload. Each task generates random (x,y) pairs and
    checks if they fall inside the unit circle. No data transfer,
    no disk I/O — only computation.
    """
    import random

    spark, init_dur = create_spark("CPU_Pi", num_executors)
    sc = spark.sparkContext

    num_slices = num_executors * 4  # 4 partitions per executor

    def inside(_):
        x, y = random.random(), random.random()
        return 1 if x * x + y * y <= 1 else 0

    start = time.time()
    count = (sc.parallelize(range(num_samples), numSlices=num_slices)
               .map(inside)
               .reduce(lambda a, b: a + b))
    end = time.time()

    pi_estimate = 4.0 * count / num_samples
    log_result("cpu_pi", num_executors, num_samples,
               start, end, init_dur, f"pi={pi_estimate:.6f}")
    spark.stop()
    return pi_estimate

#memory intensive
def bench_memory(num_executors=2, num_rows=50_000_000):
    """
    Generates a large in-memory DataFrame with multiple columns,
    performs groupBy aggregations that force shuffles and in-memory
    hash tables. Tests memory pressure and GC behavior.
    """
    from pyspark.sql import functions as F

    spark, init_dur = create_spark("Memory_Agg", num_executors)

    start = time.time()

    # large DF with synthetic data
    df = (spark.range(0, num_rows, numPartitions=num_executors * 4)
          .withColumn("group_key", (F.col("id") % 1000).cast("int"))
          .withColumn("value_a", (F.randn(seed=42) * 1000).cast("double"))
          .withColumn("value_b", (F.randn(seed=84) * 500).cast("double"))
          .withColumn("value_c", (F.randn(seed=126) * 250).cast("double"))
          .withColumn("payload", F.sha2(F.col("id").cast("string"), 256))
          )

    result = (df.groupBy("group_key")
              .agg(
                  F.count("*").alias("cnt"),
                  F.sum("value_a").alias("sum_a"),
                  F.avg("value_b").alias("avg_b"),
                  F.stddev("value_c").alias("std_c"),
                  F.min("value_a").alias("min_a"),
                  F.max("value_b").alias("max_b"),
                  F.countDistinct("payload").alias("unique_hashes")
              )
              .orderBy("group_key")
              .collect())

    end = time.time()

    log_result("memory_agg", num_executors, num_rows,
               start, end, init_dur, f"groups={len(result)}")
    spark.stop()
    return len(result)

#i/o intensive (wirte and read back large Parquet)
def bench_io(num_executors=2, num_rows=5_000_000):
    """
    Generates data, writes to Parquet (tests write throughput),
    reads it back, and performs a full scan with filter
    (tests read throughput). Measures both phases separately.
    """
    from pyspark.sql import functions as F
    import shutil

    spark, init_dur = create_spark("IO_Parquet", num_executors)

    output_path = os.path.expanduser("~/benchmark_results/io_test_data")
    if os.path.exists(output_path):
        shutil.rmtree(output_path)

    # write
    df = (spark.range(0, num_rows, numPartitions=num_executors * 4)
          .withColumn("category", (F.col("id") % 100).cast("int"))
          .withColumn("measurement", (F.randn(seed=42) * 1000))
          .withColumn("label", F.concat(F.lit("item_"), F.col("id").cast("string")))
          .withColumn("metadata", F.sha2(F.col("id").cast("string"), 256))
          )

    start_write = time.time()
    df.write.mode("overwrite").partitionBy("category").parquet(output_path)
    end_write = time.time()

    log_result("io_write", num_executors, num_rows,
               start_write, end_write, init_dur,
               f"write_secs={round(end_write - start_write, 2)}")

    #read +filter + aggregate
    start_read = time.time()
    df_read = spark.read.parquet(output_path)
    result = (df_read.filter(F.col("measurement") > 0)
              .groupBy("category")
              .agg(F.avg("measurement").alias("avg_m"),
                   F.count("*").alias("cnt"))
              .collect())
    end_read = time.time()

    log_result("io_read", num_executors, num_rows,
               start_read, end_read, 0,
               f"read_secs={round(end_read - start_read, 2)},groups={len(result)}")

    #cleanup
    shutil.rmtree(output_path, ignore_errors=True)
    spark.stop()
    return len(result)


#scalability, vary executors
def bench_scalability():
    """
    Runs the CPU Pi benchmark with 1, 2, and 4 executors.
    Demonstrates Amdahl's Law and parallel speedup on this cluster.
    """
    import random

    NUM_SAMPLES = 30_000_000
    results = {}

    for n_exec in [1, 2]:
        spark, init_dur = create_spark(f"Scale_{n_exec}exec", n_exec)
        sc = spark.sparkContext

        num_slices = max(n_exec * 4, 4)

        def inside(_):
            x, y = random.random(), random.random()
            return 1 if x * x + y * y <= 1 else 0

        start = time.time()
        count = (sc.parallelize(range(NUM_SAMPLES), numSlices=num_slices)
                   .map(inside)
                   .reduce(lambda a, b: a + b))
        end = time.time()

        pi_est = 4.0 * count / NUM_SAMPLES
        duration = end - start
        results[n_exec] = duration

        log_result("scalability_pi", n_exec, NUM_SAMPLES,
                   start, end, init_dur,
                   f"pi={pi_est:.6f},speedup_vs_1=pending")

        spark.stop()
        time.sleep(3)  # let executors fully terminate

    base = results.get(1, 1)
    print(f"\n  Scalability Summary:")
    print(f"  {'Executors':<12} {'Duration(s)':<14} {'Speedup':<10}")
    print(f"  {'-'*36}")
    for n, dur in sorted(results.items()):
        speedup = base / dur if dur > 0 else 0
        print(f"  {n:<12} {dur:<14.2f} {speedup:<10.2f}x")

    return results



# fault tolerance, kill exec during job
def bench_fault_tolerance(num_executors=2, num_samples=1_000_000_000):
    """
    Runs a long CPU job. The user manually kills an executor pod
    during execution (instructions printed). Spark should detect
    the loss, re-schedule tasks, and complete successfully.

    The script measures total time including recovery.
    Run this, then in another terminal:
        kubectl delete pod <executor-pod> -n spark
    """
    import random

    print("\n" + "="*60)
    print("  FAULT TOLERANCE TEST")
    print("  While this job runs, open ANOTHER terminal and run:")
    print("    kubectl get pods -n spark")
    print("    kubectl delete pod <any-executor-pod> -n spark")
    print("  The job should recover and complete successfully.")
    print("="*60 + "\n")

    spark, init_dur = create_spark("Fault_Tolerance", num_executors)
    sc = spark.sparkContext

    num_slices = 40

    def inside(_):
        x, y = random.random(), random.random()
        return 1 if x * x + y * y <= 1 else 0

    start = time.time()
    try:
        count = (sc.parallelize(range(num_samples), numSlices=num_slices)
                   .map(inside)
                   .reduce(lambda a, b: a + b))
        end = time.time()
        pi_est = 4.0 * count / num_samples
        status = f"RECOVERED,pi={pi_est:.6f}"
    except Exception as e:
        end = time.time()
        status = f"FAILED:{str(e)[:80]}"

    log_result("fault_tolerance", num_executors, num_samples,
               start, end, init_dur, status)

    spark.stop()
    return status


if __name__ == "__main__":
    import sys

    init_csv()

    # Allow running individual benchmarks or all
    # Usage: spark-submit ... benchmarks.py [all|cpu|memory|io|scale|fault]
    target = sys.argv[1] if len(sys.argv) > 1 else "all"

    benchmarks = {
        "cpu":    lambda: bench_cpu(num_executors=2, num_samples=1_000_000_000),
        "memory": lambda: bench_memory(num_executors=2, num_rows=50_000_000),
        "io":     lambda: bench_io(num_executors=2, num_rows=5_000_000),
        "scale":  lambda: bench_scalability(),
        "fault":  lambda: bench_fault_tolerance(num_executors=2, num_samples=200_000_000),
    }

    if target == "all":
        for name, fn in benchmarks.items():
            print(f"\n{'─'*60}")
            print(f"  Running: {name}")
            print(f"{'─'*60}")
            fn()
            time.sleep(5)  # pause between benchmarks
    elif target in benchmarks:
        benchmarks[target]()
    else:
        print(f"Unknown benchmark: {target}")
        print(f"Available: {', '.join(benchmarks.keys())}, all")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  All benchmarks complete!")
    print(f"  Results saved to: {CSV_FILE}")
    print(f"{'='*60}\n")
