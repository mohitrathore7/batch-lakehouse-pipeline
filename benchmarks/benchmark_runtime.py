"""Benchmark the silver transform: baseline vs optimized.

baseline  : AQE off, no broadcast joins, no salting, 200 shuffle partitions,
            no output file control (sort-merge joins everywhere)
optimized : date partition pruning + broadcast join for products + salted join
            for skewed customers + AQE (coalesce, skew join) + sized output files

The script generates bronze input once, then times each mode `--repeats`
times and writes a JSON summary to benchmarks/results/.

Local:    PYTHONPATH=src python benchmarks/benchmark_runtime.py --orders 5000000
Cluster:  spark-submit --master spark://spark-master:7077 --py-files dist/lakehouse.zip \
            benchmarks/benchmark_runtime.py --data-dir hdfs://namenode:9000/benchmark
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "jobs"), str(ROOT / "data_generator")]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--orders", type=int, default=5_000_000)
    p.add_argument("--days", type=int, default=1, help="Days of data; the first day is benchmarked")
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--data-dir", default=str(ROOT / "data" / "benchmark"))
    p.add_argument("--master", default=None)
    p.add_argument("--skip-generate", action="store_true")
    args = p.parse_args()

    os.environ["RAW_PATH"] = f"{args.data_dir}/raw"
    os.environ["LAKEHOUSE_PATH"] = f"{args.data_dir}/lakehouse"

    import bronze_ingest
    import generate_data
    import silver_transform
    from lakehouse.config import load_config
    from lakehouse.session import apply_profile, build_spark

    cfg = load_config()
    spark = build_spark("benchmark", cfg, master=args.master or ("local[*]" if "://" not in args.data_dir else None))
    spark.sparkContext.setLogLevel("WARN")
    ds = "2024-01-01"

    if not args.skip_generate:
        print(f"Generating {args.orders:,} orders ...")
        generate_data.write_csv(
            generate_data.generate_orders(spark, args.orders, ds, args.days, 500_000, 20_000,
                                          5, 0.25, 0.01, 0.003, 42),
            cfg.paths.raw + "/orders", partition_col="ds", files=max(4, args.days * 4))
        generate_data.write_csv(generate_data.generate_customers(spark, 500_000), cfg.paths.raw + "/customers", files=2)
        generate_data.write_csv(generate_data.generate_products(spark, 20_000), cfg.paths.raw + "/products")
        bronze_ingest.run(spark, cfg, ds)

    timings: dict[str, list[float]] = {"baseline": [], "optimized": []}
    for run in range(args.repeats):
        for mode in ("baseline", "optimized"):  # interleave so caching/JIT effects even out
            optimized = mode == "optimized"
            apply_profile(spark, cfg, optimized)
            spark.catalog.clearCache()
            started = time.perf_counter()
            silver_transform.run(spark, cfg, ds, optimized=optimized,
                                 output_root=f"{args.data_dir}/bench_out/{mode}")
            elapsed = time.perf_counter() - started
            timings[mode].append(elapsed)
            print(f"run {run + 1}/{args.repeats} {mode:<9} {elapsed:7.2f}s")

    base, opt = statistics.median(timings["baseline"]), statistics.median(timings["optimized"])
    improvement = (base - opt) / base * 100
    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "orders": args.orders,
        "master": spark.sparkContext.master,
        "repeats": args.repeats,
        "baseline_median_s": round(base, 2),
        "optimized_median_s": round(opt, 2),
        "runtime_reduction_pct": round(improvement, 1),
        "timings": timings,
    }
    spark.stop()

    out_dir = ROOT / "benchmarks" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    (out_dir / f"benchmark_{stamp}.json").write_text(json.dumps(result, indent=2))
    print(f"\nBaseline median : {base:.2f}s\nOptimized median: {opt:.2f}s\n"
          f"Runtime reduction: {improvement:.1f}%")


if __name__ == "__main__":
    main()
