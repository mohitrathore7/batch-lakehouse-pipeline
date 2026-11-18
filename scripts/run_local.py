"""Run the pipeline end to end on local Spark (no Docker needed).

Generates a small dataset under ./data, then for every day runs:
bronze -> DQ bronze -> silver -> DQ silver. The gold layer is the Scala job;
pass --gold-jar to run it too (build it with `make jar`).

  PYTHONPATH=src python scripts/run_local.py --orders 200000 --days 3
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "jobs"), str(ROOT / "data_generator")]


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--orders", type=int, default=200_000)
    p.add_argument("--days", type=int, default=3)
    p.add_argument("--start-date", default="2024-01-01")
    p.add_argument("--data-dir", default=str(ROOT / "data"))
    p.add_argument("--gold-jar", default=None)
    args = p.parse_args()

    data_dir = Path(args.data_dir).resolve()
    os.environ["RAW_PATH"] = str(data_dir / "raw")
    os.environ["LAKEHOUSE_PATH"] = str(data_dir / "lakehouse")

    import bronze_ingest
    import generate_data
    import silver_transform
    from lakehouse.cli import setup_logging
    from lakehouse.config import load_config
    from lakehouse.quality import run_suite
    from lakehouse.session import build_spark

    setup_logging()
    cfg = load_config()
    spark = build_spark("lakehouse_local", cfg, master="local[*]")
    spark.sparkContext.setLogLevel("WARN")

    generate_data.write_csv(
        generate_data.generate_orders(spark, args.orders, args.start_date, args.days,
                                      num_customers=50_000, num_products=2_000, hot_customers=5,
                                      hot_share=0.25, update_rate=0.01, invalid_rate=0.003, seed=42),
        cfg.paths.raw + "/orders", partition_col="ds", files=args.days)
    generate_data.write_csv(generate_data.generate_customers(spark, 50_000), cfg.paths.raw + "/customers")
    generate_data.write_csv(generate_data.generate_products(spark, 2_000), cfg.paths.raw + "/products")

    start = date.fromisoformat(args.start_date)
    for i in range(args.days):
        ds = (start + timedelta(days=i)).isoformat()
        bronze_ingest.run(spark, cfg, ds)
        run_suite(spark, cfg, "bronze", ds)
        silver_transform.run(spark, cfg, ds)
        run_suite(spark, cfg, "silver", ds)
        if args.gold_jar:
            subprocess.run(["spark-submit", "--class", "com.mohitrathore.lakehouse.GoldAggregations",
                            args.gold_jar, "--ds", ds, "--silver-path", cfg.paths.silver,
                            "--gold-path", cfg.paths.gold], check=True)
            run_suite(spark, cfg, "gold", ds)
        print(f"ds={ds} OK")

    spark.stop()


if __name__ == "__main__":
    main()
