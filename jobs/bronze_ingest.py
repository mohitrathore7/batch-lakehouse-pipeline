"""Bronze ingest: raw CSV on HDFS -> Parquet on S3, partitioned by ds."""

from __future__ import annotations

import logging

from lakehouse.bronze import read_raw_customers, read_raw_orders, read_raw_products
from lakehouse.cli import base_parser, setup_logging
from lakehouse.config import load_config
from lakehouse.io import write_partitioned
from lakehouse.session import build_spark

log = logging.getLogger("bronze_ingest")


def run(spark, cfg, ds: str, optimized: bool = True) -> None:
    files = cfg.spark.output_files_per_partition if optimized else None
    raw, bronze = cfg.paths.raw, cfg.paths.bronze

    # Dimensions are snapshotted per ds so parallel backfill runs never race
    # on the same files.
    write_partitioned(read_raw_customers(spark, raw, ds), f"{bronze}/customers", "ds", 1)
    write_partitioned(read_raw_products(spark, raw, ds), f"{bronze}/products", "ds", 1)
    write_partitioned(read_raw_orders(spark, raw, ds), f"{bronze}/orders", "ds", files)
    log.info("Bronze ingest finished for ds=%s", ds)


def main() -> None:
    setup_logging()
    args = base_parser(__doc__).parse_args()
    cfg = load_config(args.config)
    spark = build_spark(f"bronze_ingest_{args.ds}", cfg, args.mode == "optimized", args.master)
    try:
        run(spark, cfg, args.ds, args.mode == "optimized")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
