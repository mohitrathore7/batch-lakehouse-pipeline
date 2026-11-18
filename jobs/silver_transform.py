"""Silver transform: dedupe, validate and enrich one day of orders."""

from __future__ import annotations

import logging

from pyspark.sql import functions as F

from lakehouse.cli import base_parser, setup_logging
from lakehouse.config import load_config
from lakehouse.io import read_partition, write_partitioned
from lakehouse.session import build_spark
from lakehouse.silver import build_silver_orders

log = logging.getLogger("silver_transform")


def run(spark, cfg, ds: str, optimized: bool = True, output_root: str | None = None) -> None:
    bronze = cfg.paths.bronze
    silver = output_root or cfg.paths.silver
    files = cfg.spark.output_files_per_partition if optimized else None

    orders = read_partition(spark, f"{bronze}/orders", "ds", ds)
    customers = read_partition(spark, f"{bronze}/customers", "ds", ds)
    products = read_partition(spark, f"{bronze}/products", "ds", ds)

    silver_orders, rejected = build_silver_orders(orders, customers, products, cfg,
                                                  ds=ds, optimized=optimized)

    write_partitioned(silver_orders, f"{silver}/orders", "order_date", files)
    write_partitioned(
        rejected.select("order_id", "customer_id", "product_id", "quantity", "unit_price",
                        "order_status", "order_ts", "reject_reason", F.lit(ds).alias("ds")),
        f"{silver}/orders_rejected", "ds", 1,
    )
    log.info("Silver transform finished for ds=%s (mode=%s)", ds,
             "optimized" if optimized else "baseline")


def main() -> None:
    setup_logging()
    args = base_parser(__doc__).parse_args()
    cfg = load_config(args.config)
    optimized = args.mode == "optimized"
    spark = build_spark(f"silver_transform_{args.ds}", cfg, optimized, args.master)
    try:
        run(spark, cfg, args.ds, optimized)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
