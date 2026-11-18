"""End-to-end: raw CSV -> bronze -> silver -> DQ, plus idempotency of reruns."""

import bronze_ingest
import generate_data
import silver_transform
from lakehouse.quality import run_suite
from lakehouse.session import apply_profile


def _generate(spark, cfg, days=2):
    generate_data.write_csv(
        generate_data.generate_orders(spark, 4000, "2024-01-01", days, num_customers=500,
                                      num_products=50, hot_customers=2, hot_share=0.4,
                                      update_rate=0.02, invalid_rate=0.01, seed=7),
        f"{cfg.paths.raw}/orders", partition_col="ds", files=days)
    generate_data.write_csv(generate_data.generate_customers(spark, 500), f"{cfg.paths.raw}/customers")
    generate_data.write_csv(generate_data.generate_products(spark, 50), f"{cfg.paths.raw}/products")


def _run_day(spark, cfg, ds):
    bronze_ingest.run(spark, cfg, ds)
    run_suite(spark, cfg, "bronze", ds)
    silver_transform.run(spark, cfg, ds)
    run_suite(spark, cfg, "silver", ds)


def test_end_to_end_and_idempotent_rerun(spark, cfg):
    apply_profile(spark, cfg, optimized=True)
    _generate(spark, cfg)

    _run_day(spark, cfg, "2024-01-01")
    _run_day(spark, cfg, "2024-01-02")

    silver = spark.read.parquet(f"{cfg.paths.silver}/orders")
    day1 = silver.where("order_date = '2024-01-01'").count()
    day2 = silver.where("order_date = '2024-01-02'").count()
    assert day1 > 0 and day2 > 0
    assert silver.select("order_id").distinct().count() == silver.count()

    # Rerun (like an Airflow retry/backfill) must not duplicate or drop data.
    _run_day(spark, cfg, "2024-01-01")
    silver = spark.read.parquet(f"{cfg.paths.silver}/orders")
    assert silver.where("order_date = '2024-01-01'").count() == day1
    assert silver.where("order_date = '2024-01-02'").count() == day2

    bronze = spark.read.parquet(f"{cfg.paths.bronze}/orders")
    assert bronze.where("ds = '2024-01-01'").count() == \
        spark.read.option("header", True).csv(f"{cfg.paths.raw}/orders/ds=2024-01-01").count()
