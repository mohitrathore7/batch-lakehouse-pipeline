"""Generate synthetic raw data (default: 5M orders) with realistic problems.

The data intentionally contains:
  * skew      - a handful of "hot" customers own a large share of orders
  * updates   - a share of orders appear twice (later status update)
  * bad rows  - null customer ids and non-positive quantities

Output layout (CSV with header):
  <output>/orders/ds=YYYY-MM-DD/part-*.csv
  <output>/customers/part-*.csv
  <output>/products/part-*.csv

Example:
  spark-submit data_generator/generate_data.py --output hdfs://namenode:9000/data/raw
"""

from __future__ import annotations

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

STATUSES = ["PLACED", "PLACED", "SHIPPED", "SHIPPED", "DELIVERED", "DELIVERED",
            "DELIVERED", "CANCELLED", "RETURNED"]
SEGMENTS = ["CONSUMER", "SMB", "ENTERPRISE", "PUBLIC_SECTOR"]
COUNTRIES = ["US", "DE", "IN", "GB", "FR", "JP", "BR", "CA", "AU", "NL"]
CATEGORIES = ["ELECTRONICS", "HOME", "FASHION", "SPORTS", "BOOKS", "BEAUTY", "TOYS", "GROCERY"]


def pick(values: list[str], rand_col) -> F.Column:
    arr = F.array(*[F.lit(v) for v in values])
    return F.element_at(arr, (rand_col * len(values)).cast("int") + 1)


def generate_orders(spark: SparkSession, total: int, start_date: str, days: int,
                    num_customers: int, num_products: int, hot_customers: int,
                    hot_share: float, update_rate: float, invalid_rate: float,
                    seed: int) -> DataFrame:
    start_epoch = F.unix_timestamp(F.lit(start_date), "yyyy-MM-dd")
    day_offset = F.col("id") % days

    base = (spark.range(total)
                 .withColumn("r_cust", F.rand(seed))
                 .withColumn("r_bad", F.rand(seed + 1))
                 .select(
                     F.format_string("ORD-%010d", F.col("id")).alias("order_id"),
                     F.when(F.col("r_cust") < hot_share,
                            (F.rand(seed + 2) * hot_customers).cast("long") + 1)
                      .otherwise((F.rand(seed + 3) * num_customers).cast("long") + 1)
                      .alias("customer_id"),
                     ((F.rand(seed + 4) * num_products).cast("long") + 1).alias("product_id"),
                     ((F.rand(seed + 5) * 5).cast("int") + 1).alias("quantity"),
                     F.round(F.rand(seed + 6) * 250 + 1, 2).alias("unit_price"),
                     pick(STATUSES, F.rand(seed + 7)).alias("order_status"),
                     F.timestamp_seconds(start_epoch + day_offset * 86400
                                         + (F.rand(seed + 8) * 86399).cast("long")).alias("order_ts"),
                     F.col("r_bad"),
                 ))

    # Inject invalid rows.
    base = (base.withColumn("customer_id",
                            F.when(F.col("r_bad") < invalid_rate / 2, F.lit(None).cast("long"))
                             .otherwise(F.col("customer_id")))
                .withColumn("quantity",
                            F.when((F.col("r_bad") >= invalid_rate / 2) & (F.col("r_bad") < invalid_rate),
                                   F.lit(0))
                             .otherwise(F.col("quantity")))
                .withColumn("updated_at", F.col("order_ts"))
                .drop("r_bad"))

    # Later status updates for a sample of orders (same order_id, newer updated_at).
    updates = (base.sample(fraction=update_rate, seed=seed + 9)
                   .withColumn("order_status", F.lit("SHIPPED"))
                   .withColumn("updated_at",
                               F.least(F.col("order_ts") + F.expr("INTERVAL 1 HOUR"),
                                       F.date_trunc("day", "order_ts") + F.expr("INTERVAL 86399 SECONDS"))))

    return base.unionByName(updates).withColumn("ds", F.date_format("order_ts", "yyyy-MM-dd"))


def generate_customers(spark: SparkSession, n: int) -> DataFrame:
    return (spark.range(1, n + 1)
                 .select(F.col("id").alias("customer_id"),
                         F.format_string("Customer %d", F.col("id")).alias("customer_name"),
                         F.element_at(F.array(*[F.lit(s) for s in SEGMENTS]),
                                      (F.col("id") % len(SEGMENTS)).cast("int") + 1).alias("segment"),
                         F.element_at(F.array(*[F.lit(c) for c in COUNTRIES]),
                                      (F.col("id") % len(COUNTRIES)).cast("int") + 1).alias("country"),
                         F.date_sub(F.lit("2023-12-31").cast("date"),
                                    (F.col("id") % 1500).cast("int")).alias("signup_date")))


def generate_products(spark: SparkSession, n: int) -> DataFrame:
    return (spark.range(1, n + 1)
                 .select(F.col("id").alias("product_id"),
                         F.format_string("Product %d", F.col("id")).alias("product_name"),
                         F.element_at(F.array(*[F.lit(c) for c in CATEGORIES]),
                                      (F.col("id") % len(CATEGORIES)).cast("int") + 1).alias("category"),
                         F.round(F.col("id") % 250 + 0.99, 2).alias("list_price")))


def write_csv(df: DataFrame, path: str, partition_col: str | None = None, files: int = 1) -> None:
    writer = df.repartition(files, partition_col) if partition_col else df.repartition(files)
    writer = writer.write.mode("overwrite").option("header", "true") \
                   .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
    if partition_col:
        writer = writer.partitionBy(partition_col)
    writer.csv(path)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--output", required=True, help="Raw root, e.g. hdfs://namenode:9000/data/raw")
    p.add_argument("--orders", type=int, default=5_000_000)
    p.add_argument("--start-date", default="2024-01-01")
    p.add_argument("--days", type=int, default=10)
    p.add_argument("--customers", type=int, default=500_000)
    p.add_argument("--products", type=int, default=20_000)
    p.add_argument("--hot-customers", type=int, default=5)
    p.add_argument("--hot-share", type=float, default=0.25,
                   help="Share of orders that belong to the hot customers")
    p.add_argument("--update-rate", type=float, default=0.01)
    p.add_argument("--invalid-rate", type=float, default=0.003)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--master", default=None)
    args = p.parse_args()

    builder = SparkSession.builder.appName("generate_raw_data").config("spark.sql.session.timeZone", "UTC")
    if args.master:
        builder = builder.master(args.master)
    spark = builder.getOrCreate()

    orders = generate_orders(spark, args.orders, args.start_date, args.days, args.customers,
                             args.products, args.hot_customers, args.hot_share,
                             args.update_rate, args.invalid_rate, args.seed)
    write_csv(orders, f"{args.output}/orders", partition_col="ds", files=args.days * 4)
    write_csv(generate_customers(spark, args.customers), f"{args.output}/customers", files=2)
    write_csv(generate_products(spark, args.products), f"{args.output}/products", files=1)
    spark.stop()


if __name__ == "__main__":
    main()
