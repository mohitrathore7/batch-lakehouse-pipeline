"""Silver layer: deduplicate, validate and enrich orders."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.config import PipelineConfig
from lakehouse.schemas import SILVER_ORDER_COLUMNS, VALID_ORDER_STATUSES
from lakehouse.skew import find_hot_keys, salted_join


def deduplicate_latest(df: DataFrame, key: str, order_col: str) -> DataFrame:
    """Keep the most recent version of each record (CDC-style updates)."""
    window = Window.partitionBy(key).orderBy(F.col(order_col).desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(window)).where("_rn = 1").drop("_rn")


def reject_reason(ds: str | None = None):
    """First failed rule for a row, or null if the row is valid."""
    rule = (F.when(F.col("order_id").isNull(), "null_order_id")
             .when(F.col("customer_id").isNull(), "null_customer_id")
             .when(F.col("product_id").isNull(), "null_product_id")
             .when(F.col("order_ts").isNull(), "null_order_ts")
             .when(F.col("quantity").isNull() | (F.col("quantity") <= 0), "non_positive_quantity")
             .when(F.col("unit_price").isNull() | (F.col("unit_price") < 0), "invalid_unit_price")
             .when(F.col("order_status").isNull()
                   | ~F.col("order_status").isin(*VALID_ORDER_STATUSES), "invalid_status"))
    if ds is not None:
        rule = rule.when(F.to_date("order_ts") != F.lit(ds).cast("date"), "order_date_outside_run")
    return rule


def split_valid_invalid(df: DataFrame, ds: str | None = None) -> tuple[DataFrame, DataFrame]:
    tagged = df.withColumn("reject_reason", reject_reason(ds))
    valid = tagged.where(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = tagged.where(F.col("reject_reason").isNotNull())
    return valid, rejected


def build_silver_orders(orders: DataFrame, customers: DataFrame, products: DataFrame,
                        cfg: PipelineConfig, ds: str | None = None,
                        optimized: bool = True) -> tuple[DataFrame, DataFrame]:
    """Return (silver_orders, rejected_orders).

    optimized=True  -> broadcast join for the small product dimension and
                       salted join for skewed customer keys.
    optimized=False -> plain shuffle (sort-merge) joins; used as the benchmark baseline.
    """
    deduped = deduplicate_latest(orders, "order_id", "updated_at")
    valid, rejected = split_valid_invalid(deduped, ds)

    enriched = (valid.withColumn("order_date", F.to_date("order_ts"))
                     .withColumn("order_amount", F.round(F.col("quantity") * F.col("unit_price"), 2)))

    product_dim = products.select("product_id", "product_name", "category")
    customer_dim = customers.select(
        "customer_id",
        F.col("segment").alias("customer_segment"),
        F.col("country").alias("customer_country"),
    )

    if optimized:
        enriched = enriched.join(F.broadcast(product_dim), on="product_id", how="left")
        hot_keys = find_hot_keys(orders, "customer_id",
                                 cfg.skew.hot_key_min_share, cfg.skew.max_hot_keys)
        enriched = salted_join(enriched, customer_dim, "customer_id", hot_keys,
                               cfg.skew.salt_buckets, salt_source="order_id")
    else:
        enriched = (enriched.join(product_dim, on="product_id", how="left")
                            .join(customer_dim, on="customer_id", how="left"))

    enriched = enriched.withColumn("category", F.coalesce("category", F.lit("UNKNOWN")))
    return enriched.select(*SILVER_ORDER_COLUMNS), rejected
