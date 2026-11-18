"""Data-quality checks run after every layer. A failed check fails the task."""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import PipelineConfig
from lakehouse.io import read_partition
from lakehouse.schemas import VALID_ORDER_STATUSES

log = logging.getLogger(__name__)


class DataQualityError(Exception):
    """Raised when one or more data-quality checks fail."""


@dataclass
class CheckResult:
    check: str
    passed: bool
    observed: str
    expected: str


# ---------------------------------------------------------------- generic checks

def check_min_rows(df: DataFrame, min_rows: int, name: str = "row_count") -> CheckResult:
    count = df.count()
    return CheckResult(name, count >= min_rows, str(count), f">= {min_rows}")


def check_not_null(df: DataFrame, columns: list[str]) -> list[CheckResult]:
    nulls = df.select([F.sum(F.col(c).isNull().cast("long")).alias(c) for c in columns]).first()
    return [CheckResult(f"not_null:{c}", (nulls[c] or 0) == 0, str(nulls[c] or 0), "0 nulls")
            for c in columns]


def check_unique(df: DataFrame, columns: list[str]) -> CheckResult:
    dupes = df.groupBy(*columns).count().where("count > 1").count()
    return CheckResult(f"unique:{','.join(columns)}", dupes == 0, f"{dupes} duplicate keys", "0")


def check_min_value(df: DataFrame, column: str, minimum: float, inclusive: bool = True) -> CheckResult:
    cond = F.col(column) < minimum if inclusive else F.col(column) <= minimum
    bad = df.where(cond).count()
    op = ">=" if inclusive else ">"
    return CheckResult(f"range:{column}", bad == 0, f"{bad} rows out of range", f"{op} {minimum}")


def check_accepted_values(df: DataFrame, column: str, values: tuple) -> CheckResult:
    bad = df.where(~F.col(column).isin(*values)).count()
    return CheckResult(f"accepted_values:{column}", bad == 0, f"{bad} rows", f"in {list(values)}")


def check_ratio(name: str, numerator: int, denominator: int, max_ratio: float) -> CheckResult:
    ratio = numerator / denominator if denominator else 0.0
    return CheckResult(name, ratio <= max_ratio, f"{ratio:.4%}", f"<= {max_ratio:.2%}")


# ---------------------------------------------------------------- layer suites

def bronze_suite(spark: SparkSession, cfg: PipelineConfig, ds: str) -> list[CheckResult]:
    rules = cfg.quality_for("bronze")
    orders = read_partition(spark, f"{cfg.paths.bronze}/orders", "ds", ds)
    return [check_min_rows(orders, rules.get("min_rows", 1)),
            *check_not_null(orders, ["_ingested_at", "_source_file"])]


def silver_suite(spark: SparkSession, cfg: PipelineConfig, ds: str) -> list[CheckResult]:
    rules = cfg.quality_for("silver")
    orders = read_partition(spark, f"{cfg.paths.silver}/orders", "order_date", ds).cache()
    try:
        total = orders.count()
        rejected = read_partition(spark, f"{cfg.paths.silver}/orders_rejected", "ds", ds).count()
        unknown_customers = orders.where(F.col("customer_segment").isNull()).count()
        return [
            check_min_rows(orders, rules.get("min_rows", 1)),
            *check_not_null(orders, ["order_id", "customer_id", "product_id",
                                     "order_ts", "order_amount"]),
            check_unique(orders, ["order_id"]),
            check_min_value(orders, "quantity", 0, inclusive=False),
            check_min_value(orders, "order_amount", 0),
            check_accepted_values(orders, "order_status", VALID_ORDER_STATUSES),
            check_ratio("reject_ratio", rejected, total + rejected,
                        rules.get("max_reject_ratio", 0.02)),
            check_ratio("unknown_customer_ratio", unknown_customers, total,
                        rules.get("max_unknown_customer_ratio", 0.01)),
        ]
    finally:
        orders.unpersist()


def gold_suite(spark: SparkSession, cfg: PipelineConfig, ds: str) -> list[CheckResult]:
    rules = cfg.quality_for("gold")
    sales = read_partition(spark, f"{cfg.paths.gold}/daily_category_sales", "order_date", ds)
    silver = read_partition(spark, f"{cfg.paths.silver}/orders", "order_date", ds)

    gold_revenue = sales.agg(F.sum("revenue")).first()[0] or 0.0
    silver_revenue = (silver.where(F.col("order_status") != "CANCELLED")
                            .agg(F.sum("order_amount")).first()[0] or 0.0)
    diff = abs(gold_revenue - silver_revenue) / silver_revenue if silver_revenue else 0.0
    tolerance = rules.get("revenue_tolerance", 0.01)

    return [
        check_min_rows(sales, rules.get("min_rows", 1)),
        check_unique(sales, ["order_date", "category"]),
        check_min_value(sales, "revenue", 0),
        CheckResult("reconcile:revenue_vs_silver", diff <= tolerance,
                    f"{diff:.4%} difference", f"<= {tolerance:.2%}"),
    ]


SUITES = {"bronze": bronze_suite, "silver": silver_suite, "gold": gold_suite}


def run_suite(spark: SparkSession, cfg: PipelineConfig, layer: str, ds: str,
              persist: bool = True) -> list[CheckResult]:
    results = SUITES[layer](spark, cfg, ds)
    for r in results:
        log.info("[DQ %s %s] %-32s %s observed=%s expected=%s", layer, ds, r.check,
                 "PASS" if r.passed else "FAIL", r.observed, r.expected)

    if persist:
        rows = [{**asdict(r), "layer": layer, "ds": ds} for r in results]
        (spark.createDataFrame(rows)
              .withColumn("checked_at", F.current_timestamp())
              .coalesce(1)
              .write.mode("overwrite")
              .partitionBy("layer", "ds")
              .parquet(f"{cfg.paths.quality}/results"))

    failed = [r for r in results if not r.passed]
    if failed:
        summary = "; ".join(f"{r.check} (observed {r.observed}, expected {r.expected})" for r in failed)
        raise DataQualityError(f"{len(failed)} {layer} check(s) failed for {ds}: {summary}")
    return results
