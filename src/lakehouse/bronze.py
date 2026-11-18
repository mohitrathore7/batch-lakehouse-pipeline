"""Bronze layer: land raw CSV from HDFS as Parquet with ingestion metadata."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.schemas import RAW_CUSTOMERS_SCHEMA, RAW_ORDERS_SCHEMA, RAW_PRODUCTS_SCHEMA


def _read_csv(spark: SparkSession, path: str, schema) -> DataFrame:
    return (spark.read
                 .schema(schema)
                 .option("header", "true")
                 .option("mode", "PERMISSIVE")
                 .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                 .csv(path))


def add_metadata(df: DataFrame, ds: str) -> DataFrame:
    return (df.withColumn("_ingested_at", F.current_timestamp())
              .withColumn("_source_file", F.input_file_name())
              .withColumn("ds", F.lit(ds)))


def read_raw_orders(spark: SparkSession, raw_path: str, ds: str) -> DataFrame:
    return add_metadata(_read_csv(spark, f"{raw_path}/orders/ds={ds}", RAW_ORDERS_SCHEMA), ds)


def read_raw_customers(spark: SparkSession, raw_path: str, ds: str) -> DataFrame:
    return add_metadata(_read_csv(spark, f"{raw_path}/customers", RAW_CUSTOMERS_SCHEMA), ds)


def read_raw_products(spark: SparkSession, raw_path: str, ds: str) -> DataFrame:
    return add_metadata(_read_csv(spark, f"{raw_path}/products", RAW_PRODUCTS_SCHEMA), ds)
