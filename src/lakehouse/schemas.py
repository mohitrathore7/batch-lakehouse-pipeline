"""Explicit schemas so raw CSV is never type-inferred."""

from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

RAW_ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", LongType()),
    StructField("product_id", LongType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("order_status", StringType()),
    StructField("order_ts", TimestampType()),
    StructField("updated_at", TimestampType()),
])

RAW_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", LongType()),
    StructField("customer_name", StringType()),
    StructField("segment", StringType()),
    StructField("country", StringType()),
    StructField("signup_date", DateType()),
])

RAW_PRODUCTS_SCHEMA = StructType([
    StructField("product_id", LongType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("list_price", DoubleType()),
])

VALID_ORDER_STATUSES = ("PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED")

SILVER_ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "product_id",
    "product_name",
    "category",
    "customer_segment",
    "customer_country",
    "quantity",
    "unit_price",
    "order_amount",
    "order_status",
    "order_ts",
    "updated_at",
    "order_date",
]
