import sys
from datetime import datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "jobs"), str(ROOT / "data_generator")]

from lakehouse.config import Paths, PipelineConfig, SkewSettings, SparkSettings  # noqa: E402
from lakehouse.schemas import RAW_CUSTOMERS_SCHEMA, RAW_ORDERS_SCHEMA, RAW_PRODUCTS_SCHEMA  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder
               .master("local[2]")
               .appName("lakehouse-tests")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.ui.enabled", "false")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture
def cfg(tmp_path):
    return PipelineConfig(
        paths=Paths(raw=str(tmp_path / "raw"), bronze=str(tmp_path / "bronze"),
                    silver=str(tmp_path / "silver"), gold=str(tmp_path / "gold"),
                    quality=str(tmp_path / "quality")),
        spark=SparkSettings(shuffle_partitions=4, output_files_per_partition=1),
        skew=SkewSettings(hot_key_min_share=0.2, max_hot_keys=5, salt_buckets=4),
        quality={"bronze": {"min_rows": 1},
                 "silver": {"min_rows": 1, "max_reject_ratio": 0.5, "max_unknown_customer_ratio": 0.5},
                 "gold": {"min_rows": 1}},
    )


def ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


@pytest.fixture
def raw_orders(spark):
    rows = [
        # order_id, customer, product, qty, price, status, order_ts, updated_at
        ("o1", 1, 10, 2, 5.0, "PLACED", ts("2024-01-01 09:00:00"), ts("2024-01-01 09:00:00")),
        ("o1", 1, 10, 2, 5.0, "SHIPPED", ts("2024-01-01 09:00:00"), ts("2024-01-01 12:00:00")),
        ("o2", 1, 11, 1, 7.5, "DELIVERED", ts("2024-01-01 10:00:00"), ts("2024-01-01 10:00:00")),
        ("o3", 1, 10, 3, 5.0, "PLACED", ts("2024-01-01 11:00:00"), ts("2024-01-01 11:00:00")),
        ("o4", 2, 11, 1, 7.5, "CANCELLED", ts("2024-01-01 12:00:00"), ts("2024-01-01 12:00:00")),
        ("o5", 3, 99, 1, 1.0, "PLACED", ts("2024-01-01 13:00:00"), ts("2024-01-01 13:00:00")),
        ("o6", None, 10, 1, 5.0, "PLACED", ts("2024-01-01 14:00:00"), ts("2024-01-01 14:00:00")),
        ("o7", 2, 10, 0, 5.0, "PLACED", ts("2024-01-01 15:00:00"), ts("2024-01-01 15:00:00")),
        ("o8", 2, 10, 1, 5.0, "LOST", ts("2024-01-01 16:00:00"), ts("2024-01-01 16:00:00")),
    ]
    return spark.createDataFrame(rows, RAW_ORDERS_SCHEMA)


@pytest.fixture
def customers(spark):
    rows = [(1, "A", "SMB", "DE", None), (2, "B", "CONSUMER", "IN", None),
            (3, "C", "ENTERPRISE", "US", None)]
    return spark.createDataFrame(rows, RAW_CUSTOMERS_SCHEMA)


@pytest.fixture
def products(spark):
    rows = [(10, "Pen", "BOOKS", 5.0), (11, "Ball", "TOYS", 7.5)]
    return spark.createDataFrame(rows, RAW_PRODUCTS_SCHEMA)
