"""Read/write helpers for the lakehouse layers."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def write_partitioned(df: DataFrame, path: str, partition_col: str,
                      files_per_partition: int | None) -> None:
    """Overwrite only the partitions present in `df`.

    Relies on spark.sql.sources.partitionOverwriteMode=dynamic, so re-running a
    date replaces that date's data and leaves every other date untouched.
    That is what makes reruns and backfills idempotent.
    """
    if files_per_partition:
        df = df.repartition(files_per_partition)
    df.write.mode("overwrite").partitionBy(partition_col).parquet(path)


def read_partition(spark: SparkSession, path: str, partition_col: str, value: str) -> DataFrame:
    """Read a single partition; the filter is pushed down as partition pruning."""
    return spark.read.parquet(path).where(F.col(partition_col) == F.lit(value))
