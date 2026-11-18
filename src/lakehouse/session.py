"""SparkSession factory with optimized and baseline execution profiles."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from lakehouse.config import PipelineConfig


def sql_settings(cfg: PipelineConfig, optimized: bool) -> dict[str, str]:
    """Runtime SQL settings. Baseline = plain Spark defaults, no tuning."""
    common = {
        "spark.sql.sources.partitionOverwriteMode": "dynamic",  # idempotent partition rewrites
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.parquet.compression.codec": "snappy",
    }
    if optimized:
        return {
            **common,
            "spark.sql.shuffle.partitions": str(cfg.spark.shuffle_partitions),
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.autoBroadcastJoinThreshold": str(cfg.spark.broadcast_threshold_mb * 1024 * 1024),
        }
    return {
        **common,
        "spark.sql.shuffle.partitions": "200",
        "spark.sql.adaptive.enabled": "false",
        "spark.sql.adaptive.skewJoin.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
    }


def _s3a_settings() -> dict[str, str]:
    """S3A settings. On AWS leave S3_ENDPOINT unset and use the instance role."""
    settings = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.fast.upload": "true",
    }
    endpoint = os.environ.get("S3_ENDPOINT")
    if endpoint:  # local MinIO
        settings.update({
            "spark.hadoop.fs.s3a.endpoint": endpoint,
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": str(endpoint.startswith("https")).lower(),
        })
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        settings.update({
            "spark.hadoop.fs.s3a.access.key": os.environ["AWS_ACCESS_KEY_ID"],
            "spark.hadoop.fs.s3a.secret.key": os.environ.get("AWS_SECRET_ACCESS_KEY", ""),
            "spark.hadoop.fs.s3a.aws.credentials.provider":
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        })
    return settings


def build_spark(app_name: str, cfg: PipelineConfig, optimized: bool = True,
                master: str | None = None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name).config("spark.ui.showConsoleProgress", "false")
    if master:
        builder = builder.master(master)
    for key, value in _s3a_settings().items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    apply_profile(spark, cfg, optimized)
    return spark


def apply_profile(spark: SparkSession, cfg: PipelineConfig, optimized: bool) -> None:
    """Switch an existing session between optimized and baseline settings."""
    for key, value in sql_settings(cfg, optimized).items():
        spark.conf.set(key, value)
