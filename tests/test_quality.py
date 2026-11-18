import pytest

from lakehouse.quality import (
    DataQualityError,
    check_accepted_values,
    check_min_rows,
    check_not_null,
    check_ratio,
    check_unique,
    run_suite,
)


def test_generic_checks(spark):
    df = spark.createDataFrame([("a", 1), ("a", None), ("b", 3)], ["k", "v"])
    assert check_min_rows(df, 3).passed
    assert not check_min_rows(df, 4).passed
    assert [r.passed for r in check_not_null(df, ["k", "v"])] == [True, False]
    assert not check_unique(df, ["k"]).passed
    assert check_accepted_values(df, "k", ("a", "b")).passed
    assert check_ratio("r", 1, 100, 0.02).passed
    assert not check_ratio("r", 5, 100, 0.02).passed


def test_bronze_suite_fails_and_raises_on_too_few_rows(spark, cfg):
    spark.createDataFrame([("o1", "2024-01-01 00:00:00", "f", "2024-01-01")],
                          ["order_id", "_ingested_at", "_source_file", "ds"]) \
         .write.partitionBy("ds").parquet(f"{cfg.paths.bronze}/orders")
    strict = cfg.__class__(paths=cfg.paths, spark=cfg.spark, skew=cfg.skew,
                           quality={"bronze": {"min_rows": 10}})

    with pytest.raises(DataQualityError, match="row_count"):
        run_suite(spark, strict, "bronze", "2024-01-01")

    # Results are persisted even when the suite fails.
    saved = spark.read.parquet(f"{cfg.paths.quality}/results").where("layer = 'bronze'")
    assert saved.where("passed = false").count() == 1
