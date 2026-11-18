from pyspark.sql import functions as F

from lakehouse.skew import find_hot_keys, salted_join


def test_find_hot_keys(spark):
    df = spark.createDataFrame([(1,)] * 80 + [(2,)] * 15 + [(3,)] * 5, ["k"])
    assert find_hot_keys(df, "k", min_share=0.1, max_keys=10) == [1, 2]
    assert find_hot_keys(df, "k", min_share=0.1, max_keys=1) == [1]


def test_salted_join_matches_plain_join(spark):
    fact = spark.range(1000).select(F.col("id").cast("string").alias("order_id"),
                                    F.when(F.col("id") < 700, 1).otherwise(F.col("id") % 10)
                                     .alias("customer_id"))
    dim = spark.range(10).select(F.col("id").cast("int").alias("customer_id"),
                                 F.concat(F.lit("seg"), F.col("id")).alias("segment"))

    salted = salted_join(fact, dim, "customer_id", [1], buckets=8, salt_source="order_id")
    plain = fact.join(dim, "customer_id", "left")

    assert salted.count() == plain.count() == 1000
    assert sorted(salted.collect()) == sorted(plain.select(salted.columns).collect())


def test_salt_spreads_hot_key_over_buckets(spark):
    fact = spark.range(1000).select(F.col("id").cast("string").alias("order_id"),
                                    F.lit(1).alias("customer_id"))
    salted = fact.withColumn("_salt", F.pmod(F.xxhash64("order_id"), F.lit(8)))
    assert salted.select("_salt").distinct().count() == 8
