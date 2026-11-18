"""Skew handling: detect hot join keys and salt them across buckets."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

SALT_COL = "_salt"


def find_hot_keys(df: DataFrame, key: str, min_share: float, max_keys: int) -> list:
    """Return keys holding at least `min_share` of all rows (largest first)."""
    counts = df.where(F.col(key).isNotNull()).groupBy(key).count()
    total = counts.agg(F.sum("count")).first()[0] or 0
    if total == 0:
        return []
    threshold = max(1, int(total * min_share))
    rows = (counts.where(F.col("count") >= threshold)
                  .orderBy(F.desc("count"))
                  .limit(max_keys)
                  .collect())
    return [row[key] for row in rows]


def salted_join(fact: DataFrame, dim: DataFrame, key: str, hot_keys: list,
                buckets: int, salt_source: str, how: str = "left") -> DataFrame:
    """Join `fact` to `dim` on `key`, spreading hot keys over `buckets` tasks.

    Fact rows with a hot key get a salt in [0, buckets) derived from a hash of
    `salt_source`, so the salt is deterministic and retries produce the same
    output. The matching dim rows are replicated once per salt value. Rows with
    non-hot keys keep salt 0 and are joined normally.
    """
    if not hot_keys or buckets <= 1:
        return fact.join(dim, on=key, how=how)

    is_hot = F.col(key).isin(hot_keys)
    salted_fact = fact.withColumn(
        SALT_COL,
        F.when(is_hot, F.pmod(F.xxhash64(F.col(salt_source)), F.lit(buckets)).cast("int"))
         .otherwise(F.lit(0)),
    )
    salt_values = F.array(*[F.lit(i) for i in range(buckets)])
    salted_dim = (dim.withColumn("_salts", F.when(is_hot, salt_values).otherwise(F.array(F.lit(0))))
                     .withColumn(SALT_COL, F.explode("_salts"))
                     .drop("_salts"))

    return salted_fact.join(salted_dim, on=[key, SALT_COL], how=how).drop(SALT_COL)
