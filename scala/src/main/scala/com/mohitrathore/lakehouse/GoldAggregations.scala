package com.mohitrathore.lakehouse

import org.apache.spark.sql.{DataFrame, SparkSession, functions => F}

/** Gold layer: business aggregates built from one day of silver orders.
  *
  * Outputs (Parquet, partitioned by order_date, dynamic overwrite = idempotent):
  *   - daily_category_sales   : revenue / units / orders per category per day
  *   - customer_daily_summary : spend and order count per customer per day
  *
  * Usage:
  *   spark-submit --class com.mohitrathore.lakehouse.GoldAggregations lakehouse-gold.jar \
  *     --ds 2024-01-01 --silver-path s3a://lakehouse/silver --gold-path s3a://lakehouse/gold
  */
object GoldAggregations {

  final case class Args(
      ds: String = "",
      silverPath: String = "",
      goldPath: String = "",
      outputFiles: Int = 4
  )

  def parseArgs(args: Array[String]): Args =
    args.grouped(2).foldLeft(Args()) {
      case (a, Array("--ds", v))           => a.copy(ds = v)
      case (a, Array("--silver-path", v))  => a.copy(silverPath = v)
      case (a, Array("--gold-path", v))    => a.copy(goldPath = v)
      case (a, Array("--output-files", v)) => a.copy(outputFiles = v.toInt)
      case (_, other) =>
        throw new IllegalArgumentException(s"Unknown argument: ${other.mkString(" ")}")
    }

  /** Cancelled orders do not count as revenue. */
  def revenueOrders(orders: DataFrame): DataFrame =
    orders.where(F.col("order_status") =!= "CANCELLED")

  def dailyCategorySales(orders: DataFrame): DataFrame =
    revenueOrders(orders)
      .groupBy("order_date", "category")
      .agg(
        F.count("order_id").as("orders"),
        F.sum("quantity").as("units_sold"),
        F.round(F.sum("order_amount"), 2).as("revenue"),
        F.countDistinct("customer_id").as("unique_customers")
      )

  def customerDailySummary(orders: DataFrame): DataFrame =
    revenueOrders(orders)
      .groupBy("order_date", "customer_id", "customer_segment")
      .agg(
        F.count("order_id").as("orders"),
        F.round(F.sum("order_amount"), 2).as("total_spend"),
        F.round(F.avg("order_amount"), 2).as("avg_order_value")
      )

  private def write(df: DataFrame, path: String, files: Int): Unit =
    df.repartition(files)
      .write
      .mode("overwrite")
      .partitionBy("order_date")
      .parquet(path)

  def run(spark: SparkSession, a: Args): Unit = {
    val orders = spark.read
      .parquet(s"${a.silverPath}/orders")
      .where(F.col("order_date") === F.lit(a.ds).cast("date"))
      .cache()

    write(dailyCategorySales(orders), s"${a.goldPath}/daily_category_sales", 1)
    write(customerDailySummary(orders), s"${a.goldPath}/customer_daily_summary", a.outputFiles)
    orders.unpersist()
  }

  def main(rawArgs: Array[String]): Unit = {
    val a = parseArgs(rawArgs)
    require(a.ds.nonEmpty && a.silverPath.nonEmpty && a.goldPath.nonEmpty,
      "--ds, --silver-path and --gold-path are required")

    val spark = SparkSession.builder()
      .appName(s"gold_aggregations_${a.ds}")
      .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
      .config("spark.sql.session.timeZone", "UTC")
      .config("spark.sql.adaptive.enabled", "true")
      .getOrCreate()

    try run(spark, a)
    finally spark.stop()
  }
}
