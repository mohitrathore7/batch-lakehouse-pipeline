package com.mohitrathore.lakehouse

import java.nio.file.Files
import java.sql.Date

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

class GoldAggregationsSpec extends AnyFunSuite with BeforeAndAfterAll {

  private lazy val spark = SparkSession.builder()
    .master("local[2]")
    .appName("gold-tests")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  private def orders = {
    import spark.implicits._
    val d1 = Date.valueOf("2024-01-01")
    Seq(
      ("o1", 1L, "SMB", "BOOKS", 2, 20.0, "DELIVERED", d1),
      ("o2", 1L, "SMB", "BOOKS", 1, 10.0, "SHIPPED", d1),
      ("o3", 2L, "CONSUMER", "TOYS", 3, 30.0, "PLACED", d1),
      ("o4", 2L, "CONSUMER", "TOYS", 1, 99.0, "CANCELLED", d1)
    ).toDF("order_id", "customer_id", "customer_segment", "category", "quantity",
      "order_amount", "order_status", "order_date")
  }

  test("parseArgs reads all flags") {
    val a = GoldAggregations.parseArgs(
      Array("--ds", "2024-01-01", "--silver-path", "/s", "--gold-path", "/g", "--output-files", "2"))
    assert(a == GoldAggregations.Args("2024-01-01", "/s", "/g", 2))
  }

  test("parseArgs rejects unknown flags") {
    assertThrows[IllegalArgumentException](GoldAggregations.parseArgs(Array("--nope", "1")))
  }

  test("daily category sales excludes cancelled orders") {
    val rows = GoldAggregations.dailyCategorySales(orders)
      .collect()
      .map(r => r.getAs[String]("category") -> r.getAs[Double]("revenue"))
      .toMap
    assert(rows == Map("BOOKS" -> 30.0, "TOYS" -> 30.0))
  }

  test("customer summary aggregates per customer") {
    val c1 = GoldAggregations.customerDailySummary(orders)
      .where("customer_id = 1").head()
    assert(c1.getAs[Long]("orders") == 2L)
    assert(c1.getAs[Double]("total_spend") == 30.0)
  }

  test("rerunning a day overwrites only that day (idempotent)") {
    val root = Files.createTempDirectory("lakehouse").toString
    orders.write.partitionBy("order_date").parquet(s"$root/silver/orders")
    val args = GoldAggregations.Args("2024-01-01", s"$root/silver", s"$root/gold", 1)

    GoldAggregations.run(spark, args)
    GoldAggregations.run(spark, args)

    val count = spark.read.parquet(s"$root/gold/daily_category_sales").count()
    assert(count == 2L)
  }
}
