package com.mydataproduct.example

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("delta-smoke-test")
      .master("local[*]")
      .getOrCreate()

    val df = spark.range(0, 5).toDF("id")
    df.show(false)

    spark.stop()
  }
}
