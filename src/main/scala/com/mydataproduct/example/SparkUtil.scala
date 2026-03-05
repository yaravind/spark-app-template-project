package com.mydataproduct.example

import org.apache.logging.log4j.LogManager
import org.apache.spark.sql.SparkSession

object SparkUtil {
  private val logger = LogManager.getLogger(getClass)

  def session(appName: String): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master("local[*]")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    spark
  }
}
