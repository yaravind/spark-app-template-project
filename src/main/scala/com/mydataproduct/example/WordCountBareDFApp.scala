package com.mydataproduct.example

import org.apache.spark.sql.functions.{split, explode, col, lower, trim}
import org.apache.spark.sql.SparkSession

object WordCountBareDFApp {
  // 1. Entry point
  def main(args: Array[String]): Unit = {
    // Create a SparkSession
    val spark = SparkUtil.session("word-count-bare-df")

    // Enables implicit conversions like Seq -> DataFrame
    import spark.implicits._

    // 3. Specify input. Hardcoded string for testing
    val inputData = Seq("The cat sat on the   mat")

    // 4. Read as DF. Create a DataFrame from the sequence of strings
    val linesDf = inputData.toDF("line")

    println("Input DataFrame:")
    linesDf.show(false)

    // 5. Tokenize - split each line into words
    val wordsDf = linesDf.select(
      explode(split(col("line"), "[\\s]+")).as("word")
    )

    // 6. Clean - Clean and normalize tokens
    val cleanedDf = wordsDf
      .filter(col("word") =!= "")
      .select(lower(trim(col("word"))).as("word"))

    println("Words DataFrame:")
    wordsDf.show(false)

    println("Cleaned DataFrame:")
    cleanedDf.show(false)

    // 7. Aggregate - group and count
    val resultDf = cleanedDf
      .groupBy(col("word"))
      .count()

    println("Result:")
    resultDf.show(false)

    // Stop the SparkSession
    spark.stop()
  }
}