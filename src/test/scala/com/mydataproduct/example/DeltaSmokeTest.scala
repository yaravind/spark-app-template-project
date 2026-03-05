package com.mydataproduct.example

import org.scalatest.funsuite.AnyFunSuite

import java.nio.file.Files

class DeltaSmokeTest extends AnyFunSuite {
  test("can write and read a Delta table") {
    val spark = SparkUtil.session("delta-smoke-test")

    val tempDir = Files.createTempDirectory("delta-smoke-").toFile
    tempDir.deleteOnExit()

    val tablePath = new java.io.File(tempDir, "table").getAbsolutePath

    spark.range(0, 3).toDF("id")
      .write
      .format("delta")
      .mode("overwrite")
      .save(tablePath)

    val count = spark.read.format("delta").load(tablePath).count()
    assert(count == 3)

    spark.stop()
  }
}
