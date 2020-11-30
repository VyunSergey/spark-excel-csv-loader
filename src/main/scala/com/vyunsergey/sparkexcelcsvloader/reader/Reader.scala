package com.vyunsergey.sparkexcelcsvloader.reader

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

object Reader {
  def csv(path: Path)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("com.databricks.spark.csv")
      .option("encoding", "UTF-8")
      .option("header", value = true)
      .option("sep", ";")
      .option("inferSchema", value = true)
      .load(path.toUri.getPath)
  }

}
