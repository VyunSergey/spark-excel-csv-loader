package com.vyunsergey.sparkexcelcsvloader.reader

import java.nio.file.Path

import org.apache.spark.sql.{DataFrame, SparkSession}

object Reader {
  def csv(path: Path)(readerConf: ReaderConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("com.databricks.spark.csv")
      .options(readerConf.csvOptions())
      .load(path.toUri.getPath)
  }

  def excel(path: Path)(readerConf: ReaderConfig)(implicit spark: SparkSession): DataFrame = {
    spark.read
      .format("com.crealytics.spark.excel")
      .options(readerConf.excelOptions())
      .load(path.toUri.getPath)
  }
}
