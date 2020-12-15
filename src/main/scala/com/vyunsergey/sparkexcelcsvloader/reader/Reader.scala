package com.vyunsergey.sparkexcelcsvloader.reader

import org.apache.log4j.Logger

import java.nio.file.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

object Reader {
  def csv(path: Path)(readerConf: ReaderConfig)
         (implicit spark: SparkSession, logger: Logger): DataFrame = {
    logger.info(s"Read .csv file from path: ${path.toUri.getPath}")
    logger.info(s"Reader options:\n${readerConf.csvOptions().mkString("\n")}")

    val df = spark.read
      .format("com.databricks.spark.csv")
      .options(readerConf.csvOptions())
      .load(path.toUri.getPath)

    logger.info(s"Read DataFrame with schema:\n${df.schema.treeString}")
    df
  }

  def excel(path: Path)(readerConf: ReaderConfig)
           (implicit spark: SparkSession, logger: Logger): DataFrame = {
    logger.info(s"Read Excel file from path: ${path.toUri.getPath}")
    logger.info(s"Reader options:\n${readerConf.excelOptions().mkString("\n")}")

    val df = spark.read
      .format("com.crealytics.spark.excel")
      .options(readerConf.excelOptions())
      .load(path.toUri.getPath)

    logger.info(s"Read DataFrame with schema:\n${df.schema.treeString}")
    df
  }
}
