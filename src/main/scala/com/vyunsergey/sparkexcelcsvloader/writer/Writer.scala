package com.vyunsergey.sparkexcelcsvloader.writer

import org.apache.log4j.Logger
import org.apache.spark.sql.Dataset

import java.nio.file.Path

object Writer {
  def csv[A](ds: Dataset[A])(path: Path)(writerConf: WriterConfig)
            (implicit logger: Logger): Unit = {
    logger.info(s"Write .csv file to path: ${path.toUri.getPath}")
    logger.info(s"Writer saveMode: ${writerConf.saveMode()}")
    logger.info(s"Writer options:\n${writerConf.csvOptions().mkString("\n")}")

    ds.write
      .format("com.databricks.spark.csv")
      .mode(writerConf.saveMode())
      .options(writerConf.csvOptions())
      .save(path.toUri.getPath)

    logger.info(s"Write .csv finish successfully")
  }

  def excel[A](ds: Dataset[A])(path: Path)(writerConf: WriterConfig)
              (implicit logger: Logger): Unit = {
    logger.info(s"Write Excel file to path: ${path.toUri.getPath}")
    logger.info(s"Writer saveMode: ${writerConf.saveMode()}")
    logger.info(s"Writer options:\n${writerConf.excelOptions().mkString("\n")}")

    ds.write
      .format("com.crealytics.spark.excel")
      .mode(writerConf.saveMode())
      .options(writerConf.excelOptions())
      .save(path.toUri.getPath)

    logger.info(s"Write Excel finish successfully")
  }
}
