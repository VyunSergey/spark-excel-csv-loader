package com.vyunsergey.sparkexcelcsvloader.writer

import org.apache.spark.sql.Dataset

import java.nio.file.Path

object Writer {
  def csv[A](ds: Dataset[A])(path: Path)(writerConf: WriterConfig): Unit = {
    ds.write
      .format("com.databricks.spark.csv")
      .mode(writerConf.saveMode())
      .options(writerConf.csvOptions())
      .save(path.toUri.getPath)
  }

  def excel[A](ds: Dataset[A])(path: Path)(writerConf: WriterConfig): Unit = {
    ds.write
      .format("com.crealytics.spark.excel")
      .mode(writerConf.saveMode())
      .options(writerConf.excelOptions())
      .save(path.toUri.getPath)
  }
}
