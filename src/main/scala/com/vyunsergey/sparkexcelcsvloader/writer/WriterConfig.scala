package com.vyunsergey.sparkexcelcsvloader.writer

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import org.apache.spark.sql.SaveMode

case class WriterConfig(private val config: Map[String, String]) {
  def mode(): String = config.getOrElse(WriterConfig.writerModeKey, "csv")

  def saveMode(): SaveMode = config.get(WriterConfig.writerSaveModeKey)
    .map(WriterConfig.parseSaveMode).getOrElse(SaveMode.Overwrite)

  def csvOptions(): Map[String, String] = config.filterKeys { key =>
    !List(WriterConfig.writerModeKey, WriterConfig.writerSaveModeKey).contains(key) && key.startsWith(WriterConfig.writerCsvMask)
  }.map { case (key, value) =>
    (key.replace(s"${WriterConfig.writerCsvMask}.", ""), value)
  }

  def excelOptions(): Map[String, String] = config.filterKeys { key =>
    !List(WriterConfig.writerModeKey, WriterConfig.writerSaveModeKey).contains(key) && key.startsWith(WriterConfig.writerExcelMask)
  }.map { case (key, value) =>
    (key.replace(s"${WriterConfig.writerExcelMask}.", ""), value)
  }

  def +(kv: (String, String)): WriterConfig = {
    WriterConfig(this.config + kv)
  }

  def ++(conf: Map[String, String]): WriterConfig = {
    WriterConfig(this.config ++ conf)
  }
}

object WriterConfig {
  def make(config: Configuration.AppConf): WriterConfig = WriterConfig(config.writerConf)

  val writerModeKey = "writer.mode"
  val writerSaveModeKey = "writer.saveMode"
  val writerCsvMask = "writer.csv"
  val writerExcelMask = "writer.excel"

  def parseSaveMode(mode: String): SaveMode = mode.trim.toLowerCase match {
    case "append" => SaveMode.Append
    case "overwrite" => SaveMode.Overwrite
    case "errorifexists" => SaveMode.ErrorIfExists
    case "ignore" => SaveMode.Ignore
    case _ => SaveMode.Overwrite
  }
}
