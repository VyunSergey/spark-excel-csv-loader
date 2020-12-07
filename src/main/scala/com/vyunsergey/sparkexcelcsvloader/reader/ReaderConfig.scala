package com.vyunsergey.sparkexcelcsvloader.reader

import com.vyunsergey.sparkexcelcsvloader.config.Configuration

case class ReaderConfig(private val config: Map[String, String]) {
  def mode(): String = config.getOrElse(ReaderConfig.readerModeKey, "csv")

  def csvOptions(): Map[String, String] = config.filterKeys { key =>
    !List(ReaderConfig.readerModeKey).contains(key) && key.startsWith(ReaderConfig.readerCsvMask)
  }.map { case (key, value) =>
    (key.replace(s"${ReaderConfig.readerCsvMask}.", ""), value)
  }

  def excelOptions(): Map[String, String] = config.filterKeys { key =>
    !List(ReaderConfig.readerModeKey).contains(key) && key.startsWith(ReaderConfig.readerExcelMask)
  }.map { case (key, value) =>
    (key.replace(s"${ReaderConfig.readerExcelMask}.", ""), value)
  }

  def +(kv: (String, String)): ReaderConfig = {
    ReaderConfig(this.config + kv)
  }

  def ++(conf: Map[String, String]): ReaderConfig = {
    ReaderConfig(this.config ++ conf)
  }
}

object ReaderConfig {
  def make(config: Configuration.AppConf): ReaderConfig = ReaderConfig(config.readerConf)

  val readerModeKey = "reader.mode"
  val readerCsvMask = "reader.csv"
  val readerExcelMask = "reader.excel"
}
