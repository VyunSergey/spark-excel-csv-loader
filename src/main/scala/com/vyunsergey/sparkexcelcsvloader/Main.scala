package com.vyunsergey.sparkexcelcsvloader

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.reader.ReaderConfig
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import com.vyunsergey.sparkexcelcsvloader.writer.WriterConfig
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object Main extends App {
  lazy val conf = Configuration.config
  lazy val sparkConf = SparkConfig.make(conf)
  lazy val readerConf: ReaderConfig = ReaderConfig.make(conf)
  lazy val writerConf: WriterConfig = WriterConfig.make(conf)
  implicit lazy val spark: SparkSession = SparkConnection.make("App")(sparkConf)
  implicit lazy val logger: Logger = LogManager.getLogger(getClass)

  logger.info(s"Spark Version: ${spark.version}")

  spark.stop()
}
