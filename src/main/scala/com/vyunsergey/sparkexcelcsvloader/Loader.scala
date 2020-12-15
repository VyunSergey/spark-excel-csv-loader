package com.vyunsergey.sparkexcelcsvloader

import com.vyunsergey.sparkexcelcsvloader.arguments.Arguments
import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.reader.{Reader, ReaderConfig}
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import com.vyunsergey.sparkexcelcsvloader.transformer.Transformer
import com.vyunsergey.sparkexcelcsvloader.writer.{Writer, WriterConfig}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Path

object Loader extends App {
  lazy val arguments = Arguments(args)
  lazy val mode = arguments.mode()
  lazy val srcPath = Configuration.convertPath(arguments.srcPath())
  lazy val tgtPath = Configuration.convertPath(arguments.tgtPath())

  lazy val conf = Configuration.config
  lazy val sparkConf = SparkConfig.make(conf)
  lazy val readerConf: ReaderConfig = ReaderConfig.make(conf)
  lazy val writerConf: WriterConfig = WriterConfig.make(conf)

  implicit lazy val spark: SparkSession = SparkConnection.make("Key-Value Loader")(sparkConf)
  implicit lazy val logger: Logger = LogManager.getLogger(getClass)

  logger.info(s"Spark Version: ${spark.version}")

  program(mode, srcPath, tgtPath)(readerConf, writerConf)

  logger.info(s"Finish Spark application. Stopping Spark.")

  spark.stop()

  def program(mode: String, srcPath: Path, tgtPath: Path)
             (readerConf: ReaderConfig, writerConf: WriterConfig)
             (implicit spark: SparkSession, logger: Logger): Unit = {
    val data: DataFrame =
      if (mode == "csv") Reader.csv(srcPath)(readerConf)
      else Reader.excel(srcPath)(readerConf)

    val metaData = Transformer.metaColumns(data)
    val kvData =Transformer.keyValueColumns(data)

    Writer.csv(metaData)(tgtPath.resolve("meta"))(writerConf)
    Writer.csv(kvData)(tgtPath.resolve("data"))(writerConf)
  }
}
