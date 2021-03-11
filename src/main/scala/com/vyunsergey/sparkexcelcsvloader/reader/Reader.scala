package com.vyunsergey.sparkexcelcsvloader.reader

import org.apache.log4j.Logger
import org.apache.spark.input.PortableDataStream
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Path
import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.{ZipEntry, ZipInputStream}

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

  def zip(path: Path)(readerConf: ReaderConfig)
         (implicit spark: SparkSession, logger: Logger): DataFrame = {
    import spark.implicits._

    logger.info(s"Read Zip archive from path: ${path.toUri.getPath}")
    logger.info(s"Reader options:\n${readerConf.csvOptions().mkString("\n")}")

    val zipInputStreams: Stream[(String, PortableDataStream)] = spark.sparkContext
      .binaryFiles(path.toUri.getPath)
      .collect.toStream

    val df: DataFrame = zipInputStreams
      .flatMap { case (_: String, content: PortableDataStream) =>
        val zis = new ZipInputStream(content.open)
        Stream.continually(zis.getNextEntry)
          .takeWhile {
            case null =>
              zis.close()
              false
            case _ => true
          }
          .map { _: ZipEntry =>
            val br = new BufferedReader(new InputStreamReader(zis))
            val lines = Stream.continually(br.readLine()).takeWhile(_ != null)
            spark.read
              .options(readerConf.csvOptions())
              .csv(spark.createDataset(spark.sparkContext.parallelize(lines)))
          }
      }.reduce(_ union _)

    logger.info(s"Read DataFrame with schema:\n${df.schema.treeString}")
    df
  }
}
