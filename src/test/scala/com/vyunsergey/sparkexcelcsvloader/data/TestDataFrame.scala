package com.vyunsergey.sparkexcelcsvloader.data

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.reader.{Reader, ReaderConfig}
import org.apache.log4j.Logger
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Path

case class TestDataFrame(
                          readerConf: ReaderConfig,
                          spark: SparkSession,
                          logger: Logger
                        ) {
  def printInfo(df: DataFrame): Unit = {
    println(s"Count: ${df.count}")
    df.printSchema
    df.schema.foreach { field =>
      println(s"Filed: ${field.name}, ${field.dataType}, ${field.nullable}, ${field.metadata}")
    }
    df.show(false)
  }

  implicit lazy val sparkImpl: SparkSession = spark
  implicit lazy val loggerImpl: Logger = logger

  val test1Path: Path = Configuration.convertPath("src/test/resources/csv/test1.csv")
  val test2Path: Path = Configuration.convertPath("src/test/resources/csv/test2.csv")
  val test3Path: Path = Configuration.convertPath("src/test/resources/csv/test3.csv")

  val test1ZipPath: Path = Configuration.convertPath("src/test/resources/zip/test1.zip")
  val test2ZipPath: Path = Configuration.convertPath("src/test/resources/zip/test2.zip")
  val test3ZipPath: Path = Configuration.convertPath("src/test/resources/zip/test3.zip")

  val test1WriterPath: Path = Configuration.convertPath("src/test/resources/csv/writer/test1")
  val test2WriterPath: Path = Configuration.convertPath("src/test/resources/csv/writer/test2")
  val test3WriterPath: Path = Configuration.convertPath("src/test/resources/csv/writer/test3")

  val titanicPath: Path = Configuration.convertPath("src/test/resources/csv/titanic.csv")
  val titanicWriterPath: Path = Configuration.convertPath("src/test/resources/csv/writer/titanic")

  val gisPath: Path = Configuration.convertPath("src/test/resources/csv/benchmark/gis.csv")
  val gisWriterPath: Path = Configuration.convertPath("src/test/resources/csv/benchmark/writer/gis")

  val keyValueSchema: StructType = StructType(
    StructField("key", StringType) ::
    StructField("val", StringType) :: Nil
  )

  lazy val test1Df: DataFrame = Reader.csv(test1Path)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.delimiter" -> ";",
      "reader.csv.inferSchema" -> "true"))

  lazy val test2Df: DataFrame = Reader.csv(test2Path)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.inferSchema" -> "true"))

  lazy val test3Df: DataFrame = Reader.csv(test3Path)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.inferSchema" -> "true"))

  lazy val test1ZipDf: DataFrame = Reader.zip(test1ZipPath)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.delimiter" -> ",",
      "reader.csv.inferSchema" -> "true"))

  lazy val test2ZipDf: DataFrame = Reader.zip(test2ZipPath)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.delimiter" -> ",",
      "reader.csv.inferSchema" -> "true"))

  lazy val test3ZipDf: DataFrame = Reader.zip(test3ZipPath)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.delimiter" -> ",",
      "reader.csv.inferSchema" -> "true"))

  lazy val titanicDf: DataFrame = Reader.csv(titanicPath)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.inferSchema" -> "true"))

  lazy val integersDf: DataFrame =
    spark.createDataFrame(TestRow.genIntegersSeqRows(100))

  lazy val numbersDf: DataFrame =
    spark.createDataFrame(TestRow.genNumbersSeqRows(100))

  lazy val decimalDf: DataFrame =
    spark.createDataFrame(TestRow.genDecimalSeqRows(100))

  lazy val stringsDf: DataFrame =
    spark.createDataFrame(TestRow.genStringsSeqRows(100, 100))

  lazy val dateTimeDf: DataFrame =
    spark.createDataFrame(TestRow.genDateTimeSeqRow(100))

  lazy val randomDf: DataFrame =
    spark.createDataFrame(TestRow.genRandomSeqRows(100, 100))
}
