package com.vyunsergey.sparkexcelcsvloader.data

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.reader.{Reader, ReaderConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Path

case class TestDataFrame(readerConf: ReaderConfig, spark: SparkSession) {
  def printInfo(df: DataFrame): Unit = {
    println(s"Count: ${df.count}")
    df.printSchema
    df.schema.foreach { field =>
      println(s"Filed: ${field.name}, ${field.dataType}, ${field.nullable}, ${field.metadata}")
    }
    df.show(false)
  }

  val test1Path: Path = Configuration.convertPath("src/test/resources/csv/test1.csv")
  val test2Path: Path = Configuration.convertPath("src/test/resources/csv/test2.csv")
  val test3Path: Path = Configuration.convertPath("src/test/resources/csv/test3.csv")
  val titanicPath: Path = Configuration.convertPath("src/test/resources/csv/titanic.csv")
  val gisPath: Path = Configuration.convertPath("src/test/resources/csv/benchmark/gis.csv")

  lazy val test1Df: DataFrame = Reader.csv(test1Path)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.delimiter" -> ";",
      "reader.csv.inferSchema" -> "true"))(spark)

  lazy val test2Df: DataFrame = Reader.csv(test2Path)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.inferSchema" -> "true"))(spark)

  lazy val test3Df: DataFrame = Reader.csv(test2Path)(readerConf ++
    Map("reader.csv.header" -> "true",
      "reader.csv.inferSchema" -> "true"))(spark)

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
