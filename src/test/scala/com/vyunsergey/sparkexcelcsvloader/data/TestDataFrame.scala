package com.vyunsergey.sparkexcelcsvloader.data

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.reader.Reader
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.nio.file.Path

case class TestDataFrame(spark: SparkSession) {
  def printInfo(df: DataFrame): Unit = {
    println(s"Count: ${df.count}")
    df.printSchema
    df.schema.foreach { field =>
      println(s"Filed: ${field.name}, ${field.dataType}, ${field.nullable}, ${field.metadata}")
    }
    df.show(false)
  }

  val simpleDfPath: Path = Configuration.convertPath("src/test/resources/read/csv/test1.csv")
  lazy val simpleDf: DataFrame = Reader.csv(simpleDfPath)(spark)

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
