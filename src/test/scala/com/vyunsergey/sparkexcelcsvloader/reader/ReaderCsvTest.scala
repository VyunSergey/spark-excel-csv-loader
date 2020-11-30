package com.vyunsergey.sparkexcelcsvloader.reader

import java.io.File
import java.nio.file.Paths

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers

class ReaderCsvTest extends AnyFlatSpec with Matchers {
  lazy val conf: Configuration.AppConf = Configuration.config
  lazy val sparkConf: SparkConfig = SparkConfig.make(conf)
  lazy val spark: SparkSession = SparkConnection.make("App", sparkConf)

  "Csv Reader" should "correctly read .csv file" in {
    val path = Paths.get(new File("src/test/resources/read/csv/test1.csv").toURI)
    println(s"Path: $path")
    val df: DataFrame = Reader.csv(path)(spark)
    println(s"Count: ${df.count}")
    df.show(false)
  }

}
