package com.vyunsergey.sparkexcelcsvloader.reader

import java.nio.file.Path
import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.data.TestDataFrame
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReaderTest extends AnyFlatSpec with Matchers {
  lazy val conf: Configuration.AppConf = Configuration.config
  lazy val readerConf: ReaderConfig = ReaderConfig.make(conf)
  lazy val sparkConf: SparkConfig = SparkConfig.make(conf)
  implicit lazy val spark: SparkSession = SparkConnection.make("Reader Test")(sparkConf)
  implicit lazy val logger: Logger = LogManager.getLogger(getClass)
  val dataFrames: TestDataFrame = TestDataFrame(readerConf, spark, logger)

  "Reader" should "correctly read .csv file" in {
    def check(path: Path)(conf: ReaderConfig)
             (expectedNumCols: Int, expectedNumRows: Int): Unit = {
      val df: DataFrame = Reader.csv(path)(conf)

      df.columns.length shouldBe expectedNumCols
      df.count shouldBe expectedNumRows
    }

    check(dataFrames.test1Path)(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.delimiter" -> ";",
        "reader.csv.inferSchema" -> "true")
    )(3, 10)

    check(dataFrames.test2Path)(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.inferSchema" -> "true")
    )(5, 10)

    check(dataFrames.test3Path)(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.inferSchema" -> "true")
    )(14, 10)

    check(dataFrames.titanicPath)(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.inferSchema" -> "true")
    )(5, 1313)
  }

}
