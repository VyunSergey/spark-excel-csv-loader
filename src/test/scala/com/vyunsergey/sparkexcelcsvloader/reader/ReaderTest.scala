package com.vyunsergey.sparkexcelcsvloader.reader

import java.nio.file.Path
import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.data.TestDataFrame
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ReaderTest extends AnyFlatSpec with Matchers {
  lazy val conf: Configuration.AppConf = Configuration.config
  lazy val readerConf: ReaderConfig = ReaderConfig.make(conf)
  lazy val sparkConf: SparkConfig = SparkConfig.make(conf)
  implicit lazy val spark: SparkSession = SparkConnection.make("Reader Test")(sparkConf)
  val dataFrames: TestDataFrame = TestDataFrame(readerConf, spark)

  "Reader" should "correctly read .csv file" in {
    def check(path: Path)(conf: ReaderConfig)
             (expectedNumCols: Int, expectedNumRows: Int): Unit = {
      val df: DataFrame = Reader.csv(path)(conf)

      df.columns.length shouldBe expectedNumCols
      df.count shouldBe expectedNumRows
    }

    check(dataFrames.test1DfPath)(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.delimiter" -> ";",
        "reader.csv.inferSchema" -> "true")
    )(3, 3)

    check(dataFrames.test2DfPath)(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.inferSchema" -> "true")
    )(5, 10)

    check(dataFrames.test3DfPath)(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.inferSchema" -> "true")
    )(14, 10)
  }

}
