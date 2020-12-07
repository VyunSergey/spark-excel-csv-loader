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
  lazy val sparkConf: SparkConfig = SparkConfig.make(conf)
  implicit lazy val spark: SparkSession = SparkConnection.make("App", sparkConf)
  val dataFrames: TestDataFrame = TestDataFrame(spark)

  "Reader" should "correctly read .csv file" in {
    def check(path: Path): Unit = {
      val df: DataFrame = Reader.csv(path)
      df.columns.length > 0 shouldBe true
      df.count > 0 shouldBe true
    }

    check(dataFrames.simpleDfPath)
  }

}
