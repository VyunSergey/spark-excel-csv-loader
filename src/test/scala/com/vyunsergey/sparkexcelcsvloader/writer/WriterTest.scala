package com.vyunsergey.sparkexcelcsvloader.writer

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.data.TestDataFrame
import com.vyunsergey.sparkexcelcsvloader.reader.{Reader, ReaderConfig}
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.Path

class WriterTest extends AnyFlatSpec with Matchers {
  lazy val conf: Configuration.AppConf = Configuration.config
  lazy val readerConf: ReaderConfig = ReaderConfig.make(conf)
  lazy val writerConf: WriterConfig = WriterConfig.make(conf)
  lazy val sparkConf: SparkConfig = SparkConfig.make(conf)
  implicit lazy val spark: SparkSession = SparkConnection.make("Writer Test")(sparkConf)
  implicit lazy val logger: Logger = LogManager.getLogger(getClass)
  val dataFrames: TestDataFrame = TestDataFrame(readerConf, spark, logger)

  "Writer" should "correctly write to .csv file" in {
    def check(df: DataFrame)(path: Path)
             (writerConf: WriterConfig)(readerConf: ReaderConfig): Unit = {
      Writer.csv(df)(path, 1)(writerConf)

      val resDf: DataFrame = Reader.csv(path)(readerConf)

      resDf.columns.length shouldBe df.columns.length
      resDf.columns shouldBe df.columns
      resDf.count shouldBe df.count
    }

    check(dataFrames.test1Df)(dataFrames.test1WriterPath)(writerConf ++
      Map("writer.csv.header" -> "true",
        "writer.csv.delimiter" -> ";",
        "writer.csv.quoteMode" -> "ALL")
    )(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.delimiter" -> ";",
        "reader.csv.inferSchema" -> "true")
    )

    check(dataFrames.test2Df)(dataFrames.test2WriterPath)(writerConf ++
      Map("writer.csv.header" -> "true",
        "writer.csv.delimiter" -> ",",
        "writer.csv.quoteMode" -> "MINIMAL")
    )(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.delimiter" -> ",",
        "reader.csv.inferSchema" -> "true")
    )

    check(dataFrames.test3Df)(dataFrames.test3WriterPath)(writerConf ++
      Map("writer.csv.header" -> "true",
        "writer.csv.delimiter" -> "\t",
        "writer.csv.quoteMode" -> "NON_NUMERIC")
    )(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.delimiter" -> "\t",
        "reader.csv.inferSchema" -> "true")
    )

    check(dataFrames.titanicDf)(dataFrames.titanicWriterPath)(writerConf ++
      Map("writer.csv.header" -> "true",
        "writer.csv.delimiter" -> ",",
        "writer.csv.quoteMode" -> "MINIMAL")
    )(readerConf ++
      Map("reader.csv.header" -> "true",
        "reader.csv.delimiter" -> ",",
        "reader.csv.inferSchema" -> "true")
    )
  }
}
