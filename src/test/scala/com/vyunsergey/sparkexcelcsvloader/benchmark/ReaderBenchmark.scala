package com.vyunsergey.sparkexcelcsvloader.benchmark

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.data.TestDataFrame
import com.vyunsergey.sparkexcelcsvloader.reader.{Reader, ReaderConfig}
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.scalameter.api._

object ReaderBenchmark extends Bench.LocalTime {
  lazy val conf: Configuration.AppConf = Configuration.config
  lazy val readerConf: ReaderConfig = ReaderConfig.make(conf)
  lazy val sparkConf: SparkConfig = SparkConfig.make(conf)
  implicit lazy val spark: SparkSession = SparkConnection.make("Reader Benchmark")(sparkConf ++
    Map("spark.master" -> "local[*]"))
  implicit lazy val logger: Logger = LogManager.getLogger(getClass)
  val dataFrames: TestDataFrame = TestDataFrame(readerConf, spark, logger)

  val gen: Gen[Unit] = Gen.unit("")

  performance of "Reader" in {
    measure method "csv read test1.csv" in {
      using(gen) in { _ =>
        Reader.csv(dataFrames.test1Path)(readerConf ++
          Map("reader.csv.header" -> "true",
            "reader.csv.delimiter" -> ";",
            "reader.csv.inferSchema" -> "true")
        )
      }
    }
    measure method "csv read test2.csv" in {
      using(gen) in { _ =>
        Reader.csv(dataFrames.test2Path)(readerConf ++
          Map("reader.csv.header" -> "true",
            "reader.csv.inferSchema" -> "true")
        )
      }
    }
    measure method "csv read test3.csv" in {
      using(gen) in { _ =>
        Reader.csv(dataFrames.test3Path)(readerConf ++
          Map("reader.csv.header" -> "true",
            "reader.csv.inferSchema" -> "true")
        )
      }
    }
    measure method "csv read titanic.csv" in {
      using(gen) in { _ =>
        Reader.csv(dataFrames.titanicPath)(readerConf ++
          Map("reader.csv.header" -> "true",
            "reader.csv.inferSchema" -> "true")
        )
      }
    }
    measure method "csv read gis.csv" in {
      using(gen) in { _ =>
        Reader.csv(dataFrames.gisPath)(readerConf ++
          Map("reader.csv.header" -> "true",
            "reader.csv.inferSchema" -> "true")
        )
      }
    }
  }
}
