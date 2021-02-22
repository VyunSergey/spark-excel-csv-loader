package com.vyunsergey.sparkexcelcsvloader.transformer

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.data.TestDataFrame
import com.vyunsergey.sparkexcelcsvloader.reader.ReaderConfig
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class TransformerTest extends AnyFlatSpec with Matchers {
  lazy val conf: Configuration.AppConf = Configuration.config
  lazy val readerConf: ReaderConfig = ReaderConfig.make(conf)
  lazy val sparkConf: SparkConfig = SparkConfig.make(conf)
  implicit lazy val spark: SparkSession = SparkConnection.make("Transformer Test")(sparkConf)
  implicit lazy val logger: Logger = LogManager.getLogger(getClass)
  val dataFrames: TestDataFrame = TestDataFrame(readerConf, spark, logger)

  "stringColumns" should "convert all columns in DataFrame to StringType" in {
    def check(df: DataFrame): Unit = {
      val strDf = Transformer.stringColumns(df)

      strDf.columns shouldBe df.columns
      strDf.count shouldBe df.count
      strDf.schema.map(_.dataType).foreach { tp =>
        tp shouldBe StringType
      }
    }

    check(dataFrames.test1Df)
    check(dataFrames.test2Df)
    check(dataFrames.test3Df)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }

  "prefixColumns" should "add a prefix to all columns of DataFrame" in {
    def check(df: DataFrame, prefix: String = "__"): Unit = {
      val prefDf = Transformer.prefixColumns(df, prefix)

      prefDf.columns.length shouldBe df.columns.length
      prefDf.count shouldBe df.count
      prefDf.columns.zip(df.columns).foreach { case (prefNm, nm) =>
        prefNm shouldBe prefix + nm
      }
    }

    check(dataFrames.test1Df)
    check(dataFrames.test2Df)
    check(dataFrames.test3Df)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }

  "partitionsLength" should "correctly return all partitions with length of DataFrame" in {
    def check(df: DataFrame, partitions: Int): Unit = {
      val parts = Transformer.partitionsLength(df)

      parts.length shouldBe partitions
    }

    check(dataFrames.test1Df.repartition(1), 1)
    check(dataFrames.test2Df.repartition(2), 2)
    check(dataFrames.test3Df.repartition(3), 3)
    check(dataFrames.integersDf.repartition(10), 10)
    check(dataFrames.numbersDf.repartition(20), 20)
    check(dataFrames.decimalDf.repartition(30), 30)
    check(dataFrames.stringsDf.repartition(40), 40)
    check(dataFrames.dateTimeDf.repartition(50), 50)
    check(dataFrames.randomDf.repartition(100), 100)
  }

  "addPartitionColumn" should "correctly add column with partition index to DataFrame" in {
    def check(df: DataFrame, partitions: Int)(implicit spark: SparkSession): Unit = {
      import spark.implicits._

      val partsDf = Transformer.addPartitionColumn(df)
      val partitionsList = List.range(0, partitions)

      partsDf.columns.length shouldBe df.columns.length + 1
      partsDf.count shouldBe df.count
      partsDf.select(col(partsDf.columns.last).as[Int]).collect.foreach { i =>
        partitionsList.contains(i) shouldBe true
      }
    }

    check(dataFrames.test1Df.repartition(1), 1)
    check(dataFrames.test2Df.repartition(2), 2)
    check(dataFrames.test3Df.repartition(3), 3)
    check(dataFrames.integersDf.repartition(10), 10)
    check(dataFrames.numbersDf.repartition(20), 20)
    check(dataFrames.decimalDf.repartition(30), 30)
    check(dataFrames.stringsDf.repartition(40), 40)
    check(dataFrames.dateTimeDf.repartition(50), 50)
    check(dataFrames.randomDf.repartition(100), 100)
  }

  "numericColumns" should "convert all columns in DataFrame to StructType with struct (Key, Column)" in {
    def check(df: DataFrame): Unit = {
      val numDf = Transformer.numericColumns(df)

      numDf.columns.length shouldBe df.columns.length
      numDf.count shouldBe df.count
      numDf.schema.forall {
        case StructField(_, StructType(arr), _, _) => arr.toList match {
          case StructField(_, StructType(_), _, _) :: _ => true
          case _ => false
        }
        case _ => false
      } shouldBe true
    }

    check(dataFrames.test1Df)
    check(dataFrames.test2Df)
    check(dataFrames.test3Df)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }

  "arrayColumn" should "convert all columns in DataFrame to ArrayType" in {
    def check(df: DataFrame): Unit = {
      val arrDf = Transformer.arrayColumn(df)

      arrDf.columns.length shouldBe 1
      arrDf.count shouldBe df.count
      arrDf.schema.forall {
        case StructField(_, ArrayType(_, _), _, _) => true
        case _ => false
      } shouldBe true
    }

    check(Transformer.stringColumns(dataFrames.test1Df))
    check(Transformer.stringColumns(dataFrames.test2Df))
    check(Transformer.stringColumns(dataFrames.test3Df))
    check(Transformer.stringColumns(dataFrames.integersDf))
    check(Transformer.stringColumns(dataFrames.numbersDf))
    check(Transformer.stringColumns(dataFrames.decimalDf))
    check(Transformer.stringColumns(dataFrames.stringsDf))
    check(Transformer.stringColumns(dataFrames.dateTimeDf))
    check(Transformer.stringColumns(dataFrames.randomDf))
  }

  "explodeColumn" should "convert single column in DataFrame from ArrayType to column type" in {
    def check(df: DataFrame): Unit = {
      val numElem: Option[Int] = Try(df.first.getList(0).size).toOption
      val expDf = Transformer.explodeColumn(df)

      expDf.columns.length shouldBe 1
      numElem.forall(_ * df.count == expDf.count) shouldBe true
      expDf.schema.forall {
        case StructField(_, ArrayType(_, _), _, _) => false
        case StructField(_, _, _, _) => true
        case _ => false
      }
    }

    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.test1Df)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.test2Df)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.test3Df)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.integersDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.numbersDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.decimalDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.stringsDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.dateTimeDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.randomDf)))
  }

  "splitStructColumn" should "extract all sub-columns in struct column" in {
    def check(df: DataFrame): Unit = {
      def levelStructType(schema: StructType): Int = schema.map {
        case StructField(_, xs: StructType, _, _) => 1 + levelStructType(xs)
        case _ => 0
      }.max

      val structDf: DataFrame = Transformer.explodeColumn(
        Transformer.arrayColumn(
          Transformer.numericColumns(
            Transformer.stringColumns(df)
          )
        )
      )
      val splitDf: DataFrame = Transformer.splitStructColumn(structDf)
      val expectedColumnsNum: Int = structDf.schema.map { field =>
        field.dataType match {
          case StructType(arr) => arr.length
          case _ => 1
        }
      }.sum

      splitDf.columns.length shouldBe expectedColumnsNum
      levelStructType(splitDf.schema) shouldBe levelStructType(structDf.schema) - 1
      splitDf.count shouldBe structDf.count
    }

    check(dataFrames.test1Df)
    check(dataFrames.test2Df)
    check(dataFrames.test3Df)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }

  "convertStructColumn" should "concat all sub-columns in struct column into string column" in {
    def check(df: DataFrame): Unit = {
      def findStructType(field: StructField): Boolean = field.dataType match {
        case StructType(_) => true
        case _ => false
      }

      val structDf: DataFrame = Transformer.explodeColumn(
        Transformer.arrayColumn(
          Transformer.numericColumns(
            Transformer.stringColumns(df)
          )
        )
      )
      val convDf: DataFrame = Transformer.convertStructColumn(structDf)

      convDf.columns.length shouldBe structDf.columns.length
      convDf.schema.exists(findStructType) shouldBe false
      convDf.count shouldBe structDf.count
    }

    check(dataFrames.test1Df)
    check(dataFrames.test2Df)
    check(dataFrames.test3Df)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }

  "keyValueColumns" should "convert all data in DataFrame to (key, value)" in {
    def check(df: DataFrame): Unit = {
      def findKeyField(field: StructField): Boolean =
        field.name == "key" && field.dataType == StringType

      def findValueField(field: StructField): Boolean =
        field.name == "val" && field.dataType == StringType

      val keyValDf = Transformer.keyValueColumns(df)

      keyValDf.columns.length shouldBe 2
      keyValDf.schema.exists(findKeyField) shouldBe true
      keyValDf.schema.exists(findValueField) shouldBe true
      keyValDf.count shouldBe df.count * df.columns.length
    }

    check(dataFrames.test1Df)
    check(dataFrames.test2Df)
    check(dataFrames.test3Df)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }

  "metaColumns" should "convert all data in DataFrame to meta data in (key, value)" in {
    def check(df: DataFrame): Unit = {
      def findKeyField(field: StructField): Boolean =
        field.name == "key" && field.dataType == StringType

      def findValueField(field: StructField): Boolean =
        field.name == "val" && field.dataType == StringType

      val metaDf = Transformer.metaColumns(df, "test_name")

      println(metaDf.schema.treeString)
      println(metaDf.columns.mkString("Array(", ", ", ")"))
      println(metaDf.count)
      println(metaDf.show(20, truncate = false))

      metaDf.columns.length shouldBe 2
      metaDf.schema.exists(findKeyField) shouldBe true
      metaDf.schema.exists(findValueField) shouldBe true
      metaDf.count shouldBe df.columns.length + 1
    }

    check(dataFrames.test1Df)
    check(dataFrames.test2Df)
    check(dataFrames.test3Df)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }
}
