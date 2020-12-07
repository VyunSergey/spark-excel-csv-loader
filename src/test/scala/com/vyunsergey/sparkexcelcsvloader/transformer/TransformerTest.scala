package com.vyunsergey.sparkexcelcsvloader.transformer

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.data.TestDataFrame
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

class TransformerTest extends AnyFlatSpec with Matchers {
  lazy val conf: Configuration.AppConf = Configuration.config
  lazy val sparkConf: SparkConfig = SparkConfig.make(conf)
  implicit lazy val spark: SparkSession = SparkConnection.make("App", sparkConf)
  val dataFrames: TestDataFrame = TestDataFrame(spark)

  "stringColumns" should "convert all columns in DataFrame to StringType" in {
    def check(df: DataFrame): Unit = {
      val strDf = Transformer.stringColumns(df)

      strDf.columns shouldBe df.columns
      strDf.count shouldBe df.count
      strDf.schema.map(_.dataType).foreach { tp =>
        tp shouldBe StringType
      }
    }

    check(dataFrames.simpleDf)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
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

    check(dataFrames.simpleDf)
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

    check(Transformer.stringColumns(dataFrames.simpleDf))
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

    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.simpleDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.integersDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.numbersDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.decimalDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.stringsDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.dateTimeDf)))
    check(Transformer.arrayColumn(Transformer.stringColumns(dataFrames.randomDf)))
  }

  "splitStructColumn" should "extract all sub-columns in struct column" in {
    def check(df: DataFrame): Unit = {
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
      splitDf.count shouldBe structDf.count
    }

    check(dataFrames.simpleDf)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }

  "keyValueColumns" should "convert all data in DataFrame to (key, value)" in {
    def check(df: DataFrame): Unit = {
      def findKeyField(field: StructField): Boolean = field.dataType match {
        case StructType(arr) => arr.toList match {
          case StructField(_, LongType, _, _) :: StructField(_, LongType, _, _) :: Nil => true
          case _ => false
        }
        case _ => false
      }

      def findValueField(field: StructField): Boolean = field.dataType match {
        case StringType => true
        case _ => false
      }

      val keyValDf = Transformer.keyValueColumns(df)

      keyValDf.columns.length shouldBe 2
      keyValDf.schema.exists(findKeyField) shouldBe true
      keyValDf.schema.exists(findValueField) shouldBe true
      keyValDf.count shouldBe df.count * df.columns.length
    }

    check(dataFrames.simpleDf)
    check(dataFrames.integersDf)
    check(dataFrames.numbersDf)
    check(dataFrames.decimalDf)
    check(dataFrames.stringsDf)
    check(dataFrames.dateTimeDf)
    check(dataFrames.randomDf)
  }
}
