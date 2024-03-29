package com.vyunsergey.sparkexcelcsvloader.transformer

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.data.TestDataFrame
import com.vyunsergey.sparkexcelcsvloader.reader.ReaderConfig
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.{col, lit, length => len, regexp_extract, regexp_replace}
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

  "clearColumns" should "regexp replace all columns with StringType of DataFrame" in {
    def check(df: DataFrame, regexp: String, replacement: String, verbose: Boolean = false): Unit = {
      val clearedDf = Transformer.clearColumns(df, regexp, replacement)

      if (verbose) println(clearedDf.schema.treeString)
      if (verbose) println(clearedDf.columns.mkString("Array(", ", ", ")"))
      if (verbose) println(clearedDf.count)
      if (verbose) clearedDf.show(20, truncate = false)

      clearedDf.columns.length shouldBe df.columns.length
      clearedDf.count shouldBe df.count
      clearedDf.filter(
        clearedDf.schema
          .filter(_.dataType == StringType)
          .map { case StructField(nm, _, _, _) =>
            len(regexp_extract(col(nm), regexp, 1)) > 0
          }.foldLeft(lit(false))(_ || _)
      ).count shouldBe 0
    }

    check(dataFrames.test1Df, "\\W+", "")
    check(dataFrames.test2Df, "\\d+", "")
    check(dataFrames.test3Df, "\\d+", "")
    check(dataFrames.integersDf, "\\d+", "")
    check(dataFrames.numbersDf, "\\d+", "")
    check(dataFrames.decimalDf, "\\d+", "")
    check(dataFrames.stringsDf, "\\w+", "")
    check(dataFrames.dateTimeDf, "\\d+", "")
    check(dataFrames.randomDf, "\\W+", "")
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
    def check(df: DataFrame, verbose: Boolean = false): Unit = {
      def findKeyField(field: StructField): Boolean =
        field.name == "key" && field.dataType == StringType

      def findValueField(field: StructField): Boolean =
        field.name == "val" && field.dataType == StringType

      val keyValDf = Transformer.keyValueColumns(df)

      if (verbose) println(keyValDf.schema.treeString)
      if (verbose) println(keyValDf.columns.mkString("Array(", ", ", ")"))
      if (verbose) println(keyValDf.count)
      if (verbose) keyValDf.show(20, truncate = false)

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
    def check(df: DataFrame, verbose: Boolean = false): Unit = {
      def findKeyField(field: StructField): Boolean =
        field.name == "key" && field.dataType == StringType

      def findValueField(field: StructField): Boolean =
        field.name == "val" && field.dataType == StringType

      val metaDf = Transformer.metaColumns(df, "test_name")

      if (verbose) println(metaDf.schema.treeString)
      if (verbose) println(metaDf.columns.mkString("Array(", ", ", ")"))
      if (verbose) println(metaDf.count)
      if (verbose) metaDf.show(20, truncate = false)

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

  "schemaName" should "convert all data in MetaData Key-Value DataFrame to (schema, name)" in {
    def check(df: DataFrame, verbose: Boolean = false): Unit = {
      val name = "test_name"
      val metaDf = Transformer.metaColumns(df, name)
      val (schema, nameOp) = Transformer.schemaName(Transformer.idNumColumns(metaDf))

      if (verbose) println(nameOp)
      if (verbose) println(schema.treeString)
      if (verbose) println(schema.map(field => (field.name, field.dataType)).mkString("Array(", ", ", ")"))
      if (verbose) println(schema.length)

      nameOp shouldBe Some(name)
      schema.map(f => (f.name, f.dataType)).sortBy(_._1) shouldBe df.schema.map(f => (f.name, f.dataType)).sortBy(_._1)
      schema shouldBe df.schema
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

  "plainColumns" should "convert all data in Key-Value DataFrame to plain DataFrame with specified schema" in {
    def check(df: DataFrame, verbose: Boolean = false): Unit = {
      val name = "test_name"
      val metaDf = Transformer.metaColumns(df, name)
      val keyValDf = Transformer.keyValueColumns(df)
      val (schema, nameOp) = Transformer.schemaName(Transformer.idNumColumns(metaDf))
      val plainDf = Transformer.plainColumns(Transformer.idNumColumns(keyValDf), schema)

      if (verbose) println(plainDf.schema.treeString)
      if (verbose) println(plainDf.columns.mkString("Array(", ", ", ")"))
      if (verbose) println(plainDf.count)
      if (verbose) plainDf.show(20, truncate = false)

      plainDf.columns.length shouldBe df.columns.length
      plainDf.columns.sorted shouldBe df.columns.sorted
      plainDf.schema shouldBe df.schema
      plainDf.count shouldBe df.count
      nameOp shouldBe Some(name)
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

  "plainColumns" should "convert all data in .Zip files to plain DataFrame with specified schema" in {
    def check(keyValDf: DataFrame, verbose: Boolean = false): Unit = {
      val (schema, nameOp) = Transformer.schemaName(Transformer.idNumColumns(keyValDf))
      val plainDf = Transformer.plainColumns(Transformer.idNumColumns(keyValDf), schema)

      if (verbose) println(plainDf.schema.treeString)
      if (verbose) println(plainDf.columns.mkString("Array(", ", ", ")"))
      if (verbose) println(plainDf.count)
      if (verbose) plainDf.show(20, truncate = false)

      plainDf.columns.length shouldBe schema.length
      plainDf.columns.sorted shouldBe schema.map(_.name).toArray.sorted
      plainDf.schema shouldBe schema
      plainDf.count shouldBe (keyValDf.count - 1 - schema.length) / schema.length
      nameOp.isDefined shouldBe true
    }

    check(dataFrames.test2ZipDf)
    check(dataFrames.test3ZipDf)
  }
}
