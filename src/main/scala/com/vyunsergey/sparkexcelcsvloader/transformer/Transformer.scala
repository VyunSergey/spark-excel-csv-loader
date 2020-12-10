package com.vyunsergey.sparkexcelcsvloader.transformer

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, col, explode_outer, lit, row_number, struct}
import org.apache.spark.sql.types.{ArrayType, DataType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Transformer {
  def keyValueColumns(df: DataFrame)
                     (implicit spark: SparkSession, logger: Logger): DataFrame = {
    val strDf = stringColumns(df)
    val numDf = numericColumns(strDf)
    val arrDf = arrayColumn(numDf)
    val expDf = explodeColumn(arrDf)
    val splDf = splitStructColumn(expDf)

    logger.info(s"Transform Key-Value DataFrame with schema:\n${df.schema.treeString}\n" +
      s"to DataFrame with schema:\n${splDf.schema.treeString}")

    splDf
  }

  def stringColumns(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val columns: Array[String] = df.columns
    val types: Map[String, DataType] = df.schema.map(field => field.name -> field.dataType).toMap

    df.select(
      columns.map { colNm =>
        if (types.getOrElse(colNm, StringType) == StringType) col(colNm).as(colNm)
        else col(colNm).cast(StringType).as(colNm)
      }: _*
    )
  }

  def numericColumns(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val columns: Array[String] = df.columns
    val keyColumnNm = "id"
    val numColumnNm = "num"
    val valColumnNm = "val"
    val structColumnNm = "key"

    df.select(
        columns.map(nm => col(nm).as(s"__$nm")): _*
      )
      .withColumn(keyColumnNm, row_number().over(Window.orderBy(lit(1))).cast(LongType))
      .select(
        columns.zipWithIndex.map { case (colNm, i) =>
          struct(
            struct(col(keyColumnNm), lit(i + 1).cast(LongType).as(numColumnNm)).as(structColumnNm),
            col(s"__$colNm").as(valColumnNm)
          ).as(s"${structColumnNm}_$colNm")
        }: _*
      )
  }

  def arrayColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val columns: Array[String] = df.columns
    val arrColumnNm = "arr"

    df.select(
      array(columns.map(col): _*).as(arrColumnNm)
    )
  }

  def explodeColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val arrColumn: Column = df.schema.find { field =>
      field.dataType match {
        case ArrayType(_, _) => true
        case _ => false
      }
    }.map(field => col(field.name)).get

    val explodeColNm = "explode"

    df.select(
      explode_outer(arrColumn).as(explodeColNm)
    )
  }

  def splitStructColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    def structColumnSelector(field: StructField): Array[Column] = field match {
      case StructField(nm, StructType(arr), _, _) => arr.map(f => col(s"$nm.${f.name}"))
      case StructField(nm, _, _, _) => Array(col(nm))
    }

    df.select(
      df.schema.flatMap(structColumnSelector): _*
    )
  }

}
