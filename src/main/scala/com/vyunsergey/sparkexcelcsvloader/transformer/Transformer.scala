package com.vyunsergey.sparkexcelcsvloader.transformer

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{array, col, concat_ws, explode_outer, lit, regexp_replace, row_number, struct}
import org.apache.spark.sql.types.{ArrayType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object Transformer {
  /**
   * Convert DataFrame to Key-Value Metadata DataFrame with numerated column metadata (`name`, `type`)
   * by (`id`, `num`) pair where `id` - row number and `num` - column number.
   *
   * == Example ==
   *
   * {{{
   *   import spark.implicits._
   *
   *   case class Person(name: String, age: Int, gender: String)
   *
   *   val data = Seq(
   *     Person("Michael", 29, "M"),
   *     Person("Sara", 30, "F"),
   *     Person("Justin", 19, "M")
   *   )
   *
   *   val ds = spark.createDataset(data)
   *
   *   ds.show(false)
   *   // +-------+---+------+
   *   // |name   |age|gender|
   *   // +-------+---+------+
   *   // |Michael|29 |M     |
   *   // |Sara   |30 |F     |
   *   // |Justin |19 |M     |
   *   // +-------+---+------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *
   *   val metaDf = Transformer.metaColumns(ds.toDF, "Person")
   *
   *   metaDf.show(false)
   *   // +---+---------------+
   *   // |key|            val|
   *   // +---+---------------+
   *   // |0,0|         Person|
   *   // |0,1|  [name,string]|
   *   // |0,2|  [age,integer]|
   *   // |0,3|[gender,string]|
   *   // +---+---------------+
   *
   *   metaDf.printSchema
   *   // root
   *   //  |-- key: string (nullable = true)
   *   //  |-- val: string (nullable = true)
   * }}}
   */
  def metaColumns(df: DataFrame, name: String)
                 (implicit spark: SparkSession, logger: Logger): DataFrame = {
    import spark.implicits._

    val metaNameColumnNm = "name"
    val metaTypeColumnNm = "type"
    val metaColumnNm = "meta"
    val sep = ","

    val typesDf = Seq(1).toDF.select(
      df.schema.map { case StructField(colNm, colTp, _, _) =>
        struct(lit(colNm).as(metaNameColumnNm), lit(colTp.typeName).as(metaTypeColumnNm)).as(s"${metaColumnNm}_$colNm")
      }: _*
    )
    val kvDf = keyValueColumns(typesDf)

    val metaDf = kvDf.select(
      regexp_replace(col("key"), s".*$sep", s"0$sep").as("key"),
      col("val")
    ).union(
      Seq(1).toDF.select(
        lit("0,0").as("key"),
        lit(name).as("val")
      )
    ).orderBy(
      regexp_replace(col("key"), "\\D", "").cast(LongType).asc
    )

    logger.info(s"Transform Metadata Key-Value DataFrame with schema:\n${df.schema.treeString}\n" +
      s"to DataFrame with schema:\n${metaDf.schema.treeString}")

    metaDf
  }

  /**
   * Convert DataFrame to Key-Value DataFrame with numerated cells by (`id`, `num`) pair
   * where `id` - row number and `num` - column number.
   *
   * == Example ==
   *
   * {{{
   *   import spark.implicits._
   *
   *   case class Person(name: String, age: Int, gender: String)
   *
   *   val data = Seq(
   *     Person("Michael", 29, "M"),
   *     Person("Sara", 30, "F"),
   *     Person("Justin", 19, "M")
   *   )
   *
   *   val ds = spark.createDataset(data)
   *
   *   ds.show(false)
   *   // +-------+---+------+
   *   // |name   |age|gender|
   *   // +-------+---+------+
   *   // |Michael|29 |M     |
   *   // |Sara   |30 |F     |
   *   // |Justin |19 |M     |
   *   // +-------+---+------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *
   *   val kvDf = Transformer.keyValueColumns(ds.toDF)
   *
   *   kvDf.show(false)
   *   // +---+-------+
   *   // |key|    val|
   *   // +---+-------+
   *   // |1,1|Michael|
   *   // |1,2|     29|
   *   // |1,3|      M|
   *   // |2,1|   Sara|
   *   // |2,2|     30|
   *   // |2,3|      F|
   *   // |3,1| Justin|
   *   // |3,2|     19|
   *   // |3,3|      M|
   *   // +---+-------+
   *
   *   kvDf.printSchema
   *   // root
   *   //  |-- key: string (nullable = true)
   *   //  |-- val: string (nullable = true)
   * }}}
   */
  def keyValueColumns(df: DataFrame)
                     (implicit spark: SparkSession, logger: Logger): DataFrame = {
    val strDf = stringColumns(df)
    val numDf = numericColumns(strDf)
    val arrDf = arrayColumn(numDf)
    val expDf = explodeColumn(arrDf)
    val splDf = splitStructColumn(expDf)
    val convDf = convertStructColumn(splDf)

    logger.info(s"Transform Key-Value DataFrame with schema:\n${df.schema.treeString}\n" +
      s"to DataFrame with schema:\n${convDf.schema.treeString}")

    convDf
  }

  /**
   * Convert all columns to StringType.
   *
   * == Example ==
   *
   * {{{
   *   import spark.implicits._
   *
   *   case class Person(name: String, age: Int, gender: String)
   *
   *   val data = Seq(
   *     Person("Michael", 29, "M"),
   *     Person("Sara", 30, "F"),
   *     Person("Justin", 19, "M")
   *   )
   *
   *   val ds = spark.createDataset(data)
   *
   *   ds.show(false)
   *   // +-------+---+------+
   *   // |name   |age|gender|
   *   // +-------+---+------+
   *   // |Michael|29 |M     |
   *   // |Sara   |30 |F     |
   *   // |Justin |19 |M     |
   *   // +-------+---+------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *
   *   val strDf = Transformer.stringColumns(ds.toDF)
   *
   *   strDf.show(false)
   *   // +-------+---+------+
   *   // |   name|age|gender|
   *   // +-------+---+------+
   *   // |Michael| 29|     M|
   *   // |   Sara| 30|     F|
   *   // | Justin| 19|     M|
   *   // +-------+---+------+
   *
   *   strDf.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: string (nullable = false)
   *   //  |-- gender: string (nullable = true)
   * }}}
   */
  def stringColumns(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    df.select(
      df.schema.map { case StructField(colNm, colTp, _, _) =>
        if (colTp == StringType) col(colNm).as(colNm)
        else col(colNm).cast(StringType).as(colNm)
      }: _*
    )
  }

  /**
   * Numerate all cells of all columns with (`id`, `num`) pair where `id` - row number and `num` - column number.
   *
   * == Example ==
   *
   * {{{
   *   import spark.implicits._
   *
   *   case class Person(name: String, age: Int, gender: String)
   *
   *   val data = Seq(
   *     Person("Michael", 29, "M"),
   *     Person("Sara", 30, "F"),
   *     Person("Justin", 19, "M")
   *   )
   *
   *   val ds = spark.createDataset(data)
   *
   *   ds.show(false)
   *   // +-------+---+------+
   *   // |name   |age|gender|
   *   // +-------+---+------+
   *   // |Michael|29 |M     |
   *   // |Sara   |30 |F     |
   *   // |Justin |19 |M     |
   *   // +-------+---+------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *
   *   val numDf = Transformer.numericColumns(ds.toDF)
   *
   *   numDf.show()
   *   // +---------------+----------+----------+
   *   // |       key_name|   key_age|key_gender|
   *   // +---------------+----------+----------+
   *   // |[[1,1],Michael]|[[1,2],29]| [[1,3],M]|
   *   // |   [[2,1],Sara]|[[2,2],30]| [[2,3],F]|
   *   // | [[3,1],Justin]|[[3,2],19]| [[3,3],M]|
   *   // +---------------+----------+----------+
   *
   *   numDf.printSchema
   *   // root
   *   //  |-- key_name: struct (nullable = false)
   *   //  |    |-- key: struct (nullable = false)
   *   //  |    |    |-- id: long (nullable = true)
   *   //  |    |    |-- num: long (nullable = false)
   *   //  |    |-- val: string (nullable = true)
   *   //  |-- key_age: struct (nullable = false)
   *   //  |    |-- key: struct (nullable = false)
   *   //  |    |    |-- id: long (nullable = true)
   *   //  |    |    |-- num: long (nullable = false)
   *   //  |    |-- val: integer (nullable = false)
   *   //  |-- key_gender: struct (nullable = false)
   *   //  |    |-- key: struct (nullable = false)
   *   //  |    |    |-- id: long (nullable = true)
   *   //  |    |    |-- num: long (nullable = false)
   *   //  |    |-- val: string (nullable = true)
   *  }}}
   */
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

  /**
   * Creates a new array of all columns. The input columns must all have the same data type.
   *
   * == Example ==
   *
   * {{{
   *   import spark.implicits._
   *
   *   case class Person(name: String, age: String, gender: String)
   *
   *   val data = Seq(
   *     Person("Michael", "29", "M"),
   *     Person("Sara", "30", "F"),
   *     Person("Justin", "19", "M")
   *   )
   *
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+---+------+
   *   // |name   |age|gender|
   *   // +-------+---+------+
   *   // |Michael|29 |M     |
   *   // |Sara   |30 |F     |
   *   // |Justin |19 |M     |
   *   // +-------+---+------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: string (nullable = true)
   *   //  |-- gender: string (nullable = true)
   *
   *   val arrDf = Transformer.arrayColumn(ds.toDF)
   *
   *   arrDf.show()
   *   // +----------------+
   *   // |             arr|
   *   // +----------------+
   *   // |[Michael, 29, M]|
   *   // |   [Sara, 30, F]|
   *   // | [Justin, 19, M]|
   *   // +----------------+
   *
   *   arrDf.printSchema
   *   // root
   *   //  |-- arr: array (nullable = false)
   *   //  |    |-- element: string (containsNull = true)
   * }}}
   */
  def arrayColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val arrColumnNm = "arr"

    df.select(
      array(df.columns.map(col): _*).as(arrColumnNm)
    )
  }

  /**
   * Creates a new row for each element in the given column of ArrayType.
   *
   * == Example ==
   *
   * {{{
   *   import spark.implicits._
   *
   *   case class Person(name: String, age: Int, gender: String)
   *   case class Persons(persons: Array[Person])
   *
   *   val data = Seq(
   *     Persons(Array(
   *       Person("Michael", 29, "M"),
   *       Person("Sara", 30, "F"),
   *       Person("Justin", 19, "M")
   *     ))
   *   )
   *
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +--------------------------------------------+
   *   // |persons                                     |
   *   // +--------------------------------------------+
   *   // |[[Michael,29,M], [Sara,30,F], [Justin,19,M]]|
   *   // +--------------------------------------------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- persons: array (nullable = true)
   *   //  |    |-- element: struct (containsNull = true)
   *   //  |    |    |-- name: string (nullable = true)
   *   //  |    |    |-- age: integer (nullable = false)
   *   //  |    |    |-- gender: string (nullable = true)
   *
   *   val expDf = Transformer.explodeColumn(ds.toDF)
   *
   *   expDf.show()
   *   // +--------------+
   *   // |       explode|
   *   // +--------------+
   *   // |[Michael,29,M]|
   *   // |   [Sara,30,F]|
   *   // | [Justin,19,M]|
   *   // +--------------+
   *
   *   expDf.printSchema
   *   // root
   *   //  |-- explode: struct (nullable = true)
   *   //  |    |-- name: string (nullable = true)
   *   //  |    |-- age: integer (nullable = false)
   *   //  |    |-- gender: string (nullable = true)
   * }}}
   */
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

  /**
   * Split all inner columns of StructType into separated columns of inner types.
   *
   * == Example ==
   *
   * {{{
   *   import spark.implicits._
   *
   *   case class PersonInfo(age: Int, gender: String)
   *   case class Person(name: String, info: PersonInfo)
   *
   *   val data = Seq(
   *     Person("Michael", PersonInfo(29, "M")),
   *     Person("Sara", PersonInfo(30, "F")),
   *     Person("Justin", PersonInfo(19, "M"))
   *   )
   *
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+------+
   *   // |   name|  info|
   *   // +-------+------+
   *   // |Michael|[29,M]|
   *   // |   Sara|[30,F]|
   *   // | Justin|[19,M]|
   *   // +-------+------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- info: struct (nullable = true)
   *   //  |    |-- age: integer (nullable = false)
   *   //  |    |-- gender: string (nullable = true)
   *
   *   val splitDf = Transformer.splitStructColumn(ds.toDF)
   *
   *   splitDf.show()
   *   // +-------+---+------+
   *   // |   name|age|gender|
   *   // +-------+---+------+
   *   // |Michael| 29|     M|
   *   // |   Sara| 30|     F|
   *   // | Justin| 19|     M|
   *   // +-------+---+------+
   *
   *   splitDf.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = true)
   *   //  |-- gender: string (nullable = true)
   * }}}
   */
  def splitStructColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    def structColumnSelector(field: StructField): Array[Column] = field match {
      case StructField(nm, StructType(arr), _, _) => arr.map(f => col(s"$nm.${f.name}"))
      case StructField(nm, _, _, _) => Array(col(nm))
    }

    df.select(
      df.schema.flatMap(structColumnSelector): _*
    )
  }

  /**
   * Concat with `","` all inner columns of StructType into one column of StringType.
   *
   * == Example ==
   *
   * {{{
   *   import spark.implicits._
   *
   *   case class PersonInfo(age: Int, gender: String)
   *   case class Person(name: String, info: PersonInfo)
   *
   *   val data = Seq(
   *     Person("Michael", PersonInfo(29, "M")),
   *     Person("Sara", PersonInfo(30, "F")),
   *     Person("Justin", PersonInfo(19, "M"))
   *   )
   *
   *   val ds = spark.createDataset(data)
   *
   *   ds.show()
   *   // +-------+------+
   *   // |   name|  info|
   *   // +-------+------+
   *   // |Michael|[29,M]|
   *   // |   Sara|[30,F]|
   *   // | Justin|[19,M]|
   *   // +-------+------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- info: struct (nullable = true)
   *   //  |    |-- age: integer (nullable = false)
   *   //  |    |-- gender: string (nullable = true)
   *
   *   val convDf = Transformer.convertStructColumn(ds.toDF)
   *
   *   convDf.show()
   *   // +-------+----+
   *   // |   name|info|
   *   // +-------+----+
   *   // |Michael|29,M|
   *   // |   Sara|30,F|
   *   // | Justin|19,M|
   *   // +-------+----+
   *
   *   convDf.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- info: string (nullable = true)
   * }}}
   */
  def convertStructColumn(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    def structColumnConvertor(field: StructField): Array[Column] = field match {
      case StructField(nm, StructType(arr), _, _) => Array(concat_ws(",", arr.map { f =>
        col(s"$nm.${f.name}").cast(StringType)
      }: _*).as(nm))
      case StructField(nm, _, _, _) => Array(col(nm))
    }

    df.select(
      df.schema.flatMap(structColumnConvertor): _*
    )
  }
}
