package com.vyunsergey.sparkexcelcsvloader.transformer

import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}

import scala.util.Try

object Transformer {
  /**
   * Convert DataFrame to Key-Value Metadata DataFrame with numerated column metadata as JSON
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
   *   val df = spark.createDataset(data).toDF
   *   df.show(false)
   *   // +-------+---+------+
   *   // |name   |age|gender|
   *   // +-------+---+------+
   *   // |Michael|29 |M     |
   *   // |Sara   |30 |F     |
   *   // |Justin |19 |M     |
   *   // +-------+---+------+
   *
   *   df.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *
   *   val metaDf = Transformer.metaColumns(df.toDF, "Person")
   *
   *   metaDf.show(false)
   *   // +---+--------------------------------------------------------------------------------------------+
   *   // |key|val                                                                                         |
   *   // +---+--------------------------------------------------------------------------------------------+
   *   // |0,0|Person                                                                                      |
   *   // |0,1|{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}  |
   *   // |0,2|{"type":"struct","fields":[{"name":"age","type":"integer","nullable":false,"metadata":{}}]} |
   *   // |0,3|{"type":"struct","fields":[{"name":"gender","type":"string","nullable":true,"metadata":{}}]}|
   *   // +---+--------------------------------------------------------------------------------------------+
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

    val metaColumnNm = "meta"
    val sep = ","

    val typesDf = Seq(1).toDF.select(
      df.schema.map { field =>
        lit(StructType(field :: Nil).json).as(s"${metaColumnNm}_${field.name}")
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
    ).repartition(1)
      .sortWithinPartitions(
        regexp_replace(col("key"), "\\D", "").cast(LongType).asc
      )

    logger.info(s"Transform DataFrame with schema:\n${df.schema.treeString}\n" +
      s"to Metadata Key-Value DataFrame with schema:\n${metaDf.schema.treeString}")

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

    logger.info(s"Transform DataFrame with schema:\n${df.schema.treeString}\n" +
      s"to Key-Value DataFrame with schema:\n${convDf.schema.treeString}")

    convDf
  }

  /**
   * Convert Key-Value DataFrame to Plain DataFrame
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
   *   val df = spark.createDataset(data).toDF
   *   df.show(false)
   *   // +-------+---+------+
   *   // |name   |age|gender|
   *   // +-------+---+------+
   *   // |Michael|29 |M     |
   *   // |Sara   |30 |F     |
   *   // |Justin |19 |M     |
   *   // +-------+---+------+
   *
   *   df.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *
   *   val kvDf = Transformer.keyValueColumns(df)
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
   *
   *   val metaDf = Transformer.metaColumns(df, "test")
   *   metaDf.show(false)
   *   // +---+--------------------------------------------------------------------------------------------+
   *   // |key|val                                                                                         |
   *   // +---+--------------------------------------------------------------------------------------------+
   *   // |0,0|test                                                                                        |
   *   // |0,1|{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}  |
   *   // |0,2|{"type":"struct","fields":[{"name":"age","type":"integer","nullable":false,"metadata":{}}]} |
   *   // |0,3|{"type":"struct","fields":[{"name":"gender","type":"string","nullable":true,"metadata":{}}]}|
   *   // +---+--------------------------------------------------------------------------------------------+
   *
   *   metaDf.printSchema
   *   // root
   *   //  |-- key: string (nullable = false)
   *   //  |-- val: string (nullable = true)
   *
   *   val (schema, nameOp) = Transformer.schemaName(Transformer.idNumColumns(metaDf))
   *   println(nameOp)
   *   // Some(test)
   *
   *   println(schema.treeString)
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *
   *   val plainDf = Transformer.plainColumns(Transformer.idNumColumns(kvDf), schema)
   *
   *   plainDf.show(false)
   *   // +-------+---+------+
   *   // |name   |age|gender|
   *   // +-------+---+------+
   *   // |Justin |19 |M     |
   *   // |Sara   |30 |F     |
   *   // |Michael|29 |M     |
   *   // +-------+---+------+
   *
   *   plainDf.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   * }}}
   */
  def plainColumns(df: DataFrame, schema: StructType)
                  (implicit spark: SparkSession, logger: Logger): DataFrame = {
    import spark.implicits._

    val idColumn = "id"
    val numColumn = "num"
    val nameColumn = "name"
    val valColumn = "val"
    val arrColumn = "arr"

    val dataDf = df.filter(col(idColumn) > 0)
    val groupDF = dataDf.groupBy(idColumn)
      .agg(
        collect_list(
          struct(col(numColumn), col(valColumn)).as(s"${numColumn}_$valColumn")
        ).as(arrColumn)
      )

    val structDf = arrayToStruct(groupDF, schema.length)
    val splitDf = Transformer.splitStructColumn(structDf)

    def numSelector(field: StructField): Array[Column] = field match {
      case StructField(nm, StructType(Array(StructField(nm1, LongType, _, _), StructField(_, StringType, _, _))), _, _) =>
        Array(struct(lit(nm).as(nameColumn), col(s"$nm.$nm1")).as(s"${nm}_$nm1"))
      case _ => Array.empty[Column]
    }

    val mapping: Array[(String, Long)] = Transformer.arrayColumn(
      splitDf.filter(col(idColumn) === 1)
        .select(splitDf.schema.flatMap(numSelector): _*)
    ).select(col(arrColumn).as[Array[(String, Long)]])
      .collect
      .head

    val plainDf =
      spark.createDataFrame(
        splitDf.select(
          schema.zip(mapping.sortBy(_._2).map(_._1)).map { case (field, nm) =>
            col(s"$nm.$valColumn").cast(field.dataType).as(field.name)
          }: _*
        ).rdd, schema
      )

    logger.info(s"Transform Key-Value DataFrame with schema:\n${df.schema.treeString}\n" +
      s"to DataFrame with schema:\n${plainDf.schema.treeString}")

    plainDf
  }

  /**
   * Convert a `key` column into two columns: `id` and `num` in DataFrame.
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
   *   val df = Transformer.keyValueColumns(spark.createDataset(data).toDF)
   *
   *   df.show(false)
   *   // +---+-------+
   *   // |key|val    |
   *   // +---+-------+
   *   // |1,1|Michael|
   *   // |1,2|29     |
   *   // |1,3|M      |
   *   // |2,1|Sara   |
   *   // |2,2|30     |
   *   // |2,3|F      |
   *   // |3,1|Justin |
   *   // |3,2|19     |
   *   // |3,3|M      |
   *   // +---+-------+
   *
   *   df.printSchema
   *   // root
   *   //  |-- key: string (nullable = false)
   *   //  |-- val: string (nullable = true)
   *
   *   val idNumDf = Transformer.idNumColumns(df)
   *
   *   idNumDf.show(false)
   *   // +---+---+-------+
   *   // |id |num|val    |
   *   // +---+---+-------+
   *   // |1  |1  |Michael|
   *   // |1  |2  |29     |
   *   // |1  |3  |M      |
   *   // |2  |1  |Sara   |
   *   // |2  |2  |30     |
   *   // |2  |3  |F      |
   *   // |3  |1  |Justin |
   *   // |3  |2  |19     |
   *   // |3  |3  |M      |
   *   // +---+---+-------+
   *
   *   idNumDf.printSchema
   *   // root
   *   //  |-- id: long (nullable = true)
   *   //  |-- num: long (nullable = true)
   *   //  |-- val: string (nullable = true)
   * }}}
   */
  def idNumColumns(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val keyColumn = "key"
    val idColumn = "id"
    val numColumn = "num"

    df.select(
      Array(
        split(col(keyColumn), ",").getItem(0).cast(LongType).as(idColumn),
        split(col(keyColumn), ",").getItem(1).cast(LongType).as(numColumn)
      ) ++ df.columns.filterNot(_ == keyColumn).map(col): _*
    )
  }

  /**
   * Convert a DataFrame with columns: `id`, `num` and `val` into schema and name
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
   *   val df = Transformer.idNumColumns(Transformer.metaColumns(spark.createDataset(data).toDF, "test"))
   *
   *   df.show(false)
   *   // +---+---+--------------------------------------------------------------------------------------------+
   *   // |id |num|val                                                                                         |
   *   // +---+---+--------------------------------------------------------------------------------------------+
   *   // |0  |0  |test                                                                                        |
   *   // |0  |1  |{"type":"struct","fields":[{"name":"name","type":"string","nullable":true,"metadata":{}}]}  |
   *   // |0  |2  |{"type":"struct","fields":[{"name":"age","type":"integer","nullable":false,"metadata":{}}]} |
   *   // |0  |3  |{"type":"struct","fields":[{"name":"gender","type":"string","nullable":true,"metadata":{}}]}|
   *   // +---+---+--------------------------------------------------------------------------------------------+
   *
   *   df.printSchema
   *   // root
   *   //  |-- id: long (nullable = true)
   *   //  |-- num: long (nullable = true)
   *   //  |-- val: string (nullable = true)
   *
   *   val (schema, nameOp) = Transformer.schemaName(df)
   *
   *   println(nameOp)
   *   // Some(test)
   *
   *   println(schema.treeString)
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   * }}}
   */
  def schemaName(df: DataFrame)(implicit spark: SparkSession): (StructType, Option[String]) = {
    import spark.implicits._

    val idColumn = "id"
    val numColumn = "num"
    val valColumn = "val"

    val metaDf = df.filter(col(idColumn) === 0)

    val name = metaDf.filter(col(numColumn) === 0)
      .select(col(valColumn).as[String])
      .collect.headOption

    val schema = StructType(
      metaDf.filter(col(numColumn) > 0)
        .select(
          col(numColumn).as[Long],
          col(valColumn).as(valColumn).as[String]
        )
        .collect.sortBy(_._1).flatMap { case (_, value) =>
        DataType.fromJson(value) match {
          case StructType(arr) => arr
          case _ => Array.empty[StructField]
        }
      }
    )

    (schema, name)
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
   * Add a `prefix` to all columns of DataFrame
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
   *   val prefDf = Transformer.prefixColumns(ds.toDF)
   *
   *   prefDf.show(false)
   *   // +---------+-----+--------+
   *   // |   __name|__age|__gender|
   *   // +---------+-----+--------+
   *   // |  Michael|   29|       M|
   *   // |     Sara|   30|       F|
   *   // |   Justin|   19|       M|
   *   // +---------+-----+--------+
   *
   *   prefDf.printSchema
   *   // root
   *   //  |-- __name: string (nullable = true)
   *   //  |-- __age: integer (nullable = false)
   *   //  |-- __gender: string (nullable = true)
   * }}}
   */
  def prefixColumns(df: DataFrame, prefix: String = "__")(implicit spark: SparkSession): DataFrame = {
    df.select(
      df.columns.map(nm => col(nm).as(prefix + nm)): _*
    )
  }

  /**
   * Get all partitions with length of DataFrame
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
   *   val parts1 = Transformer.partitionsLength(ds.toDF.repartition(1))
   *   println(parts1.length)
   *   // 1
   *
   *   val parts2 = Transformer.partitionsLength(ds.toDF.repartition(2))
   *   println(parts2.length)
   *   // 2
   *
   *   val parts3 = Transformer.partitionsLength(ds.toDF.repartition(3))
   *   println(parts3.length)
   *   // 3
   * }}}
   */
  def partitionsLength(df: DataFrame)(implicit spark: SparkSession): Array[(Int, Int)] = {
    df.rdd.mapPartitionsWithIndex { case (ind, rows) =>
      Iterator((ind, rows.length))
    }.collect
  }

  /**
   * Add a column with partition index to DataFrame
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
   *   // +-------+----+------+
   *   // |name   |age |gender|
   *   // +-------+----+------+
   *   // |Michael|29  |M     |
   *   // |Sara   |30  |F     |
   *   // |Justin |19  |M     |
   *   // +-------+----+------+
   *
   *   ds.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *
   *   val partDf1 = Transformer.addPartitionColumn(ds.toDF.repartition(1))
   *
   *   partDf1.show(false)
   *   // +-------+----+------+----+
   *   // |name   |age |gender|part|
   *   // +-------+----+------+----+
   *   // |Michael|29  |M     |0   |
   *   // |Sara   |30  |F     |0   |
   *   // |Justin |19  |M     |0   |
   *   // +-------+----+------+----+
   *
   *   partDf1.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *   //  |-- part: integer (nullable = false)
   *
   *   val partDf2 = Transformer.addPartitionColumn(ds.toDF.repartition(2))
   *
   *   partDf2.show(false)
   *   // +-------+----+------+----+
   *   // |name   |age |gender|part|
   *   // +-------+----+------+----+
   *   // |Michael|29  |M     |0   |
   *   // |Sara   |30  |F     |1   |
   *   // |Justin |19  |M     |1   |
   *   // +-------+----+------+----+
   *
   *   partDf2.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *   //  |-- part: integer (nullable = false)
   *
   *   val partDf3 = Transformer.addPartitionColumn(ds.toDF.repartition(3))
   *
   *   partDf3.show(false)
   *   // +-------+----+------+----+
   *   // |name   |age |gender|part|
   *   // +-------+----+------+----+
   *   // |Michael|29  |M     |0   |
   *   // |Sara   |30  |F     |1   |
   *   // |Justin |19  |M     |2   |
   *   // +-------+----+------+----+
   *
   *   partDf3.printSchema
   *   // root
   *   //  |-- name: string (nullable = true)
   *   //  |-- age: integer (nullable = false)
   *   //  |-- gender: string (nullable = true)
   *   //  |-- part: integer (nullable = false)
   * }}}
   */
  def addPartitionColumn(df: DataFrame, partColumnNm: String = "part")(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(
      df.rdd.mapPartitionsWithIndex(
        preservesPartitioning = true,
        f = { case (index, rows) =>
          rows.map { row =>
            Row.fromSeq(row.toSeq :+ index)
          }
        }
      ),
      df.schema
        .add(StructField(partColumnNm, IntegerType, nullable = false))
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
   *   numDf.show(false)
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
    val idColumnNm = "id"
    val numColumnNm = "num"
    val keyColumnNm = "key"
    val valColumnNm = "val"
    val partColumnNm = "part"

    val partitionMaxSize = partitionsLength(df).maxBy(_._2)._2
    val dfRenamed = prefixColumns(df)
    val dfPart = addPartitionColumn(dfRenamed)

    val window: WindowSpec = Window.partitionBy(col(partColumnNm)).orderBy(lit(1))
    val keyColumn: Column = col(partColumnNm).cast(LongType) * lit(partitionMaxSize) + col(idColumnNm)

    dfPart
      .withColumn(idColumnNm, row_number().over(window).cast(LongType))
      .select(
        df.columns.zip(dfRenamed.columns).zipWithIndex.map { case ((colNm, renamedColNm), i) =>
          struct(
            struct(keyColumn, lit(i + 1).cast(LongType).as(numColumnNm)).as(keyColumnNm),
            col(renamedColNm).as(valColumnNm)
          ).as(s"${keyColumnNm}_$colNm")
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
    val arrColumn: String = df.schema.find { field =>
      field.dataType match {
        case ArrayType(_, _) => true
        case _ => false
      }
    }.map(field => field.name).get

    val explodeColNm = "explode"

    df.select(
      df.columns.filterNot(_ == arrColumn).map(col) :+
        explode_outer(col(arrColumn)).as(explodeColNm): _*
    )
  }

  /**
   * Creates a struct column of all elements in the given column of ArrayType.
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
   *   val df = spark.createDataset(data).toDF
   *
   *   df.show(false)
   *   // +--------------------------------------------------+
   *   // |persons                                           |
   *   // +--------------------------------------------------+
   *   // |[[Michael, 29, M], [Sara, 30, F], [Justin, 19, M]]|
   *   // +--------------------------------------------------+
   *
   *   df.printSchema
   *   // root
   *   //  |-- persons: array (nullable = true)
   *   //  |    |-- element: struct (containsNull = true)
   *   //  |    |    |-- name: string (nullable = true)
   *   //  |    |    |-- age: integer (nullable = false)
   *   //  |    |    |-- gender: string (nullable = true)
   *
   *   val structDf = Transformer.arrayToStruct(df, data.map(_.persons.length).max)
   *
   *   structDf.show(false)
   *   // +--------------------------------------------------+
   *   // |struct                                            |
   *   // +--------------------------------------------------+
   *   // |[[Michael, 29, M], [Sara, 30, F], [Justin, 19, M]]|
   *   // +--------------------------------------------------+
   *
   *   structDf.printSchema
   *   // root
   *   //  |-- struct: struct (nullable = false)
   *   //  |    |-- col_0: struct (nullable = true)
   *   //  |    |    |-- name: string (nullable = true)
   *   //  |    |    |-- age: integer (nullable = false)
   *   //  |    |    |-- gender: string (nullable = true)
   *   //  |    |-- col_1: struct (nullable = true)
   *   //  |    |    |-- name: string (nullable = true)
   *   //  |    |    |-- age: integer (nullable = false)
   *   //  |    |    |-- gender: string (nullable = true)
   *   //  |    |-- col_2: struct (nullable = true)
   *   //  |    |    |-- name: string (nullable = true)
   *   //  |    |    |-- age: integer (nullable = false)
   *   //  |    |    |-- gender: string (nullable = true)
   * }}}
   */
  def arrayToStruct(df: DataFrame, length: Int)(implicit spark: SparkSession): DataFrame = {
    val structColumn = "struct"
    val arrColumn: String = df.schema.find { field =>
      field.dataType match {
        case ArrayType(_, _) => true
        case _ => false
      }
    }.map(field => field.name).get

    df.select(
      df.columns.filterNot(_ == arrColumn).map(col) :+
        struct(
          0 until length map { i =>
            Try(col(arrColumn).getItem(i)).getOrElse(lit(null)).as(s"col_$i")
          }: _*
        ).as(structColumn): _*
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
