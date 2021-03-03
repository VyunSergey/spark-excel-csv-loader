import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.reader.ReaderConfig
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import com.vyunsergey.sparkexcelcsvloader.transformer.Transformer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

lazy val conf: Configuration.AppConf = Configuration.config
lazy val readerConf: ReaderConfig = ReaderConfig.make(conf)
lazy val sparkConf: SparkConfig = SparkConfig.make(conf)

implicit lazy val spark: SparkSession = SparkConnection.make("Key-Value Loader")(sparkConf)
implicit lazy val logger: Logger = LogManager.getLogger(getClass)

import spark.implicits._

case class Person(name: String, age: Int, gender: String)

val data = Seq(
  Person("Michael", 29, "M"),
  Person("Sara", 30, "F"),
  Person("Justin", 19, "M")
)

//-----------------------------------------------------------------------
val df = spark.createDataset(data).toDF
//-----------------------------------------------------------------------
df
  .show(



    false
  )
//-----------------------------------------------------------------------
df


  .printSchema
//-----------------------------------------------------------------------

//-----------------------------------------------------------------------
val kvDf = Transformer.keyValueColumns(df)
//-----------------------------------------------------------------------
kvDf
  .show(









    false
  )
//-----------------------------------------------------------------------
kvDf

  .printSchema
//-----------------------------------------------------------------------

//-----------------------------------------------------------------------
val metaDf = Transformer.metaColumns(df, "test_table_name")
//-----------------------------------------------------------------------
metaDf
  .show(




    false
  )
//-----------------------------------------------------------------------
metaDf

  .printSchema
//-----------------------------------------------------------------------

//-----------------------------------------------------------------------
val resDf = (metaDf union kvDf).orderBy(col("key"))
//-----------------------------------------------------------------------
resDf
  .show(













    false
  )
//-----------------------------------------------------------------------
resDf

  .printSchema
//-----------------------------------------------------------------------

//-----------------------------------------------------------------------
val idNumDf = Transformer.idNumColumns(resDf)
//-----------------------------------------------------------------------
idNumDf
  .show(













    false
  )
//-----------------------------------------------------------------------
idNumDf


  .printSchema
//-----------------------------------------------------------------------

//-----------------------------------------------------------------------
val (schema, nameOp) = Transformer.schemaName(idNumDf)
//-----------------------------------------------------------------------
println(nameOp)
//-----------------------------------------------------------------------
println(
  schema
    .treeString
)
//-----------------------------------------------------------------------

//-----------------------------------------------------------------------
val plainDf = Transformer.plainColumns(idNumDf, schema)
//-----------------------------------------------------------------------
plainDf
  .show(



    false
  )
//-----------------------------------------------------------------------
plainDf


  .printSchema
//-----------------------------------------------------------------------
