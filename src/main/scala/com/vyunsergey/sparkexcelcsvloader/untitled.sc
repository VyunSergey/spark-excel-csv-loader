import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.reader.ReaderConfig
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}
import com.vyunsergey.sparkexcelcsvloader.transformer.Transformer
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

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

val ds = spark.createDataset(data)

ds.show(false)

ds.printSchema

val numDf = Transformer.numericColumns(ds.toDF)

numDf.show(false)

numDf.printSchema

