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
//ds.show(false)
//ds.printSchema

   val partDf1 = Transformer.addPartitionColumn(ds.toDF.repartition(1))

   partDf1.show(false)
   // +-------+----+------+----+
   // |name   |age |gender|part|
   // +-------+----+------+----+
   // |Michael|29  |M     |0   |
   // |Sara   |30  |F     |0   |
   // |Justin |19  |M     |0   |
   // +-------+----+------+----+

   partDf1.printSchema
   // root
   //  |-- name: string (nullable = true)
   //  |-- age: integer (nullable = false)
   //  |-- gender: string (nullable = true)
   //  |-- part: integer (nullable = false)

   val partDf2 = Transformer.addPartitionColumn(ds.toDF.repartition(2))

   partDf2.show(false)
   // +---------+-----+--------+
   // |   __name|__age|__gender|
   // +---------+-----+--------+
   // |  Michael|   29|       M|
   // |     Sara|   30|       F|
   // |   Justin|   19|       M|
   // +---------+-----+--------+

   partDf2.printSchema
   // root
   //  |-- __name: string (nullable = true)
   //  |-- __age: integer (nullable = false)
   //  |-- __gender: string (nullable = true)

   val partDf3 = Transformer.addPartitionColumn(ds.toDF.repartition(3))

   partDf3.show(false)
   // +---------+-----+--------+
   // |   __name|__age|__gender|
   // +---------+-----+--------+
   // |  Michael|   29|       M|
   // |     Sara|   30|       F|
   // |   Justin|   19|       M|
   // +---------+-----+--------+

   partDf3.printSchema
   // root
   //  |-- __name: string (nullable = true)
   //  |-- __age: integer (nullable = false)
   //  |-- __gender: string (nullable = true)