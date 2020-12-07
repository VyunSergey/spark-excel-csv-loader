package com.vyunsergey.sparkexcelcsvloader

import com.vyunsergey.sparkexcelcsvloader.config.Configuration
import com.vyunsergey.sparkexcelcsvloader.spark.{SparkConfig, SparkConnection}

object Main extends App {
  lazy val conf = Configuration.config
  lazy val sparkConf = SparkConfig.make(conf)
  lazy val spark = SparkConnection.make("App")(sparkConf)

  println(spark.version)
  spark.stop()
}
