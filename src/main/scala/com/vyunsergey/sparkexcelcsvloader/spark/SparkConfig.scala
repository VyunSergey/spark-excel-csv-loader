package com.vyunsergey.sparkexcelcsvloader.spark

import com.vyunsergey.sparkexcelcsvloader.config.Configuration

case class SparkConfig(private val config: Map[String, String]) {
  def master(): String = config.getOrElse(SparkConfig.sparkMasterKey, "local")

  def enableHiveSupport(): Boolean = config.get(SparkConfig.sparkSqlCatalogImplementationKey).contains(SparkConfig.hive)

  def options(): Map[String, String] = config.filterKeys(
    !List(SparkConfig.sparkMasterKey, SparkConfig.sparkSqlCatalogImplementationKey).contains(_))

  def +(kv: (String, String)): SparkConfig = {
    SparkConfig(this.config + kv)
  }

  def ++(conf: Map[String, String]): SparkConfig = {
    SparkConfig(this.config ++ conf)
  }
}

object SparkConfig {
  def make(config: Configuration.AppConf): SparkConfig = SparkConfig(config.sparkConf)

  val sparkMasterKey = "spark.master"
  val sparkSqlCatalogImplementationKey = "spark.sql.catalogImplementation"
  val hive = "hive"
}
