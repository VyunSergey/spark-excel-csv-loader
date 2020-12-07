package com.vyunsergey.sparkexcelcsvloader.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkConnection {
  def make(name: String)(sparkConf: SparkConfig): SparkSession = {
    lazy val spark: SparkSession = SparkSession.builder
      .master(sparkConf.master())
      .appName(name)
      .hiveSupport(sparkConf.enableHiveSupport())
      .config(new SparkConf().setAll(sparkConf.options()))
      .getOrCreate()

    spark
  }

  implicit class SparkSessionBuilderOps(builder: SparkSession.Builder) {
    def hiveSupport(enabled: Boolean): SparkSession.Builder = {
      if (enabled) builder.enableHiveSupport() else builder
    }
  }
}
