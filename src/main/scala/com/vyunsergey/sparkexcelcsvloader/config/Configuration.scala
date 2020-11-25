package com.vyunsergey.sparkexcelcsvloader.config

import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.semiauto.deriveReader

object Configuration {
  lazy val config: AppConf = ConfigSource.default.loadOrThrow[AppConf]

  case class AppConf(sparkConf: Map[String, String])

  implicit val appConfReader: ConfigReader[AppConf] = deriveReader[AppConf]
}
