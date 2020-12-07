package com.vyunsergey.sparkexcelcsvloader.config

import pureconfig.{ConfigReader, ConfigSource}
import pureconfig.generic.semiauto.deriveReader
import java.io.File
import java.nio.file.{Path, Paths}

object Configuration {
  lazy val config: AppConf = ConfigSource.default.loadOrThrow[AppConf]

  case class AppConf(readerConf: Map[String, String], sparkConf: Map[String, String])

  def convertPath(path: String): Path = {
    Paths.get(new File(path).toURI)
  }

  implicit val appConfReader: ConfigReader[AppConf] = deriveReader[AppConf]
}
