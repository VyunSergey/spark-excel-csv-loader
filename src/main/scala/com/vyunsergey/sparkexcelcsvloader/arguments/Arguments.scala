package com.vyunsergey.sparkexcelcsvloader.arguments

import org.rogach.scallop.{ScallopConf, ScallopOption}

class Arguments(args: Seq[String]) extends ScallopConf(args) {
  val mode: ScallopOption[String] = opt[String](name = "mode", descr = "transformation mode, supported modes: csv, excel", default = Some("csv"), validate = _.nonEmpty)
  val numParts: ScallopOption[Int] = opt[Int](name = "num-parts", descr = "number of output files e.t. partitions", required = false, validate = _ > 0)
  val srcPath: ScallopOption[String] = opt[String](name = "src-path", descr = "location path of the source files", required = true, validate = _.nonEmpty)
  val tgtPath: ScallopOption[String] = opt[String](name = "tgt-path", descr = "location path of the target files", required = true, validate = _.nonEmpty)
  verify()
}

object Arguments {
  def apply(args: Seq[String]): Arguments = new Arguments(args)
}
