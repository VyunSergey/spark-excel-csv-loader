lazy val scalaVersion2_11   = "2.11.12"
scalaVersion := scalaVersion2_11
val SparkVersion            = "2.4.7"
val SparkExcelVersion       = "0.13.1"
val SparkCsvVersion         = "1.5.0"
val PureConfigVersion       = "0.12.0"
val ScallopVersion          = "4.0.0"
val KindProjectorVersion    = "0.9.6"
val BetterMonadicForVersion = "0.2.4"
val ScalaTestVersion        = "3.2.2"
val ScalaCheckVersion       = "1.14.1"
val ScalaTestPlusVersion    = "3.2.2.0"
val ScalaMeterVersion       = "0.18"

lazy val organizationSettings = Seq(
  organization := "com.vyunsergey",
  name := "spark-excel-csv-loader",
  homepage := Some(url("https://github.com/VyunSergey")),
  licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")))
)

lazy val testSettings = Seq(
  testFrameworks += new TestFramework("org.scalameter.ScalaMeterFramework"),
  parallelExecution in Test := false,
  Test / testOptions := Seq(Tests.Filter(_.endsWith("Test"))),
  fork := true,
  outputStrategy := Some(StdoutOutput),
  connectInput := true
)

lazy val assemblySettings = Seq(
  assemblyJarName in assembly := s"${name.value}-${scalaVersion.value}_${version.value}.jar",
  mainClass in assembly := Some("com.vyunsergey.sparkexcelcsvloader.Loader"),
  test in assembly := {},
  assemblyShadeRules in assembly := Seq(
    ShadeRule.rename("org.apache.http.**" -> "shaded.org.apache.http.@1").inAll
  ),
  assemblyMergeStrategy in assembly := {
    case "application.conf" => MergeStrategy.concat
    case "reference.conf" => MergeStrategy.concat
    case PathList("META-INF", xs @ _*) => xs match {
      case "MANIFEST.MF" :: Nil => MergeStrategy.discard
      case "services" :: _ :: Nil => MergeStrategy.concat
      case name :: Nil =>
        if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF")) MergeStrategy.discard
        else MergeStrategy.first
      case _ => MergeStrategy.first
    }
    case _ => MergeStrategy.first
  }
)

lazy val commonLibraryDependencies = Seq(
  // Spark
  "org.apache.spark"           %% "spark-core"         % SparkVersion,
  "org.apache.spark"           %% "spark-sql"          % SparkVersion,
  "org.apache.spark"           %% "spark-hive"         % SparkVersion,
  // Spark Excel
  "com.crealytics"             %% "spark-excel"        % SparkExcelVersion,
  // Spark Csv
  "com.databricks"             %% "spark-csv"          % SparkCsvVersion,
  // PureConfig
  "com.github.pureconfig"      %% "pureconfig"         % PureConfigVersion,
  // Scallop
  "org.rogach"                 %% "scallop"            % ScallopVersion,
  // ScalaTest
  "org.scalatest"              %% "scalatest"          % ScalaTestVersion % Test,
  // ScalaCheck
  "org.scalacheck"             %% "scalacheck"         % ScalaCheckVersion % Test,
  // ScalaTestPlus
  "org.scalatestplus"          %% "scalacheck-1-14"    % ScalaTestPlusVersion % Test,
  // ScalaMeter
  "com.storm-enroute"          %% "scalameter"         % ScalaMeterVersion
)

lazy val scalaCompilerOptions = Seq(
  "-deprecation",                  // Emit warning and location for usages of deprecated APIs
  "-unchecked",                    // Enable additional warnings where generated code depends on assumptions
  "-feature",                      // Emit warning and location for usages of features that should be imported explicitly
  "-encoding", "UTF-8",            // Specify character encoding used by source files
  "-language:implicitConversions", // Allow definition of implicit functions called views
  "-language:existentials",        // Existential types (besides wildcard types) can be written and inferred
  "-language:higherKinds",         // Allow higher-kinded types
  "-language:postfixOps",          // Allows operator syntax in postfix position (deprecated since Scala 2.10)
  "-Ypartial-unification",         // Enable partial unification in type constructor inference
  "-Xfatal-warnings"               // Fail the compilation if there are any warnings
)

lazy val root = (project in file(".")).settings(
  organizationSettings,
  testSettings,
  assemblySettings,
  libraryDependencies ++= commonLibraryDependencies,
  scalacOptions ++= scalaCompilerOptions
)
