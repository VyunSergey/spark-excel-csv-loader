
lazy val scalaVersion2_11   = "2.11.12"
scalaVersion := scalaVersion2_11
val SparkVersion            = "2.2.0"
val SparkExcelVersion       = "0.13.1"
val SparkCsvVersion         = "1.5.0"
val PureConfigVersion       = "0.12.0"
val KindProjectorVersion    = "0.9.6"
val BetterMonadicForVersion = "0.2.4"
val ScalaTestVersion        = "3.2.2"
val ScalaCheckVersion       = "1.14.1"
val ScalaTestPlusVersion    = "3.2.2.0"
val SparkTestingBaseVersion = "2.2.0_0.12.0"
val ScalaMockVersion        = "3.6.0"

lazy val organizationSettings = Seq(
  organization := "com.vyunsergey",
  name := "spark-excel-csv-loader",
  homepage := Some(url("https://github.com/VyunSergey")),
  licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")))
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
  // ScalaTest
  "org.scalatest"              %% "scalatest"          % ScalaTestVersion % Test,
  // ScalaCheck
  "org.scalacheck"             %% "scalacheck"         % ScalaCheckVersion % Test,
  // ScalaTestPlus
  "org.scalatestplus"          %% "scalacheck-1-14"    % ScalaTestPlusVersion % Test,
  // SparkTestingBase
  "com.holdenkarau"            %% "spark-testing-base" % SparkTestingBaseVersion % Test,
  // ScalaMock
  "org.scalamock"              %% "scalamock-scalatest-support" % ScalaMockVersion % Test
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
  libraryDependencies ++= commonLibraryDependencies,
  scalacOptions ++= scalaCompilerOptions,
  addCompilerPlugin("org.spire-math" %% "kind-projector"     % KindProjectorVersion),
  addCompilerPlugin("com.olegpy"     %% "better-monadic-for" % BetterMonadicForVersion)
)
