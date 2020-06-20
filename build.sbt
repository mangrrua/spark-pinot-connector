name := "spark-pinot-connector"

version := "0.1.0"

scalaVersion := "2.12.11"

val sparkVersion = "2.4.3"
val pinotVersion = "0.4.0"
val circeVersion = "0.13.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided, test",
  "org.apache.pinot" % "pinot-spi" % pinotVersion,
  "org.apache.pinot" % "pinot-core" % pinotVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.8",
  "commons-io" % "commons-io" % "2.6",
  "org.apache.httpcomponents" % "httpclient" % "4.5.12",
  "io.circe" %% "circe-generic" % circeVersion,
  "io.circe" %% "circe-parser" % circeVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)
