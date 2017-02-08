name := "fdp-sample-loganalysis"

organization := "lightbend"

version := "0.1"

scalaVersion := "2.11.8"

val spark = "2.1.0"

mainClass in (Compile, run) := Some("com.lightbend.fdp.sample.LogAnalysis")

mainClass in (Compile, packageBin) := Some("com.lightbend.fdp.sample.LogAnalysis")

libraryDependencies ++= Seq(
  "org.apache.kafka"      %   "kafka-streams"                  % "0.10.1.0",
  "joda-time"             %   "joda-time"                      % "2.9.7",
  "org.apache.spark"     %%   "spark-streaming-kafka-0-10"     % spark,
  "org.apache.spark"     %%   "spark-core"                     % spark % "provided",
  "org.apache.spark"     %%   "spark-streaming"                % spark % "provided",
  "org.apache.spark"     %%   "spark-sql"                      % spark % "provided"
)
