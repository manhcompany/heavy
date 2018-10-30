import sbt.Keys.libraryDependencies

name := "heavy"

version := "0.1"

scalaVersion := "2.11.11"

// https://mvnrepository.com/artifact/com.github.pureconfig/pureconfig
libraryDependencies += "com.github.pureconfig" %% "pureconfig" % "0.9.2"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP10"
libraryDependencies += "org.scalacheck" %% "scalacheck" % "1.13.5" % Test

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.2" % "provided"


// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.2"



lazy val commondDependencies = Seq(
  "com.github.pureconfig" %% "pureconfig" % "0.9.2",
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10",
  "org.scalacheck" %% "scalacheck" % "1.13.5" % Test,
  "org.apache.spark" %% "spark-core" % "2.3.2" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.3.2"
)

lazy val commonSettings = Seq(
  organization := "com.heavy",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.11"
)

lazy val core = (project in file("core"))
  .settings(
    commonSettings,
    name:="core",
    libraryDependencies:=commondDependencies)

lazy val root = (project in file("."))
  .aggregate(core)

