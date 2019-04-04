import com.typesafe.sbt.packager.MappingsHelper.fromClasspath
import sbt.Keys.{libraryDependencies, name}

val sparkV = "2.1.1"

lazy val commonDependencies = Seq(
  "com.github.pureconfig" %% "pureconfig" % "0.9.2",
  "com.github.pureconfig" %% "pureconfig-enumeratum" % "0.9.2",
  "org.scalatest" %% "scalatest" % "3.2.0-SNAP10" % "test",
  "org.scalacheck" %% "scalacheck" % "1.13.5" % "test",
  "org.slf4j" % "slf4j-api" % "1.7.25",
  "com.typesafe" % "config" % "1.3.3",
  "org.rogach" %% "scallop" % "3.1.5"
)


lazy val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkV % "provided",
  "org.apache.spark" %% "spark-sql" % sparkV % "provided",
  "org.apache.spark" %% "spark-hive" % sparkV,
  "org.eclipse.jetty" % "jetty-servlet" % "9.4.14.v20181114"
)

lazy val elasticsearchDependencies = Seq(
  "org.elasticsearch" %% "elasticsearch-spark-20" % "5.5.3"
)

lazy val prometheusDependencies = Seq(
  "io.prometheus" % "simpleclient" % "0.6.0",
  "io.prometheus" % "simpleclient_hotspot" % "0.6.0",
  "io.prometheus" % "simpleclient_httpserver" % "0.6.0",
  "io.prometheus" % "simpleclient_pushgateway" % "0.6.0"
)

lazy val commonSettings = Seq(
  organization := "com.heavy",
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.11.12",
  test in assembly := {},
  javaOptions ++= Seq("-Dconfig.file=etl/src/test/resources/test.conf", "-Dspark.master=local[4]", "-Ddate=20181028", "-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
  assemblyMergeStrategy in assembly := {
    case PathList("META-INF", xs @ _*) => MergeStrategy.discard
    case x => MergeStrategy.first
  },
  scalacOptions := Seq(
    "-encoding", "UTF-8",
    "-deprecation",
    "-feature",
    "-language:higherKinds",
    "-Ywarn-dead-code",
    "-Xlint",
    "-Xexperimental"
  ),
  assemblyShadeRules in assembly := Seq(ShadeRule.rename("shapeless.**" -> "new_shapeless.@1").inAll),
  assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
)

lazy val core = (project in file("core"))
    .settings(
      commonSettings,
      name:="core",
      libraryDependencies:=commonDependencies)

lazy val etl = (project in file("etl"))
      .dependsOn(core, monitoring)
      .settings(
        commonSettings,
        libraryDependencies:=sparkDependencies ++ commonDependencies ++ elasticsearchDependencies,
        name:="etl")

lazy val spark_ext = (project in file("spark-extension"))
  .settings(
    commonSettings,
    libraryDependencies:=sparkDependencies ++ commonDependencies,
    name:="spark-extension"
  )

lazy val monitoring = (project in file("monitoring"))
  .dependsOn(core)
  .settings(
    commonSettings,
    name:="monitoring",
    libraryDependencies:=sparkDependencies ++ commonDependencies ++ prometheusDependencies
  )

lazy val root = (project in file("."))
    .settings(
      commonSettings,
      name:="heavy",
      packageName in Universal := packageName.value + "-" + version.value,
      mappings in Universal ++= fromClasspath((managedClasspath in Runtime).value, "lib", _ => true)
    )
    .enablePlugins(UniversalPlugin)
    .aggregate(core, etl, monitoring)
