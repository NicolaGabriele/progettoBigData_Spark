ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "progettoBigData"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.3.1",
  "org.apache.spark" %% "spark-sql" % "3.3.1",
  "org.apache.spark" %% "spark-mllib" % "3.3.1",
  "org.apache.spark" %% "spark-hive" % "3.3.1" ,
  "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.13.0",
  "com.typesafe" % "config" % "1.4.0",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.0"
)

