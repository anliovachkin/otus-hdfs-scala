name := "otus-hdfs-scala"

version := "0.1"
version := "0.1.0-SNAPSHOT"
scalaVersion := "2.13.5"

lazy val root = (project in file("."))
  .settings(
    name := "otus-hdfs",
  )

libraryDependencies ++= Seq("org.apache.hadoop" % "hadoop-client" % "3.2.1")