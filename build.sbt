import Dependencies._

name := "common-pipeline"

libraryDependencies ++= Seq(sprayJson,
  awsJavaSdk,
  commonsIO)

dependencyOverrides <+= scalaVersion (sv => scalaReflection(sv))
