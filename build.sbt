import Dependencies._

name := "common-pipeline"

libraryDependencies ++= Seq(sprayJson,
  awsJavaSdk,
  commonsIO)

libraryDependencies <+= scalaVersion (sv => scalaReflection(sv))

dependencyOverrides <+= scalaVersion (sv => scalaReflection(sv))
