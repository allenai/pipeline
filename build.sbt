import Dependencies._

name := "common-pipeline"

libraryDependencies ++= Seq(sprayJson,
  awsJavaSdk,
  commonsIO,
  scalaReflection)

dependencyOverrides += scalaReflection
