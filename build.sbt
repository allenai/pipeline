import Dependencies._

name := "common-pipeline"

libraryDependencies ++= Seq(sprayJson,
  awsJavaSdk exclude ("com.fasterxml.jackson.core", "jackson-annotations"),
  commonsIO,
  scalaReflection)
 
