import Dependencies._

dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.11.5"

libraryDependencies ++= Seq(
  sprayJson,
  awsJavaSdk,
  commonsIO,
  ai2Common,
  allenAiTestkit % "test",
  scalaReflection
)
