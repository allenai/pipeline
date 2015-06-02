import Dependencies._

name := "pipeline-core"
organization := "org.allenai"

StylePlugin.enableLineLimit := false

dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.11.5"
libraryDependencies ++= Seq(
  sprayJson,
  commonsIO,
  ai2Common,
  allenAiTestkit % "test",
  scalaReflection
)
