import Dependencies._

name := "pipeline-examples"
organization := "org.allenai"

publishTo := None

publishArtifact := false

StylePlugin.enableLineLimit := false

libraryDependencies ++= Seq(
  allenAiTestkit % "test",
  awsJavaSdk
)
