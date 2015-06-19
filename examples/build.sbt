import Dependencies._

name := "pipeline-examples"
organization := "org.allenai"

StylePlugin.enableLineLimit := false

libraryDependencies ++= Seq(
  awsJavaSdk
)
