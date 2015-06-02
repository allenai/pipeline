import Dependencies._

name := "pipeline-s3"
organization := "org.allenai"

StylePlugin.enableLineLimit := false

libraryDependencies ++= Seq(
  awsJavaSdk
)
