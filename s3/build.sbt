import Dependencies._

organization := "org.allenai"

StylePlugin.enableLineLimit := false

libraryDependencies ++= Seq(
  awsJavaSdk
)
