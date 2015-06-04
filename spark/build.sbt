import Dependencies._

name := "pipeline-spark"
organization := "org.allenai"

StylePlugin.enableLineLimit := false

dependencyOverrides += "commons-codec" % "commons-codec" % "1.6"
dependencyOverrides += "commons-io" % "commons-io" % "2.4"
dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.11.5"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.4.4"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-annotations" % "2.4.4"
dependencyOverrides += "org.scala-lang.modules" %% "scala-xml" % "1.0.2"

libraryDependencies ++= Seq(
  sparkCore,
  allenAiTestkit % "test"
)
