import Dependencies._

import ReleaseKeys._

val pipeline = Project(
  id = "allenai-pipeline",
  base = file(".")
)

StylePlugin.enableLineLimit := false

organization := "org.allenai"
crossScalaVersions := Seq("2.11.5")
scalaVersion <<= crossScalaVersions { (vs: Seq[String]) => vs.head }
publishMavenStyle := true
publishArtifact in Test := false
pomIncludeRepository := { _ => false }
licenses := Seq("Apache 2.0" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/allenai/pipeline"))
scmInfo := Some(
  ScmInfo(url("https://github.com/allenai/pipeline"), "https://github.com/allenai/pipeline.git")
)
pomExtra := (
  <developers>
    <developer>
      <id>allenai-dev-role</id>
      <name>Allen Institute for Artificial Intelligence</name>
      <email>dev-role@allenai.org</email>
    </developer>
  </developers>
)

// Enable the LibraryPlugin for release workflow support
enablePlugins(LibraryPlugin)
PublishTo.ai2Public

dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.11.5"
libraryDependencies ++= Seq(
  sprayJson,
  commonsIO,
  allenAiCommon,
  allenAiTestkit % "test",
  scalaReflection,
  awsJavaSdk,
  parserCombinators,
  apacheCommonsLang
)

// To create release zip: sbt universal:packageBin
enablePlugins(JavaAppPackaging)

packageName in Universal := "pipeScript"

executableScriptName := "runPipeScript"

javaOptions in Universal ++= Seq(
    "-Dlogback.configurationFile=logback-pipeScript.xml"	    
)
