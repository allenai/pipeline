lazy val buildSettings = Seq(
  organization := "org.allenai",
  crossScalaVersions := Seq("2.11.5"),
  scalaVersion <<= crossScalaVersions { (vs: Seq[String]) => vs.head },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  licenses += ("Apache-2.0", url("http://www.apache.org/licenses/LICENSE-2.0.html")),
  homepage := Some(url("https://github.com/allenai/pipeline")),
  scmInfo := Some(ScmInfo(
    url("https://github.com/allenai/pipeline"),
    "https://github.com/allenai/pipeline.git")),
  pomExtra := (
    <developers>
      <developer>
        <id>allenai-dev-role</id>
        <name>Allen Institute for Artificial Intelligence</name>
        <email>dev-role@allenai.org</email>
      </developer>
    </developers>),
  bintrayPackage := s"${organization.value}:${name.value}_${scalaBinaryVersion.value}"
)

val pipeline = Project(
  id = "pipeline",
  settings = buildSettings,
  base = file(".")
).enablePlugins(LibraryPlugin, JavaAppPackaging)

val examples = Project(
  id = "examples",
  base = file("examples")
).dependsOn("pipeline").settings(
  publishArtifact := false,
  publishTo := Some("dummy" at "nowhere"),
  publish := { },
  publishLocal := { }
).enablePlugins(LibraryPlugin)


name := "pipeline"

StylePlugin.enableLineLimit := false
logLevel in compile := Level.Error

import Dependencies._

dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.11.5"
libraryDependencies ++= Seq(
  sprayJson,
  commonsIO,
  allenAiCommon,
  allenAiTestkit % "test",
  scalaReflection,
  awsJavaSdk,
  parserCombinators,
  apacheCommonsLang,
  apacheCompress
)

// For JavaAppPackaging:
//
// To create release zip: sbt universal:packageBin
packageName in Universal := "pipeScript"
executableScriptName := "runPipeScript"
javaOptions in Universal ++= Seq(
    "-Dlogback.configurationFile=logback-pipeScript.xml"	    
)
