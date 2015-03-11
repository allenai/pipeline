import Dependencies._

lazy val buildSettings = Seq(
  organization := "org.allenai",
  crossScalaVersions := Seq("2.11.5"),
  scalaVersion <<= crossScalaVersions { (vs: Seq[String]) => vs.head },
  publishMavenStyle := true,
  publishArtifact in Test := false,
  pomIncludeRepository := { _ => false },
  licenses := Seq("Apache 2.0" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt")),
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
  dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.11.5") ++
  PublishTo.ai2Public

lazy val pipeline = Project(
  id = "allenai-pipeline",
  base = file("."),
  settings = buildSettings
).enablePlugins(LibraryPlugin)

libraryDependencies ++= Seq(sprayJson,
  awsJavaSdk,
  commonsIO,
  ai2Common,
  allenAiTestkit,
  scalaReflection)
