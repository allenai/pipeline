import Dependencies._

import ReleaseKeys._

val core = Project(
  id = "core",
  base = file("core")
)

val s3 = Project(
  id = "s3",
  base = file("s3")
).dependsOn(core)

val spark = Project(
  id = "spark",
  base = file("spark")
).dependsOn(core, s3)

val examples = Project(
  id = "examples",
  base = file("examples")
).dependsOn(core, s3)


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
