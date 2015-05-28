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
// We need to override the default versioning scheme provided by the LibraryPlugin
// because we want to Use semantic versioning for public releases.
// TODO(markschaake|?): enhance the org.allenai.plugins.ReleasePlugin to make it simple
// to switch between versioning schemes.
// These overrides are copied from the underlying sbt-release plugin's sources for default
// semantic version.
// See: https://github.com/sbt/sbt-release/blob/master/src/main/scala/ReleasePlugin.scala
nextVersion <<= (versionBump) { bumpType: sbtrelease.Version.Bump =>
  ver => sbtrelease.Version(ver).map(_.bump(bumpType).asSnapshot.string).getOrElse(sbtrelease.versionFormatError)
}
releaseVersion := { ver =>
  sbtrelease.Version(ver).map(_.withoutQualifier.string).getOrElse(sbtrelease.versionFormatError)
}

dependencyOverrides += "org.scala-lang" % "scala-reflect" % "2.11.5"
