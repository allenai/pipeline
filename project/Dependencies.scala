import sbt._

import org.allenai.plugins.CoreDependencies

/** Object holding the dependencies Common has, plus resolvers and overrides. */
object Dependencies extends CoreDependencies {
  val awsJavaSdk = "com.amazonaws" % "aws-java-sdk-s3" % "1.9.40"
  val scalaReflection = "org.scala-lang" % "scala-reflect" % "2.11.5"
  val commonsIO = "commons-io" % "commons-io" % "2.4"
  override val allenAiCommon = "org.allenai.common" %% "common-core" % "1.0.1"
}
