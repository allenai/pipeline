import sbt._

import org.allenai.plugins.CoreDependencies

/** Object holding the dependencies Common has, plus resolvers and overrides. */
object Dependencies extends CoreDependencies {
  val apacheCommonsLang = "org.apache.commons" % "commons-lang3" % "3.4"
  val apacheCompress = "org.apache.commons" % "commons-compress" % "1.10"
  val awsJavaSdk = "com.amazonaws" % "aws-java-sdk-s3" % "1.9.40"
  val scalaReflection = "org.scala-lang" % "scala-reflect" % "2.11.5"
  val commonsIO = "commons-io" % "commons-io" % "2.4"
  val parserCombinators = "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.4"
  override val allenAiCommon = "org.allenai.common" %% "common-core" % "1.0.1"
}
