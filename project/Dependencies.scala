import sbt._

import org.allenai.plugins.CoreDependencies

/** Object holding the dependencies Common has, plus resolvers and overrides. */
object Dependencies extends CoreDependencies {
  val awsJavaSdk = "com.amazonaws" % "aws-java-sdk" % "1.8.9.1"
  val scalaReflection = "org.scala-lang" % "scala-reflect" % "2.11.5"
  val commonsIO = "commons-io" % "commons-io" % "2.4"
  val ai2Common = allenAiCommon exclude ("org.allenai", "pipeline")
  val sparkCore = "org.apache.spark" %% "spark-core" % "1.4.0" % "provided" excludeAll (
    ExclusionRule(organization = "org.slf4j", name = "slf4j-log4j12")
  )
}
