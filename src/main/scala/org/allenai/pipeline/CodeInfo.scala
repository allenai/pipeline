package org.allenai.pipeline

import java.net.URI
import java.util.UUID

/** Contains information about the origin of the compiled class implementing a Producer
  * @param buildId A version number, e.g. git tag
  * @param unchangedSince The latest version number at which the logic for this class changed.
  *                    Classes in which the buildIds differ but the unchangedSince field is
  *                    the same are assumed to produce the same outputs when given the same
  *                    inputs
  * @param srcUrl Link to source (e.g. in GitHub)
  * @param binaryUrl Link to binaries (e.g. in Nexus)
  */
case class CodeInfo(buildId: String,
  unchangedSince: String,
  srcUrl: Option[URI],
  binaryUrl: Option[URI])

trait HasCodeInfo {
  def codeInfo: CodeInfo
}

/** Represents code from an unspecified location
  */
trait UnknownCodeInfo extends HasCodeInfo {
  override def codeInfo: CodeInfo = CodeInfo("0", "0", None, None)
}

/** Reads the version number, nexus URL, and GitHub URL from
  * configuration file bundled into the jar.
  * These are populated by the AI2 sbt-release plugin.
  */
trait Ai2CodeInfo extends HasCodeInfo {
  override def codeInfo: CodeInfo = {
    val version = unchangedSince
    this.getClass.getPackage.getImplementationVersion match {
      case null => CodeInfo(version, version, None, None)
      case buildId => CodeInfo(buildId, version, None, None)
    }
  }

  def unchangedSince: String = ("0" +: updateVersionHistory).last

  /** Whenever the logic of this class is updated, the corresponding release number should
    * be added to this list.  The unchangedSince field will be set to the latest version that is
    * still earlier than the version in the jar file.
    */
  def updateVersionHistory: Seq[String] = List()
}

/** For convenience, case classes can mix in this single trait to implement PipelineRunnerSupport
  */
trait Ai2Signature extends PipelineRunnerSupport with Ai2CodeInfo {
  this: Product =>
  override def signature: Signature = Signature.fromObject(this)
}
