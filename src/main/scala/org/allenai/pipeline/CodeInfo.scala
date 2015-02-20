package org.allenai.pipeline

import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol._
import spray.json.{ JsString, JsValue, JsonFormat }

import scala.collection.JavaConverters._

import java.net.URI

/** Contains information about the origin of the compiled class implementing a Producer
  * @param buildId A version number, e.g. git tag
  * @param unchangedSince The latest version number at which the logic for this class changed.
  *            Classes in which the buildIds differ but the unchangedSince field is
  *            the same are assumed to produce the same outputs when given the same
  *            inputs
  * @param srcUrl Link to source (e.g. in GitHub)
  * @param binaryUrl Link to binaries (e.g. in Nexus)
  */
case class CodeInfo(
  className: String,
  buildId: String,
  unchangedSince: String,
  srcUrl: Option[URI],
  binaryUrl: Option[URI]
)

object CodeInfo {
  implicit val uriFormat = new JsonFormat[URI] {
    override def write(uri: URI): JsValue = JsString(uri.toString)

    override def read(value: JsValue): URI = value match {
      case JsString(uri) => new URI(uri)
      case _ => sys.error("Invalid format for URI")
    }
  }
  implicit val jsFormat = jsonFormat5(apply)
}

trait HasCodeInfo {
  def codeInfo: CodeInfo
}

/** Represents code from an unspecified location.
  */
trait UnknownCodeInfo extends HasCodeInfo {
  override def codeInfo: CodeInfo = CodeInfo(this.getClass.getSimpleName, "0", "0", None, None)
}

/** Reads the version number and GitHub URL from
  * configuration file bundled into the jar.
  * These are populated by the AI2 sbt plugins.
  */
trait Ai2CodeInfo extends HasCodeInfo {
  override def codeInfo: CodeInfo = {
    val lastChangedVersion = unchangedSince
    val className = this.getClass.getSimpleName.reverse.dropWhile(_ == '$').reverse
    val info = {
      try {
        // This information is stored in META-INF/MANIFEST.MF
        val pack = this.getClass.getPackage
        val buildId = pack.getImplementationVersion
        // This resource is written by allenai/sbt-release
        val configPath = s"""${pack.getImplementationVendor}/${
          pack.getImplementationTitle
            .replaceAll("-", "")
        }/git.conf"""
        val config = ConfigFactory.load(configPath)
        // Commit sha
        val sha = config.getString("sha1")
        // The list of remote git repo's
        val remotes = config.getStringList("remotes").asScala.map(parseRemote)
        // The sbt-release plugin requires the git repo to be clean.
        // However, we don't know for sure whether the repo has been pushed to a remote repo
        // It may not have been pushed at the time of build, but might be pushed later
        // We have to guess which remote will have the commit in it
        val useRemote = remotes.size match {
          // If there is only one remote, use it
          case 1 => remotes(0)
          // People shouldn't push directly to the upstream allenai repo.  Instead the upstream
          // repo gets updated via a pull request, which will have a different commit sha
          // Use the first non-allenai repo found in the list, which will typically be
          // the user's forked repo
          case _ => remotes.find(u => !u.getPath.startsWith("allenai")).get
        }
        val newPath = s"""${useRemote.getPath.replaceAll(".git/?$", "")}/tree/$sha"""
        val srcUrl = new URI(useRemote.getScheme, useRemote.getUserInfo, useRemote.getHost,
          useRemote.getPort, newPath, useRemote.getQuery, useRemote.getFragment)
        CodeInfo(className, buildId, lastChangedVersion, Some(srcUrl), None)
      } catch {
        // Fall-back.  If the config doesn't exist or doesn't contain the expected information
        // Then get the version number from MANIFEST.MF and don't give a source URL
        case ex: Exception => this.getClass.getPackage.getImplementationVersion match {
          case null => CodeInfo(className, lastChangedVersion, lastChangedVersion, None, None)
          case buildId => CodeInfo(className, buildId, lastChangedVersion, None, None)
        }
      }
    }
    info
  }

  private val sshRemotePattern = """([^@]+)@([^:]+):(.*)""".r
  def parseRemote(remote: String): URI = {
    remote match {
      case sshRemotePattern(user, host, path) =>
        val absPath = if (path.startsWith("/")) path else s"/$path"
        new URI("https", host, absPath, null)
      case x => new URI(x)
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
