package org.allenai.pipeline

import com.typesafe.config.ConfigFactory
import spray.json.DefaultJsonProtocol._
import spray.json.{ JsString, JsValue, JsonFormat }

import scala.collection.JavaConverters._

import java.net.URI

/** Contains information about the origin of the compiled class implementing a Producer
  * @param unchangedSince The latest version number at which the logic for this class changed.
  *            Classes in which the buildIds differ but the unchangedSince field is
  *            the same are assumed to produce the same outputs when given the same
  *            inputs
  * @param srcUrl Link to source (e.g. in GitHub)
  * @param binaryUrl Link to binaries (e.g. in Nexus)
  */
case class CodeInfo(
  className: String,
  unchangedSince: String,
  srcUrl: Option[URI] = None,
  binaryUrl: Option[URI] = None
)

object CodeInfo {
  implicit val uriFormat = new JsonFormat[URI] {
    override def write(uri: URI): JsValue = JsString(uri.toString)

    override def read(value: JsValue): URI = value match {
      case JsString(uri) => new URI(uri)
      case s => sys.error(s"Invalid URI: $s")
    }
  }
  implicit val jsFormat = jsonFormat4(apply)

  def unknown(target: Any) = CodeInfo(target.getClass.getSimpleName, "")
}

/** Reads the version number and GitHub URL from
  * configuration file bundled into the jar.
  * These are populated by the AI2 sbt plugins.
  */
object Ai2CodeInfo {
  def apply(target: Any): PipelineStepInfo = {
    val lastChangedVersion = unchangedSince
    val className = target.getClass.getSimpleName.reverse.dropWhile(_ == '$').reverse
    val info = {
      try {
        // This information is stored in META-INF/MANIFEST.MF
        val pack = target.getClass.getPackage
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
        PipelineStepInfo(className, lastChangedVersion, Some(srcUrl))
      } catch {
        // Fall-back.  If the config doesn't exist or doesn't contain the expected information
        // Then get the version number from MANIFEST.MF and don't give a source URL
        case ex: Exception => target.getClass.getPackage.getImplementationVersion match {
          case null => PipelineStepInfo(className, lastChangedVersion)
          case buildId => PipelineStepInfo(className, lastChangedVersion)
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

  def unchangedSince: String = ("" +: updateVersionHistory).last

  /** Whenever the logic of this class is updated, the corresponding release number should
    * be added to this list.  The unchangedSince field will be set to the latest version that is
    * still earlier than the version in the jar file.
    */
  def updateVersionHistory: Seq[String] = List()
}
