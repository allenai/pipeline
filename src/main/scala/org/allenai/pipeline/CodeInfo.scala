package org.allenai.pipeline

import java.net.URI
import java.util.UUID

case class CodeInfo(buildId: String,
                    unchangedSince: String,
                    srcUrl: Option[URI],
                    binaryUrl: Option[URI])

trait HasCodeInfo {
  def codeInfo: CodeInfo
}

trait UnknownCodeInfo extends HasCodeInfo {
  private lazy val uuid = UUID.randomUUID.toString

  override def codeInfo = CodeInfo(uuid, uuid, None, None)
}

trait Ai2CodeInfo extends HasCodeInfo {
  override def codeInfo: CodeInfo = {
    this.getClass.getPackage.getImplementationVersion match {
      case null =>
        val lastChange = ("0" +: updateVersionHistory).last
        CodeInfo(lastChange, lastChange, None, None)
      case buildId => CodeInfo(buildId, lastPrecedingChangeId(buildId), None, None)
    }
  }

  def updateVersionHistory: Seq[String] = List()

  def lastPrecedingChangeId(buildId: String): String = {
    MavenVersionId(buildId) match {
      case Some(myVersion) =>
        val previousVersions = ("0" +: updateVersionHistory).distinct.map(MavenVersionId.apply)
        require(previousVersions.forall(_.isDefined), s"Current version $myVersion cannot be compared " +
          s"with past version ${previousVersions.zip(updateVersionHistory).find(!_._1.isDefined).get._2}")
        val sortedVersions = previousVersions.flatten.sorted.reverse
        val latestEquivalentVersion = sortedVersions.find(
          v => v.compareTo(myVersion) <= 0 || v.equalsIgnoreQualifier(myVersion)).get
          .versionId
        latestEquivalentVersion
      case None => buildId
    }
  }
}

case class MavenVersionId(major: Int,
                          minor: Option[Int] = None,
                          incremental: Option[Int] = None,
                          build: Option[Int] = None,
                          qualifier: Option[String] = None) extends Comparable[MavenVersionId] {
  require(!incremental.isDefined || minor.isDefined, s"Invalid version $this")

  def versionId = s"""$major""" +
    s"""${minor.map(i => s".$i").getOrElse("")}""" +
    s"""${incremental.map(i => s".$i").getOrElse("")}""" +
    s"""${build.map(i => s"-$i").getOrElse("")}""" +
    s"""${qualifier.map(q => s"-$q").getOrElse("")}"""

  override def compareTo(other: MavenVersionId) = {
    def compare(a: Option[Int], b: Option[Int]) = (a, b) match {
      case (Some(v), None) => 1
      case (None, Some(ov)) => -1 // Note:  version 1 < version 1.0
      case (None, None) => 0
      case (Some(v), Some(ov)) => v compareTo ov
    }
    major compareTo other.major match {
      case 0 => compare(minor, other.minor) match {
        case 0 => compare(incremental, other.incremental) match {
          case 0 => compare(build, other.build) match {
            case 0 => (qualifier, other.qualifier) match {
              case (None, None) => 0
              case (Some(_), None) => -1 // Having a qualifier indicates an earlier version
              case (None, Some(_)) => 1
              case (Some(v), Some(ov)) => v compareTo ov
            }
            case i => i
          }
          case i => i
        }
        case i => i
      }
      case i => i
    }
  }

  def equalsIgnoreQualifier(other: MavenVersionId) =
    major == other.major && minor == other.minor &&
      incremental == other.incremental && build == other.build
}

object MavenVersionId {
  val versionRegex = """^(\d+)(?:.(\d+))?(?:.(\d+))?(?:(?:-|_)([^-]*))?(?:(?:-|_)([^-]*))?$""".r

  def apply(versionId: String): Option[MavenVersionId] = {
    versionRegex findFirstIn versionId match {
      case Some(versionRegex(major, minor, incremental, suffix1, suffix2)) => try {
        val (build: Option[Int], qualifier: Option[String]) = (suffix1, suffix2) match {
          case (null, null) => (None, None)
          case (s, null) => try {
            (Some(s.toInt), None)
          }
          catch {
            case ex: NumberFormatException => (None, Some(s))
          }
          case (s1, s2) => try {
            (Some(s1.toInt), Some(s2))
          }
          catch {
            case ex: NumberFormatException => (Some(s2.toInt), Some(s1))
          }
        }
        Some(new MavenVersionId(major.toInt,
          if (minor == null) None else Some(minor.toInt),
          if (incremental == null) None else Some(incremental.toInt),
          build,
          qualifier))
      }
      catch {
        case ex: NumberFormatException => None
      }
      case _ => None
    }
  }
}
