package org.allenai.pipeline

import java.net.{URI, URL}
import java.text.SimpleDateFormat
import java.util.Date

case class CodeInfo(latestEquivalentVersionId: String, srcUrl: Option[URI], binaryUrl: Option[URI])

trait HasVersionHistory {
  def versionIds: Seq[String]
}

object VersionHistory {
  def latestEquivalentVersion(obj: Any): String = obj match {
    case hv: HasVersionHistory => {
      obj.getClass.getPackage.getImplementationVersion match {
        case null => null
        case objVersion => {
          val versionIds = hv.versionIds
          require(objVersion != null, "Cannot store data in repository when running from " +
            "locally-compiled code.  Please run 'sbt package' and execute from packaged jar files.")
          val previousVersions =
            Some(MavenVersionId(0, None, None, None, None)) +: versionIds.map(MavenVersionId.apply)
          MavenVersionId(objVersion) match {
            case Some(v) => {
              require(previousVersions.forall(_.isDefined), s"Current version $v cannot be compared " +
                s"with past version ${previousVersions.zip(versionIds).find(!_._1.isDefined).get._2}")
              val sortedVersions = previousVersions.flatten.sorted.reverse
              val latestEquivalentVersion = sortedVersions.find(_.compareTo(v) <= 0).get.versionId
              latestEquivalentVersion
            }
            case None => sys.error(s"Cannot parse current version number $objVersion")
          }
        }
      }
    }
    case _ => "0"
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
