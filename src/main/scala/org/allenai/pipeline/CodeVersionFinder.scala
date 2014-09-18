package org.allenai.pipeline

import java.net.URL
import java.text.SimpleDateFormat
import java.util.Date

case class CodeVersion(versionId: String, url: Option[URL])

case class MavenCodeVersion(major: Int,
                            minor: Option[Int],
                            incremental: Option[Int],
                            build: Option[Int],
                            qualifier: Option[String],
                            url: Option[URL]) extends Comparable[MavenCodeVersion] {
  override def compareTo(other: MavenCodeVersion) = {
    major compareTo other.major match {
      case 0 => (minor, other.minor) match {
        case (None, None) => 0
        case (Some(v), None) => 1
        case (None, Some(ov)) => -1
        case (Some(v), Some(ov)) => v compareTo ov match {
          case 0 => (incremental, other.incremental) match {
            case (None, None) => 0
            case (Some(_), None) => 1
            case (None, Some(_)) => -1
            case (Some(v), Some(ov)) => v compareTo ov match {
              case 0 => (build, other.build) match {
                case (None, None) => 0
                case (Some(_), None) => 1
                case (None, Some(_)) => -1
                case (Some(v), Some(ov)) => v compareTo ov match {
                  case 0 => (qualifier, other.qualifier) match {
                    case (None, None) => 0
                    case (Some(_), None) => -1 // Having a qualifier indicates an earlier version
                    case (None, Some(_)) => 1
                    case (Some(v), Some(ov)) => v compareTo ov
                  }
                  case i => i

                }
              }
              case i => i
            }
          }
          case i => i
        }
      }
      case i => i
    }
  }
}

object MavenCodeVersion {
  val versionRegex = """^(\d+)(.(\d+))?(.(\d+))?(-([^-]*))?(-([^-]*))?$""".r

  def apply(versionId: String, url: Option[URL]): Option[MavenCodeVersion] = {
    versionRegex findFirstIn versionId match {
      case Some(versionRegex(major, _, minor, _, incremental, _, suffix1, _, suffix2)) => try {
        val (build: Option[Int], qualifier: Option[String]) = (suffix1, suffix2) match {
          case (null, null) => (None, None)
          case (s, null) => try {
            (Some(s.toInt), None)
          }
          catch {
            case ex: Exception => (None, Some(s))
          }
          case (s1, s2) => (Some(s1.toInt), Some(s2))
        }
        Some(new MavenCodeVersion(major.toInt,
          if (minor == null) None else Some(minor.toInt),
          if (incremental == null) None else Some(incremental.toInt),
          build,
          qualifier,
          url))
      }
      catch {
        case ex: Exception => None
      }
      case _ => None
    }
  }
}

trait CodeVersionFinder {
  def codeVersion(obj: Any): CodeVersion
}

object JarNameCodeVersionFinder extends CodeVersionFinder {
  def codeVersion(obj: Any) = {
    val id = obj.getClass.getPackage.getImplementationVersion match {
      case null => {
        val date = new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss").format(new Date())
        s"local-${System.getProperty("user.name")}-$date"
      }
      case s if s.endsWith("SNAPSHOT") => {
        val date = new SimpleDateFormat("yyyy-MM-dd:hh:mm:ss").format(new Date())
        s"$s-${System.getProperty("user.name")}-$date"
      }
      case s => s
    }
    CodeVersion(id, None)
  }
}

class FixedNameCodeVersionFinder(name: String) extends CodeVersionFinder {
  def codeVersion(obj: Any) = CodeVersion(name, None)
}

class VersionHistoryCodeVersionFinder()