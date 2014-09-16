package org.allenai.pipeline

import java.text.SimpleDateFormat
import java.util.Date

case class CodeVersion(id: String, url: Option[String])

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