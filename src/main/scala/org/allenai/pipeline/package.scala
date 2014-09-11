package org.allenai

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by rodneykinney on 9/10/14.
 */
package object pipeline {
  def codeVersionOf(obj: Any) = {
    obj.getClass.getPackage.getImplementationVersion match {
      case null => s"local-${System.getProperty("user.name")}-${
        new SimpleDateFormat
        ("YYYY-MM-dd:hh:mm:ss").format(new Date())
      }"
      case s if s.endsWith("SNAPSHOT") => s"$s-${System.getProperty("user.name")}-${
        new SimpleDateFormat
        ("YYYY-MM-dd:hh:mm:ss").format(new Date())
      }"
      case s => s
    }
  }
}
