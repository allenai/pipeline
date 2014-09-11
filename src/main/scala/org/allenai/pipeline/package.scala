package org.allenai

import java.text.SimpleDateFormat
import java.util.Date

/**
 * Created by rodneykinney on 9/10/14.
 */
package object pipeline {
  def codeVersionOf(obj: Any) = {
    obj.getClass.getPackage.getImplementationVersion match {
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
  }
}
