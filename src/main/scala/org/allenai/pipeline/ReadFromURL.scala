package org.allenai.pipeline

import java.io.{ FileOutputStream, File }
import java.net.URI

import org.allenai.common.Resource
import org.apache.commons.io.IOUtils

import scala.sys.process._

case class ReadFromUrl(url: URI) extends Producer[File] with Ai2StepInfo {
  override def create: File = {
    val outputFile = File.createTempFile(url.getPath().replace("/", "$") + "url", "")
    Resource.using(new FileOutputStream(outputFile)) {
      w => IOUtils.copy(url.toURL.openConnection.getInputStream, w)
    }
    outputFile
  }

  protected[this] def resourceName = {
    url.getPath match {
      case "/" | "" => url.toString
      case path => path.split('/').last
    }
  }

  override def stepInfo = super.stepInfo.copy(outputLocation = Some(url), className = resourceName)
}
