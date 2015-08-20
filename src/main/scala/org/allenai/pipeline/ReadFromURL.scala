package org.allenai.pipeline

import java.io.File
import java.net.URI

import scala.sys.process._

case class ReadFromURL(url: URI, name: String = "ReadFromURL",
    dataUrl: Option[URI] = None) extends Producer[File] with Ai2StepInfo {
  override def create: File = {
    val outputFile = File.createTempFile(url.getPath().replace("/", "$") + "url", "")
    (dataUrl.getOrElse(url).toURL #> outputFile).!!
    outputFile
  }

  override def stepInfo = super.stepInfo.copy(outputLocation = Some(url), className = name)
}
