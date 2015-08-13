package org.allenai.pipeline

import java.io.File
import java.net.URI

import sys.process._

case class ReadFromURL(url: URI) extends Producer[File] with Ai2StepInfo {
  override def create: File = {
    val outputFile = File.createTempFile(url.getPath().replace("/", "$"), "")
    (url.toURL #> outputFile).!!
    outputFile
  }

  override def stepInfo = super.stepInfo.copy(outputLocation = Some(url))
}
