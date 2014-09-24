package org.allenai.pipeline

import java.io.File
import org.allenai.pipeline.IoHelpers._

import java.text.SimpleDateFormat
import java.util.{Date, UUID}

abstract class PipelineRunner(persistence: FlatArtifactFactory[String] with
  StructuredArtifactFactory[String])
  extends FlatArtifactFactory[(Signature, String)]
  with StructuredArtifactFactory[(Signature, String)] {

  def flatArtifact(signatureSuffix: (Signature, String)): FlatArtifact = {
    val (signature, suffix) = signatureSuffix
    persistence.flatArtifact(path(signature, suffix))
  }

  def structuredArtifact(signatureSuffix: (Signature, String)): StructuredArtifact = {
    val (signature, suffix) = signatureSuffix
    persistence.structuredArtifact(path(signature, suffix))
  }

  def path(signature: Signature, suffix: String): String

  def run[T](outputs: Producer[_]*): String = {
    outputs.foreach(_.get)
    val workflow = Workflow.forPipeline(outputs: _*)
    val version = s"${System.getProperty("user.name")}-${
      new SimpleDateFormat
      ("YYYY-MM-dd:HH:mm:ss").format(new Date())
    }"
    val sig = Signature("experiment", version)
    val outputPath = path(sig, "html")
    SingletonIo.text[String].write(Workflow.renderHtml(workflow),
      persistence.flatArtifact(outputPath))
    SingletonIo.json[Workflow].write(workflow, flatArtifact((sig, "json")))
    outputPath
  }

}

object PipelineRunner {
  def writeToDirectory(dir: File) = {
    val persistence = new RelativeFileSystem(dir)
    new PipelineRunner(persistence) {
      def path(signature: Signature, suffix: String) = s"${signature.name}" +
        s".${signature.id}.$suffix"
    }
  }

  def writeToS3(config: S3Config, rootPath: String) = {
    val persistence = new S3(config, Some(rootPath))
    new PipelineRunner(persistence) {
      def path(signature: Signature, suffix: String) = s"${
        signature
          .name
      }/${signature.name}.${signature.id}.$suffix"
    }
  }
}
