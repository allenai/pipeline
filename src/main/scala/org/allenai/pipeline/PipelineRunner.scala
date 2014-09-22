package org.allenai.pipeline

import java.io.File
import org.allenai.pipeline.IoHelpers._

import java.util.UUID

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
    val sig = Signature("experiment", UUID.randomUUID.toString)
    val outputPath = path(sig, "html")
    SingletonIo.text[String].write(Workflow.renderHtml(workflow),
      persistence.flatArtifact(outputPath))
    SingletonIo.json[Workflow].write(workflow, flatArtifact((sig, "json")))
    outputPath
  }

}

object PipelineRunner {
  def sandbox(dir: File) = {
    val persistence = new RelativeFileSystem(dir)
    val rootDirPath = dir.getCanonicalPath
    new PipelineRunner(persistence) {
      def path(signature: Signature, suffix: String) = s"$rootDirPath/${signature.name}.$suffix"
    }
  }

  def s3(config: S3Config, rootPath: String) = {
    val persistence = new S3(config, Some(rootPath))
    new PipelineRunner(persistence) {
      def path(signature: Signature, suffix: String) = s"$rootPath/${
        signature
          .name
      }/${signature.name}.${signature.id}.$suffix"
    }
  }
}
