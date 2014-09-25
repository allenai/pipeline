package org.allenai.pipeline

import org.allenai.pipeline.IoHelpers._

import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

/** Executes a pipeline represented by a set of Producer instances
  * Inspects the meta-info about the pipeline steps (represented by PipelineRunnerSupport interface)
  * and builds a DAG representation of the pipeline.  Visualizes the DAG in HTML and stores the
  * HTML page along with the pipeline output.
  * The output location of each pipeline step is not specified by the code that builds the
  * pipeline.  Instead, each step's output location is determined by the PipelineRunner based on
  * the Signature of that step.  This allows independent processes for pipelines with overlapping
  * steps in their DAGs to re-use past calculations.
  * @param persistence
  */
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

  /** Run the pipeline and return a URL pointing to the experiment-visualization page */
  def run[T](outputs: Producer[_]*): URI = {
    val workflow = Workflow.forPipeline(outputs: _*)
    outputs.foreach(_.get)
    val today = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())
    val version = s"${System.getProperty("user.name")}-$today"
    val sig = Signature("experiment", version)
    val htmlArtifact = persistence.flatArtifact(s"experiment-$version.html")
    SingletonIo.text[String].write(Workflow.renderHtml(workflow), htmlArtifact)
    val jsonArtifact = persistence.flatArtifact(s"experiment-$version.json")
    SingletonIo.json[Workflow].write(workflow, jsonArtifact)
    htmlArtifact.url
  }

}

object PipelineRunner {
  /** Store results in a single directory */
  def writeToDirectory(dir: File) = {
    val persistence = new RelativeFileSystem(dir)
    new PipelineRunner(persistence) {
      def path(signature: Signature, suffix: String) = s"${signature.name}" +
        s".${signature.id}.$suffix"
    }
  }

  /** Store results in S3 */
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
