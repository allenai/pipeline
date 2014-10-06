package org.allenai.pipeline

import org.allenai.pipeline.IoHelpers._

import scala.reflect.ClassTag

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
  * @param persistence factory for turning paths into Artifacts
  */
class PipelineRunner(
  persistence: ArtifactFactory[String])
    extends ArtifactFactory[(Signature, String)] {

  def flatArtifact(signatureSuffix: (Signature, String)): FlatArtifact = {
    val (signature, suffix) = signatureSuffix
    persistence.flatArtifact(path(signature, suffix))
  }

  def structuredArtifact(signatureSuffix: (Signature, String)): StructuredArtifact = {
    val (signature, suffix) = signatureSuffix
    persistence.structuredArtifact(path(signature, suffix))
  }

  def persist[T, A <: Artifact: ClassTag](producer: Producer[T], io: ArtifactIo[T, A],
    suffix: String): PersistedProducer[T, A] = {
    implicitly[ClassTag[A]].runtimeClass match {
      case c if c == classOf[FlatArtifact] => producer.persisted(io,
        flatArtifact((producer.signature, suffix)).asInstanceOf[A])
      case c if c == classOf[StructuredArtifact] => producer.persisted(io,
        structuredArtifact((producer.signature, suffix)).asInstanceOf[A])
      case _ => sys.error(s"Cannot persist using io class of unknown type $io")
    }
  }

  def path(signature: Signature, suffix: String): String =
    s"${signature.name}.${signature.id}.$suffix"

  /** Run the pipeline and return a URL pointing to the experiment-visualization page */
  def run[T](outputs: Producer[_]*): URI = {
    val workflow = Workflow.forPipeline(outputs: _*)
    outputs.foreach(_.get)
    val today = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())
    val version = s"${System.getProperty("user.name")}-$today"
    val sig = Signature("experiment", version)
    val (htmlArtifact, workflowArtifact, signatureArtifact) =
      (for {
        i <- (0 to 100).iterator
        h = persistence.flatArtifact(s"experiment-$version.$i.html")
        w = persistence.flatArtifact(s"experiment-$version.$i.workflow.json")
        s = persistence.flatArtifact(s"experiment-$version.$i.signatures.json")
        if !h.exists && !w.exists && !s.exists
      } yield (h, w, s)).next()
    SingletonIo.text[String].write(Workflow.renderHtml(workflow), htmlArtifact)
    SingletonIo.json[Workflow].write(workflow, workflowArtifact)
    import spray.json.DefaultJsonProtocol._
    val signatureFormat = Signature.jsonWriter
    val signatures = outputs.map(p => signatureFormat.write(p.signature)).toList.toJson
    signatureArtifact.write { writer => writer.write(signatures.prettyPrint) }
    htmlArtifact.url
  }

}

object PipelineRunner {
  /** Store results in a single directory */
  def writeToDirectory(dir: File): PipelineRunner = {
    val persistence = new RelativeFileSystem(dir)
    new PipelineRunner(persistence)
  }

  /** Store results in S3 */
  def writeToS3(config: S3Config, rootPath: String): PipelineRunner = {
    val persistence = new S3(config, Some(rootPath))
    new PipelineRunner(persistence)
  }
}
