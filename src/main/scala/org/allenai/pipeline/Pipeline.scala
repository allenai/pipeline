package org.allenai.pipeline

import org.allenai.common.Logging
import org.allenai.pipeline.IoHelpers._

import com.typesafe.config.Config
import spray.json.DefaultJsonProtocol._

import scala.reflect.ClassTag

import java.io.File
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

/** A fully-configured end-to-end pipeline
  * A top-level main can be constructed as:
  * object Foo extends App with Step1 with Step2 {
  * run(step1, step2)
  * }
  */
trait Pipeline extends Logging {
  def config: Config
  lazy val input: ArtifactFactory[String] =
    fromUrl(new URI(config.getString("input.dir")))
  // Initializing a tuple.
  lazy val (dataOutput: ArtifactFactory[String], summaryOutput: ArtifactFactory[String]) = {
    val url = new URI(config.getString("output.dir"))
    val path = url.getPath.stripSuffix("/")
    val dataDir = new URI(url.getScheme, url.getHost, s"$path/data", null)
    logger.info(s"Writing output to $dataDir")
    val summaryDir = new URI(url.getScheme, url.getHost, s"$path/summary", null)
    (fromUrl(dataDir),
        fromUrl(summaryDir))
  }

  def persist[T, A <: Artifact : ClassTag](
    original: Producer[T], io: ArtifactIo[T, A], path: String
  ) = {
    implicitly[ClassTag[A]].runtimeClass match {
      case c if c == classOf[FlatArtifact] => original.persisted(
        io,
        dataOutput.flatArtifact(path).asInstanceOf[A]
      )
      case c if c == classOf[StructuredArtifact] => original.persisted(
        io,
        dataOutput.structuredArtifact(path).asInstanceOf[A]
      )
      case _ => sys.error(s"Cannot persist using io class of unknown type $io")
    }
  }

  def getAutoPersistPath[T, A <: Artifact](original: Producer[T], io: ArtifactIo[T, A], suffix: String): String =
    s"${original.stepInfo.signature.name}.${original.stepInfo.signature.id}$suffix"

  def autoPersist[T, A <: Artifact : ClassTag](
    original: Producer[T], io: ArtifactIo[T, A], suffix: String
  ): Producer[T] = {
    val path = getAutoPersistPath(original, io, suffix)
    persist(original, io, path)
  }

  def optionallyPersist[T, A <: Artifact : ClassTag](stepName: String, suffix: String)(
    original: Producer[T], io: ArtifactIo[T, A]
  ): Producer[T] = {

    val configKey = s"output.persist.$stepName"
    if (config.hasPath(configKey)) {
      config.getValue(configKey).unwrapped() match {
        case java.lang.Boolean.TRUE =>
          persist(original, io, getAutoPersistPath(original, io, suffix))
        case path: String =>
          persist(original, io, path)
        case _ => original
      }
    } else {
      original
    }

  }

  def run(rawTitle: String, outputs: Producer[_]*): Unit = {
    try {
      require(
        outputs.forall(_.isInstanceOf[PersistedProducer[_, _]]),
        s"Running a pipeline without persisting the output: ${
          outputs.filter(p => !p
              .isInstanceOf[PersistedProducer[_, _]]).map(_.stepInfo.className).mkString(",")
        }}"
      )

      val start = System.currentTimeMillis
      outputs.foreach(_.get)
      val duration = (System.currentTimeMillis - start) / 1000.0
      logger.info(f"Ran pipeline in $duration%.3f s")

      val title = rawTitle.replaceAll( """\s+""", "-")
      val today = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss").format(new Date())

      val workflowArtifact = summaryOutput.flatArtifact(s"$title-$today.workflow.json")
      val workflow = Workflow.forPipeline(outputs: _*)
      SingletonIo.json[Workflow].write(workflow, workflowArtifact)

      val htmlArtifact = summaryOutput.flatArtifact(s"$title-$today.html")
      SingletonIo.text[String].write(Workflow.renderHtml(workflow), htmlArtifact)

      val signatureArtifact = summaryOutput.flatArtifact(s"$title-$today.signatures.json")
      val signatureFormat = Signature.jsonWriter
      val signatures = outputs.map(p => signatureFormat.write(p.stepInfo.signature)).toList.toJson
      signatureArtifact.write { writer => writer.write(signatures.prettyPrint)}

      logger.info(s"Summary written to ${toHttpUrl(htmlArtifact.url)}")
    } catch {
      case e: Throwable => logger.error("Untrapped exception", e)
    }
  }

  // Convert S3 URLs to an http: URL viewable in a browser
  def toHttpUrl(url: URI): URI = {
    url.getScheme match {
      case "s3" | "s3n" =>
        new java.net.URI("http", s"${url.getHost}.s3.amazonaws.com", url.getPath, null)
      case "file" =>
        new java.net.URI(null, null, url.getPath, null)
      case _ => url
    }
  }

  def fromUrl(outputUrl: URI): ArtifactFactory[String] = {
    outputUrl match {
      case url if url.getScheme == "s3" || url.getScheme == "s3n" => new S3(S3Config(url.getHost), Some(url.getPath))
      case url if url.getScheme == "file" || url.getScheme == null =>
        new RelativeFileSystem(new File(url.getPath))
      case _ => sys.error(s"Illegal dir: $outputUrl")
    }
  }

}
