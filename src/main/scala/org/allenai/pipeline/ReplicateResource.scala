package org.allenai.pipeline

import java.io.{ SequenceInputStream, InputStream, FileInputStream, File }
import java.util.Collections

import scala.collection.JavaConverters._

import org.allenai.common.Resource

/** Replicates a file (or directory) to a shared location
  *
  * If resource is an Artifact, then it has already been uploaded:
  * Download and return a local copy
  * If resource is a File:
  * compute a checksum,
  * upload the file (if a copy with that checksum has not already been uploaded),
  * return the original file
  */
abstract class ReplicateResource[T <: Artifact](
  resource: Either[(File, String => T), (T, T => String)]
)
    extends Producer[File] with Ai2SimpleStepInfo {

  protected[this] def upload(file: File)

  protected[this] def download(artifact: T): File

  protected[this] def computeChecksum(file: File): String

  override def create = {
    resource match {
      case Left((file, _)) =>
        if (!artifact.exists) {
          upload(file)
        }
        file
      case Right((artifact, _)) =>
        download(artifact)
    }
  }

  lazy val artifact: T =
    resource match {
      case Left((file, createArtifact)) =>
        val checksum = computeChecksum(file)
        val idx = file.getName.lastIndexOf('.')
        val fileName =
          if (idx < 0) {
            s"${file.getName}.$checksum"
          } else {
            val prefix = file.getName.take(idx)
            val suffix = file.getName.drop(idx + 1)
            s"$prefix.$checksum.$suffix"
          }
        createArtifact(fileName)
      case Right((a, _)) => a
    }

  lazy val resourceName =
    resource match {
      case Left((file, createArtifact)) => file.getName
      case Right((a, shortName)) => shortName(a)
    }

  override def stepInfo =
    super.stepInfo.copy(
      className = resourceName,
      outputLocation = Some(artifact.url)
    )
      .addParameters("src" -> artifact.url)
}

case class ReplicateFile(
  resource: Either[(File, String => FlatArtifact), (FlatArtifact, FlatArtifact => String)]
)
    extends ReplicateResource[FlatArtifact](resource) {
  override protected[this] def computeChecksum(file: File) =
    InputStreamChecksum(new FileInputStream(file))

  override protected[this] def upload(file: File) = UploadFile.write(file, artifact)

  override protected[this] def download(artifact: FlatArtifact): File = UploadFile.read(artifact)
}

case class ReplicateDirectory(
    resource: Either[(File, String => StructuredArtifact), (StructuredArtifact, StructuredArtifact => String)]
) extends ReplicateResource[StructuredArtifact](resource) {
  override protected[this] def computeChecksum(file: File) =
    InputStreamChecksum.forDirectory(file)

  override protected[this] def upload(file: File) = UploadDirectory.write(file, artifact)

  override protected[this] def download(artifact: StructuredArtifact): File = UploadDirectory.read(artifact)
}

object InputStreamChecksum {
  def apply(input: InputStream) = {
    var hash = 0L
    val buffer = new Array[Byte](16384)
    Resource.using(input) { is =>
      Iterator.continually(is.read(buffer)).takeWhile(_ != -1)
        .foreach(n => buffer.take(n).foreach(b => hash = hash * 31 + b))
    }
    hash.toHexString
  }

  def forDirectory(dir: File) = {
    val streams =
      for (f <- dir.listFiles.sorted) yield {
        new FileInputStream(f)
      }
    this(new SequenceInputStream(Collections.enumeration(streams.toList.asJava)))
  }
}

