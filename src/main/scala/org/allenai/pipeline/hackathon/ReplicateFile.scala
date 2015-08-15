package org.allenai.pipeline.hackathon

import org.allenai.common.Resource
import org.allenai.pipeline._

import java.io.{ File, FileInputStream, InputStream, SequenceInputStream }
import java.util.Collections

case class ReplicateFile(
    name: String,
    resource: Either[File, FlatArtifact],
    createArtifact: String => FlatArtifact
) extends ReplicateResource[FlatArtifact](name, resource, createArtifact) {
  override protected[this] def computeChecksum(file: File) =
    InputStreamChecksum(new FileInputStream(file))

  override protected[this] def upload(file: File) = UploadFile.write(file, artifact)

  protected[this] def download(artifact: FlatArtifact): File = UploadFile.read(artifact)
}

case class ReplicateDirectory(
    name: String,
    resource: Either[File, StructuredArtifact],
    createArtifact: String => StructuredArtifact
) extends ReplicateResource[StructuredArtifact](name, resource, createArtifact) {
  override protected[this] def computeChecksum(file: File) =
    InputStreamChecksum.forDirectory(file)

  override protected[this] def upload(file: File) = UploadDirectory.write(file, artifact)

  protected[this] def download(artifact: StructuredArtifact): File = UploadDirectory.read(artifact)
}

/** If resource is an Artifact, then it has already been uploaded. Download and return a local copy
  * If resource is a File, compute a checksum, upload the file if a copy with that checksum
  * has not been uploaded, and return the original file
  */
abstract class ReplicateResource[T <: Artifact](
  name: String,
  resource: Either[File, T],
  createArtifact: String => T
)
    extends Producer[File] with Ai2SimpleStepInfo {
  override def create = {
    resource match {
      case Left(file) =>
        if (!artifact.exists) {
          upload(file)
        }
        file
      case Right(artifact) =>
        download(artifact)
    }
  }

  protected[this] def upload(file: File)

  protected[this] def download(artifact: T): File

  protected[this] def computeChecksum(file: File)

  lazy val artifact: T =
    resource match {
      case Left(file) =>
        val checksum = computeChecksum(file)
        val idx = file.getName.lastIndexOf('.')
        val fileName =
          if (idx < 0) {
            s"${file.getName}.$checksum"
          } else {
            val prefix = file.getName.take(idx - 1)
            val suffix = file.getName.drop(idx)
            s"$prefix.$checksum.$suffix"
          }
        createArtifact(fileName)
      case Right(a) => a
    }

  override def stepInfo =
    super.stepInfo.copy(
      className = name,
      outputLocation = Some(artifact.url)
    )
      .addParameters("src" -> artifact.url)
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
    import scala.collection.JavaConverters._
    val streams =
      for (f <- dir.listFiles.sorted) yield {
        new FileInputStream(f)
      }
    this(new SequenceInputStream(Collections.enumeration(streams.toList.asJava)))
  }
}
