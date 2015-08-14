package org.allenai.pipeline.hackathon

import java.io.{ SequenceInputStream, File, FileInputStream, InputStream }
import java.net.URI
import java.util.Collections

import org.allenai.common.Resource
import org.allenai.pipeline._

/** Computes a checksum of a local file and replicates it to a shared store
  */
case class ReplicateFile(
  file: File,
  checksum: Option[String],
  rootOutputUrl: URI,
  artifactFactory: ArtifactFactory
)
    extends Producer[File] with Ai2SimpleStepInfo {
  override def create = {
    checksum match {
      case None =>
        if (!artifact.exists) {
          UploadFile.write(file, artifact)
        }
        file
      case Some(x) =>
        if (x != resolvedChecksum) {
          UploadFile.write(file, artifact)
        }
        file
    }
  }

  private lazy val resolvedChecksum = {
    checksum match {
      case None if file.exists =>
        InputStreamChecksum(new FileInputStream(file))
      case None if !file.exists =>
        sys.error(s"$file does not exist.")
      case Some(x) if !file.exists =>
        x
      case Some(x) if file.exists =>
        InputStreamChecksum(new FileInputStream(file))
    }
  }

  lazy val artifact = artifactForChecksum(resolvedChecksum)

  override def stepInfo =
    super.stepInfo.copy(
      className = file.getName,
      outputLocation = Some(artifact.url)
    )
      .addParameters("src" -> artifact.url)

  private def artifactForChecksum(checksum: String) = {
    val idx = file.getName.lastIndexOf('.')
    val name =
      if (idx < 0) {
        s"${file.getName}.$checksum"
      } else {
        val prefix = file.getName.take(idx - 1)
        val suffix = file.getName.drop(idx)
        s"$prefix.$checksum.$suffix"
      }
    artifactFactory.createArtifact[FlatArtifact](rootOutputUrl, name)
  }

}

case class ReplicateDirectory(
  file: File,
  checksum: Option[String],
  rootOutputUrl: URI,
  artifactFactory: ArtifactFactory
)
    extends Producer[File] with Ai2SimpleStepInfo {
  require(file.isDirectory, s"$file not a directory")
  override def create = {
    checksum match {
      case None =>
        if (!artifact.exists) {
          UploadDirectory.write(file, artifact)
        }
        file
      case Some(x) =>
        if (x != resolvedChecksum) {
          UploadDirectory.write(file, artifact)
        }
        file
    }
  }

  private lazy val resolvedChecksum = {
    checksum match {
      case None if file.exists =>
        InputStreamChecksum.forDirectory(file)
      case None if !file.exists =>
        sys.error(s"$file does not exist.")
      case Some(x) if !file.exists =>
        x
      case Some(x) if file.exists =>
        InputStreamChecksum.forDirectory(file)
    }
  }

  lazy val artifact = artifactForChecksum(resolvedChecksum)

  override def stepInfo =
    super.stepInfo.copy(
      className = file.getName,
      outputLocation = Some(artifact.url)
    )
      .addParameters("src" -> artifact.url)

  private def artifactForChecksum(checksum: String) = {
    val idx = file.getName.lastIndexOf('.')
    val name =
      if (idx < 0) {
        s"${file.getName}.$checksum.zip"
      } else {
        val prefix = file.getName.take(idx - 1)
        val suffix = file.getName.drop(idx)
        s"$prefix.$checksum.$suffix"
      }
    artifactFactory.createArtifact[StructuredArtifact](rootOutputUrl, name)
  }

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
