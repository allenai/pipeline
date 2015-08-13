package org.allenai.pipeline.hackathon

import java.io.{ FileInputStream, InputStream, File }
import java.net.URI

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
    extends Producer[File] {
  override def create = {
    checksum match {
      case None if file.exists =>
        val checksum = InputStreamChecksum(new FileInputStream(file))
        val artifact = artifactForChecksum(checksum)
        if (!artifact.exists) {
          UploadFile.write(file, artifact)
        }
        file
      case None if !file.exists =>
        sys.error(s"$file does not exist.")
      case Some(x) if !file.exists =>
        UploadFile.read(artifactForChecksum(x))
      case Some(x) if file.exists =>
        val checksum = InputStreamChecksum(new FileInputStream(file))
        if (checksum != x) {
          UploadFile.write(file, artifactForChecksum(checksum))
        }
        file
    }
  }

  override def stepInfo =
    PipelineStepInfo(file.getName)

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
}
