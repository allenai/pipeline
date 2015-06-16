package org.allenai.pipeline

import org.allenai.common.Resource

import java.io.InputStream
import java.net.URI

/**
 * Replicates an artifact from a primary location (assumed local)
 * to a backup location (assumed shared)
 * The intended usage is that a pipeline can use local filesystem resources
 * as inputs, and when a pipeline is run, the local contents are copied transparently
 * to a shared location, allowing other users to share the inputs of the pipeline run.
 * If the contents of the local resource change, they are given a different name
 * when uploaded.
 */
class ReplicatedFlatArtifact(
  primary: FlatArtifact,
  backupUrl: String => URI,
  artifactFactory: ArtifactFactory
  ) extends FlatArtifact {
  private lazy val backup = {
    require(primary.exists,s"Read-only artifact $primary must exist")
    // Compute hash of contents
    val id = {
      var hash = 0L
      val buffer = new Array[Byte](16384)
      Resource.using(primary.read) { is =>
        Iterator.continually(is.read(buffer)).takeWhile(_ != -1)
          .foreach(n => buffer.take(n).foreach(b => hash = hash * 31 + b))
      }
      hash.toHexString
    }
    // Create artifact from the url
    val backupArtifact = artifactFactory.createArtifact[FlatArtifact](backupUrl(id))
    // Copy contents to backup location
    if (!backupArtifact.exists) {
      primary.copyTo(backupArtifact)
    }
    backupArtifact
  }

  override def read: InputStream = backup.read

  override def write[T](writer: (ArtifactStreamWriter) => T): T =
    sys.error(s"$primary is read-only")

  override def url: URI = backup.url

  override def exists: Boolean = backup.exists
}
