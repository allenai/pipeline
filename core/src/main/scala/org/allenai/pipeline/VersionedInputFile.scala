package org.allenai.pipeline

import org.allenai.common.Resource

import java.io.InputStream
import java.net.URI

/** Creates a versioned copy of an input file from an original location (assumed local)
  * to a backup location (assumed shared)
  * The intended usage is that a pipeline can use local filesystem resources
  * as inputs, and when a pipeline is run, the local contents are copied transparently
  * to a shared location, allowing other users to share the inputs of the pipeline run.
  * If the contents of the local resource change in between pipeline runs, the new versions will
  * be uploaded with different names.
  */
class VersionedInputFile(
    original: FileArtifact,
    createVersionedArtifact: String => FlatArtifact
) extends FlatArtifact {
  protected[this] lazy val versioned = {
    require(original.exists, s"Read-only artifact $original must exist")
    // Compute hash of contents
    val id = {
      var hash = 0L
      val buffer = new Array[Byte](16384)
      Resource.using(original.read) { is =>
        Iterator.continually(is.read(buffer)).takeWhile(_ != -1)
          .foreach(n => buffer.take(n).foreach(b => hash = hash * 31 + b))
      }
      hash.toHexString
    }
    // Create artifact from the version id
    val versioned = createVersionedArtifact(id)
    // Copy contents to versioned copy
    if (!versioned.exists) {
      original.copyTo(versioned)
    }
    versioned
  }

  override def read: InputStream = versioned.read

  override def write[T](writer: (ArtifactStreamWriter) => T): T =
    sys.error(s"$original is read-only")

  override def url: URI = versioned.url

  override def exists: Boolean = versioned.exists
}
