package org.allenai.pipeline

import java.io.{ InputStream, OutputStream }
import java.net.URI
import java.nio.charset.StandardCharsets

import org.allenai.common.Resource

/** Represents data in a persistent store. */
trait Artifact {
  /** Return true if this data has been written to the persistent store. */
  def exists: Boolean
  def url: URI
}

/** Generic data blob.  */
trait FlatArtifact extends Artifact {
  /** Reading from a flat file gives an InputStream.
    * The client code is responsible for closing this.  This is necessary to support streaming.
    */
  def read: InputStream

  /** The write interface enforces atomic writes to a FlatArtifact
    * The client code is unable to close any OutputStream, nor to keep it open after the writer
    * function terminates.  Furthermore, if a write operation fails (throws an Exception),
    * the resulting Artifact must not be created.
    */
  def write[T](writer: ArtifactStreamWriter => T): T

  private val BUFFER_SIZE = 16384
  def copyTo(other: FlatArtifact): Unit = {
    other.write { writer =>
      val buffer = new Array[Byte](BUFFER_SIZE)
      Resource.using(read) { is =>
        Iterator.continually(is.read(buffer)).takeWhile(_ != -1).foreach(n =>
          writer.write(buffer, 0, n))
      }
    }
  }
}

object StructuredArtifact {

  trait Reader {
    /** Read a single entry by name. */
    def read(entryName: String): InputStream

    /** Read all entries in order. */
    def readAll: Iterator[(String, InputStream)]
  }

  trait Writer {
    /** Write a single entry. */
    def writeEntry[T](name: String)(writer: ArtifactStreamWriter => T): T
  }

}

/** Artifact with nested structure, containing multiple data blobs identified by String names.
  * Only one level of structure is supported.
  */
trait StructuredArtifact extends Artifact {

  import org.allenai.pipeline.StructuredArtifact.{ Reader, Writer }

  def reader: Reader

  /** Like FlatArtifacts, the write interface enforces atomic writes to a StructuredArtifact
    * Client code cannot open/close an OutputStream, and a failed write should create no Artifact.
    */
  def write[T](writer: Writer => T): T

  def copyTo(other: StructuredArtifact): Unit = {
    other.write { writer =>
      for ((name, is) <- reader.readAll) {
        // scalastyle:off
        val buffer = new Array[Byte](16384)
        // scalastyle:on
        writer.writeEntry(name) { entryWriter =>
          Iterator.continually(is.read(buffer)).takeWhile(_ != -1).foreach(n => entryWriter.
            write(buffer, 0, n))
          is.close()
        }
      }
    }
  }
}

/** Class for writing that exposes a more restrictive interface than OutputStream
  * In particular, we don't want clients to close the stream
  * Also, we force character encoding to UTF-8.
  */
class ArtifactStreamWriter(out: OutputStream) {
  def write(data: Array[Byte]): Unit = {
    out.write(data, 0, data.length)
  }

  def write(data: Array[Byte], offset: Int, size: Int): Unit = {
    out.write(data, offset, size)
  }

  def write(s: String): Unit = {
    write(asUTF8(s))
  }

  def println(s: String): Unit = {
    write(s)
    out.write('\n')
  }

  private def asUTF8(s: String): Array[Byte] = s.getBytes(StandardCharsets.UTF_8)
}
