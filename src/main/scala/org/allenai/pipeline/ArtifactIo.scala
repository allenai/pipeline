package org.allenai.pipeline

import org.allenai.common.Resource
import org.allenai.pipeline.IoHelpers._

import spray.json.JsonFormat

import scala.io.Source

/** Interface for defining how to persist a data type.  */
trait ArtifactIo[T, -A <: Artifact] {
  def read(artifact: A): T

  def write(data: T, artifact: A): Unit
}

// Classes below implement the common case of serializing an object to a string,
// typically using JSON or delimited columns,
// then serializing a collection or iterator of objects with one per line in a flat file

/** Serialize an object to/from a String */
trait StringSerializable[T] {
  def fromString(s: String): T

  def toString(param: T): String
}

/** Persist a single object to a flat file.  */
class SingletonIo[T: StringSerializable] extends ArtifactIo[T, FlatArtifact] {
  override def read(artifact: FlatArtifact): T = {
    Resource.using(Source.fromInputStream(artifact.read)) { src =>
      implicitly[StringSerializable[T]].fromString(src.mkString)
    }
  }

  override def write(data: T, artifact: FlatArtifact): Unit = artifact.write {
    _.write(implicitly[StringSerializable[T]].toString(data))
  }
}

object SingletonIo {
  def text[T: StringSerializable]: ArtifactIo[T, FlatArtifact] = new SingletonIo[T]

  def json[T: JsonFormat]: ArtifactIo[T, FlatArtifact] = {
    implicit val format: StringSerializable[T] = asStringSerializable(implicitly[JsonFormat[T]])
    new SingletonIo[T]
  }
}

/** Persist a collection of string-serializable objects to a flat file, one line per object.  */
class LineCollectionIo[T: StringSerializable] extends ArtifactIo[Iterable[T], FlatArtifact] {
  private val delegate = new LineIteratorIo[T]

  override def read(artifact: FlatArtifact): Iterable[T] = delegate.read(artifact).toList

  override def write(data: Iterable[T], artifact: FlatArtifact): Unit = delegate.write(data.iterator, artifact)
}

object LineCollectionIo {
  def text[T: StringSerializable]: ArtifactIo[Iterable[T], FlatArtifact] = new LineCollectionIo[T]

  def json[T: JsonFormat]: ArtifactIo[Iterable[T], FlatArtifact] = {
    implicit val format: StringSerializable[T] = asStringSerializable(implicitly[JsonFormat[T]])
    new LineCollectionIo[T]
  }
}

/** Persist an iterator of string-serializable objects to a flat file, one line per object.  */
class LineIteratorIo[T: StringSerializable] extends ArtifactIo[Iterator[T], FlatArtifact] {
  val format = implicitly[StringSerializable[T]]

  override def read(artifact: FlatArtifact): Iterator[T] =
    StreamClosingIterator(artifact.read) { is =>
      Source.fromInputStream(is).getLines.map(s => format.fromString(s))
    }

  override def write(data: Iterator[T], artifact: FlatArtifact): Unit = {
    artifact.write { w =>
      for (d <- data)
        w.println(format.toString(d))
    }
  }
}

object LineIteratorIo {
  def text[T: StringSerializable]: ArtifactIo[Iterator[T], FlatArtifact] = new LineIteratorIo[T]

  def json[T: JsonFormat]: ArtifactIo[Iterator[T], FlatArtifact] = {
    implicit val format: StringSerializable[T] = asStringSerializable(implicitly[JsonFormat[T]])
    new LineIteratorIo[T]
  }
}
