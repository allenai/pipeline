package org.allenai.pipeline

import org.allenai.common.Resource

import spray.json._

import scala.io.Source

/** Interface for defining how to persist a data type.  */
trait ArtifactIo[T, -A <: Artifact] {
  def read(artifact: A): T

  def write(data: T, artifact: A): Unit
}

/** Persist a collection of json-serializable objects to a flat file.  */
class JsonSingletonIo[T: JsonFormat] extends ArtifactIo[T, FlatArtifact] {
  def read(artifact: FlatArtifact): T = {
    Resource.using(Source.fromInputStream(artifact.read)) { src =>
      src.mkString.parseJson.convertTo[T]
    }
  }

  def write(data: T, artifact: FlatArtifact): Unit = artifact.write { _.write(data.toJson.prettyPrint) }
}

/** Persist a collection of json-serializable objects to a flat file, one line per object.  */
class JsonCollectionIo[T: JsonFormat] extends ArtifactIo[Iterable[T], FlatArtifact] {
  private val delegate = new JsonIteratorIo[T]

  def read(artifact: FlatArtifact): Iterable[T] = delegate.read(artifact).toList

  def write(data: Iterable[T], artifact: FlatArtifact): Unit = delegate.write(data.iterator, artifact)
}

/** Persist an iterator of json-serializable objects to a flat file, one line per object.  */
class JsonIteratorIo[T: JsonFormat] extends ArtifactIo[Iterator[T], FlatArtifact] {
  def read(artifact: FlatArtifact): Iterator[T] =
    StreamClosingIterator(artifact.read) { is =>
      Source.fromInputStream(is).getLines.map(_.parseJson.convertTo[T])
    }

  def write(data: Iterator[T], artifact: FlatArtifact): Unit = {
    artifact.write { w =>
      for (d <- data)
        w.println(d.toJson.compactPrint)
    }
  }
}
