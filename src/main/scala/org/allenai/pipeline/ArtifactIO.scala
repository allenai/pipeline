package org.allenai.pipeline

import org.allenai.common.Resource
import spray.json._

/** Interface for defining how to persist a data type
  */
trait ArtifactIO[T, -A <: Artifact] {
  def read(artifact: A): T

  def write(data: T, artifact: A): Unit
}

/** Persist a collection of json-serializable objects to a flat file
  * @tparam T
  */
class JsonSingletonIO[T: JsonFormat] extends ArtifactIO[T, FlatArtifact] {
  def read(artifact: FlatArtifact): T = {
    Resource.using(io.Source.fromInputStream(artifact.read)) { src =>
      src.mkString.parseJson.convertTo[T]
    }
  }

  def write(data: T, artifact: FlatArtifact): Unit = artifact.write { _.write(data.toJson.prettyPrint) }
}

/** Persist a collection of json-serializable objects to a flat file, one line per object
  * @tparam T
  */
class JsonCollectionIO[T: JsonFormat] extends ArtifactIO[Iterable[T], FlatArtifact] {
  private val delegate = new JsonIteratorIO[T]

  def read(artifact: FlatArtifact): Iterable[T] = delegate.read(artifact).toList

  def write(data: Iterable[T], artifact: FlatArtifact): Unit = delegate.write(data.iterator, artifact)
}

/** Persist an iterator of json-serializable objects to a flat file, one line per object
  * @tparam T
  */
class JsonIteratorIO[T: JsonFormat] extends ArtifactIO[Iterator[T], FlatArtifact] {
  def read(artifact: FlatArtifact): Iterator[T] =
    StreamClosingIterator(artifact.read) { is =>
      io.Source.fromInputStream(is).getLines.map(_.parseJson.convertTo[T])
    }

  def write(data: Iterator[T], artifact: FlatArtifact): Unit = {
    artifact.write { w =>
      for (d <- data)
        w.println(d.toJson.compactPrint)
    }
  }
}
