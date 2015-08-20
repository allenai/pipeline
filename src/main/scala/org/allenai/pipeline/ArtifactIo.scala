package org.allenai.pipeline

import org.allenai.common.Resource
import org.allenai.pipeline.IoHelpers._
import spray.json.JsonFormat

import scala.io.{ Codec, Source }
import scala.reflect.ClassTag

trait ArtifactIo[T, -A <: Artifact]
  extends Serializer[T, A] with Deserializer[T, A]

/** Interface for defining how to persist a data type.
  *
  * @tparam  T  the type of the data being serialized
  * @tparam  A  the type of the artifact being written (e.g. FileArtifact)
  */
trait Serializer[-T, -A <: Artifact] extends PipelineStep {
  def write(data: T, artifact: A): Unit
}

/** Interface for defining how to persist a data type.
  *
  * @tparam  T  the type of the data being serialized
  * @tparam  A  the type of the artifact being read (e.g. FileArtifact)
  */
trait Deserializer[+T, -A <: Artifact] extends PipelineStep {
  def read(artifact: A): T
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
class SingletonIo[T: StringSerializable: ClassTag](implicit codec: Codec)
    extends ArtifactIo[T, FlatArtifact]
    with Ai2SimpleStepInfo {
  override def read(artifact: FlatArtifact): T = {
    Resource.using(Source.fromInputStream(artifact.read)) { src =>
      implicitly[StringSerializable[T]].fromString(src.mkString)
    }
  }

  override def write(data: T, artifact: FlatArtifact): Unit = artifact.write {
    _.write(implicitly[StringSerializable[T]].toString(data))
  }

  override def stepInfo: PipelineStepInfo = {
    val className = scala.reflect.classTag[T].runtimeClass.getSimpleName
    super.stepInfo.copy(
      className = s"ReadObject[$className]",
      parameters = Map("charSet" -> codec.charSet.toString),
      description = Some(s"Read [$className] into memory")
    )
  }
}

object SingletonIo {
  def text[T: StringSerializable: ClassTag](implicit codec: Codec): ArtifactIo[T, FlatArtifact] =
    new SingletonIo[T]

  def json[T: JsonFormat: ClassTag](implicit codec: Codec): ArtifactIo[T, FlatArtifact] = {
    implicit val format: StringSerializable[T] = asStringSerializable(implicitly[JsonFormat[T]])
    new SingletonIo[T]
  }
}

/** Persist a collection of string-serializable objects to a flat file, one line per object.  */
class LineCollectionIo[T: StringSerializable: ClassTag](implicit codec: Codec)
    extends ArtifactIo[Iterable[T], FlatArtifact] with Ai2SimpleStepInfo {
  private val delegate = new LineIteratorIo[T]

  override def read(artifact: FlatArtifact): Iterable[T] =
    delegate.read(artifact).toList

  override def write(data: Iterable[T], artifact: FlatArtifact): Unit =
    delegate.write(data.iterator, artifact)

  override def stepInfo: PipelineStepInfo = {
    val className = scala.reflect.classTag[T].runtimeClass.getSimpleName
    super.stepInfo.copy(
      className = s"ReadCollection[$className]",
      parameters = Map("charSet" -> codec.charSet.toString),
      description = Some(s"Read collection of [$className] into memory")
    )
  }

}

object LineCollectionIo {
  def text[T: StringSerializable: ClassTag](
    implicit
    codec: Codec
  ): ArtifactIo[Iterable[T], FlatArtifact] =
    new LineCollectionIo[T]

  def json[T: JsonFormat: ClassTag](
    implicit
    codec: Codec
  ): ArtifactIo[Iterable[T], FlatArtifact] = {
    implicit val format: StringSerializable[T] = asStringSerializable(implicitly[JsonFormat[T]])
    new LineCollectionIo[T]
  }

}

/** Persist an iterator of string-serializable objects to a flat file, one line per object.  */
class LineIteratorIo[T: StringSerializable: ClassTag](implicit codec: Codec)
    extends ArtifactIo[Iterator[T], FlatArtifact] with Ai2SimpleStepInfo {
  val format = implicitly[StringSerializable[T]]

  override def read(artifact: FlatArtifact): Iterator[T] =
    StreamClosingIterator(artifact.read) { is =>
      Source.fromInputStream(is).getLines().map(s => format.fromString(s))
    }

  override def write(data: Iterator[T], artifact: FlatArtifact): Unit = {
    artifact.write { w =>
      for (d <- data)
        w.println(format.toString(d))
    }
  }

  override def stepInfo: PipelineStepInfo = {
    val className = scala.reflect.classTag[T].runtimeClass.getSimpleName
    super.stepInfo.copy(
      className =
      s"ReadIterator[$className]",
      parameters = Map("charSet" -> codec.charSet.toString),
      description = Some(s"Stream iterator of [$className]")
    )
  }
}

object LineIteratorIo {
  def text[T: StringSerializable: ClassTag](
    implicit
    codec: Codec
  ): ArtifactIo[Iterator[T], FlatArtifact] =
    new LineIteratorIo[T]

  def json[T: JsonFormat: ClassTag](
    implicit
    codec: Codec
  ): ArtifactIo[Iterator[T], FlatArtifact] = {
    implicit val format: StringSerializable[T] = asStringSerializable(implicitly[JsonFormat[T]])
    new LineIteratorIo[T]
  }
}
