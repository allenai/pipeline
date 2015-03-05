package org.allenai.pipeline

import spray.json.JsonFormat

import scala.io.Codec
import scala.reflect.ClassTag

import java.net.URI

trait ReadHelpers extends ColumnFormats {

  object Read {
    /** General deserialization method. */
    def fromArtifact[T, A <: Artifact](reader: DeserializeFromArtifact[T, A], artifact: A): Producer[T] =
      new ReadFromArtifact(reader, artifact)

    /** Read single object from flat file */
    object Singleton {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact)(
        implicit
        codec: Codec
      ): Producer[T] =
        fromArtifact(SingletonIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact)(
        implicit
        codec: Codec
      ): Producer[T] =
        fromArtifact(SingletonIo.json[T], artifact)
    }

    /** Read collection of type T from flat file. */
    object Collection {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact)(
        implicit
        codec: Codec
      ): Producer[Iterable[T]] =
        fromArtifact(LineCollectionIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact)(
        implicit
        codec: Codec
      ): Producer[Iterable[T]] =
        fromArtifact(LineCollectionIo.json[T], artifact)
    }

    /** Read iterator of type T from flat file. */
    object Iterator {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact)(
        implicit
        codec: Codec
      ): Producer[Iterator[T]] =
        fromArtifact(LineIteratorIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact)(
        implicit
        codec: Codec
      ): Producer[Iterator[T]] =
        fromArtifact(LineIteratorIo.json[T], artifact)
    }

    /** Read a collection of arrays of a single type from a flat file. */
    object ArrayCollection {
      def fromText[T: StringSerializable : ClassTag](
        artifact: FlatArtifact,
        sep: Char = '\t'
      )(implicit codec: Codec): Producer[Iterable[Array[T]]] = {
        val io = {
          implicit val colFormat = columnArrayFormat[T](sep)
          implicit val arrayClassTag = implicitly[ClassTag[T]].wrap
          LineCollectionIo.text[Array[T]]
        }
        fromArtifact(io, artifact)
      }
    }

    /** Read an iterator of arrays of a single type from a flat file. */
    object ArrayIterator {
      def fromText[T: StringSerializable : ClassTag](
        artifact: FlatArtifact,
        sep: Char = '\t'
      )(implicit codec: Codec): Producer[Iterator[Array[T]]] = {
        val io = {
          implicit val colFormat = columnArrayFormat[T](sep)
          implicit val arrayClassTag = implicitly[ClassTag[T]].wrap
          LineIteratorIo.text[Array[T]]
        }
        fromArtifact(io, artifact)
      }
    }

  }

}
