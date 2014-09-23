package org.allenai.pipeline

import spray.json.JsonFormat

import scala.reflect.ClassTag

trait ReadHelpers extends ColumnFormats {
  /** General deserialization method. */
  def readFromArtifact[T, A <: Artifact](io: ArtifactIo[T, A], artifact: A): Producer[T] = {
    require(artifact.exists, s"$artifact does not exist")
    new PersistedProducer(null, io, artifact) {
      override def signature = Signature(io.toString,
        io.codeInfo.unchangedSince, "src" -> artifact.path)
      override def codeInfo = io.codeInfo
    }
  }

  /** General deserialization method. */
  def readFromArtifactProducer[T, A <: Artifact](io: ArtifactIo[T, A],
                                                 src: Producer[A]): Producer[T] =
    new Producer[T] with UnknownCodeInfo {
      def create = io.read(src.get)

      def signature = Signature(io.toString,
        io.codeInfo.unchangedSince, "src" -> src)
    }

  object Read {

    /** Read single object from flat file */
    object singleton {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact): Producer[T] =
        readFromArtifact(SingletonIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact): Producer[T] =
        readFromArtifact(SingletonIo.json[T], artifact)
    }

    /** Read collection of type T from flat file. */
    object collection {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact):
      Producer[Iterable[T]] =
        readFromArtifact(LineCollectionIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact): Producer[Iterable[T]] =
        readFromArtifact(LineCollectionIo.json[T], artifact)
    }

    /** Read iterator of type T from flat file. */
    object iterator {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact):
      Producer[Iterator[T]] =
        readFromArtifact(LineIteratorIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact): Producer[Iterator[T]] =
        readFromArtifact(LineIteratorIo.json[T], artifact)
    }

    /** Read a collection of arrays of a single type from a flat file. */
    object arrayCollection {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact,
                                                     sep: Char = '\t'): Producer[Iterable[Array[T]]] = {
        val io = LineCollectionIo.text[Array[T]](columnArrayFormat[T](sep), implicitly[ClassTag[T]].wrap)
        readFromArtifact(io, artifact)
      }
    }

    /** Read an iterator of arrays of a single type from a flat file. */
    object arrayIterator {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact,
                                                     sep: Char = '\t'): Producer[Iterator[Array[T]]] = {
        val io = LineIteratorIo.text[Array[T]](columnArrayFormat[T](sep), implicitly[ClassTag[T]].wrap)
        readFromArtifact(io, artifact)
      }
    }

  }

}
