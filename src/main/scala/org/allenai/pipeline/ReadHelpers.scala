package org.allenai.pipeline

import spray.json.JsonFormat

import scala.reflect.ClassTag

trait ReadHelpers extends ColumnFormats {

  object Read {
    /** General deserialization method. */
    def fromArtifact[T, A <: Artifact](io: ArtifactIo[T, A], artifact: A): Producer[T] = {
      require(artifact.exists, s"$artifact does not exist")
      new PersistedProducer(null, io, artifact) {
        override def signature = Signature(io.toString,
          io.codeInfo.unchangedSince, "src" -> artifact.path)

        override def codeInfo = io.codeInfo
      }
    }

    /** General deserialization method. */
    def fromArtifactProducer[T, A <: Artifact](io: ArtifactIo[T, A],
                                               src: Producer[A]): Producer[T] =
      src.copy(create = () => io.read(src.get),
        signature = () => Signature(io.toString, io.codeInfo.unchangedSince, "src" -> src),
        codeInfo = () => io.codeInfo)

    /** Read single object from flat file */
    object singleton {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact): Producer[T] =
        fromArtifact(SingletonIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact): Producer[T] =
        fromArtifact(SingletonIo.json[T], artifact)
    }

    /** Read collection of type T from flat file. */
    object collection {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact):
      Producer[Iterable[T]] =
        fromArtifact(LineCollectionIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact): Producer[Iterable[T]] =
        fromArtifact(LineCollectionIo.json[T], artifact)
    }

    /** Read iterator of type T from flat file. */
    object iterator {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact):
      Producer[Iterator[T]] =
        fromArtifact(LineIteratorIo.text[T], artifact)

      def fromJson[T: JsonFormat : ClassTag](artifact: FlatArtifact): Producer[Iterator[T]] =
        fromArtifact(LineIteratorIo.json[T], artifact)
    }

    /** Read a collection of arrays of a single type from a flat file. */
    object arrayCollection {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact,
                                                     sep: Char = '\t'): Producer[Iterable[Array[T]]] = {
        val io = LineCollectionIo.text[Array[T]](columnArrayFormat[T](sep), implicitly[ClassTag[T]].wrap)
        fromArtifact(io, artifact)
      }
    }

    /** Read an iterator of arrays of a single type from a flat file. */
    object arrayIterator {
      def fromText[T: StringSerializable : ClassTag](artifact: FlatArtifact,
                                                     sep: Char = '\t'): Producer[Iterator[Array[T]]] = {
        val io = LineIteratorIo.text[Array[T]](columnArrayFormat[T](sep), implicitly[ClassTag[T]].wrap)
        fromArtifact(io, artifact)
      }
    }

  }

}
