package org.allenai.pipeline

import java.io.File
import java.net.URI

import spray.json._

import scala.reflect.ClassTag

/** Utility methods for Artifact reading/writing.  */
object IoHelpers extends ColumnFormats {

  object Read {
    /** General deserialization method. */
    def fromArtifact[T, A <: Artifact](
      reader: Deserializer[T, A],
      artifact: A
    ): Producer[T] =
      new ReadFromArtifact(reader, artifact.asInstanceOf[A])

    /** Read single object from flat file */
    object Singleton {
      def fromText[T: StringSerializable: ClassTag](artifact: FlatArtifact): Producer[T] =
        fromArtifact(SingletonIo.text[T], artifact)

      def fromJson[T: JsonFormat: ClassTag](artifact: FlatArtifact): Producer[T] =
        fromArtifact(SingletonIo.json[T], artifact)
    }

    /** Read collection of type T from flat file. */
    object Collection {
      def fromText[T: StringSerializable: ClassTag](artifact: FlatArtifact): Producer[Iterable[T]] =
        fromArtifact(LineCollectionIo.text[T], artifact)

      def fromJson[T: JsonFormat: ClassTag](artifact: FlatArtifact): Producer[Iterable[T]] =
        fromArtifact(LineCollectionIo.json[T], artifact)
    }

    /** Read iterator of type T from flat file. */
    object Iterator {
      def fromText[T: StringSerializable: ClassTag](artifact: FlatArtifact): Producer[Iterator[T]] =
        fromArtifact(LineIteratorIo.text[T], artifact)

      def fromJson[T: JsonFormat: ClassTag](artifact: FlatArtifact): Producer[Iterator[T]] =
        fromArtifact(LineIteratorIo.json[T], artifact)
    }

    /** Read a collection of arrays of a single type from a flat file. */
    object CollectionOfArrays {
      def fromText[T: StringSerializable: ClassTag](
        artifact: FlatArtifact,
        sep: Char = '\t'
      ): Producer[Iterable[Array[T]]] = {
        val io = {
          implicit val colFormat = columnArrayFormat[T](sep)
          implicit val arrayClassTag = implicitly[ClassTag[T]].wrap
          LineCollectionIo.text[Array[T]]
        }
        fromArtifact(io, artifact)
      }
    }

    /** Read an iterator of arrays of a single type from a flat file. */
    object IteratorOfArrays {
      def fromText[T: StringSerializable: ClassTag](
        artifact: FlatArtifact,
        sep: Char = '\t'
      ): Producer[Iterator[Array[T]]] = {
        val io = {
          implicit val colFormat = columnArrayFormat[T](sep)
          implicit val arrayClassTag = implicitly[ClassTag[T]].wrap
          LineIteratorIo.text[Array[T]]
        }
        fromArtifact(io, artifact)
      }
    }
  }
  import scala.language.implicitConversions

  implicit def asFileArtifact(f: File) = new FileArtifact(f)
  implicit def asStructuredArtifact(f: File): StructuredArtifact = f match {
    case f if f.exists && f.isDirectory => new DirectoryArtifact(f)
    case _ => new ZipFileArtifact(f)
  }
  implicit def asFlatArtifact(url: URI) =
    CreateCoreArtifacts.fromFileUrls.urlToArtifact[FlatArtifact].apply(url)
  implicit def asStructuredArtifact(url: URI) =
    CreateCoreArtifacts.fromFileUrls.urlToArtifact[StructuredArtifact].apply(url)

  implicit def asStringSerializable[T](jsonFormat: JsonFormat[T]): StringSerializable[T] =
    new StringSerializable[T] {
      override def fromString(s: String): T = jsonFormat.read(s.parseJson)

      override def toString(data: T): String = jsonFormat.write(data).compactPrint
    }

}
