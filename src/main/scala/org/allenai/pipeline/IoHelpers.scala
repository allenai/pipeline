package org.allenai.pipeline

import org.allenai.pipeline.TsvFormats._

import spray.json.JsonFormat

import scala.language.implicitConversions
import scala.reflect.ClassTag

import java.io.File

/** Utility methods for Artifact reading/writing.  */
object IoHelpers {

  /** General deserialization method. */
  def readFromArtifact[T, A <: Artifact](io: ArtifactIo[T, A], artifact: A): Producer[T] = {
    require(artifact.exists, s"$artifact does not exist")
    new PersistedProducer(null, io, artifact)
  }

  /** General deserialization method. */
  def readFromArtifactProducer[T, A <: Artifact](io: ArtifactIo[T, A],
    src: Producer[A]): Producer[T] = new Producer[T] {
    def create = io.read(src.get)
  }

  /** Factory interface for creating Artifact instances. */
  trait FlatArtifactFactory[T] {
    def flatArtifact(input: T): FlatArtifact
  }

  trait StructuredArtifactFactory[T] {
    def structuredArtifact(input: T): StructuredArtifact
  }

  class RelativeFileSystem(rootDir: File) extends FlatArtifactFactory[String] with StructuredArtifactFactory[String] {
    private def toFile(path: String): File = new File(rootDir, path)

    override def flatArtifact(name: String): FlatArtifact = new FileArtifact(toFile(name))

    override def structuredArtifact(name: String): StructuredArtifact = {
      val file = toFile(name)
      if (file.exists && file.isDirectory) {
        new DirectoryArtifact(file)
      } else {
        new ZipFileArtifact(file)
      }
    }
  }

  object AbsoluteFileSystem extends FlatArtifactFactory[File] with StructuredArtifactFactory[File] {
    override def flatArtifact(file: File) = new FileArtifact(file)

    override def structuredArtifact(file: File) = {
      if (file.exists && file.isDirectory) {
        new DirectoryArtifact(file)
      } else
        new ZipFileArtifact(file)
    }

    def usingPaths = new FlatArtifactFactory[String] with StructuredArtifactFactory[String] {
      override def flatArtifact(path: String) = AbsoluteFileSystem.flatArtifact(new File(path))

      override def structuredArtifact(path: String) = AbsoluteFileSystem.structuredArtifact(new File(path))
    }
  }

  implicit object IdentityFlatArtifactFactory extends FlatArtifactFactory[FlatArtifact] {
    override def flatArtifact(a: FlatArtifact) = a
  }

  implicit object IdentityStructuredArtifactFactory extends StructuredArtifactFactory[StructuredArtifact] {
    override def structuredArtifact(a: StructuredArtifact) = a
  }

  class S3(config: S3Config, rootPath: Option[String] = None) extends FlatArtifactFactory[String] with StructuredArtifactFactory[String] {
    // Drop leading and training slashes
    private def toPath(path: String): String = rootPath match {
      case None => path
      case Some(dir) => {
        val base = dir.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
        s"$base/$path"
      }
    }

    override def flatArtifact(path: String) = new S3FlatArtifact(toPath(path), config)

    override def structuredArtifact(path: String) = new S3ZipArtifact(toPath(path), config)
  }

  /** Read collection of type T from flat file. */
  def readTsvAsCollection[T: StringStorable](artifact: FlatArtifact): Producer[Iterable[T]] =
    readFromArtifact(new TsvCollectionIo[T], artifact)

  /** Read iterator of type T from flat file. */
  def readTsvAsIterator[T: StringStorable](artifact: FlatArtifact): Producer[Iterator[T]] =
    readFromArtifact(new TsvIteratorIo[T], artifact)

  /** Read a collection of arrays of a single type from a flat file. */
  def readTsvAsArrayCollection[T: StringStorable: ClassTag](artifact: FlatArtifact,
    sep: Char = '\t'): Producer[Iterable[Array[T]]] =
    readFromArtifact(new TsvCollectionIo[Array[T]]()(tsvArrayFormat[T](sep)), artifact)

  /** Read an iterator of arrays of a single type from a flat file. */
  def readTsvAsArrayIterator[T: StringStorable: ClassTag](artifact: FlatArtifact,
    sep: Char = '\t'): Producer[Iterator[Array[T]]] =
    readFromArtifact(new TsvIteratorIo[Array[T]]()(tsvArrayFormat[T](sep)), artifact)

  /** Read collection of json-serializable objects. */
  def readJsonAsCollection[T: JsonFormat](artifact: FlatArtifact): Producer[Iterable[T]] =
    readFromArtifact(new JsonCollectionIo[T], artifact)

  /** Read iterator of json-serializable objects. */
  def readJsonAsIterator[T: JsonFormat](artifact: FlatArtifact): Producer[Iterator[T]] =
    readFromArtifact(new JsonIteratorIo[T], artifact)

  /** Read single json-serializable object. */
  def readJsonAsSingleton[T: JsonFormat](artifact: FlatArtifact): Producer[T] = {
    val collectionProducer = readJsonAsCollection[T](artifact)
    new Producer[T] {
      def create = collectionProducer.get.head
    }
  }

  /** A Pipeline step wrapper for in-memory data. */
  object FromMemory {
    def apply[T](data: T): Producer[T] = new Producer[T] {
      def create = data
    }
  }

  trait JsonPersistable[T] {
    def saveAsJson[I: FlatArtifactFactory](input: I): Producer[T]
  }

  trait TsvPersistable[T] {
    def saveAsTsv[I: FlatArtifactFactory](input: I): Producer[T]
  }

  implicit def enableSaveJsonSingleton[T: JsonFormat](step: Producer[T]) =
    new JsonPersistable[T] {
      def saveAsJson[I: FlatArtifactFactory](input: I) = step.persisted(new JsonSingletonIo[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }

  implicit def enableSaveJsonCollection[T: JsonFormat](step: Producer[Iterable[T]]) =
    new JsonPersistable[Iterable[T]] {
      def saveAsJson[I: FlatArtifactFactory](input: I) = step.persisted(new JsonCollectionIo[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }

  implicit def enableSaveJsonIterator[T: JsonFormat](step: Producer[Iterator[T]]) =
    new JsonPersistable[Iterator[T]] {
      def saveAsJson[I: FlatArtifactFactory](input: I) = step.persisted(new JsonIteratorIo[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }

  implicit def enableSaveTsvCollection[T: StringStorable](step: Producer[Iterable[T]]) =
    new TsvPersistable[Iterable[T]] {
      def saveAsTsv[I: FlatArtifactFactory](input: I) = step.persisted(new TsvCollectionIo[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }

  implicit def enableSaveTsvIterator[T: StringStorable](step: Producer[Iterator[T]]) =
    new TsvPersistable[Iterator[T]] {
      def saveAsTsv[I: FlatArtifactFactory](input: I) = step.persisted(new TsvIteratorIo[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }

}
