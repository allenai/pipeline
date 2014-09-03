package org.allenai.pipeline

import java.io.File

import org.allenai.pipeline.TsvFormats._
import spray.json.JsonFormat

import scala.language.implicitConversions
import scala.reflect.ClassTag

/** Utility methods for Artifact reading/writing
  */
object IOHelpers {

  /** General deserialization method */
  def ReadFromArtifact[T, A <: Artifact](io: ArtifactIO[T, A], artifact: A): Producer[T] = {
    require(artifact.exists, s"$artifact does not exist")
    new PersistedProducer(null, io, artifact)
  }

  /** General deserialization method */
  def ReadFromArtifactProducer[T, A <: Artifact](io: ArtifactIO[T, A], src: Producer[A]): Producer[T] = new Producer[T] {
    def create = io.read(src.get)
  }

  /** Factory interface for creating Artifact instances */
  trait FlatArtifactFactory[T] {
    def flatArtifact(input: T): FlatArtifact
  }

  trait StructuredArtifactFactory[T] {
    def structuredArtifact(input: T): StructuredArtifact
  }

  class FileSystem(rootDir: Option[File]) extends FlatArtifactFactory[String] with StructuredArtifactFactory[String] {
    def this() = this(None)

    def this(rootDir: File) = this(Some(rootDir))

    private def toFile(path: String): File = rootDir match {
      case None => new File(path)
      case Some(dir) => new File(dir, path)
    }

    def flatArtifact(name: String) = new FileArtifact(toFile(name))

    def structuredArtifact(name: String) = {
      val file = toFile(name)
      if (file.exists && file.isDirectory) {
        new DirectoryArtifact(file)
      } else {
        new ZipFileArtifact(file)
      }
    }
  }

  object FileSystem extends FlatArtifactFactory[File] with StructuredArtifactFactory[File] {
    def flatArtifact(file: File) = new FileArtifact(file)

    def structuredArtifact(file: File) = {
      if (file.exists && file.isDirectory) {
        new DirectoryArtifact(file)
      } else
        new ZipFileArtifact(file)
    }
  }

  implicit object IdentityFlatArtifactFactory extends FlatArtifactFactory[FlatArtifact] {
    def flatArtifact(a: FlatArtifact) = a
  }

  implicit object IdentityStructuredArtifactFactory extends StructuredArtifactFactory[StructuredArtifact] {
    def structuredArtifact(a: StructuredArtifact) = a
  }

  class S3(config: S3Config, rootPath: Option[String] = None) extends FlatArtifactFactory[String] with StructuredArtifactFactory[String] {
    def this(config: S3Config, rootPath: String) = this(config, Some(rootPath))

    // Drop leading and training slashes
    private def toPath(path: String): String = rootPath match {
      case None => path
      case Some(dir) => {
        val base = dir.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
        s"$base/$path"
      }
    }

    def flatArtifact(path: String) = new S3FlatArtifact(toPath(path), config)

    def structuredArtifact(path: String) = new S3ZipArtifact(toPath(path), config)
  }

  /** Read collection of type T from flat file */
  def ReadTsvAsCollection[T: StringStorable](artifact: FlatArtifact): Producer[Iterable[T]] = ReadFromArtifact(new TsvCollectionIO[T], artifact)

  /** Read iterator of type T from flat file */
  def ReadTsvAsIterator[T: StringStorable](artifact: FlatArtifact): Producer[Iterator[T]] = ReadFromArtifact(new TsvIteratorIO[T], artifact)

  /** Read a collection of arrays of a single type from a flat file */
  def ReadTsvAsArrayCollection[T: StringStorable : ClassTag](artifact: FlatArtifact, sep: String = "\t"): Producer[Iterable[Array[T]]] =
    ReadFromArtifact(new TsvCollectionIO[Array[T]]()(tsvArrayFormat[T](sep)), artifact)

  /** Read an iterator of arrays of a single type from a flat file */
  def ReadTsvAsArrayIterator[T: StringStorable : ClassTag](artifact: FlatArtifact, sep: String = "\t"): Producer[Iterator[Array[T]]] =
    ReadFromArtifact(new TsvIteratorIO[Array[T]]()(tsvArrayFormat[T](sep)), artifact)

  /** Read collection of json-serializable objects */
  def ReadJsonAsCollection[T: JsonFormat](artifact: FlatArtifact): Producer[Iterable[T]] = ReadFromArtifact(new JsonCollectionIO[T], artifact)

  /** Read iterator of json-serializable objects */
  def ReadJsonAsIterator[T: JsonFormat](artifact: FlatArtifact): Producer[Iterator[T]] = ReadFromArtifact(new JsonIteratorIO[T], artifact)

  /** Read single json-serializable object */
  def ReadJsonAsSingleton[T: JsonFormat](artifact: FlatArtifact): Producer[T] = {
    val collectionProducer = ReadJsonAsCollection[T](artifact)
    new Producer[T] {
      def create = collectionProducer.get.head
    }
  }

  /** A Pipeline step wrapper for in-memory data */
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

  implicit def enableSaveJsonSingleton[T: JsonFormat](step: Producer[T]) = new JsonPersistable[T] {
    def saveAsJson[I: FlatArtifactFactory](input: I) = step.persisted(new JsonSingletonIO[T], implicitly[FlatArtifactFactory[I]].flatArtifact(input))
  }

  implicit def enableSaveJsonCollection[T: JsonFormat](step: Producer[Iterable[T]]) = new JsonPersistable[Iterable[T]] {
    def saveAsJson[I: FlatArtifactFactory](input: I) = step.persisted(new JsonCollectionIO[T], implicitly[FlatArtifactFactory[I]].flatArtifact(input))
  }

  implicit def enableSaveJsonIterator[T: JsonFormat](step: Producer[Iterator[T]]) = new JsonPersistable[Iterator[T]] {
    def saveAsJson[I: FlatArtifactFactory](input: I) = step.persisted(new JsonIteratorIO[T], implicitly[FlatArtifactFactory[I]].flatArtifact(input))
  }

  implicit def enableSaveTsvCollection[T: StringStorable](step: Producer[Iterable[T]]) = new TsvPersistable[Iterable[T]] {
    def saveAsTsv[I: FlatArtifactFactory](input: I) = step.persisted(new TsvCollectionIO[T], implicitly[FlatArtifactFactory[I]].flatArtifact(input))
  }

  implicit def enableSaveTsvIterator[T: StringStorable](step: Producer[Iterator[T]]) = new TsvPersistable[Iterator[T]] {
    def saveAsTsv[I: FlatArtifactFactory](input: I) = step.persisted(new TsvIteratorIO[T], implicitly[FlatArtifactFactory[I]].flatArtifact(input))
  }

}
