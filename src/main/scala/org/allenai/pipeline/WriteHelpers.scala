package org.allenai.pipeline

import spray.json.JsonFormat

import scala.reflect.ClassTag

import java.io.File

trait WriteHelpers {

  /** Factory interface for creating flat Artifact instances. */
  trait FlatArtifactFactory[T] {
    def flatArtifact(input: T): FlatArtifact
  }

  /** Factory interface for creating structured Artifact instances. */
  trait StructuredArtifactFactory[T] {
    def structuredArtifact(input: T): StructuredArtifact
  }

  trait ArtifactFactory[T] extends FlatArtifactFactory[T] with StructuredArtifactFactory[T]

  class RelativeFileSystem(rootDir: File)
      extends ArtifactFactory[String] {
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

  object AbsoluteFileSystem extends ArtifactFactory[File] {
    override def flatArtifact(file: File): FlatArtifact = new FileArtifact(file)

    override def structuredArtifact(file: File): StructuredArtifact = {
      if (file.exists && file.isDirectory) {
        new DirectoryArtifact(file)
      } else {
        new ZipFileArtifact(file)
      }
    }

    def usingPaths: ArtifactFactory[String] =
      new ArtifactFactory[String] {
        override def flatArtifact(path: String): FlatArtifact =
          AbsoluteFileSystem.flatArtifact(new File(path))

        override def structuredArtifact(path: String): StructuredArtifact =
          AbsoluteFileSystem.structuredArtifact(new File(path))
      }
  }

  implicit object IdentityFlatArtifactFactory
      extends FlatArtifactFactory[FlatArtifact] {
    override def flatArtifact(a: FlatArtifact): FlatArtifact = a
  }

  implicit object IdentityStructuredArtifactFactory
      extends StructuredArtifactFactory[StructuredArtifact] {
    override def structuredArtifact(a: StructuredArtifact): StructuredArtifact = a
  }

  class S3(config: S3Config, rootPath: Option[String] = None)
      extends ArtifactFactory[String] {
    // Drop leading and training slashes
    private def toPath(path: String): String = rootPath match {
      case None => path
      case Some(dir) =>
        val base = dir.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
        s"$base/$path"
    }

    override def flatArtifact(path: String): FlatArtifact =
      new S3FlatArtifact(toPath(path), config)

    override def structuredArtifact(path: String): StructuredArtifact =
      new S3ZipArtifact(toPath(path), config)
  }

//  object Persist  {
//    // Reduce line length
//    type FAF[T] = FlatArtifactFactory[T]
//
//    object Iterator {
//      def asText[T: StringSerializable: ClassTag](step: Producer[Iterator[T]], path: String)(
//        implicit
//        faf: FAF[String]
//      ): PersistedProducer[Iterator[T], FlatArtifact] = {
//        step.persisted(
//          LineIteratorIo.text[T],
//          faf.flatArtifact(path)
//        )
//      }
//
//      def asJson[T: JsonFormat: ClassTag](step: Producer[Iterator[T]], path: String)(
//        implicit
//        faf: FAF[String]
//      ): PersistedProducer[Iterator[T], FlatArtifact] = {
//        step.persisted(
//          LineIteratorIo.json[T],
//          faf.flatArtifact(path)
//        )
//      }
//
//      def asText[T: StringSerializable: ClassTag](step: Producer[Iterator[T]])(
//        implicit
//        faf: FAF[(Signature, String)]
//      ): PersistedProducer[Iterator[T], FlatArtifact] = {
//        step.persisted(
//          LineIteratorIo.text[T],
//          faf.flatArtifact((step.signature, "txt"))
//        )
//      }
//
//      def asJson[T: JsonFormat: ClassTag](step: Producer[Iterator[T]])(
//        implicit
//        faf: FAF[(Signature, String)]
//      ): PersistedProducer[Iterator[T], FlatArtifact] = {
//        step.persisted(
//          LineIteratorIo.json[T],
//          faf.flatArtifact((step.signature, "json"))
//        )
//      }
//    }
//
//    object Collection {
//      def asText[T: StringSerializable: ClassTag](step: Producer[Iterable[T]], path: String)(
//        implicit
//        faf: FAF[String]
//      ): PersistedProducer[Iterable[T], FlatArtifact] = {
//        step.persisted(
//          LineCollectionIo.text[T],
//          faf.flatArtifact(path)
//        )
//      }
//
//      def asJson[T: JsonFormat: ClassTag](step: Producer[Iterable[T]], path: String)(
//        implicit
//        faf: FAF[String]
//      ): PersistedProducer[Iterable[T], FlatArtifact] = {
//        step.persisted(
//          LineCollectionIo.json[T],
//          faf.flatArtifact(path)
//        )
//      }
//
//      def asText[T: StringSerializable: ClassTag](step: Producer[Iterable[T]])(
//        implicit
//        faf: FAF[(Signature, String)]
//      ): PersistedProducer[Iterable[T], FlatArtifact] = {
//        step.persisted(
//          LineCollectionIo.text[T],
//          faf.flatArtifact((step.signature, "txt"))
//        )
//      }
//
//      def asJson[T: JsonFormat: ClassTag](step: Producer[Iterable[T]])(
//        implicit
//        faf: FAF[(Signature, String)]
//      ): PersistedProducer[Iterable[T], FlatArtifact] = {
//        step.persisted(
//          LineCollectionIo.json[T],
//          faf.flatArtifact((step.signature, "json"))
//        )
//      }
//
//    }
//
//    object Singleton {
//      def asText[T: StringSerializable: ClassTag](step: Producer[T], path: String)(
//        implicit
//        factory: FAF[String]
//      ): PersistedProducer[T, FlatArtifact] = {
//        step.persisted(
//          SingletonIo.text[T],
//          factory.flatArtifact(path)
//        )
//      }
//
//      def asJson[T: JsonFormat: ClassTag](step: Producer[T], path: String)(
//        implicit
//        factory: FAF[String]
//      ): PersistedProducer[T, FlatArtifact] = {
//        step.persisted(
//          SingletonIo.json[T],
//          factory.flatArtifact(path)
//        )
//      }
//
//      def asText[T: StringSerializable: ClassTag](step: Producer[T])(
//        implicit
//        faf: FAF[(Signature, String)]
//      ): PersistedProducer[T, FlatArtifact] = {
//        step.persisted(
//          SingletonIo.text[T],
//          faf.flatArtifact((step.signature, "txt"))
//        )
//      }
//
//      def asJson[T: JsonFormat: ClassTag](step: Producer[T])(
//        implicit
//        faf: FAF[(Signature, String)]
//      ): PersistedProducer[T, FlatArtifact] = {
//        step.persisted(
//          SingletonIo.json[T],
//          faf.flatArtifact((step.signature, "json"))
//        )
//      }
//
//    }
//
//  }

}
