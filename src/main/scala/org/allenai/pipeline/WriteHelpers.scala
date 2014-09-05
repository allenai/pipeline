package org.allenai.pipeline

import spray.json.JsonFormat

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

  class RelativeFileSystem(rootDir: File)
    extends FlatArtifactFactory[String] with StructuredArtifactFactory[String] {
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

      override def structuredArtifact(path: String) =
        AbsoluteFileSystem.structuredArtifact(new File(path))
    }
  }

  implicit object IdentityFlatArtifactFactory
    extends FlatArtifactFactory[FlatArtifact] {
    override def flatArtifact(a: FlatArtifact) = a
  }

  implicit object IdentityStructuredArtifactFactory
    extends StructuredArtifactFactory[StructuredArtifact] {
    override def structuredArtifact(a: StructuredArtifact) = a
  }

  class S3(config: S3Config, rootPath: Option[String] = None)
    extends FlatArtifactFactory[String] with StructuredArtifactFactory[String] {
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

  object PersistedSingleton {
    def text[T: StringSerializable, I: FlatArtifactFactory]
    (input: I)(step: Producer[T]): PersistedProducer[T, FlatArtifact] = {
      step.persisted(SingletonIo.text[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }

    def json[T: JsonFormat, I: FlatArtifactFactory]
    (input: I)(step: Producer[T]): PersistedProducer[T, FlatArtifact] = {
      step.persisted(SingletonIo.json[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }
  }

  object PersistedCollection {
    def text[T: StringSerializable, I: FlatArtifactFactory]
    (input: I)(step: Producer[Iterable[T]]): PersistedProducer[Iterable[T], FlatArtifact] = {
      step.persisted(LineCollectionIo.text[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }

    def json[T: JsonFormat, I: FlatArtifactFactory]
    (input: I)(step: Producer[Iterable[T]]): PersistedProducer[Iterable[T], FlatArtifact] = {
      step.persisted(LineCollectionIo.json[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }
  }

  object PersistedIterator {
    def text[T: StringSerializable, I: FlatArtifactFactory]
    (input: I)(step: Producer[Iterator[T]]): PersistedProducer[Iterator[T], FlatArtifact] = {
      step.persisted(LineIteratorIo.text[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }

    def json[T: JsonFormat, I: FlatArtifactFactory]
    (input: I)(step: Producer[Iterator[T]]): PersistedProducer[Iterator[T], FlatArtifact] = {
      step.persisted(LineIteratorIo.json[T],
        implicitly[FlatArtifactFactory[I]].flatArtifact(input))
    }
  }

}
