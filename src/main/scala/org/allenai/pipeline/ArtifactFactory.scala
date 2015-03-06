package org.allenai.pipeline

import java.io.File
import java.net.URI

/** Factory interface for creating flat Artifact instances. */
trait FlatArtifactFactory[T] {
  def flatArtifact(input: T): FlatArtifact
}

/** Factory interface for creating structured Artifact instances. */
trait StructuredArtifactFactory[T] {
  def structuredArtifact(input: T): StructuredArtifact
}

trait ArtifactFactory[T] extends FlatArtifactFactory[T] with StructuredArtifactFactory[T]

object ArtifactFactory {
  def fromUrl(outputUrl: URI): ArtifactFactory[String] = {
    outputUrl match {
      case url if url.getScheme == "s3" || url.getScheme == "s3n" => new S3(S3Config(url.getHost), Some(url.getPath))
      case url if url.getScheme == "file" || url.getScheme == null =>
        new RelativeFileSystem(new File(url.getPath))
      case _ => sys.error(s"Illegal dir: $outputUrl")
    }
  }
}

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
