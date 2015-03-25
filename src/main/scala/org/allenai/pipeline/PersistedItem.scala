package org.allenai.pipeline

import com.amazonaws.services.s3.{ AmazonS3Client, AmazonS3 }

import scala.reflect.ClassTag

import java.io.File

/** File-like object that can be checked for existence / read / written.
  * Abstracts over an absolute path and a serialization method.
  * Examples: flat file (single file), RDD (multiple files).
  *
  * The content of a FileItem is unreliable in presence of write interruptions.
  * Use a Producer[T].persisted(...) to ensure write atomicity.
  */
trait PersistedItem[T] {
  def exists: Boolean
  def read: T
  def write(t: T)
}
/** Partial FileItem, still needing the relative path. */
trait PersistedItemFactory[T] {
  def withPath(path: String): PersistedItem[T]
}
/** Operations on files. */
trait Storage {
  // TODO(cristipp) Implement this using logic from RddPersister.
  def prefixScan(prefix: String): Seq[String] = Seq()
}
/** Factory for PartialFileItems containing one structured element. */
trait FlatStorage {
  def flat[T: StringSerializable: ClassTag]: PersistedItemFactory[T]
}

/** Simple implementation of FlatFileSystem using Artifact library. */
abstract class ArtifactFlatStorage extends FlatStorage {
  protected def newArtifact(path: String): FlatArtifact

  def flat[T: StringSerializable: ClassTag]: PersistedItemFactory[T] = new PersistedItemFactory[T] {
    def withPath(path: String): PersistedItem[T] = new PersistedItem[T] {
      val artifact = newArtifact(path)
      def exists: Boolean = {
        artifact.exists
      }
      def read: T = {
        SingletonIo.text[T].read(artifact)
      }
      def write(t: T) = {
        SingletonIo.text[T].write(t, artifact)
      }
    }
  }
}
class LocalFlatStorage(prefixDir: String) extends ArtifactFlatStorage {
  protected override def newArtifact(path: String) = {
    new FileArtifact(new File(s"$prefixDir/$path"))
  }
}
class S3FlatStorage(s3: AmazonS3, bucket: String, prefixDir: String) extends ArtifactFlatStorage {
  protected override def newArtifact(path: String) =
    new S3FlatArtifact(s"$prefixDir/$path", S3Config(new AmazonS3Client(), bucket))
}

/** Importing Spark is a major dependency hassle. Defer to s2-offline :( */
//trait SparkFileSystem extends FlatFileSystem {
//  def rdd[T]: PartialDiskItem[RDD[T]]
//}

