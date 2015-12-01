package org.allenai.pipeline.s3

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.compress.compressors.gzip.{ GzipCompressorInputStream, GzipCompressorOutputStream }

import java.io._
import java.net.URI

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{ BasicAWSCredentials, EnvironmentVariableCredentialsProvider }
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ CannedAccessControlList, ObjectMetadata, PutObjectRequest }
import org.allenai.common.{ Resource, Logging }
import org.allenai.pipeline.StructuredArtifact.{ Reader, Writer }
import org.allenai.pipeline._

import java.nio.file.{ StandardCopyOption, Files }
import scala.annotation.tailrec
import scala.util.{ Success, Failure, Try }

case class S3Config(bucket: String, credentials: S3Credentials = S3Config.environmentCredentials()) {
  @transient
  lazy val service = {
    val S3Credentials(accessKey, secretKey) = credentials
    new AmazonS3Client(new BasicAWSCredentials(accessKey, secretKey))
  }
}

case class S3Credentials(accessKey: String, secretKey: String)

object S3Config {
  def environmentCredentials(): S3Credentials = {
    val credentials = new EnvironmentVariableCredentialsProvider().getCredentials
    val accessKey = credentials.getAWSAccessKeyId
    val secretKey = credentials.getAWSSecretKey
    new S3Credentials(accessKey, secretKey)
  }
}

/** Artifact implementations using S3 storage. */
class S3FlatArtifact(
  val path: String,
  val config: S3Config,
  val contentTypeOverride: Option[String] = None
)(implicit cachingImpl: S3Cache = DefaultS3Cache)
    extends FlatArtifact with S3Artifact {

  override def read: InputStream = {
    require(exists, s"Attempt to read from non-existent S3 location: $path")
    cachingImpl.readFlat(this)
  }

  override def write[T](writer: ArtifactStreamWriter => T): T =
    cachingImpl.writeFlat(this, writer)

  override def toString: String = s"S3Artifact[${config.bucket}, $path}]"
}

class CompressedS3FlatArtifact(
  path: String,
  config: S3Config,
  contentTypeOverride: Option[String] = None
)(implicit cachingImpl: S3Cache = DefaultS3Cache)
    extends S3FlatArtifact(path, config, contentTypeOverride) with CompressedS3Artifact {

  override def toString: String = s"CompressedS3Artifact[${config.bucket}, $path}]"
}

/** Zip file stored in S3.  */
class S3ZipArtifact(
  val path: String,
  val config: S3Config,
  val contentTypeOverride: Option[String] = None
)(implicit cachingImpl: S3Cache = DefaultS3Cache)
    extends StructuredArtifact with S3Artifact {

  import org.allenai.pipeline.StructuredArtifact._

  protected def makeLocalArtifact(f: File) = new ZipFileArtifact(f)

  override def reader: Reader = {
    require(exists, s"Attempt to read from non-existent S3 location: $path")
    cachingImpl.readZip(this)
  }

  override def write[T](writer: Writer => T): T = {
    cachingImpl.writeZip(this, writer)
  }

  override def toString: String = s"S3ZipArtifact[${config.bucket}, $path}]"
}

trait S3Artifact extends Artifact with Logging {
  def config: S3Config

  protected def contentTypeOverride: Option[String]

  def path: String

  override def url: URI = new URI("s3", bucket, s"/$path", null)

  val service = config.service
  val bucket = config.bucket

  override def exists: Boolean = {
    val result = try {
      service.getObjectMetadata(config.bucket, path)
      true
    } catch {
      case e: AmazonServiceException if e.getStatusCode == 404 => false
      case ex: Exception => throw ex
    }
    result
  }

  def contentType: String = contentTypeOverride.getOrElse(defaultContentType)

  def defaultContentType: String = path match {
    case s if s.endsWith(".html") => "text/html"
    case s if s.endsWith(".txt") => "text/plain"
    case s if s.endsWith(".json") => "application/json"
    case _ => "application/octet-stream"
  }

  def upload(file: File): Unit = {
    logger.debug(s"Uploading $file to $bucket/$path")
    val metadata = new ObjectMetadata()
    metadata.setContentType(contentType)
    val request = new PutObjectRequest(bucket, path, file).withMetadata(metadata)
    request.setCannedAcl(CannedAccessControlList.PublicRead)
    service.putObject(request)
  }

  protected def bufferedInputStreamFromS3(bucket: String, path: String) = {
    // We can't stream this directly out of S3, because it's not reliable enough. If the caller
    // reads half the input stream, then waits for 5 minutes, and then attempts to read again, S3
    // will time out the connection in the meantime.
    val file = File.createTempFile(s"$bucket/$path".replace('/', '$'), ".tmp")
    file.deleteOnExit()
    Resource.using(service.getObject(bucket, path).getObjectContent) { objectContent =>
      Files.copy(objectContent, file.toPath, StandardCopyOption.REPLACE_EXISTING)
    }
    val inputStream = new BufferedInputStream(new FileInputStream(file))
    file.delete()
    inputStream
  }

  def readContents(): InputStream = bufferedInputStreamFromS3(bucket, path)

  def download(file: File): Unit = {
    logger.debug(s"Downloading $bucket/$path to $file")
    val tmpFile = new File(file.getCanonicalFile.getParent, file.getName + ".tmp")
    tmpFile.deleteOnExit()
    try {
      Resource.using(readContents()) { is =>
        Files.copy(is, tmpFile.toPath)
      }
      require(tmpFile.renameTo(file), s"Unable to create $file")
    } finally {
      tmpFile.delete() // just in case
    }
  }
}

trait CompressedS3Artifact extends S3Artifact {
  import CompressedS3Artifact._

  private def activeDecompressor = {
    decompressors.iterator.find {
      case Decompressor(suffix, _) =>
        try {
          service.getObjectMetadata(config.bucket, path + suffix)
          true
        } catch {
          case e: AmazonServiceException if e.getStatusCode == 404 => false
          case ex: Exception => throw ex
        }
    }
  }

  override def exists: Boolean = activeDecompressor.isDefined

  def compressionSuffix = activeDecompressor.map(_.suffix)

  override def upload(file: File): Unit = {
    val compressedFile = File.createTempFile(file.getName, ".tmp.gz")
    compressedFile.deleteOnExit()
    try {
      Resource.using(
        new GzipCompressorOutputStream(
          new BufferedOutputStream(
            new FileOutputStream(compressedFile)
          )
        )
      ) { os =>
          Files.copy(file.toPath, os)
        }
      val gzmetadata = new ObjectMetadata() // This object is mutated when we call putObject(), so
      // we have to have our own copy.
      gzmetadata.setContentType("application/gzip")
      val request = new PutObjectRequest(bucket, path + ".gz", compressedFile).withMetadata(gzmetadata)
      request.setCannedAcl(CannedAccessControlList.PublicRead)
      service.putObject(request)
    } finally {
      compressedFile.delete()
    }
  }

  override def readContents(): InputStream = {
    val decompressedTries = decompressors.iterator.map {
      case Decompressor(suffix, decompress) =>
        Try {
          // This relies on the fact that bufferedInputStreamFromS3 will throw an exception when
          // path + suffix can't be found in the bucket.
          val compressed = bufferedInputStreamFromS3(bucket, path + suffix)
          decompress(compressed)
        }
    }

    /** Returns the first failure or the first success out of an iterator of Try, taking care not to
      * exhaust the iterator when possible.
      *
      * @param tries          The iterator of Try. Must not be empty.
      * @param defaultFailure The return value to use if all tries left in the iterators are
      *                       failures. If this is None, use the first failure from the iterator.
      * @return the first success out of the iterator, or the first failure if there is no success
      */
    @tailrec
    def firstSuccessOrFirstFailure[A](
      tries: Iterator[Try[A]],
      defaultFailure: Option[Failure[A]] = None
    ): Try[A] = {
      tries.next() match {
        case s: Success[A] => s
        case f: Failure[A] =>
          val newDefaultFailure = defaultFailure.getOrElse(f)
          if (tries.hasNext) {
            firstSuccessOrFirstFailure(tries, Some(newDefaultFailure))
          } else {
            newDefaultFailure
          }
      }
    }

    firstSuccessOrFirstFailure(decompressedTries).get
  }
}

object CompressedS3Artifact {
  case class Decompressor(suffix: String, decompress: (InputStream => InputStream))
  val decompressors = Seq(
    Decompressor(".gz", new GzipCompressorInputStream(_)),
    Decompressor(".bz2", new BZip2CompressorInputStream(_)),
    Decompressor("", identity)
  )
}

/** Implements policy for making local caches of artifacts in S3
  */
trait S3Cache {
  def readFlat(artifact: S3Artifact): InputStream

  def readZip(artifact: S3Artifact): Reader

  def writeFlat[T](artifact: S3Artifact, writer: ArtifactStreamWriter => T): T

  def writeZip[T](artifact: S3Artifact, writer: Writer => T): T
}

object DefaultS3Cache extends LocalS3Cache()

/** Caches S3 artifacts on the local filesystem
  * @param persistentCacheDir If specified, cached objects will be stored in a directory
  *                           that is persistent across JVM processes. There are no limits
  *                           on the size of the data stored in the persistent directory
  */
case class LocalS3Cache(
    persistentCacheDir: Option[File] = Some(new File(System.getProperty("java.io.tmpdir"), "pipeline-cache"))
) extends S3Cache with Logging {

  override def readFlat(artifact: S3Artifact) = {
    if (usePersistentCache) {
      withDownloadedLocalCopy(artifact)(new FileArtifact(_).read)
    } else {
      artifact.readContents()
    }
  }

  def usePersistentCache = persistentCacheDir.isDefined

  override def writeFlat[T](artifact: S3Artifact, writer: ArtifactStreamWriter => T): T = {
    withLocalFile(artifact) { local =>
      val result = new FileArtifact(local).write(writer)
      artifact.upload(local)
      result
    }
  }

  override def readZip(artifact: S3Artifact): Reader = {
    withDownloadedLocalCopy(artifact) { file =>
      new ZipFileArtifact(file).reader
    }
  }

  override def writeZip[T](artifact: S3Artifact, writer: Writer => T) = {
    withLocalFile(artifact) { local =>
      val result = new ZipFileArtifact(local).write(writer)
      artifact.upload(local)
      result
    }
  }

  // Create persistent cache directory if necessary
  persistentCacheDir match {
    case Some(cacheDir) =>
      if (cacheDir.exists && !cacheDir.isDirectory) {
        cacheDir.delete()
      }
      if (!cacheDir.exists) {
        cacheDir.mkdirs()
      }
      require(
        cacheDir.exists && cacheDir.isDirectory,
        s"Unable to create cache directory ${cacheDir.getCanonicalPath}"
      )
    case None => ()
  }

  protected[this] def withLocalFile[T](artifact: S3Artifact)(function: File => T): T = {
    val fileName = artifact.path.replaceAll("""/""", """\$""")
    persistentCacheDir match {
      case Some(cacheDir) => function(new File(cacheDir, fileName))
      case None =>
        val f = File.createTempFile(fileName, ".tmp")
        f.deleteOnExit()
        val result = function(f)
        f.delete()
        result
    }
  }

  protected[this] def withDownloadedLocalCopy[T](artifact: S3Artifact)(function: File => T): T = {
    withLocalFile(artifact) { file =>
      if (!(usePersistentCache && file.exists)) {
        artifact.download(file)
      }
      function(file)
    }
  }
}
