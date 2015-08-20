package org.allenai.pipeline.s3

import java.io.{ File, FileOutputStream, InputStream }
import java.net.URI

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{ BasicAWSCredentials, EnvironmentVariableCredentialsProvider }
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ CannedAccessControlList, ObjectMetadata, PutObjectRequest }
import org.allenai.common.Logging
import org.allenai.pipeline.StructuredArtifact.{ Reader, Writer }
import org.allenai.pipeline._

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

  def readContents(): InputStream = service.getObject(bucket, path).getObjectContent

  def download(file: File, bufferSize: Int = 1048576): Unit = {
    logger.debug(s"Downloading $bucket/$path to $file")
    val tmpFile = new File(file.getCanonicalFile.getParent, file.getName + ".tmp")
    val os = new FileOutputStream(tmpFile)
    val is = readContents()
    val buffer = new Array[Byte](bufferSize)
    Iterator.continually(is.read(buffer)).takeWhile(_ != -1).foreach(n =>
      os.write(buffer, 0, n))
    is.close()
    os.close()
    require(tmpFile.renameTo(file), s"Unable to create $file")
  }
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
      new FileArtifact(downloadLocalCopy(artifact)).read
    } else {
      artifact.readContents()
    }
  }

  def usePersistentCache = persistentCacheDir.isDefined

  override def writeFlat[T](artifact: S3Artifact, writer: ArtifactStreamWriter => T): T = {
    val local = localFile(artifact)
    val result = new FileArtifact(local).write(writer)
    artifact.upload(local)
    result
  }

  override def readZip(artifact: S3Artifact): Reader = {
    new ZipFileArtifact(downloadLocalCopy(artifact)).reader
  }

  override def writeZip[T](artifact: S3Artifact, writer: Writer => T) = {
    val local = localFile(artifact)
    val result = new ZipFileArtifact(local).write(writer)
    artifact.upload(local)
    result
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

  protected[this] val bufferSize = 1048576 // 1 MB buffer

  protected[this] def localFile(artifact: S3Artifact): File = {
    val fileName = artifact.path.replaceAll("""/""", """\$""")
    persistentCacheDir match {
      case Some(cacheDir) => new File(cacheDir, fileName)
      case None =>
        val f = File.createTempFile(fileName, "tmp")
        f.deleteOnExit()
        f
    }
  }

  protected[this] def downloadLocalCopy(artifact: S3Artifact): File = {
    val file = localFile(artifact)
    if (!(usePersistentCache && file.exists)) {
      artifact.download(file, bufferSize)
    }
    file
  }
}