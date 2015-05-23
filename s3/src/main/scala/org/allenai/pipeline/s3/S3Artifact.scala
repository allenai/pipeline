package org.allenai.pipeline.s3

import java.io.{ File, FileOutputStream, InputStream }
import java.net.URI

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.{ BasicAWSCredentials, EnvironmentVariableCredentialsProvider }
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ CannedAccessControlList, ObjectMetadata, PutObjectRequest }
import org.allenai.common.Logging
import org.allenai.pipeline._

case class S3Config(bucket: String, credentials: BasicAWSCredentials = S3Config.environmentCredentials()) {
  @transient
  lazy val service = new AmazonS3Client(credentials)
}

object S3Config {
  def environmentCredentials(): BasicAWSCredentials = {
    val credentials = new EnvironmentVariableCredentialsProvider().getCredentials
    val accessKey = credentials.getAWSAccessKeyId
    val secretKey = credentials.getAWSSecretKey
    new BasicAWSCredentials(accessKey, secretKey)
  }
}

/** Artifact implementations using S3 storage. */
class S3FlatArtifact(
  val path: String,
  val config: S3Config,
  val contentTypeOverride: Option[String] = None
)
    extends FlatArtifact with S3Artifact[FileArtifact] {
  protected def makeLocalArtifact(f: File) = new FileArtifact(f)

  override def read: InputStream = {
    require(exists, s"Attempt to read from non-existent S3 location: $path")
    getCachedArtifact.read
  }

  override def write[T](writer: ArtifactStreamWriter => T): T = {
    val result = getCachedArtifact.write(writer)
    upload(cachedFile.get.file)
    result
  }

  override def toString: String = s"S3Artifact[${config.bucket}, $path}]"
}

/** Zip file stored in S3.  */
class S3ZipArtifact(
  val path: String,
  val config: S3Config,
  val contentTypeOverride: Option[String] = None
)
    extends StructuredArtifact with S3Artifact[ZipFileArtifact] {

  import org.allenai.pipeline.StructuredArtifact._

  protected def makeLocalArtifact(f: File) = new ZipFileArtifact(f)

  override def reader: Reader = {
    require(exists, s"Attempt to read from non-existent S3 location: $path")
    getCachedArtifact.reader
  }

  override def write[T](writer: Writer => T): T = {
    val result = getCachedArtifact.write(writer)
    upload(cachedFile.get.file)
    result
  }

  override def toString: String = s"S3ZipArtifact[${config.bucket}, $path}]"
}

trait S3Artifact[A <: Artifact] extends Logging {
  this: Artifact =>
  def config: S3Config

  protected def makeLocalArtifact(f: File): A

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

  protected def upload(file: File): Unit = {
    logger.debug(s"Uploading $file to $bucket/$path")
    val metadata = new ObjectMetadata()
    metadata.setContentType(contentType)
    val request = new PutObjectRequest(bucket, path, file).withMetadata(metadata)
    request.setCannedAcl(CannedAccessControlList.PublicRead)
    service.putObject(request)
  }

  protected var cachedFile: Option[A] = None

  private val BUFFER_SIZE = 1048576
  // 1 MB buffer
  protected def getCachedArtifact: A = cachedFile match {
    case Some(f) => f
    case None =>
      val cacheDir = {
        val f = new File(System.getProperty("java.io.tmpdir"), "pipeline-cache")
        if (f.exists && !f.isDirectory) {
          f.delete()
        }
        if (!f.exists) {
          f.mkdirs()
        }
        f
      }
      require(
        cacheDir.exists && cacheDir.isDirectory,
        s"Unable to create cache directory ${cacheDir.getCanonicalPath}"
      )
      val downloadFile = new File(cacheDir, path.replaceAll("""/""", """\$"""))
      if (exists && !downloadFile.exists) {
        logger.debug(s"Downloading $bucket/$path to $downloadFile")
        val tmpFile = File.createTempFile(downloadFile.getName, "tmp", downloadFile.getParentFile)
        tmpFile.deleteOnExit()
        val os = new FileOutputStream(tmpFile)
        val is = service.getObject(bucket, path).getObjectContent
        val buffer = new Array[Byte](BUFFER_SIZE)
        Iterator.continually(is.read(buffer)).takeWhile(_ != -1).foreach(n =>
          os.write(buffer, 0, n))
        is.close()
        os.close()
        require(
          tmpFile.renameTo(downloadFile),
          s"Unable to create download file ${downloadFile.getCanonicalPath}"
        )
      }
      cachedFile = Some(makeLocalArtifact(downloadFile))
      cachedFile.get
  }
}