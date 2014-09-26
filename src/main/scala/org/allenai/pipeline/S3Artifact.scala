package org.allenai.pipeline

import org.allenai.common.Logging

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.{ CannedAccessControlList, PutObjectRequest, ObjectMetadata }

import java.io.{ InputStream, File, FileOutputStream }
import java.net.URI

case class S3Config(service: AmazonS3Client, bucket: String)

object S3Config {
  def apply(accessKey: String, secretAccessKey: String, bucket: String): S3Config =
    S3Config(new AmazonS3Client(new BasicAWSCredentials(accessKey, secretAccessKey)), bucket)

  def apply(bucket: String): S3Config = S3Config(new AmazonS3Client(), bucket)
}

/** Artifact implementations using S3 storage. */
class S3FlatArtifact(val path: String, val config: S3Config)
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
class S3ZipArtifact(val path: String, val config: S3Config)
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

  def path: String

  override def url: URI = new URI("s3", bucket, s"/$path", null)

  protected val S3Config(service, bucket) = config

  override def exists: Boolean = {
    val result = try {
      val resp = service.getObjectMetadata(bucket, path)
      true
    } catch {
      case e: AmazonServiceException if e.getStatusCode == 404 => false
      case ex: Exception => throw ex
    }
    result
  }

  protected def contentType = path match {
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

  private val BUFFER_SIZE = 1048576 // 1 MB buffer
  protected def getCachedArtifact: A = cachedFile match {
    case Some(f) => f
    case None =>
      val tmpFile = File.createTempFile(path.replaceAll("""/""", """\$"""), "tmp")
      if (exists) {
        logger.debug(s"Downloading $bucket/$path to $tmpFile")
        val os = new FileOutputStream(tmpFile)
        val is = service.getObject(bucket, path).getObjectContent
        val buffer = new Array[Byte](BUFFER_SIZE)
        Iterator.continually(is.read(buffer)).takeWhile(_ != -1).foreach(n =>
          os.write(buffer, 0, n))
        is.close()
        os.close()
      }
      tmpFile.deleteOnExit()
      cachedFile = Some(makeLocalArtifact(tmpFile))
      cachedFile.get
  }
}
