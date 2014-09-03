package org.allenai.pipeline

import java.io.{ File, FileOutputStream }

import com.amazonaws.AmazonServiceException
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3Client
import org.allenai.common.Logging

case class S3Config(service: AmazonS3Client, bucket: String)

object S3Config {
  def apply(accessKey: String, secretAccessKey: String, bucket: String): S3Config =
    S3Config(new AmazonS3Client(new BasicAWSCredentials(accessKey, secretAccessKey)), bucket)
  def apply(bucket: String): S3Config = S3Config(new AmazonS3Client(), bucket)
}

/** Artifact implementations using S3 storage */
class S3FlatArtifact(val path: String, val config: S3Config) extends FlatArtifact with S3Artifact[FileArtifact] {
  def makeLocalArtifact(f: File) = new FileArtifact(f)

  def read = {
    require(exists, s"Attempt to read from non-existent S3 location: $path")
    getCachedArtifact.read
  }

  def write[T](writer: ArtifactStreamWriter => T): T = {
    val result = getCachedArtifact.write(writer)
    logger.debug(s"Uploading ${cachedFile.get.file} to $bucket/$path")
    service.putObject(bucket, path, cachedFile.get.file)
    result
  }

  override def toString = s"S3Artifact[${config.bucket}, $path}]"
}

/** Zip file stored in S3
  * @param path
  * @param config
  */
class S3ZipArtifact(val path: String, val config: S3Config) extends StructuredArtifact with S3Artifact[ZipFileArtifact] {
  def makeLocalArtifact(f: File) = new ZipFileArtifact(f)

  def reader: StructuredArtifactReader = {
    require(exists, s"Attempt to read from non-existent S3 location: $path")
    getCachedArtifact.reader
  }

  def write[T](writer: StructuredArtifactWriter => T): T = {
    val result = getCachedArtifact.write(writer)
    logger.debug(s"Uploading ${cachedFile.get.file} to $bucket/$path")
    service.putObject(bucket, path, cachedFile.get.file)
    result
  }

  override def toString = s"S3ZipArtifact[${config.bucket}, $path}]"
}

trait S3Artifact[A <: Artifact] extends Logging {
  this: Artifact =>
  def config: S3Config

  protected def makeLocalArtifact(f: File): A

  def path: String

  protected val S3Config(service, bucket) = config

  def exists = {
    val result = try {
      val resp = service.getObjectMetadata(bucket, path)
      true
    } catch {
      case e: AmazonServiceException if e.getStatusCode == 404 => false
      case ex: Exception => throw ex
    }
    result
  }

  protected var cachedFile: Option[A] = None

  protected def getCachedArtifact: A = cachedFile match {
    case Some(f) => f
    case None =>
      val tmpFile = File.createTempFile(path.replaceAll("""/""", """\$"""), "tmp")
      if (exists) {
        logger.debug(s"Downloading $bucket/$path to $tmpFile")
        val os = new FileOutputStream(tmpFile)
        val is = service.getObject(bucket, path).getObjectContent
        val buffer = new Array[Byte](math.pow(2, 20).toInt) // 1 MB buffer
        Iterator.continually(is.read(buffer)).takeWhile(_ != -1).foreach(n => os.write(buffer, 0, n))
        is.close()
        os.close()
      }
      tmpFile.deleteOnExit
      cachedFile = Some(makeLocalArtifact(tmpFile))
      cachedFile.get
  }
}
