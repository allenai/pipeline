package org.allenai.pipeline.spark

import org.allenai.pipeline.s3.{ S3Config, S3FlatArtifact }

import com.amazonaws.services.s3.model.ObjectListing

import scala.collection.JavaConverters._

import java.net.URI

class PartitionedRddS3Artifact(
    val s3Config: S3Config,
    val rootPath: String,
    val prefix: String = "part-",
    val maxPartitions: Int = 100000
) extends PartitionedRddArtifact {
  private val cleanRoot = rootPath.reverse.dropWhile(_ == '/').reverse

  private val successArtifact = new S3FlatArtifact(s"$cleanRoot/_SUCCESS", s3Config)
  private val digits = (math.log(maxPartitions - 1) / math.log(10)).toInt + 1

  def makePartitionArtifact: Int => S3FlatArtifact = {
    val root = cleanRoot
    val cfg = s3Config
    val prefix = this.prefix
    val digits = this.digits
    i: Int => new S3FlatArtifact(s"$root/$prefix%0${digits}d".format(i), cfg)
  }

  def saveWasSuccessful(): Unit = {
    successArtifact.write(w => w.write(""))
  }

  /** Return true if this data has been written to the persistent store. */
  override def exists: Boolean = successArtifact.exists

  override def url: URI = new URI("s3", s3Config.bucket, s"/$rootPath", null)

  override def getExistingPartitions: Iterable[Int] = {
    val client = s3Config.service
    def extractKeys(resp: ObjectListing) =
      resp.getObjectSummaries.asScala
        .filter(_.getSize > 0)
        .map(_.getKey)

    val fullPrefix = s"$cleanRoot/$prefix"
    var resp = client.listObjects(s3Config.bucket, fullPrefix)
    var keys = extractKeys(resp)
    while (resp.isTruncated) {
      resp = client.listNextBatchOfObjects(resp)
      val newKeys = extractKeys(resp)
      keys ++= newKeys
    }
    keys.toList.map(k => k.substring(fullPrefix.length).toInt)
  }
}