package org.allenai.pipeline.spark

import org.allenai.pipeline.s3.{ S3Config, S3FlatArtifact }

import com.amazonaws.services.s3.model.ObjectListing

import scala.collection.JavaConverters._

import java.net.URI

class PartitionedRddS3Artifact(val s3Config: S3Config, val rootPath: String) extends PartitionedRddArtifact[S3FlatArtifact] {
  private val cleanRoot = rootPath.reverse.dropWhile(_ == '/').reverse

  private val successArtifact = new S3FlatArtifact(s"$cleanRoot/_SUCCESS", s3Config)

  def makePartitionArtifact: Int => S3FlatArtifact = {
    val root = cleanRoot
    val cfg = s3Config
    i: Int => new S3FlatArtifact(s"$root/part-%05d".format(i), cfg)
  }

  def markWriteSuccess(): Unit = {
    successArtifact.write(w => w.write(""))
  }

  /** Return true if this data has been written to the persistent store. */
  override def exists: Boolean = successArtifact.exists

  override def url: URI = new URI("s3", s3Config.bucket, s"/$rootPath", null)

  override def getExistingPartitionArtifacts: Iterable[S3FlatArtifact] = {
    val client = s3Config.service
    def extractKeys(resp: ObjectListing) =
      resp.getObjectSummaries.asScala
        .map(_.getKey)
        .filter(path => path.substring(rootPath.size).startsWith("part-"))

    var resp = client.listObjects(s3Config.bucket, rootPath)
    var keys = extractKeys(resp)
    while (resp.isTruncated) {
      resp = client.listNextBatchOfObjects(resp)
      val newKeys = extractKeys(resp)
      keys ++= newKeys
    }
    keys.map(k => new S3FlatArtifact(k, s3Config))
  }
}