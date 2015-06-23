package org.allenai.pipeline.s3

import org.allenai.pipeline._

import com.typesafe.config.Config

import scala.reflect.ClassTag

import java.net.URI

/** A trait that gives a pipeline the capability to read/write artifacts in S3 by specifying a s3:// URL */
trait S3Pipeline extends Pipeline {
  def credentials: S3Credentials = S3Config.environmentCredentials()

  override def urlToArtifact = UrlToArtifact.chain(super.urlToArtifact, CreateCoreArtifacts.fromS3Urls(credentials))
}

object S3Pipeline {
  def apply(
    rootUrl: URI,
    awsCredentials: S3Credentials = S3Config.environmentCredentials()
  ) = new S3Pipeline {
    def rootOutputDataUrl = ArtifactFactory.appendPath(rootUrl, "data")
    def rootOutputReportUrl = ArtifactFactory.appendPath(rootUrl, "reports")

    override def credentials = awsCredentials
  }

  def configured(
    cfg: Config,
    awsCredentials: S3Credentials = S3Config.environmentCredentials()
  ) = {
    new ConfiguredPipeline with S3Pipeline {
      override val config = cfg

      override def credentials = awsCredentials
    }
  }
}
