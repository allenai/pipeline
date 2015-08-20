package org.allenai.pipeline.s3

import java.net.URI

import com.typesafe.config.Config
import org.allenai.pipeline.{ ConfiguredPipeline, Pipeline, UrlToArtifact }

/** Created by rodneykinney on 5/24/15.
  */

trait S3Pipeline extends Pipeline {
  def credentials: S3Credentials = S3Config.environmentCredentials()
  implicit val s3Cache: S3Cache = DefaultS3Cache
  override def urlToArtifact = UrlToArtifact.chain(super.urlToArtifact, CreateCoreArtifacts.fromS3Urls(credentials))
}

object S3Pipeline {
  def apply(
    rootUrl: URI,
    awsCredentials: S3Credentials = S3Config.environmentCredentials()
  ) = new S3Pipeline {
    def rootOutputUrl = rootUrl
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
