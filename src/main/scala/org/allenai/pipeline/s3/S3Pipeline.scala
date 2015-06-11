package org.allenai.pipeline.s3

import org.allenai.pipeline.{ ConfiguredPipeline, UrlToArtifact, Pipeline }

import com.typesafe.config.Config

import java.net.URI

/** Created by rodneykinney on 5/24/15.
  */

trait S3Pipeline extends Pipeline {
  def credentials: S3Credentials = S3Config.environmentCredentials()
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
