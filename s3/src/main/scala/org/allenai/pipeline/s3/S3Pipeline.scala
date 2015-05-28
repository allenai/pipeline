package org.allenai.pipeline.s3

import org.allenai.pipeline.{ ConfiguredPipeline, UrlToArtifact, Pipeline }

import com.amazonaws.auth.BasicAWSCredentials
import com.typesafe.config.Config

import java.net.URI

/** Created by rodneykinney on 5/24/15.
  */

trait S3Pipeline extends Pipeline {
  def credentials: BasicAWSCredentials = S3Config.environmentCredentials()
  override def urlToArtifact = UrlToArtifact.chain(super.urlToArtifact, CreateCoreArtifacts.fromS3Urls(credentials))
}

object S3Pipeline {
  def apply(
    rootUrl: URI,
    awsCredentials: BasicAWSCredentials = S3Config.environmentCredentials()
  ) = new S3Pipeline {
    def rootOutputUrl = rootUrl
    override def credentials = awsCredentials
  }
  def configured(
    cfg: Config,
    awsCredentials: BasicAWSCredentials = S3Config.environmentCredentials()
  ) = {
    new ConfiguredPipeline with S3Pipeline {
      override val config = cfg
      override def credentials = awsCredentials
    }
  }
}
