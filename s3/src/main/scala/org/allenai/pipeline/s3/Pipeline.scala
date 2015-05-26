package org.allenai.pipeline.s3

import org.allenai.pipeline.{ Pipeline => basePipeline, ConfiguredPipeline => baseConfiguredPipeline, ArtifactFactory }

import com.amazonaws.auth.BasicAWSCredentials
import com.typesafe.config.Config

import java.net.URI

/** Created by rodneykinney on 5/24/15.
  */
object Pipeline {
  // Create a Pipeline that writes output to the given root path in S3
  def apply(credentials: BasicAWSCredentials, bucket: String, rootPath: String) = {
    val artifactFactory = ArtifactFactory(CreateCoreArtifacts.fromFileOrS3Urls(credentials))
    val rootUrl = new URI("s3", bucket, rootPath, null)
    new basePipeline(rootUrl, artifactFactory)
  }
}

object ConfiguredPipeline {
  def apply(config: Config, credentials: BasicAWSCredentials) = {
    new baseConfiguredPipeline(config, ArtifactFactory(CreateCoreArtifacts.fromFileOrS3Urls(credentials)))
  }
}
