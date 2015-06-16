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

/** A trait that gives a pipeline the capability to back up local filesystem artifacts to S3 */
trait S3PipelineWithReplication extends S3Pipeline {
  override def urlToArtifact = UrlToArtifact.chain(super.urlToArtifact, createReplicatedArtifacts(credentials))

  private val baseUrlToArtifact = super.urlToArtifact

  def createReplicatedArtifacts(credentials: => S3Credentials) = new UrlToArtifact {
    def urlToArtifact[A <: Artifact : ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[ReplicatedFlatArtifact])
          && url.getScheme == "file" || url.getScheme == null =>
          val primary = ArtifactFactory(baseUrlToArtifact).createArtifact[FlatArtifact](url)
          def createBackupUrl(id: String) =
            new URI(rootOutputUrl.getScheme,
              rootOutputUrl.getHost,
              rootOutputUrl.getPath + url.getPath.split('/').last + id)
          new ReplicatedFlatArtifact(primary, createBackupUrl, artifactFactory).asInstanceOf[A]
      }
      fn
    }
  }
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
