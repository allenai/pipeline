package org.allenai.pipeline.s3

import org.allenai.pipeline.{Pipeline => basePipeline, UrlToArtifact, CoreArtifacts, Artifact, ArtifactFactory}

import com.amazonaws.auth.BasicAWSCredentials

import scala.reflect.ClassTag

import java.net.URI

/** Created by rodneykinney on 5/22/15.
  */
object S3Artifacts {
  // Create a FlatArtifact or StructuredArtifact from an absolute s3:// URL
  def handleS3Urls(credentials: => BasicAWSCredentials) = new UrlToArtifact {
    def urlToArtifact[A <: Artifact : ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[S3FlatArtifact])
          && List("s3", "s3n").contains(url.getScheme) =>
          val bucket = url.getHost
          val path = url.getPath.dropWhile(_ == '/')
          new S3FlatArtifact(path, S3Config(bucket, credentials)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[S3ZipArtifact])
          && List("s3", "s3n").contains(url.getScheme) =>
          val bucket = url.getHost
          val path = url.getPath.dropWhile(_ == '/')
          new S3ZipArtifact(path, S3Config(bucket, credentials)).asInstanceOf[A]
      }
      fn
    }
  }

  // Create a FlatArtifact or StructuredArtifact from an input file:// or s3:// URL
  def absoluteUrl(credentials: => BasicAWSCredentials = S3Config.environmentCredentials): UrlToArtifact =
    UrlToArtifact.chain(CoreArtifacts.handleFileUrls, handleS3Urls(credentials))
}

object Pipeline {
  // Create a Pipeline that writes output to the given root path in S3
  def saveToS3(credentials: BasicAWSCredentials, bucket: String, rootPath: String) = {
    val artifactFactory = ArtifactFactory(S3Artifacts.handleS3Urls(credentials))
    val rootUrl = new URI("s3", null, bucket, -1, rootPath, null, null)
    new basePipeline(rootUrl, artifactFactory)
  }

}
