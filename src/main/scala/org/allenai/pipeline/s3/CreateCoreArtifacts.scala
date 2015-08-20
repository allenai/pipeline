package org.allenai.pipeline.s3

import java.net.URI

import org.allenai.pipeline.{ Artifact, CreateCoreArtifacts => CreateCoreFileArtifacts, Pipeline => basePipeline, UrlToArtifact }

import scala.reflect.ClassTag

/** Created by rodneykinney on 5/22/15.
  */
object CreateCoreArtifacts {
  // Create a FlatArtifact or StructuredArtifact from an absolute s3:// URL
  def fromS3Urls(credentials: => S3Credentials)(
    implicit
    s3Cache: S3Cache
  ) = new UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
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
  def fromFileOrS3Urls(credentials: => S3Credentials = S3Config.environmentCredentials)(
    implicit
    s3Cache: S3Cache
  ): UrlToArtifact =
    UrlToArtifact.chain(CreateCoreFileArtifacts.fromFileUrls, fromS3Urls(credentials))
}

