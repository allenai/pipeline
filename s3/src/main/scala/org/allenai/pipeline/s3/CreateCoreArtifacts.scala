package org.allenai.pipeline.s3

import org.allenai.pipeline.{ Artifact, ArtifactFactory, CreateCoreArtifacts => CreateCoreFileArtifacts, Pipeline => basePipeline, UrlToArtifact }

import com.amazonaws.auth.BasicAWSCredentials

import scala.reflect.ClassTag

import java.net.URI

/** Created by rodneykinney on 5/22/15.
  */
object CreateCoreArtifacts {
  // Create a FlatArtifact or StructuredArtifact from an absolute s3:// URL
  def fromS3Urls(credentials: => BasicAWSCredentials) = new UrlToArtifact {
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
  def fromFileOrS3Urls(credentials: => BasicAWSCredentials = S3Config.environmentCredentials): UrlToArtifact =
    UrlToArtifact.chain(CreateCoreFileArtifacts.fromFileUrls, fromS3Urls(credentials))
}

