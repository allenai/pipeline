package org.allenai.pipeline.spark

import org.allenai.pipeline._
import org.allenai.pipeline.s3.{ S3Credentials, S3Config }

import scala.reflect.ClassTag

import java.io.File
import java.net.URI

object CreateRddArtifacts {
  val fromFileUrls: UrlToArtifact = new UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[PartitionedRddFileArtifact])
          && "file" == url.getScheme =>
          new PartitionedRddFileArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[PartitionedRddFileArtifact])
          && null == url.getScheme =>
          new PartitionedRddFileArtifact(new File(url.getPath)).asInstanceOf[A]
      }
      fn
    }
  }

  def fromS3Urls(credentials: => S3Credentials): UrlToArtifact = new UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[PartitionedRddS3Artifact])
          && List("s3", "s3n").contains(url.getScheme) =>
          val bucket = url.getHost
          val path = url.getPath.dropWhile(_ == '/')
          new PartitionedRddS3Artifact(S3Config(bucket, credentials), path).asInstanceOf[A]
      }
      fn
    }
  }

  def fromFileOrS3Urls(credentials: => S3Credentials) =
    UrlToArtifact.chain(fromFileUrls, fromS3Urls(credentials))
}
