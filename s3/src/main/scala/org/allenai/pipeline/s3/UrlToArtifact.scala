package org.allenai.pipeline.s3

import java.io.File
import java.net.URI

import com.amazonaws.auth.BasicAWSCredentials
import org.allenai.pipeline.{ ArtifactFactory, UrlToArtifact => baseUrlToArtifact, Artifact, Pipeline => basePipeline }

import scala.reflect.ClassTag

/** Created by rodneykinney on 5/22/15.
  */
object UrlToArtifact {
  import org.allenai.pipeline.UrlToArtifact._
  // Create a FlatArtifact or StructuredArtifact from an absolute s3:// URL
  def absoluteS3(credentials: => BasicAWSCredentials) = new baseUrlToArtifact {
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

  // Create a FlatArtifact or StructuredArtifact from a path relative to the input s3:// URL
  def relativeS3(
    cfg: => S3Config,
    rootPath: String
  ) = new baseUrlToArtifact {
    val cleanRoot = rootPath.dropWhile(_ == '/').reverse.dropWhile(_ == '/').reverse
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[S3FlatArtifact])
          && url.getScheme == null =>
          val absPath = s"$cleanRoot/${url.getPath}"
          new S3FlatArtifact(absPath, cfg).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[S3ZipArtifact])
          && url.getScheme == null =>
          val absPath = s"$cleanRoot/${url.getPath}"
          new S3ZipArtifact(absPath, cfg).asInstanceOf[A]
      }
      fn
    }
  }

  // Create a FlatArtifact or StructuredArtfact from an input file:// or s3:// URL
  def absoluteUrl(credentials: => BasicAWSCredentials = S3Config.environmentCredentials): baseUrlToArtifact =
    chain(absoluteFile, absoluteS3(credentials))

  // Create a FlatArtifact or StructuredArtifact from a path relative to the input file:// or s3:// URL
  def relativeToUrl[A <: Artifact: ClassTag](
    rootUrl: URI,
    credentials: => BasicAWSCredentials = S3Config.environmentCredentials
  ): baseUrlToArtifact = {
    rootUrl match {
      case url if url.getScheme == "s3" || url.getScheme == "s3n" =>
        val cfg = S3Config(url.getHost, credentials)
        val rootPath = url.getPath
        chain(relativeS3(cfg, rootPath), absoluteS3(credentials))
    }
  }

}

object Pipeline {

  // Create a Pipeline that writes output to the given root path in S3
  def saveToS3(cfg: S3Config, rootPath: String) = {
    val artifactFactory = ArtifactFactory(UrlToArtifact.relativeS3(cfg, rootPath), UrlToArtifact.absoluteS3(cfg.credentials))
    new basePipeline(artifactFactory)
  }

}
