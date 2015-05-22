package org.allenai.pipeline

import java.io.File
import java.net.URI

import com.amazonaws.auth.BasicAWSCredentials

import scala.reflect.ClassTag

trait ArtifactFactory {
  /** Create an Artifact at the given path.
    * The type tag determines the type of the Artifact, which may be an abstract type
    */
  def createArtifact[A <: Artifact: ClassTag](path: String): A
}

object ArtifactFactory {
  def apply(resolver: UrlToArtifact, fallbacks: UrlToArtifact*): ArtifactFactory =
    new ArtifactFactory {
      def createArtifact[A <: Artifact: ClassTag](path: String): A = {
        var fn = resolver.urlToArtifact[A]
        for (f <- fallbacks) {
          fn = fn orElse f.urlToArtifact[A]
        }
        val url = new URI(path)
        val clazz = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
        require(fn.isDefinedAt(url), s"Cannot create $clazz from $path")
        fn(url)
      }
    }
}

trait UrlToArtifact {
  def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A]
}

object UrlToArtifact {
  def chain(first: UrlToArtifact, second: UrlToArtifact, others: UrlToArtifact*) =
    new UrlToArtifact {
      override def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
        var fn = first.urlToArtifact[A] orElse second.urlToArtifact[A]
        for (o <- others) {
          fn = fn orElse o.urlToArtifact[A]
        }
        fn
      }
    }
  object Empty extends UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] =
      PartialFunction.empty[URI, A]
  }
  object FromAbsoluteFileUrl extends UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[FileArtifact])
          && "file" == url.getScheme =>
          new FileArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[FileArtifact])
          && null == url.getScheme =>
          new FileArtifact(new File(url.getPath)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && "file" == url.getScheme =>
          new ZipFileArtifact(new File(url)).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && null == url.getScheme =>
          new ZipFileArtifact(new File(url.getPath)).asInstanceOf[A]
      }
      fn
    }
  }
  def FromRelativeFilePath(rootDir: File) = new UrlToArtifact {
    def urlToArtifact[A <: Artifact: ClassTag]: PartialFunction[URI, A] = {
      val c = implicitly[ClassTag[A]].runtimeClass.asInstanceOf[Class[A]]
      val fn: PartialFunction[URI, A] = {
        case url if c.isAssignableFrom(classOf[FileArtifact])
          && null == url.getScheme =>
          val file = new File(rootDir, url.getPath)
          new FileArtifact(file).asInstanceOf[A]
        case url if c.isAssignableFrom(classOf[ZipFileArtifact])
          && null == url.getScheme =>
          val file = new File(rootDir, url.getPath)
          new ZipFileArtifact(file).asInstanceOf[A]
      }
      fn
    }
  }
  def FromAbsoluteS3Url(
    credentials: => BasicAWSCredentials
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
  def FromRelativeS3Path(
    cfg: => S3Config,
    rootPath: String
  ) = new UrlToArtifact {
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
  def fromUrl(credentials: => BasicAWSCredentials = S3Config.environmentCredentials): UrlToArtifact =
    UrlToArtifact.chain(FromAbsoluteFileUrl, new FromAbsoluteS3Url(credentials))

  def relativeToUrl[A <: Artifact: ClassTag](
    rootUrl: URI,
    credentials: => BasicAWSCredentials = S3Config.environmentCredentials
  ): UrlToArtifact = {
    rootUrl match {
      case url if url.getScheme == "file" || url.getScheme == null =>
        val rootDir = new File(url.getPath)
        UrlToArtifact.chain(FromRelativeFilePath(rootDir), FromAbsoluteFileUrl)
      case url if url.getScheme == "s3" || url.getScheme == "s3n" =>
        val cfg = S3Config(url.getHost, credentials)
        val rootPath = url.getPath
        UrlToArtifact.chain(FromRelativeS3Path(cfg, rootPath), FromAbsoluteS3Url(credentials))
    }
  }
}

